//! Structured local logging and optional OTLP logs, traces, and metrics export.

use std::{
    collections::{HashMap, HashSet},
    env,
    error::Error as StdError,
    fmt::Debug,
    io::stdout,
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use anyhow::{Context, Result, anyhow, bail};
use opentelemetry::{
    Key, KeyValue, global,
    logs::{AnyValue, LogRecord as _, Logger as _, LoggerProvider as _, Severity},
    trace::TracerProvider as _,
};
use opentelemetry_otlp::{LogExporter, MetricExporter, Protocol, SpanExporter, WithExportConfig as _};
use opentelemetry_sdk::{
    Resource,
    logs::{SdkLogger, SdkLoggerProvider, log_processor_with_async_runtime::BatchLogProcessor},
    metrics::{SdkMeterProvider, periodic_reader_with_async_runtime::PeriodicReader},
    runtime::Tokio,
    trace::{RandomIdGenerator, Sampler, SdkTracerProvider, span_processor_with_async_runtime::BatchSpanProcessor},
};
use tracing::{
    Event, Level,
    field::{Field, Visit},
};
use tracing_subscriber::{
    EnvFilter, Layer, Registry, fmt,
    layer::{Context as LayerContext, Identity, SubscriberExt},
    reload::{Handle as ReloadHandle, Layer as ReloadLayer},
};
use url::Url;

use crate::config::TelemetryConfig;

type BoxedLayer = Box<dyn Layer<Registry> + Send + Sync + 'static>;

static OTLP_HANDLE: OnceLock<ReloadHandle<Vec<BoxedLayer>, Registry>> = OnceLock::new();
static HEAD_BLOCK: AtomicU64 = AtomicU64::new(0);
static INDEXED_BLOCK: AtomicU64 = AtomicU64::new(0);

/// Providers kept alive for the lifetime of the daemon and flushed on shutdown.
#[derive(Default)]
pub struct TelemetryHandles {
    tracer_provider: Option<SdkTracerProvider>,
    logger_provider: Option<SdkLoggerProvider>,
    meter_provider: Option<SdkMeterProvider>,
}

impl Drop for TelemetryHandles {
    fn drop(&mut self) {
        if let Some(provider) = self.tracer_provider.take() {
            let _ = provider.shutdown();
        }
        if let Some(provider) = self.logger_provider.take() {
            let _ = provider.shutdown();
        }
        if let Some(provider) = self.meter_provider.take() {
            let _ = provider.shutdown();
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum Signal {
    Traces,
    Logs,
    Metrics,
}

impl Signal {
    const fn path(self) -> &'static str {
        match self {
            Self::Traces => "/v1/traces",
            Self::Logs => "/v1/logs",
            Self::Metrics => "/v1/metrics",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Transport {
    Grpc,
    Http,
}

#[derive(Debug)]
struct OtlpConfig {
    endpoint: String,
    transport: Transport,
    signals: HashSet<Signal>,
    metric_export_interval: Duration,
}

impl OtlpConfig {
    fn from_config(config: &TelemetryConfig) -> Result<Option<Self>> {
        let Some(endpoint) = config
            .otlp_endpoint
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            return Ok(None);
        };
        let transport = infer_transport(endpoint)?;
        let endpoint = if transport == Transport::Grpc {
            endpoint.replacen("grpc://", "http://", 1)
        } else {
            endpoint.to_owned()
        };
        Ok(Some(Self {
            endpoint,
            transport,
            signals: parse_signals(&config.otlp_signals)?,
            metric_export_interval: config.metric_export_interval,
        }))
    }

    fn has(&self, signal: Signal) -> bool {
        self.signals.contains(&signal)
    }

    fn endpoint_for(&self, signal: Signal) -> Result<String> {
        if self.transport == Transport::Grpc {
            return Ok(self.endpoint.clone());
        }
        resolve_http_endpoint(&self.endpoint, signal)
    }
}

/// Installs the local `tracing` subscriber before configuration is loaded.
pub fn install_base_subscriber() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let format = fmt::layer()
        .with_level(true)
        .with_target(true)
        .with_thread_ids(true)
        .with_writer(stdout);
    let format = if env::var("BLOKLI_LIGHT_LOG_FORMAT").is_ok_and(|value| value.eq_ignore_ascii_case("json")) {
        format.json().boxed()
    } else {
        format.boxed()
    };
    let (reload_layer, handle) = ReloadLayer::new(identity_if_empty(Vec::new()));
    OTLP_HANDLE
        .set(handle)
        .map_err(|_| anyhow!("tracing subscriber was already installed"))?;
    tracing::subscriber::set_global_default(Registry::default().with(reload_layer).with(filter).with(format))
        .context("failed to install tracing subscriber")?;
    Ok(())
}

/// Configures optional OTLP exporters after the Tokio runtime is available.
pub fn configure(config: &TelemetryConfig) -> Result<TelemetryHandles> {
    let Some(config) = OtlpConfig::from_config(config)? else {
        return Ok(TelemetryHandles::default());
    };
    let resource = Resource::builder()
        .with_service_name(env!("CARGO_PKG_NAME"))
        .with_attributes([KeyValue::new("service.version", env!("CARGO_PKG_VERSION"))])
        .build();
    let mut handles = TelemetryHandles::default();
    let mut layers = Vec::new();

    if config.has(Signal::Traces) {
        let exporter = match config.transport {
            Transport::Grpc => SpanExporter::builder()
                .with_tonic()
                .with_protocol(Protocol::Grpc)
                .with_endpoint(config.endpoint_for(Signal::Traces)?)
                .build(),
            Transport::Http => SpanExporter::builder()
                .with_http()
                .with_protocol(Protocol::HttpBinary)
                .with_endpoint(config.endpoint_for(Signal::Traces)?)
                .build(),
        }
        .context("failed to build OTLP trace exporter")?;
        let provider = SdkTracerProvider::builder()
            .with_span_processor(BatchSpanProcessor::builder(exporter, Tokio).build())
            .with_sampler(Sampler::AlwaysOn)
            .with_id_generator(RandomIdGenerator::default())
            .with_resource(resource.clone())
            .build();
        let tracer = provider.tracer(env!("CARGO_PKG_NAME"));
        layers.push(tracing_opentelemetry::layer().with_tracer(tracer).boxed());
        handles.tracer_provider = Some(provider);
    }

    if config.has(Signal::Logs) {
        let exporter = match config.transport {
            Transport::Grpc => LogExporter::builder()
                .with_tonic()
                .with_protocol(Protocol::Grpc)
                .with_endpoint(config.endpoint_for(Signal::Logs)?)
                .build(),
            Transport::Http => LogExporter::builder()
                .with_http()
                .with_protocol(Protocol::HttpJson)
                .with_endpoint(config.endpoint_for(Signal::Logs)?)
                .build(),
        }
        .context("failed to build OTLP log exporter")?;
        let provider = SdkLoggerProvider::builder()
            .with_log_processor(BatchLogProcessor::builder(exporter, Tokio).build())
            .with_resource(resource.clone())
            .build();
        layers.push(Box::new(OtelLogsLayer::new(provider.logger(env!("CARGO_PKG_NAME")))));
        handles.logger_provider = Some(provider);
    }

    if config.has(Signal::Metrics) {
        let exporter = match config.transport {
            Transport::Grpc => MetricExporter::builder()
                .with_tonic()
                .with_protocol(Protocol::Grpc)
                .with_endpoint(config.endpoint_for(Signal::Metrics)?)
                .build(),
            Transport::Http => MetricExporter::builder()
                .with_http()
                .with_protocol(Protocol::HttpBinary)
                .with_endpoint(config.endpoint_for(Signal::Metrics)?)
                .build(),
        }
        .context("failed to build OTLP metric exporter")?;
        let reader = PeriodicReader::builder(exporter, Tokio)
            .with_interval(config.metric_export_interval)
            .build();
        let provider = SdkMeterProvider::builder()
            .with_reader(reader)
            .with_resource(resource)
            .build();
        global::set_meter_provider(provider.clone());
        let meter = global::meter(env!("CARGO_PKG_NAME"));
        meter
            .u64_observable_gauge("blokli_light_head_block")
            .with_description("Latest Ethereum RPC head observed")
            .with_callback(|observer| observer.observe(HEAD_BLOCK.load(Ordering::Relaxed), &[]))
            .build();
        meter
            .u64_observable_gauge("blokli_light_indexed_block")
            .with_description("Highest finalised built-in block committed")
            .with_callback(|observer| observer.observe(INDEXED_BLOCK.load(Ordering::Relaxed), &[]))
            .build();
        handles.meter_provider = Some(provider);
    }

    OTLP_HANDLE
        .get()
        .ok_or_else(|| anyhow!("base tracing subscriber is not initialized"))?
        .reload(identity_if_empty(layers))
        .context("failed to install OTLP tracing layers")?;
    tracing::info!(endpoint = %config.endpoint, signals = %format_signals(&config.signals), "OpenTelemetry export enabled");
    Ok(handles)
}

/// Updates the gauges exported by the optional OTLP metrics provider.
pub fn record_indexer_state(head_block: u64, indexed_block: u64) {
    HEAD_BLOCK.store(head_block, Ordering::Relaxed);
    INDEXED_BLOCK.store(indexed_block, Ordering::Relaxed);
}

fn identity_if_empty(layers: Vec<BoxedLayer>) -> Vec<BoxedLayer> {
    if layers.is_empty() {
        vec![Box::new(Identity::new())]
    } else {
        layers
    }
}

fn parse_signals(raw: &str) -> Result<HashSet<Signal>> {
    let signals = raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| match value {
            "traces" => Ok(Signal::Traces),
            "logs" => Ok(Signal::Logs),
            "metrics" => Ok(Signal::Metrics),
            _ => bail!("unsupported OTLP signal '{value}'; use metrics,traces,logs"),
        })
        .collect::<Result<HashSet<_>>>()?;
    if signals.is_empty() {
        bail!("telemetry.otlp_signals must contain at least one signal");
    }
    Ok(signals)
}

fn format_signals(signals: &HashSet<Signal>) -> String {
    [
        (Signal::Metrics, "metrics"),
        (Signal::Traces, "traces"),
        (Signal::Logs, "logs"),
    ]
    .into_iter()
    .filter_map(|(signal, name)| signals.contains(&signal).then_some(name))
    .collect::<Vec<_>>()
    .join(",")
}

fn infer_transport(endpoint: &str) -> Result<Transport> {
    match endpoint.split_once("://").map(|(scheme, _)| scheme) {
        Some("grpc") => Ok(Transport::Grpc),
        Some("http" | "https") => Ok(Transport::Http),
        Some(scheme) => bail!("unsupported telemetry endpoint scheme '{scheme}'"),
        None => bail!("telemetry.otlp_endpoint must include a URL scheme"),
    }
}

fn resolve_http_endpoint(endpoint: &str, signal: Signal) -> Result<String> {
    let mut url = Url::parse(endpoint).context("invalid telemetry.otlp_endpoint")?;
    let path = url.path();
    let prefix = ["/v1/traces", "/v1/logs", "/v1/metrics"]
        .into_iter()
        .find_map(|suffix| path.strip_suffix(suffix))
        .unwrap_or(path)
        .trim_end_matches('/');
    url.set_path(&format!("{prefix}{}", signal.path()));
    Ok(url.to_string())
}

#[derive(Clone)]
struct OtelLogsLayer {
    logger: SdkLogger,
}

impl OtelLogsLayer {
    fn new(logger: SdkLogger) -> Self {
        Self { logger }
    }
}

impl<S> Layer<S> for OtelLogsLayer
where
    S: tracing::Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _context: LayerContext<'_, S>) {
        let metadata = event.metadata();
        let mut visitor = EventVisitor::default();
        event.record(&mut visitor);
        let mut record = self.logger.create_log_record();
        let timestamp = SystemTime::now();
        let (severity, text) = match *metadata.level() {
            Level::ERROR => (Severity::Error, "ERROR"),
            Level::WARN => (Severity::Warn, "WARN"),
            Level::INFO => (Severity::Info, "INFO"),
            Level::DEBUG => (Severity::Debug, "DEBUG"),
            Level::TRACE => (Severity::Trace, "TRACE"),
        };
        record.set_timestamp(timestamp);
        record.set_observed_timestamp(timestamp);
        record.set_target(metadata.target().to_owned());
        record.set_severity_number(severity);
        record.set_severity_text(text);
        if let Some(message) = visitor.message {
            record.set_body(AnyValue::Map(Box::new(HashMap::from([(
                Key::new("message"),
                AnyValue::String(message.into()),
            )]))));
        }
        record.add_attributes(visitor.attributes);
        self.logger.emit(record);
    }
}

#[derive(Default)]
struct EventVisitor {
    message: Option<String>,
    attributes: Vec<(String, AnyValue)>,
}

impl EventVisitor {
    fn add<V>(&mut self, field: &Field, value: V)
    where
        V: Into<AnyValue> + ToString,
    {
        if field.name() == "message" {
            self.message = Some(value.to_string());
        } else {
            self.attributes.push((field.name().to_owned(), value.into()));
        }
    }
}

impl Visit for EventVisitor {
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.add(field, value);
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        if let Ok(value) = i64::try_from(value) {
            self.add(field, value);
        } else {
            self.add(field, value.to_string());
        }
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.add(field, value);
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.add(field, value.to_owned());
    }

    fn record_error(&mut self, field: &Field, value: &(dyn StdError + 'static)) {
        self.add(field, value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.add(field, format!("{value:?}"));
    }
}

#[cfg(test)]
mod tests {
    use super::{Signal, Transport, infer_transport, parse_signals, resolve_http_endpoint};

    #[test]
    fn parses_transport_and_signal_paths() {
        assert_eq!(
            infer_transport("grpc://localhost:4317").expect("valid endpoint"),
            Transport::Grpc
        );
        assert_eq!(
            infer_transport("https://collector.example").expect("valid endpoint"),
            Transport::Http
        );
        assert_eq!(
            resolve_http_endpoint("http://localhost:4318/v1/metrics", Signal::Logs).expect("valid endpoint"),
            "http://localhost:4318/v1/logs"
        );
    }

    #[test]
    fn rejects_unknown_or_empty_signals() {
        assert!(parse_signals("metrics,traces").is_ok());
        assert!(parse_signals("unknown").is_err());
        assert!(parse_signals("").is_err());
    }
}
