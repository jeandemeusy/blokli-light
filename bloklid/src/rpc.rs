//! Minimal JSON-RPC client for the reads required by the raw-log indexer.

use std::sync::atomic::{AtomicU64, Ordering};

use reqwest::Client;
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};
use thiserror::Error;
use url::Url;

use crate::{database::RawLog, source::format_address};

/// Filter sent to `eth_getLogs`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogFilter {
    /// Contract addresses included by the filter.
    pub addresses: Vec<[u8; 20]>,
    /// Event signature hashes to match in topic0. `None` matches every event.
    pub events: Option<Vec<String>>,
    /// Inclusive first block.
    pub from_block: u64,
    /// Inclusive last block.
    pub to_block: u64,
}

/// HTTP JSON-RPC client with a learned `eth_getLogs` block-range limit.
#[derive(Debug)]
pub struct RpcClient {
    client: Client,
    url: Url,
    next_request_id: AtomicU64,
}

impl RpcClient {
    /// Creates an RPC client for an HTTP(S) endpoint.
    pub fn new(rpc_url: &str) -> Result<Self, RpcError> {
        let url = Url::parse(rpc_url).map_err(RpcError::InvalidUrl)?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(RpcError::UnsupportedUrlScheme(url.scheme().to_owned()));
        }

        Ok(Self {
            client: Client::new(),
            url,
            next_request_id: AtomicU64::new(1),
        })
    }

    /// Returns the current chain height without applying finality.
    pub async fn head_block(&self) -> Result<u64, RpcError> {
        let head: String = self.call("eth_blockNumber", json!([])).await?;
        parse_quantity(&head)
    }

    /// Returns the chain ID reported by the endpoint.
    pub async fn chain_id(&self) -> Result<u64, RpcError> {
        let chain_id: String = self.call("eth_chainId", json!([])).await?;
        parse_quantity(&chain_id)
    }

    /// Returns whether an address has contract code at the current chain head.
    pub async fn has_code(&self, address: &[u8; 20]) -> Result<bool, RpcError> {
        let code: String = self
            .call("eth_getCode", json!([format_address(address), "latest"]))
            .await?;
        Ok(code != "0x" && code != "0x0")
    }

    /// Retrieves raw logs for one inclusive range.
    pub async fn get_logs(&self, filter: &LogFilter) -> Result<Vec<RawLog>, RpcError> {
        if filter.addresses.is_empty() {
            return Err(RpcError::EmptyAddressFilter);
        }
        if filter.from_block > filter.to_block {
            return Err(RpcError::InvalidBlockRange {
                from_block: filter.from_block,
                to_block: filter.to_block,
            });
        }

        let addresses = filter.addresses.iter().map(format_address).collect::<Vec<_>>();
        let topics = filter
            .events
            .as_ref()
            .map(|events| vec![Value::Array(events.iter().cloned().map(Value::String).collect())]);
        let filter = json!({
            "address": addresses,
            "topics": topics,
            "fromBlock": format_quantity(filter.from_block),
            "toBlock": format_quantity(filter.to_block),
        });
        let logs: Vec<RpcLog> = self.call("eth_getLogs", json!([filter])).await?;

        logs.into_iter().map(TryInto::try_into).collect()
    }

    async fn call<T: DeserializeOwned>(&self, method: &str, params: Value) -> Result<T, RpcError> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let response = self
            .client
            .post(self.url.clone())
            .json(&json!({
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
                "params": params,
            }))
            .send()
            .await?
            .error_for_status()?
            .json::<RpcResponse<T>>()
            .await?;

        if let Some(error) = response.error {
            return Err(RpcError::Response {
                code: error.code,
                message: error.message,
            });
        }

        response.result.ok_or(RpcError::MissingResult)
    }
}

#[derive(Debug, Deserialize)]
struct RpcResponse<T> {
    result: Option<T>,
    error: Option<RpcResponseError>,
}

#[derive(Debug, Deserialize)]
struct RpcResponseError {
    code: i64,
    message: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcLog {
    address: String,
    topics: Vec<String>,
    data: String,
    transaction_index: String,
    block_number: String,
    block_hash: String,
    transaction_hash: String,
    log_index: String,
    #[serde(default)]
    removed: bool,
}

impl TryFrom<RpcLog> for RawLog {
    type Error = RpcError;

    fn try_from(value: RpcLog) -> Result<Self, Self::Error> {
        let topics = value
            .topics
            .iter()
            .map(|topic| parse_fixed_hex::<32>(topic))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect();

        Ok(Self {
            tx_index: parse_quantity(&value.transaction_index)?,
            log_index: parse_quantity(&value.log_index)?,
            block_number: parse_quantity(&value.block_number)?,
            block_hash: parse_fixed_hex(&value.block_hash)?,
            transaction_hash: parse_fixed_hex(&value.transaction_hash)?,
            address: parse_fixed_hex(&value.address)?,
            topics,
            data: parse_hex(&value.data)?,
            removed: value.removed,
        })
    }
}

fn parse_quantity(quantity: &str) -> Result<u64, RpcError> {
    let Some(quantity) = quantity.strip_prefix("0x") else {
        return Err(RpcError::InvalidQuantity(quantity.to_owned()));
    };
    u64::from_str_radix(quantity, 16).map_err(|_| RpcError::InvalidQuantity(format!("0x{quantity}")))
}

fn format_quantity(value: u64) -> String {
    format!("0x{value:x}")
}

fn parse_fixed_hex<const LENGTH: usize>(value: &str) -> Result<[u8; LENGTH], RpcError> {
    let bytes = parse_hex(value)?;
    bytes.try_into().map_err(|bytes: Vec<u8>| RpcError::InvalidHexLength {
        value: value.to_owned(),
        expected: LENGTH,
        actual: bytes.len(),
    })
}

fn parse_hex(value: &str) -> Result<Vec<u8>, RpcError> {
    let Some(value) = value.strip_prefix("0x") else {
        return Err(RpcError::InvalidHex(value.to_owned()));
    };
    hex::decode(value).map_err(|_| RpcError::InvalidHex(format!("0x{value}")))
}

/// Errors produced by Ethereum JSON-RPC reads.
#[derive(Debug, Error)]
pub enum RpcError {
    /// RPC URL parsing failed.
    #[error("invalid RPC URL")]
    InvalidUrl(#[source] url::ParseError),
    /// Only HTTP(S) RPC endpoints are supported.
    #[error("unsupported RPC URL scheme: {0}")]
    UnsupportedUrlScheme(String),
    /// The transport failed.
    #[error(transparent)]
    Request(#[from] reqwest::Error),
    /// The node returned a JSON-RPC error.
    #[error("RPC error {code}: {message}")]
    Response {
        /// JSON-RPC error code.
        code: i64,
        /// JSON-RPC error message.
        message: String,
    },
    /// The node returned neither a result nor an error.
    #[error("RPC response did not contain a result")]
    MissingResult,
    /// A hexadecimal quantity was malformed or too large for u64.
    #[error("invalid RPC quantity: {0}")]
    InvalidQuantity(String),
    /// A hexadecimal byte value was malformed.
    #[error("invalid hexadecimal RPC value: {0}")]
    InvalidHex(String),
    /// A fixed-width RPC value had the wrong length.
    #[error("RPC value {value} must contain {expected} bytes, got {actual}")]
    InvalidHexLength {
        /// Value returned by the node.
        value: String,
        /// Required byte length.
        expected: usize,
        /// Actual decoded byte length.
        actual: usize,
    },
    /// Log queries require at least one address.
    #[error("log filter must contain at least one address")]
    EmptyAddressFilter,
    /// A requested range had its bounds reversed.
    #[error("invalid block range {from_block}..={to_block}")]
    InvalidBlockRange {
        /// Inclusive first block.
        from_block: u64,
        /// Inclusive last block.
        to_block: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::{RpcError, format_quantity, parse_fixed_hex, parse_quantity};

    #[test]
    fn parses_rpc_quantities() {
        assert_eq!(parse_quantity("0x2a").expect("valid quantity"), 42);
        assert_eq!(format_quantity(42), "0x2a");
    }

    #[test]
    fn rejects_non_hex_quantities() {
        assert!(matches!(parse_quantity("42"), Err(RpcError::InvalidQuantity(_))));
    }

    #[test]
    fn checks_fixed_width_rpc_values() {
        assert_eq!(
            parse_fixed_hex::<2>("0x0011").expect("valid fixed-width value"),
            [0, 17]
        );
        assert!(matches!(
            parse_fixed_hex::<2>("0x00"),
            Err(RpcError::InvalidHexLength { .. })
        ));
    }
}
