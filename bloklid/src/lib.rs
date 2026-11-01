//! PostgreSQL persistence and process primitives for the blokli-light daemon.
//!
//! The crate deliberately models only raw, already-mined Ethereum logs. Decoding
//! event payloads and materialising contract state are responsibilities of SQL
//! consumers, not of the indexer.

pub mod builtin;
pub mod config;
pub mod database;
pub mod indexer;
pub mod rpc;
pub mod source;
pub mod telemetry;
