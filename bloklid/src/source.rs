//! Validation and normalisation for operator-defined log sources.

use alloy_primitives::keccak256;
use thiserror::Error;

/// Parses an Ethereum address from its canonical hexadecimal representation.
pub fn parse_address(address: &str) -> Result<[u8; 20], SourceError> {
    if address.len() != 42 || !address.starts_with("0x") {
        return Err(SourceError::InvalidAddress(address.to_owned()));
    }

    let bytes = hex::decode(&address[2..]).map_err(|_| SourceError::InvalidAddress(address.to_owned()))?;
    bytes
        .try_into()
        .map_err(|_: Vec<u8>| SourceError::InvalidAddress(address.to_owned()))
}

/// Formats an address using lowercase hexadecimal digits for RPC and SQL display.
pub fn format_address(address: &[u8; 20]) -> String {
    format!("0x{}", hex::encode(address))
}

/// Normalises one configured event into a lowercase, `0x`-prefixed topic0 hash.
///
/// A complete 32-byte hash is retained as supplied; a textual Solidity event
/// signature is whitespace-normalised then Keccak-256 hashed.
pub fn normalize_event(event: &str) -> Result<String, SourceError> {
    let event = event.trim();
    if event.starts_with("0x") {
        return normalize_hash(event);
    }

    let signature = event
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect::<String>();
    validate_signature(&signature)?;
    Ok(format!("0x{}", hex::encode(keccak256(signature.as_bytes()))))
}

/// Normalises a set of configured events while removing duplicates in first-seen order.
pub fn normalize_events(events: &[String]) -> Result<Vec<String>, SourceError> {
    let mut normalized = Vec::with_capacity(events.len());

    for event in events {
        let event = normalize_event(event)?;
        if !normalized.contains(&event) {
            normalized.push(event);
        }
    }

    if normalized.is_empty() {
        return Err(SourceError::EmptyEventSet);
    }

    Ok(normalized)
}

fn normalize_hash(hash: &str) -> Result<String, SourceError> {
    if hash.len() != 66 {
        return Err(SourceError::InvalidTopicHashLength(hash.len()));
    }

    let bytes = hex::decode(&hash[2..]).map_err(|_| SourceError::InvalidTopicHash(hash.to_owned()))?;
    if bytes.len() != 32 {
        return Err(SourceError::InvalidTopicHashLength(hash.len()));
    }

    Ok(format!("0x{}", hex::encode(bytes)))
}

fn validate_signature(signature: &str) -> Result<(), SourceError> {
    let Some(open) = signature.find('(') else {
        return Err(SourceError::InvalidEventSignature(signature.to_owned()));
    };
    if signature.len() < 3 || !signature.ends_with(')') || !is_identifier(&signature[..open]) {
        return Err(SourceError::InvalidEventSignature(signature.to_owned()));
    }

    let mut depth = 0_u32;
    let mut previous_was_separator = false;
    for character in signature[open..].chars() {
        match character {
            '(' => {
                depth = depth
                    .checked_add(1)
                    .ok_or_else(|| SourceError::InvalidEventSignature(signature.to_owned()))?;
                previous_was_separator = false;
            }
            ')' => {
                if depth == 0 || previous_was_separator {
                    return Err(SourceError::InvalidEventSignature(signature.to_owned()));
                }
                depth -= 1;
                previous_was_separator = false;
            }
            ',' => {
                if depth == 0 || previous_was_separator {
                    return Err(SourceError::InvalidEventSignature(signature.to_owned()));
                }
                previous_was_separator = true;
            }
            character if character.is_ascii_alphanumeric() || matches!(character, '_' | '[' | ']') => {
                previous_was_separator = false;
            }
            _ => return Err(SourceError::InvalidEventSignature(signature.to_owned())),
        }
    }

    if depth != 0 || previous_was_separator {
        return Err(SourceError::InvalidEventSignature(signature.to_owned()));
    }

    Ok(())
}

fn is_identifier(value: &str) -> bool {
    let mut characters = value.chars();
    matches!(characters.next(), Some(character) if character.is_ascii_alphabetic() || character == '_')
        && characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
}

/// Errors returned while parsing custom source configuration or CLI arguments.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum SourceError {
    /// A provided Ethereum address did not contain exactly 20 hexadecimal bytes.
    #[error("Ethereum address must be 0x-prefixed and 20 bytes: {0}")]
    InvalidAddress(String),
    /// A source that supplies an event list must contain at least one event.
    #[error("event list cannot be empty; omit it to index every event")]
    EmptyEventSet,
    /// A provided topic hash was not a complete 32-byte hexadecimal value.
    #[error("event topic hash must be 0x-prefixed and 32 bytes: {0}")]
    InvalidTopicHash(String),
    /// A provided topic hash had an invalid character width.
    #[error("event topic hash must be 66 characters including 0x, got {0}")]
    InvalidTopicHashLength(usize),
    /// A textual Solidity event signature was malformed.
    #[error("invalid Solidity event signature: {0}")]
    InvalidEventSignature(String),
}

#[cfg(test)]
mod tests {
    use super::{SourceError, normalize_event, normalize_events};

    #[test]
    fn hashes_a_text_signature_after_whitespace_normalisation() {
        assert_eq!(
            normalize_event(" Transfer( address, address, uint256 ) "),
            Ok("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef".to_owned())
        );
    }

    #[test]
    fn parses_and_formats_an_address() {
        let address = super::parse_address("0x00112233445566778899aabbccddeeff00112233").expect("valid address");
        assert_eq!(
            super::format_address(&address),
            "0x00112233445566778899aabbccddeeff00112233"
        );
    }

    #[test]
    fn accepts_and_lowercases_an_existing_topic_hash() {
        assert_eq!(
            normalize_event("0xDDF252AD1BE2C89B69C2B068FC378DAA952BA7F163C4A11628F55A4DF523B3EF"),
            Ok("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef".to_owned())
        );
    }

    #[test]
    fn rejects_malformed_event_input() {
        assert!(matches!(
            normalize_event("Transfer(address,,uint256)"),
            Err(SourceError::InvalidEventSignature(_))
        ));
        assert!(matches!(
            normalize_event("0xdeadbeef"),
            Err(SourceError::InvalidTopicHashLength(10))
        ));
    }

    #[test]
    fn removes_duplicate_events_without_changing_order() {
        let transfer = "Transfer(address,address,uint256)".to_owned();
        let approval = "Approval(address,address,uint256)".to_owned();

        assert_eq!(
            normalize_events(&[transfer.clone(), approval, transfer]),
            Ok(vec![
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef".to_owned(),
                "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925".to_owned(),
            ])
        );
    }
}
