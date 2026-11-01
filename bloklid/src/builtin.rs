//! Built-in HOPR source definitions supplied by `hopr-bindings`.

use hopr_bindings::{
    config::NetworksWithContractAddresses,
    exports::alloy::{primitives::{Address, B256}, sol_types::SolEvent},
    hopr_announcements_events::HoprAnnouncementsEvents::{AddressAnnouncement, KeyBinding, KeyBindingFeeUpdate, RevokeAnnouncement},
    hopr_channels::HoprChannels::LedgerDomainSeparatorUpdated,
    hopr_channels_events::HoprChannelsEvents::{ChannelBalanceDecreased, ChannelBalanceIncreased, ChannelClosed, ChannelOpened, DomainSeparatorUpdated, OutgoingChannelClosureInitiated, TicketRedeemed},
    hopr_node_safe_registry_events::HoprNodeSafeRegistryEvents::{DeregisteredNodeSafe, RegisteredNodeSafe},
    hopr_node_stake_factory_events::HoprNodeStakeFactoryEvents::NewHoprNodeStakeModuleForSafe,
    hopr_service_registry_events::HoprServiceRegistryEvents::{Deregistered, NodeSafeRegistryUpdated, Registered, RequirementUpdated, SelfRegistrationBurnUpdated, SelfUpdateBurnUpdated, ServiceTypeRegistered, TypeOwnershipTransferred, TypeRegistrationFeeUpdated, Updated},
    hopr_ticket_price_oracle_events::HoprTicketPriceOracleEvents::TicketPriceUpdated,
    hopr_winning_probability_oracle_events::HoprWinningProbabilityOracleEvents::WinProbUpdated,
};
use thiserror::Error;

use crate::database::{NewSource, SourceKind};

/// Built-in topic selection mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BuiltinTopics {
    /// Preserve Blokli's known event lists.
    Known,
    /// Persist every event emitted by each built-in contract.
    All,
}

/// Resolves the HOPR contracts watched by blokli-light for a binding network.
pub fn sources(network: &str, topics: BuiltinTopics) -> Result<Vec<NewSource>, BuiltinError> {
    let networks = NetworksWithContractAddresses::default();
    let network = networks.networks.get(network).ok_or_else(|| BuiltinError::UnknownNetwork(network.to_owned()))?;
    let from_block = u64::from(network.indexer_start_block_number);
    let addresses = network.addresses;
    let mut sources = vec![
        source("channels", addresses.channels, from_block, topics, channel_topics()),
        source("announcements", addresses.announcements, from_block, topics, announcement_topics()),
        source("node-safe-registry", addresses.node_safe_registry, from_block, topics, registry_topics()),
        source("node-stake-factory", addresses.node_stake_factory, from_block, topics, stake_factory_topics()),
        source("ticket-price-oracle", addresses.ticket_price_oracle, from_block, topics, vec![TicketPriceUpdated::SIGNATURE_HASH]),
        source("winning-probability-oracle", addresses.winning_probability_oracle, from_block, topics, vec![WinProbUpdated::SIGNATURE_HASH]),
    ];
    if addresses.service_registry != Address::ZERO {
        sources.push(source("service-registry", addresses.service_registry, from_block, topics, service_registry_topics()));
    }
    Ok(sources)
}

fn source(name: &str, address: Address, from_block: u64, mode: BuiltinTopics, known: Vec<B256>) -> NewSource {
    NewSource {
        name: name.to_owned(),
        address: address.into_array(),
        kind: SourceKind::Builtin,
        from_block,
        events: match mode { BuiltinTopics::Known => Some(known.into_iter().map(topic).collect()), BuiltinTopics::All => None },
    }
}

fn topic(topic: B256) -> String { format!("0x{}", hex::encode(topic.as_slice())) }
fn channel_topics() -> Vec<B256> { vec![ChannelBalanceDecreased::SIGNATURE_HASH, ChannelBalanceIncreased::SIGNATURE_HASH, ChannelClosed::SIGNATURE_HASH, ChannelOpened::SIGNATURE_HASH, OutgoingChannelClosureInitiated::SIGNATURE_HASH, TicketRedeemed::SIGNATURE_HASH, DomainSeparatorUpdated::SIGNATURE_HASH, LedgerDomainSeparatorUpdated::SIGNATURE_HASH] }
fn announcement_topics() -> Vec<B256> { vec![AddressAnnouncement::SIGNATURE_HASH, KeyBinding::SIGNATURE_HASH, KeyBindingFeeUpdate::SIGNATURE_HASH, RevokeAnnouncement::SIGNATURE_HASH, LedgerDomainSeparatorUpdated::SIGNATURE_HASH] }
fn registry_topics() -> Vec<B256> { vec![RegisteredNodeSafe::SIGNATURE_HASH, DeregisteredNodeSafe::SIGNATURE_HASH, hopr_bindings::hopr_node_safe_registry_events::HoprNodeSafeRegistryEvents::DomainSeparatorUpdated::SIGNATURE_HASH] }
fn stake_factory_topics() -> Vec<B256> { vec![NewHoprNodeStakeModuleForSafe::SIGNATURE_HASH] }
fn service_registry_topics() -> Vec<B256> { vec![Registered::SIGNATURE_HASH, Updated::SIGNATURE_HASH, Deregistered::SIGNATURE_HASH, ServiceTypeRegistered::SIGNATURE_HASH, TypeOwnershipTransferred::SIGNATURE_HASH, RequirementUpdated::SIGNATURE_HASH, SelfRegistrationBurnUpdated::SIGNATURE_HASH, SelfUpdateBurnUpdated::SIGNATURE_HASH, TypeRegistrationFeeUpdated::SIGNATURE_HASH, NodeSafeRegistryUpdated::SIGNATURE_HASH] }

/// Failed built-in binding resolution.
#[derive(Debug, Error)]
pub enum BuiltinError { #[error("unknown HOPR binding network: {0}")] UnknownNetwork(String) }
