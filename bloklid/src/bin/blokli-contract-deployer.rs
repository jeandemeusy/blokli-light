//! Development-only HOPR contract deployment helper for an Anvil chain.

use std::{str::FromStr, time::Duration};

use anyhow::Context;
use clap::Parser;
use hopr_bindings::{
    config::ContractInstances,
    exports::alloy::{
        network::EthereumWallet,
        providers::{Provider, ProviderBuilder},
        signers::local::PrivateKeySigner,
    },
};
use url::Url;

const HOPR_DEPLOYER_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
const COMMON_DEPLOYER_KEY: &str = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";

#[derive(Debug, Parser)]
#[command(name = "blokli-contract-deployer")]
struct Args {
    #[arg(long, env = "BLOKLI_DEPLOYER_RPC_URL", default_value = "http://127.0.0.1:8545")]
    rpc_url: Url,
    #[arg(long, env = "ANVIL_DEPLOYER_PRIVATE_KEY", default_value = HOPR_DEPLOYER_KEY)]
    hopr_private_key: String,
    #[arg(long, env = "ANVIL_COMMON_DEPLOYER_PRIVATE_KEY", default_value = COMMON_DEPLOYER_KEY)]
    common_private_key: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let hopr_signer = PrivateKeySigner::from_str(&args.hopr_private_key).context("invalid HOPR deployer key")?;
    let common_signer = PrivateKeySigner::from_str(&args.common_private_key).context("invalid common deployer key")?;
    let hopr_address = hopr_signer.address();
    let common_address = common_signer.address();
    let mut wallet = EthereumWallet::from(common_signer);
    wallet.register_default_signer(hopr_signer);
    let provider = ProviderBuilder::new().wallet(wallet).connect_http(args.rpc_url);
    provider.client().set_poll_interval(Duration::from_millis(100));
    let instances = ContractInstances::deploy_for_testing(provider, hopr_address, common_address).await?;
    let addresses = instances.get_contract_addresses();
    println!("channels={}", addresses.channels);
    println!("announcements={}", addresses.announcements);
    println!("node_safe_registry={}", addresses.node_safe_registry);
    println!("node_stake_factory={}", addresses.node_stake_factory);
    println!("ticket_price_oracle={}", addresses.ticket_price_oracle);
    println!("winning_probability_oracle={}", addresses.winning_probability_oracle);
    println!("service_registry={}", addresses.service_registry);
    println!("token={}", addresses.token);
    println!("xhopr_token={}", addresses.xhopr_token);
    Ok(())
}
