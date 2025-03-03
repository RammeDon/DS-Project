use crate::{configs::OmniPaxosKVConfig, server::OmniPaxosServer};
use env_logger;

mod configs;
mod database;
mod network;
mod server;

#[tokio::main]
pub async fn main() {
    env_logger::init();
    let server_config = match OmniPaxosKVConfig::new() {
        Ok(parsed_config) => parsed_config,
        Err(e) => panic!("{e}"),
    };
    let mut server = OmniPaxosServer::new(server_config).await;
    server.run().await;
}
