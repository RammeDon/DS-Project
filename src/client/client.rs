use crate::{configs::ClientConfig, data_collection::ClientData, network::Network};
use chrono::Utc;
use log::*;
use omnipaxos_kv::common::{kv::*, messages::*};
use rand::Rng;
use std::time::Duration;
use tokio::time::interval;

const NETWORK_BATCH_SIZE: usize = 100;

pub struct Client {
    id: ClientId,
    network: Network,
    client_data: ClientData,
    config: ClientConfig,
    active_server: NodeId,
    final_request_count: Option<usize>,
    next_request_id: usize,
}

impl Client {
    pub async fn new(config: ClientConfig) -> Self {
        let network = Network::new(
            vec![(config.server_id, config.server_address.clone())],
            NETWORK_BATCH_SIZE,
        )
        .await;
        Client {
            id: config.server_id,
            network,
            client_data: ClientData::new(),
            active_server: config.server_id,
            config,
            final_request_count: None,
            next_request_id: 0,
        }
    }

    pub async fn run(&mut self) {
        // Wait for server to signal start
        info!("{}: Waiting for start signal from server", self.id);
        match self.network.server_messages.recv().await {
            Some(ServerMessage::StartSignal(start_time)) => {
                Self::wait_until_sync_time(&mut self.config, start_time).await;
            }
            _ => panic!("Error waiting for start signal"),
        }

        // Initialize SQL database with a table
        info!("{}: Initializing SQL database", self.id);
        
        // Create table for testing if it doesn't exist
        self.send_sql_write("CREATE TABLE IF NOT EXISTS test_data (id SERIAL PRIMARY KEY, key TEXT NOT NULL, value TEXT NOT NULL)").await;
        
        // Early end
        let intervals = self.config.requests.clone();
        if intervals.is_empty() {
            self.save_results().expect("Failed to save results");
            return;
        }

        // Initialize intervals
        let mut rng = rand::thread_rng();
        let mut intervals = intervals.iter();
        let first_interval = intervals.next().unwrap();
        let mut read_ratio = first_interval.get_read_ratio();
        let mut request_interval = interval(first_interval.get_request_delay());
        let mut next_interval = interval(first_interval.get_interval_duration());
        let _ = next_interval.tick().await;

        // Main event loop
        info!("{}: Starting requests", self.id);
        loop {
            tokio::select! {
                biased;
                Some(msg) = self.network.server_messages.recv() => {
                    self.handle_server_message(msg);
                    if self.run_finished() {
                        break;
                    }
                }
                _ = request_interval.tick(), if self.final_request_count.is_none() => {
                    let is_write = rng.gen::<f64>() > read_ratio;
                    if self.next_request_id % 10 == 0 {
                        // Every 10th request, use SQL instead of KV
                        if is_write {
                            // SQL write
                            self.send_sql_demo_write().await;
                        } else {
                            // SQL read with different consistency levels
                            let consistency = match self.next_request_id % 3 {
                                0 => ReadConsistency::Leader,
                                1 => ReadConsistency::Local,
                                _ => ReadConsistency::Linearizable,
                            };
                            self.send_sql_demo_read(consistency).await;
                        }
                    } else {
                        // Use regular KV operations for most requests
                        self.send_regular_request(is_write).await;
                    }
                },
                _ = next_interval.tick() => {
                    match intervals.next() {
                        Some(new_interval) => {
                            read_ratio = new_interval.read_ratio;
                            next_interval = interval(new_interval.get_interval_duration());
                            next_interval.tick().await;
                            request_interval = interval(new_interval.get_request_delay());
                        },
                        None => {
                            self.final_request_count = Some(self.client_data.request_count());
                            if self.run_finished() {
                                break;
                            }
                        },
                    }
                },
            }
        }

        info!(
            "{}: Client finished: collected {} responses",
            self.id,
            self.client_data.response_count(),
        );
        self.network.shutdown();
        self.save_results().expect("Failed to save results");
    }

    fn handle_server_message(&mut self, msg: ServerMessage) {
        debug!("Received {msg:?}");
        match msg {
            ServerMessage::StartSignal(_) => (),
            ServerMessage::QueryResult(cmd_id, rows) => {
                info!("Received SQL query result for command {}: {} rows", cmd_id, rows.len());
                if !rows.is_empty() {
                    for (i, row) in rows.iter().enumerate().take(5) {
                        info!("Row {}: {:?}", i, row);
                    }
                    if rows.len() > 5 {
                        info!("... and {} more rows", rows.len() - 5);
                    }
                }
                self.client_data.new_response(cmd_id);
            },
            ServerMessage::Error(cmd_id, error) => {
                error!("Received error for command {}: {}", cmd_id, error);
                self.client_data.new_response(cmd_id);
            },
            ServerMessage::Write(cmd_id) => {
                debug!("Received Write({})", cmd_id);
                self.client_data.new_response(cmd_id);
            },
            ServerMessage::Read(cmd_id, value) => {
                debug!("Received Read({}, {:?})", cmd_id, value);
                self.client_data.new_response(cmd_id);
            },
        }
    }

    async fn send_regular_request(&mut self, is_write: bool) {
        let key = self.next_request_id.to_string();
        let cmd = match is_write {
            true => KVCommand::Put(key.clone(), key),
            false => KVCommand::Get(key),
        };
        let request = ClientMessage::Append(self.next_request_id, cmd);
        debug!("Sending {request:?}");
        self.network.send(self.active_server, request).await;
        self.client_data.new_request(is_write);
        self.next_request_id += 1;
    }

    async fn send_sql_demo_write(&mut self) {
        let key = format!("sql_key_{}", self.next_request_id);
        let value = format!("sql_value_{}", self.next_request_id);
        
        let query = format!(
            "INSERT INTO test_data (key, value) VALUES ('{}', '{}')",
            key, value
        );
        
        info!("Sending SQL write: {}", query);
        self.send_sql_write(&query).await;
    }
    
    async fn send_sql_demo_read(&mut self, consistency: ReadConsistency) {
        let query = "SELECT id, key, value FROM test_data ORDER BY id DESC LIMIT 5";
        
        info!("Sending SQL read with {:?} consistency: {}", consistency, query);
        self.send_sql_read(query, consistency).await;
    }

    async fn send_sql_write(&mut self, query: &str) {
        let cmd = KVCommand::ExecuteQuery(query.to_string());
        let request = ClientMessage::Append(self.next_request_id, cmd);
        self.network.send(self.active_server, request).await;
        self.client_data.new_request(true);
        self.next_request_id += 1;
    }
    
    async fn send_sql_read(&mut self, query: &str, consistency: ReadConsistency) {
        let request = ClientMessage::Read(
            self.next_request_id,
            query.to_string(),
            consistency
        );
        self.network.send(self.active_server, request).await;
        self.client_data.new_request(false);
        self.next_request_id += 1;
    }

    fn run_finished(&self) -> bool {
        if let Some(count) = self.final_request_count {
            if self.client_data.request_count() >= count {
                return true;
            }
        }
        return false;
    }

    // Wait until the scheduled start time to synchronize client starts.
    // If start time has already passed, start immediately.
    async fn wait_until_sync_time(config: &mut ClientConfig, scheduled_start_utc_ms: i64) {
        let now = Utc::now();
        let milliseconds_until_sync = scheduled_start_utc_ms - now.timestamp_millis();
        config.sync_time = Some(milliseconds_until_sync);
        if milliseconds_until_sync > 0 {
            tokio::time::sleep(Duration::from_millis(milliseconds_until_sync as u64)).await;
        } else {
            warn!("Started after synchronization point!");
        }
    }

    fn save_results(&self) -> Result<(), std::io::Error> {
        self.client_data.save_summary(self.config.clone())?;
        self.client_data
            .to_csv(self.config.output_filepath.clone())?;
        Ok(())
    }
}