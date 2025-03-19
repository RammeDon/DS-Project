use crate::{configs::OmniPaxosKVConfig, database::{Database, QueryResult}, network::Network};
use chrono::Utc;
use log::*;
use omnipaxos::{
    messages::Message,
    util::{LogEntry, NodeId},
    OmniPaxos, OmniPaxosConfig,
};
use omnipaxos_kv::common::{kv::*, messages::*, utils::Timestamp};
use omnipaxos_storage::memory_storage::MemoryStorage;
use std::{fs::File, io::Write, time::Duration};
use std::sync::Arc;
use tokio::sync::RwLock;

type OmniPaxosInstance = OmniPaxos<Command, MemoryStorage<Command>>;
const NETWORK_BATCH_SIZE: usize = 100;
const LEADER_WAIT: Duration = Duration::from_secs(1);
const ELECTION_TIMEOUT: Duration = Duration::from_secs(1);

pub struct OmniPaxosServer {
    id: NodeId,
    database: Arc<RwLock<Database>>,
    network: Network,
    omnipaxos: OmniPaxosInstance,
    current_decided_idx: usize,
    omnipaxos_msg_buffer: Vec<Message<Command>>,
    config: OmniPaxosKVConfig,
    peers: Vec<NodeId>,
}

impl OmniPaxosServer {
    pub async fn new(config: OmniPaxosKVConfig) -> Self {
        // Initialize OmniPaxos instance
        let storage: MemoryStorage<Command> = MemoryStorage::default();
        let omnipaxos_config: OmniPaxosConfig = config.clone().into();
        let omnipaxos_msg_buffer = Vec::with_capacity(omnipaxos_config.server_config.buffer_size);
        let omnipaxos = omnipaxos_config.build(storage).unwrap();
        
        // Initialize SQL database
        info!("Server {}: Initializing SQL database", config.local.server_id);
        let database = Arc::new(RwLock::new(Database::new().await));
        
        // Waits for client and server network connections to be established
        let network = Network::new(config.clone(), NETWORK_BATCH_SIZE).await;
        
        OmniPaxosServer {
            id: config.local.server_id,
            database,
            network,
            omnipaxos,
            current_decided_idx: 0,
            omnipaxos_msg_buffer,
            peers: config.get_peers(config.local.server_id),
            config,
        }
    }

    pub async fn run(&mut self) {
        // Save config to output file
        self.save_output().expect("Failed to write to file");
        let mut client_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        let mut cluster_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        // We don't use Omnipaxos leader election at first and instead force a specific initial leader
        self.establish_initial_leader(&mut cluster_msg_buf, &mut client_msg_buf)
            .await;
        // Main event loop with leader election
        let mut election_interval = tokio::time::interval(ELECTION_TIMEOUT);
        loop {
            tokio::select! {
                _ = election_interval.tick() => {
                    self.omnipaxos.tick();
                    self.send_outgoing_msgs();
                },
                _ = self.network.cluster_messages.recv_many(&mut cluster_msg_buf, NETWORK_BATCH_SIZE) => {
                    self.handle_cluster_messages(&mut cluster_msg_buf).await;
                },
                _ = self.network.client_messages.recv_many(&mut client_msg_buf, NETWORK_BATCH_SIZE) => {
                    self.handle_client_messages(&mut client_msg_buf).await;
                },
            }
        }
    }

    // Ensures cluster is connected and initial leader is promoted before returning.
    // Once the leader is established it chooses a synchronization point which the
    // followers relay to their clients to begin the experiment.
    async fn establish_initial_leader(
        &mut self,
        cluster_msg_buffer: &mut Vec<(NodeId, ClusterMessage)>,
        client_msg_buffer: &mut Vec<(ClientId, ClientMessage)>,
    ) {
        let mut leader_takeover_interval = tokio::time::interval(LEADER_WAIT);
        loop {
            tokio::select! {
                _ = leader_takeover_interval.tick(), if self.config.cluster.initial_leader == self.id => {
                    if let Some((curr_leader, is_accept_phase)) = self.omnipaxos.get_current_leader(){
                        if curr_leader == self.id && is_accept_phase {
                            info!("{}: Leader fully initialized", self.id);
                            let experiment_sync_start = (Utc::now() + Duration::from_secs(2)).timestamp_millis();
                            self.send_cluster_start_signals(experiment_sync_start);
                            self.send_client_start_signals(experiment_sync_start);
                            break;
                        }
                    }
                    info!("{}: Attempting to take leadership", self.id);
                    self.omnipaxos.try_become_leader();
                    self.send_outgoing_msgs();
                },
                _ = self.network.cluster_messages.recv_many(cluster_msg_buffer, NETWORK_BATCH_SIZE) => {
                    let recv_start = self.handle_cluster_messages(cluster_msg_buffer).await;
                    if recv_start {
                        break;
                    }
                },
                _ = self.network.client_messages.recv_many(client_msg_buffer, NETWORK_BATCH_SIZE) => {
                    self.handle_client_messages(client_msg_buffer).await;
                },
            }
        }
    }

    async fn handle_decided_entries(&mut self) {
        // Get newly decided entries
        let new_decided_idx = self.omnipaxos.get_decided_idx();
        if self.current_decided_idx < new_decided_idx {
            let decided_entries = self
                .omnipaxos
                .read_decided_suffix(self.current_decided_idx)
                .unwrap();
            self.current_decided_idx = new_decided_idx;
            debug!("Decided {new_decided_idx}");
            
            let decided_commands = decided_entries
                .into_iter()
                .filter_map(|e| match e {
                    LogEntry::Decided(cmd) => Some(cmd),
                    _ => unreachable!(),
                })
                .collect();
            
            // Update the database with the new commands
            self.update_database_and_respond(decided_commands).await;
        }
    }

    async fn update_database_and_respond(&mut self, commands: Vec<Command>) {
        for command in commands {
            let database = self.database.clone();
            let client_id = command.client_id;
            let command_id = command.id;
            let coordinator_id = command.coordinator_id;
            let kv_cmd = command.kv_cmd.clone();
            
            // Process each command
            if coordinator_id == self.id {
                // Only process commands if we're the coordinator
                let mut db_lock = database.write().await;
                match db_lock.handle_command(kv_cmd).await {
                    Some(Ok(QueryResult::SingleValue(value))) => {
                        debug!("Query result: SingleValue({:?})", value);
                        let response = ServerMessage::Read(command_id, value);
                        self.network.send_to_client(client_id, response);
                    },
                    Some(Ok(QueryResult::Rows(rows))) => {
                        info!("Query result: {} rows", rows.len());
                        let response = ServerMessage::QueryResult(command_id, rows);
                        self.network.send_to_client(client_id, response);
                    },
                    Some(Ok(QueryResult::Success)) => {
                        debug!("Query result: Success");
                        let response = ServerMessage::Write(command_id);
                        self.network.send_to_client(client_id, response);
                    },
                    Some(Err(error)) => {
                        error!("Query error: {}", error);
                        let response = ServerMessage::Error(command_id, error);
                        self.network.send_to_client(client_id, response);
                    },
                    None => {
                        let response = ServerMessage::Write(command_id);
                        self.network.send_to_client(client_id, response);
                    },
                }
            }
        }
    }

    fn send_outgoing_msgs(&mut self) {
        self.omnipaxos
            .take_outgoing_messages(&mut self.omnipaxos_msg_buffer);
        for msg in self.omnipaxos_msg_buffer.drain(..) {
            let to = msg.get_receiver();
            let cluster_msg = ClusterMessage::OmniPaxosMessage(msg);
            self.network.send_to_cluster(to, cluster_msg);
        }
    }

    async fn handle_client_messages(&mut self, messages: &mut Vec<(ClientId, ClientMessage)>) {
        for (from, message) in messages.drain(..) {
            match message {
                ClientMessage::Append(command_id, kv_command) => {
                    debug!("Received Append({}, {:?})", command_id, kv_command);
                    self.append_to_log(from, command_id, kv_command)
                },
                ClientMessage::Read(command_id, query, consistency) => {
                    info!("Received Read({}, {:?}, {:?})", command_id, query, consistency);
                    self.handle_read(from, command_id, query, consistency).await
                }
            }
        }
        self.send_outgoing_msgs();
    }

    async fn handle_read(&mut self, from: ClientId, command_id: CommandId, query: String, consistency: ReadConsistency) {
        match consistency {
            ReadConsistency::Leader => {
                // Only the leader can respond to leader reads
                if let Some((current_leader, _)) = self.omnipaxos.get_current_leader() {
                    if current_leader == self.id {
                        // If I'm the leader, execute the read locally
                        info!("Node {} handling leader read for query: {}", self.id, query);
                        let database = self.database.clone();
                        let db = database.read().await;
                        match db.direct_read(&query).await {
                            Ok(QueryResult::Rows(rows)) => {
                                info!("Leader read result: {} rows", rows.len());
                                self.network.send_to_client(from, ServerMessage::QueryResult(command_id, rows));
                            },
                            Ok(QueryResult::SingleValue(value)) => {
                                debug!("Leader read result: {:?}", value);
                                self.network.send_to_client(from, ServerMessage::Read(command_id, value));
                            },
                            Ok(QueryResult::Success) => {
                                debug!("Leader read result: Success");
                                self.network.send_to_client(from, ServerMessage::Write(command_id));
                            },
                            Err(error) => {
                                error!("Leader read error: {}", error);
                                self.network.send_to_client(from, ServerMessage::Error(command_id, error));
                            },
                        }
                    } else {
                        // If I'm not the leader, return an error
                        error!("Not the leader for a leader read. Current leader is {}", current_leader);
                        let error_response = ServerMessage::Error(
                            command_id, 
                            format!("Not the leader. Current leader is {}", current_leader)
                        );
                        self.network.send_to_client(from, error_response);
                    }
                } else {
                    error!("No leader available for a leader read");
                    let error_response = ServerMessage::Error(
                        command_id, 
                        "No leader available".to_string()
                    );
                    self.network.send_to_client(from, error_response);
                }
            },
            ReadConsistency::Local => {
                // Execute read locally regardless of leader status
                info!("Node {} handling local read for query: {}", self.id, query);
                let database = self.database.clone();
                let db = database.read().await;
                match db.direct_read(&query).await {
                    Ok(QueryResult::Rows(rows)) => {
                        info!("Local read result: {} rows", rows.len());
                        self.network.send_to_client(from, ServerMessage::QueryResult(command_id, rows));
                    },
                    Ok(QueryResult::SingleValue(value)) => {
                        debug!("Local read result: {:?}", value);
                        self.network.send_to_client(from, ServerMessage::Read(command_id, value));
                    },
                    Ok(QueryResult::Success) => {
                        debug!("Local read result: Success");
                        self.network.send_to_client(from, ServerMessage::Write(command_id));
                    },
                    Err(error) => {
                        error!("Local read error: {}", error);
                        self.network.send_to_client(from, ServerMessage::Error(command_id, error));
                    },
                }
            },
            ReadConsistency::Linearizable => {
                // For linearizable reads, we put the read through the log
                info!("Node {} handling linearizable read for query: {}", self.id, query);
                let kv_command = KVCommand::QueryRead(query);
                self.append_to_log(from, command_id, kv_command);
            }
        }
    }

    async fn handle_cluster_messages(
        &mut self,
        messages: &mut Vec<(NodeId, ClusterMessage)>,
    ) -> bool {
        let mut received_start_signal = false;
        for (from, message) in messages.drain(..) {
            trace!("{}: Received {message:?}", self.id);
            match message {
                ClusterMessage::OmniPaxosMessage(m) => {
                    self.omnipaxos.handle_incoming(m);
                    self.handle_decided_entries().await;
                }
                ClusterMessage::LeaderStartSignal(start_time) => {
                    debug!("Received start message from peer {from}");
                    received_start_signal = true;
                    self.send_client_start_signals(start_time);
                }
            }
        }
        self.send_outgoing_msgs();
        received_start_signal
    }

    fn append_to_log(&mut self, from: ClientId, command_id: CommandId, kv_command: KVCommand) {
        let command = Command {
            client_id: from,
            coordinator_id: self.id,
            id: command_id,
            kv_cmd: kv_command,
        };
        match self.omnipaxos.append(command) {
            Ok(_) => debug!("Appended to OmniPaxos log"),
            Err(e) => error!("Failed to append to OmniPaxos log: {:?}", e),
        }
    }

    fn send_cluster_start_signals(&mut self, start_time: Timestamp) {
        for peer in &self.peers {
            debug!("Sending start message to peer {peer}");
            let msg = ClusterMessage::LeaderStartSignal(start_time);
            self.network.send_to_cluster(*peer, msg);
        }
    }

    fn send_client_start_signals(&mut self, start_time: Timestamp) {
        for client_id in 1..self.config.local.num_clients as ClientId + 1 {
            debug!("Sending start message to client {client_id}");
            let msg = ServerMessage::StartSignal(start_time);
            self.network.send_to_client(client_id, msg);
        }
    }

    fn save_output(&mut self) -> Result<(), std::io::Error> {
        let config_json = serde_json::to_string_pretty(&self.config)?;
        let mut output_file = File::create(&self.config.local.output_filepath)?;
        output_file.write_all(config_json.as_bytes())?;
        output_file.flush()?;
        Ok(())
    }
}