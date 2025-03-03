#!/bin/bash

client1_id=1
client2_id=2
rust_log="debug"

# Clean up child processes
interrupt() {
    pkill -P $$
}
trap "interrupt" SIGINT

# Clients' output is saved into logs dir
local_experiment_dir="./logs"
mkdir -p "${local_experiment_dir}"

# Run clients
client1_config_path="./client-${client1_id}-config.toml"
client2_config_path="./client-${client2_id}-config.toml"
RUST_LOG=$rust_log CONFIG_FILE="$client1_config_path"  cargo run --manifest-path="../Cargo.toml" --bin client &
RUST_LOG=$rust_log CONFIG_FILE="$client2_config_path"  cargo run --manifest-path="../Cargo.toml" --bin client
