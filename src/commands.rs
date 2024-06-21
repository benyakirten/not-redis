use std::collections::HashMap;

use tokio::sync::broadcast::{Receiver, Sender};

use crate::request::{
    self, CommandExpiration, SetCommand, XAddCommand, XRangeCommand, XReadCommand,
};
use crate::{data, encoding, server, transmission};

pub fn pong(body: Option<String>) -> Result<Vec<Vec<u8>>, anyhow::Error> {
    let response = match body {
        Some(body) => encoding::simple_string(&body),
        None => encoding::simple_string("PONG"),
    }
    .as_bytes()
    .to_vec();
    let response = vec![response];

    Ok(response)
}

pub fn echo_response(body: String) -> Result<Vec<Vec<u8>>, anyhow::Error> {
    let response = encoding::bulk_string(&body).as_bytes().to_vec();
    let response = vec![response];

    Ok(response)
}

pub fn get_value(database: &data::Database, key: String) -> Result<Vec<Vec<u8>>, anyhow::Error> {
    let value = database.get(&key);
    let response = match value {
        Ok(v) => match v {
            Some(v) => encoding::bulk_string(&v),
            None => encoding::empty_string(),
        },
        Err(v) => encoding::error_string(&v.to_string()),
    }
    .as_bytes()
    .to_vec();

    let response = vec![response];

    Ok(response)
}

pub async fn get_info(server: &server::RedisServer) -> Result<Vec<Vec<u8>>, anyhow::Error> {
    let server = server.read().await;
    let role = match server.role {
        server::ServerRole::Master(..) => "master",
        server::ServerRole::Slave => "slave",
    };

    let mut map = HashMap::new();

    let master_replid = server.replication.id.as_str();
    let master_repl_offset = server.replication.offset.to_string();

    map.insert("role", role);
    map.insert("master_replid", master_replid);
    map.insert("master_repl_offset", &master_repl_offset);

    let response = encoding::bulk_string_from_hashmap(&map).as_bytes().to_vec();
    let response = vec![response];

    Ok(response)
}

pub async fn perform_psync(server: &server::RedisServer) -> Result<Vec<Vec<u8>>, anyhow::Error> {
    let repl_id = &server.read().await.replication.id;
    let encoded = encoding::simple_string(&format!("FULLRESYNC {} 0", repl_id));

    // TODO: Get the actual database
    let empty_rdb = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
    let empty_rdb = hex::decode(empty_rdb)?;
    let rdb_sync = encoding::encode_rdb(empty_rdb);

    Ok(vec![encoded.as_bytes().to_vec(), rdb_sync])
}

pub fn replica_confirm(
    repl: request::ReplicationCommand,
    size: usize,
) -> Result<Vec<Vec<u8>>, anyhow::Error> {
    let response = match repl {
        request::ReplicationCommand::Ack => {
            encoding::encode_string_array(&["REPLCONF", "ACK", &size.to_string()])
                .as_bytes()
                .to_vec()
        }
        _ => encoding::okay_string().as_bytes().to_vec(),
    };
    let response = vec![response];

    Ok(response)
}

pub fn set_value(
    database: &data::Database,
    set_command: SetCommand,
) -> Result<Vec<Vec<u8>>, anyhow::Error> {
    let result = match database.set_value(
        set_command.key,
        set_command.value,
        set_command.get_old_value,
        set_command.overwrite,
        set_command.expires,
    ) {
        Ok(v) => v,
        Err(e) => encoding::error_string(&e.to_string()),
    };

    let response = result.as_bytes().to_vec();
    let response = vec![response];

    Ok(response)
}

pub fn delete_keys(
    database: &data::Database,
    keys: Vec<String>,
) -> Result<Vec<Vec<u8>>, anyhow::Error> {
    let count = database.remove_multiple(keys);

    let response = encoding::encode_integer(count as i64).as_bytes().to_vec();
    let response = vec![response];

    Ok(response)
}

pub fn update_expiration(
    database: &data::Database,
    key: String,
    expiration: CommandExpiration,
) -> Result<Vec<Vec<u8>>, anyhow::Error> {
    let response = match database.update_expiration(&key, expiration) {
        Ok(v) => v,
        Err(e) => encoding::error_string(&e.to_string()),
    };
    let response = response.as_bytes().to_vec();
    let response = vec![response];

    Ok(response)
}

pub fn get_delete_key(
    database: &data::Database,
    key: String,
) -> Result<Vec<Vec<u8>>, anyhow::Error> {
    let response = match database.get_remove(&key) {
        Ok(v) => match v {
            Some(v) => v,
            None => encoding::empty_string(),
        },
        Err(e) => encoding::error_string(&e.to_string()),
    }
    .as_bytes()
    .to_vec();

    let response = vec![response];

    Ok(response)
}

pub async fn transmit_wait(
    server: &server::RedisServer,
    num_replicas: usize,
    timeout: u64,
) -> Result<Vec<Vec<u8>>, anyhow::Error> {
    let num_respondents = server.perform_wait(num_replicas, timeout).await?;
    let response = encoding::encode_integer(num_respondents as i64)
        .as_bytes()
        .to_vec();
    let response = vec![response];

    Ok(response)
}

pub async fn view_config(
    server: &server::RedisServer,
    config_command: request::ConfigCommand,
) -> Result<Vec<Vec<u8>>, anyhow::Error> {
    let read = server.read().await;
    let (key, val) = match config_command {
        request::ConfigCommand::Get(key) => {
            let config_option = match &key {
                request::ConfigKey::Dir => read.config.dir.clone(),
                request::ConfigKey::Dbfilename => read.config.db_file_name.clone(),
            }
            // TODO: Add a proper fallback/
            .unwrap_or_else(|| encoding::error_string("ERR Unable to get config"));

            (key.to_string(), config_option)
        }
    };

    let response = encoding::encode_string_array(&[&key, &val])
        .as_bytes()
        .to_vec();
    let response = vec![response];
    Ok(response)
}

pub fn get_keys(
    database: &data::Database,
    _key_group: String,
) -> Result<Vec<Vec<u8>>, anyhow::Error> {
    // TODO: Handle empty key group
    let keys = database.keys()?;
    let keys: Vec<&str> = keys.iter().map(|k| k.as_str()).collect();
    let response = encoding::encode_string_array(keys.as_slice())
        .as_bytes()
        .to_vec();

    let responses = vec![response];
    Ok(responses)
}

pub fn get_type(database: &data::Database, key: String) -> Result<Vec<Vec<u8>>, anyhow::Error> {
    let value = database.get_type(&key);
    let response = match value {
        Some(data_type) => data_type,
        None => encoding::bulk_string("none"),
    }
    .as_bytes()
    .to_vec();

    let responses = vec![response];
    Ok(responses)
}

pub fn add_stream(
    database: &data::Database,
    command: XAddCommand,
    sender: Sender<transmission::Transmission>,
) -> Result<Vec<Vec<u8>>, anyhow::Error> {
    let response = match database.add_stream(command, sender) {
        Err(e) => encoding::error_string(&e.to_string()),
        Ok(stream_id) => encoding::bulk_string(&stream_id),
    }
    .as_bytes()
    .to_vec();

    let responses = vec![response];

    Ok(responses)
}

pub fn get_stream_range(
    database: &data::Database,
    command: XRangeCommand,
) -> Result<Vec<Vec<u8>>, anyhow::Error> {
    let response = database
        .read_from_stream(command.key, command.start, command.end)?
        .as_bytes()
        .to_vec();

    let responses = vec![response];
    Ok(responses)
}

pub async fn read_streams(
    database: &data::Database,
    command: XReadCommand,
    receiver: Receiver<transmission::Transmission>,
) -> Result<Vec<Vec<u8>>, anyhow::Error> {
    let response = database
        .read_from_streams(command.block, command.streams, receiver)
        .await?
        .as_bytes()
        .to_vec();

    let responses = vec![response];
    Ok(responses)
}

pub fn increment_value_by_int(
    database: &data::Database,
    key: String,
    adjustment: i64,
) -> Result<Vec<Vec<u8>>, anyhow::Error> {
    let response = match database.adjust_value_by_int(&key, adjustment) {
        Ok(value) => value,
        Err(e) => encoding::error_string(&e.to_string()),
    }
    .as_bytes()
    .to_vec();

    let responses = vec![response];
    Ok(responses)
}

pub fn increment_value_by_float(
    database: &data::Database,
    key: String,
    adjustment: f64,
) -> Result<Vec<Vec<u8>>, anyhow::Error> {
    let response = match database.adjust_value_by_float(&key, adjustment) {
        Ok(value) => value,
        Err(e) => encoding::error_string(&e.to_string()),
    }
    .as_bytes()
    .to_vec();

    let responses = vec![response];
    Ok(responses)
}
