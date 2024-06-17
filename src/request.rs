use std::{fmt::Display, time::Duration};

use anyhow::Context;

use crate::data::{DatabaseItem, RedisStreamItem};

#[derive(Debug)]
pub enum Command {
    Ping(Option<String>),
    Echo(String),
    Set(String, DatabaseItem),
    Get(String),
    Info,
    ReplConf(ReplicationCommand),
    Psync(String, PsyncOffset),
    Wait(usize, u64),
    Config(ConfigCommand),
    Keys(String),
    Type(String),
    Xadd(XAddCommand),
    Xrange(XRangeCommand),
    Xread(XReadCommand),
}

#[derive(Debug)]
pub struct XReadCommand {
    pub streams: Vec<XReadCommandStream>,
    pub block: Option<XReadBlock>,
}

#[derive(Debug)]
pub enum XReadBlock {
    Limited(u64),
    Unlimited,
}

#[derive(Debug)]
pub struct XReadCommandStream {
    pub key: String,
    pub start: XReadNumber,
}

#[derive(Debug)]
pub enum XReadNumber {
    AllNewEntries,
    Specified(u128, usize),
}

// Add count
#[derive(Debug)]
pub struct XRangeCommand {
    pub key: String,
    pub start: XRangeNumber,
    pub end: XRangeNumber,
}

#[derive(Debug)]
pub enum XRangeNumber {
    Unspecified,
    Specified(u128, usize),
}

#[derive(Debug)]
pub enum XAddNumber {
    Autogenerate,
    Predetermined(usize),
}

#[derive(Debug)]
pub struct XAddCommand {
    pub stream_key: String,
    pub ms_time: XAddNumber,
    pub sequence_number: XAddNumber,
    pub data: Vec<RedisStreamItem>,
}

#[derive(Debug)]
pub enum ConfigCommand {
    Get(ConfigKey),
}

#[derive(Debug, PartialEq)]
pub enum ConfigKey {
    Dir,
    Dbfilename,
}

impl Display for ConfigKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Dir => write!(f, "dir"),
            Self::Dbfilename => write!(f, "dbfilename"),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum ReplicationCommand {
    ListeningPort(u16),
    Capabilities,
    Ack,
}

#[derive(Debug)]
pub enum PsyncOffset {
    None,
    Offset(u16),
}

impl Command {
    pub fn new(route: &str, body: Vec<String>) -> Result<Self, anyhow::Error> {
        match route.to_ascii_lowercase().as_str() {
            "ping" => parse_ping(body),
            "echo" => parse_echo(body),
            "set" => parse_set(body),
            "get" => parse_get(body),
            "info" => parse_info(body),
            "replconf" => parse_replconf(body),
            "psync" => parse_psync(body),
            "wait" => parse_wait(body),
            "config" => parse_config(body),
            "keys" => parse_keys(body),
            "type" => parse_type(body),
            "xadd" => parse_xadd(body),
            "xrange" => parse_xrange(body),
            "xread" => parse_xread(body),
            _ => anyhow::bail!("unknown command: {}", route),
        }
    }
}

pub fn parse_request(raw_request: Vec<String>) -> Result<Command, anyhow::Error> {
    let command_type = raw_request
        .get(2)
        .ok_or_else(|| anyhow::anyhow!("missing route"))?;

    // Skip the route - then skip the size for every part
    let body = raw_request.iter().step_by(2).skip(2).cloned().collect();

    Command::new(command_type, body)
}

fn parse_ping(body: Vec<String>) -> Result<Command, anyhow::Error> {
    let ping_message = match body {
        msg if msg.is_empty() => None,
        msg if msg.len() == 1 => Some(msg[0].clone()),
        _ => anyhow::bail!("too many arguments for PING command"),
    };

    Ok(Command::Ping(ping_message))
}

fn parse_echo(body: Vec<String>) -> Result<Command, anyhow::Error> {
    let echo_message = match body {
        msg if msg.is_empty() => anyhow::bail!("missing body for ECHO command"),
        msg if msg.len() == 1 => msg[0].clone(),
        _ => anyhow::bail!("too many arguments for ECHO command"),
    };

    Ok(Command::Echo(echo_message))
}

fn parse_set(body: Vec<String>) -> Result<Command, anyhow::Error> {
    let key = body
        .first()
        .ok_or_else(|| anyhow::anyhow!("missing key for SET command"))?
        .clone();

    let value = body
        .get(1)
        .ok_or_else(|| anyhow::anyhow!("missing value for SET command"))?
        .clone();

    let duration = parse_expiry(body)?;

    let item = DatabaseItem::new(value, duration);

    Ok(Command::Set(key, item))
}

/// TODO: Add all set commands
/// See: https://redis.io/commands/set/
fn parse_expiry(body: Vec<String>) -> Result<Option<Duration>, anyhow::Error> {
    let px = body.get(2);
    if px.is_none() || px.unwrap().to_ascii_lowercase().as_str() != "px" {
        return Ok(None);
    }

    let expiry = body.get(3);

    if expiry.is_none() {
        anyhow::bail!("missing expiry for SET command")
    }

    let duration = str::parse(expiry.unwrap()).context("Parsing expiry into number")?;
    let duration = Duration::from_millis(duration);

    Ok(Some(duration))
}

fn parse_get(body: Vec<String>) -> Result<Command, anyhow::Error> {
    let key = body
        .first()
        .ok_or_else(|| anyhow::anyhow!("missing key for GET command"))?
        .clone();

    Ok(Command::Get(key))
}

fn parse_info(body: Vec<String>) -> Result<Command, anyhow::Error> {
    if body.len() != 1 {
        anyhow::bail!("usage info replication")
    }

    Ok(Command::Info)
}

fn parse_replconf(body: Vec<String>) -> Result<Command, anyhow::Error> {
    if body.len() != 2 {
        anyhow::bail!("usage REPLCONF [listening-port <port>] | [capa psync2]")
    }

    let subcommand = body.first().unwrap();
    match subcommand.to_ascii_lowercase().as_str() {
        "listening-port" => {
            let port: u16 = str::parse(body.get(1).unwrap()).context("Parsing port into number")?;
            Ok(Command::ReplConf(ReplicationCommand::ListeningPort(port)))
        }
        "capa" => {
            if body.get(1).unwrap() != "psync2" {
                anyhow::bail!("capa command must be followed by psync2");
            }
            Ok(Command::ReplConf(ReplicationCommand::Capabilities))
        }
        "getack" => {
            if body.get(1).unwrap() != "*" {
                anyhow::bail!("gatack command must be followed by wildcard *");
            }
            Ok(Command::ReplConf(ReplicationCommand::Ack))
        }
        _ => anyhow::bail!("unknown subcommand: {}", subcommand),
    }
}

fn parse_psync(body: Vec<String>) -> Result<Command, anyhow::Error> {
    if body.len() != 2 {
        anyhow::bail!("usage resync <replication-id> <offset>")
    }

    let replication_id = body.first().unwrap().to_string();
    let offset = match body.get(1).unwrap().as_str() {
        "-1" => PsyncOffset::None,
        offset => {
            let offset = str::parse(offset).context("Parsing offset into number")?;
            PsyncOffset::Offset(offset)
        }
    };

    Ok(Command::Psync(replication_id, offset))
}
fn parse_wait(body: Vec<String>) -> Result<Command, anyhow::Error> {
    let num_replicas = body
        .first()
        .ok_or_else(|| anyhow::anyhow!("usage wait <numreplicas> <timeout>"))?;

    let timeout = body
        .get(1)
        .ok_or_else(|| anyhow::anyhow!("usage wait <numreplicas> <timeout>"))?;

    let num_replicas: usize = str::parse(num_replicas)?;
    let timeout: u64 = str::parse(timeout)?;

    let command = Command::Wait(num_replicas, timeout);
    Ok(command)
}

fn parse_config(body: Vec<String>) -> Result<Command, anyhow::Error> {
    let get_cmd = body
        .first()
        .ok_or_else(|| anyhow::anyhow!("config must specify a command"))?;

    if get_cmd.to_ascii_lowercase() != "get" {
        anyhow::bail!("only get commands supported for config for now");
    }

    let get_option = body
        .get(1)
        .ok_or_else(|| anyhow::anyhow!("command must specify key"))?;

    let key = match get_option.to_ascii_lowercase().as_str() {
        "dir" => ConfigKey::Dir,
        "dbfilename" => ConfigKey::Dbfilename,
        _ => anyhow::bail!("supported keys are dir and dbfilename"),
    };

    let config_command = ConfigCommand::Get(key);
    let command = Command::Config(config_command);
    Ok(command)
}

fn parse_keys(body: Vec<String>) -> Result<Command, anyhow::Error> {
    // TODO: Add handling for searching
    // TODO: Add better error handling
    let key_group = body
        .first()
        .ok_or_else(|| anyhow::anyhow!("keys must specify a command"))?;

    if key_group != "*" {
        anyhow::bail!("Only * command supported for keys, received {}", key_group)
    }

    let command = Command::Keys(key_group.to_string());
    Ok(command)
}

fn parse_type(body: Vec<String>) -> Result<Command, anyhow::Error> {
    let key = body
        .first()
        .ok_or_else(|| anyhow::anyhow!("usage type <key>"))?;

    let command = Command::Type(key.to_string());
    Ok(command)
}

fn parse_xadd(body: Vec<String>) -> Result<Command, anyhow::Error> {
    let stream_key = body
        .first()
        .ok_or_else(|| anyhow::anyhow!("usage xadd <key> [entry_id] [...key value]"))?
        .to_string();

    let stream_id = body.get(1);
    let stream_id = get_stream_id(stream_id);

    let (ms_time, sequence_number) = match stream_id {
        None => anyhow::bail!("Stream ID format not recognized"),
        Some(stream_id) => stream_id,
    };

    let args = body[2..].chunks(2);
    let mut items: Vec<RedisStreamItem> = Vec::with_capacity(args.len());

    for data in args {
        let k = data[0].to_string();
        let v = data[1].to_string();

        let item = RedisStreamItem::new(k, v);
        items.push(item)
    }

    let command = XAddCommand {
        stream_key,
        data: items,
        ms_time,
        sequence_number,
    };
    let command = Command::Xadd(command);
    Ok(command)
}

fn get_stream_id(stream_id: Option<&String>) -> Option<(XAddNumber, XAddNumber)> {
    let stream_id = match stream_id {
        Some(stream_id) => stream_id,
        None => return None,
    };

    if stream_id == "*" {
        return Some((XAddNumber::Autogenerate, XAddNumber::Autogenerate));
    }

    let (time_part, sequence_number) = match stream_id.split_once('-') {
        Some((seq, time)) => (seq, time),
        None => return None,
    };

    let time_part = match str::parse::<usize>(time_part) {
        Ok(val) => XAddNumber::Predetermined(val),
        Err(_) => return None,
    };

    let sequence_number = match sequence_number {
        "*" => XAddNumber::Autogenerate,
        val => match str::parse::<usize>(val) {
            Ok(val) => XAddNumber::Predetermined(val),
            Err(_) => return None,
        },
    };

    Some((time_part, sequence_number))
}

fn parse_xrange(body: Vec<String>) -> Result<Command, anyhow::Error> {
    let key = body
        .first()
        .ok_or_else(|| anyhow::anyhow!("usage xrange <key> <start> <end>"))?
        .to_string();

    let start = body
        .get(1)
        .ok_or_else(|| anyhow::anyhow!("usage xrange <key> <start> <end>"))?;
    let start = if start.len() == 1 && start.starts_with('-') {
        XRangeNumber::Unspecified
    } else {
        let (ms_time, sequence_number) = parse_xadd_specified_number(start)?;
        XRangeNumber::Specified(ms_time, sequence_number)
    };

    let end = body
        .get(2)
        .ok_or_else(|| anyhow::anyhow!("usage xrange <key> <start> <end>"))?;
    let end = if end.len() == 1 && end.starts_with('+') {
        XRangeNumber::Unspecified
    } else {
        let (ms_time, sequence_number) = parse_xadd_specified_number(end)?;
        XRangeNumber::Specified(ms_time, sequence_number)
    };

    let command = XRangeCommand { key, start, end };
    let command = Command::Xrange(command);
    Ok(command)
}

fn parse_xadd_specified_number(nums: &str) -> Result<(u128, usize), anyhow::Error> {
    let (ms_time, sequence_number) = match nums.split_once('-') {
        Some((ms_time, sequence_number)) => {
            let ms_time = str::parse::<u128>(ms_time)?;
            let sequence_number = str::parse::<usize>(sequence_number)?;
            (ms_time, sequence_number)
        }
        None => {
            let ms_time = str::parse::<u128>(nums)?;
            let sequence_number = 0;
            (ms_time, sequence_number)
        }
    };

    Ok((ms_time, sequence_number))
}

fn parse_xread(body: Vec<String>) -> Result<Command, anyhow::Error> {
    let block_index = body.iter().position(|cmd| cmd.to_lowercase() == "block");
    let block_len = match block_index {
        None => None,
        Some(block_idx) => {
            let block_amt = body
                .get(block_idx + 1)
                .map(|v| str::parse::<u64>(v))
                .ok_or_else(|| anyhow::anyhow!("Expected block length after block option"))??;
            Some(block_amt)
        }
    };
    let block = match block_len {
        None => None,
        Some(0) => Some(XReadBlock::Unlimited),
        Some(n) => Some(XReadBlock::Limited(n)),
    };

    let starting_index = body
        .iter()
        .position(|cmd| cmd.to_ascii_lowercase() == "streams")
        .ok_or_else(|| anyhow::anyhow!("usage streams ..<stream_key> ..<start>"))?
        + 1;

    let mut num_streams = 0;
    for (i, item) in body[starting_index..].iter().enumerate() {
        if parse_xadd_specified_number(item).is_ok() || (item.len() == 1 && item.starts_with('$')) {
            num_streams = i;
            break;
        }
    }

    if num_streams == 0 {
        anyhow::bail!("Expected at least one stream");
    }

    let mut streams: Vec<XReadCommandStream> = Vec::with_capacity(num_streams);
    for i in 0..num_streams {
        let key_index = i + starting_index;
        let stream_start_index = i + starting_index + num_streams;

        let key = body
            .get(key_index)
            .ok_or_else(|| anyhow::anyhow!("Unable to read stream key at index {}", key_index))?
            .to_string();

        let stream_start = body.get(stream_start_index).ok_or_else(|| {
            anyhow::anyhow!(
                "Unable to read stream start at index {}",
                stream_start_index
            )
        })?;

        let stream_start = if stream_start.len() == 1 && stream_start.starts_with('$') {
            if block.is_none() {
                anyhow::bail!("$ entry ID can only be used in blocking commands")
            }
            XReadNumber::AllNewEntries
        } else {
            let (ms_time, sequence_number) = parse_xadd_specified_number(stream_start)?;
            XReadNumber::Specified(ms_time, sequence_number)
        };

        let xread_command = XReadCommandStream {
            key,
            start: stream_start,
        };
        streams.push(xread_command);
    }

    let command = XReadCommand { streams, block };
    let command = Command::Xread(command);

    Ok(command)
}
