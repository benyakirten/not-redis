use std::fmt::Display;
use std::time::Duration;

use anyhow::Context;

use crate::{data::RedisStreamItem, errors::not_an_integer, utils::current_unix_timestamp};

#[derive(Debug)]
pub struct SetCommand {
    pub key: String,
    pub value: String,
    pub get_old_value: bool,
    pub overwrite: SetOverride,
    pub expires: CommandExpiration,
}

#[derive(Debug)]
pub enum SetOverride {
    Normal,
    NeverOverwrite,
    OnlyOverwrite,
}

#[derive(Debug)]
pub enum CommandExpiration {
    None,
    Expiry(Duration),
    Other,
}

#[derive(Debug)]
pub enum Command {
    Ping(Option<String>),
    Echo(String),
    Set(SetCommand),
    Get(String),
    GetDel(String),
    GetEx(String, CommandExpiration),
    Del(Vec<String>),
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
    Incr(String),
    IncrBy(String, i64),
    IncrByFloat(String, f64),
    Decr(String),
    DecrBy(String, i64),
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
            "getdel" => parse_get_delete(body),
            "getex" => parse_getex(body),
            "del" => parse_delete(body),
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
            "incr" => parse_increment(body),
            "incrby" => parse_increment_by(body),
            "incrbyfloat" => parse_increment_by_float(body),
            "decr" => parse_decrement(body),
            "decrby" => parse_decrement_by(body),
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
        _ => anyhow::bail!("usage ping [message]"),
    };

    Ok(Command::Ping(ping_message))
}

fn parse_echo(body: Vec<String>) -> Result<Command, anyhow::Error> {
    let echo_message = match body {
        msg if msg.is_empty() => anyhow::bail!("usage echo message"),
        msg if msg.len() == 1 => msg[0].clone(),
        _ => anyhow::bail!("usage echo message"),
    };

    Ok(Command::Echo(echo_message))
}

fn parse_set(body: Vec<String>) -> Result<Command, anyhow::Error> {
    let mut body_iter = body.iter();

    let set_explanation = "SET key value [NX | XX] [GET] [EX seconds | PX milliseconds |
  EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]";
    let key = body_iter
        .next()
        .ok_or_else(|| anyhow::anyhow!("Missing key: {}", set_explanation))?
        .clone();
    let value = body_iter
        .next()
        .ok_or_else(|| anyhow::anyhow!("Missing value: {}", set_explanation))?
        .clone();

    let mut overwrite = SetOverride::Normal;
    let mut get_old_value = false;
    let mut expires = CommandExpiration::None;

    while let Some(item) = body_iter.next() {
        match item.to_ascii_lowercase().as_str() {
            "xx" => overwrite = SetOverride::OnlyOverwrite,
            "nx" => overwrite = SetOverride::NeverOverwrite,
            "get" => get_old_value = true,
            "ex" => {
                let amount = body_iter
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("Missing EX seconds: {}", set_explanation))?;
                let duration = parse_expiry(amount, 1000)?;
                expires = CommandExpiration::Expiry(duration);
            }
            "px" => {
                let amount = body_iter.next().ok_or_else(|| {
                    anyhow::anyhow!("Missing PX milliseconds: {}", set_explanation)
                })?;
                let duration = parse_expiry(amount, 1)?;
                expires = CommandExpiration::Expiry(duration);
            }
            "exat" => {
                let time = body_iter.next().ok_or_else(|| {
                    anyhow::anyhow!("Missing UNIX timestamp (in seconds): {}", set_explanation)
                })?;
                let duration = parse_expiry_at(time, 1000)?;
                expires = CommandExpiration::Expiry(duration);
            }
            "pxat" => {
                let time = body_iter.next().ok_or_else(|| {
                    anyhow::anyhow!(
                        "Missing UNIX timestamp (in milliseconds): {}",
                        set_explanation
                    )
                })?;
                let duration = parse_expiry_at(time, 1)?;
                expires = CommandExpiration::Expiry(duration);
            }
            "keepttl" => expires = CommandExpiration::Other,
            _ => anyhow::bail!("unknown option: {}", item),
        }
    }

    let command = SetCommand {
        key,
        value,
        get_old_value,
        overwrite,
        expires,
    };

    Ok(Command::Set(command))
}

fn parse_expiry(amount: &str, multiplier: u64) -> Result<Duration, anyhow::Error> {
    let amount = str::parse::<u64>(amount)
        .context("Parsing amount into number")
        .map_err(|_| not_an_integer())?;
    Ok(Duration::from_millis(amount * multiplier))
}

fn parse_expiry_at(time: &str, multiplier: u64) -> Result<Duration, anyhow::Error> {
    let time = str::parse::<u64>(time)
        .map_err(|_| not_an_integer())?
        .checked_mul(multiplier)
        .ok_or_else(|| anyhow::anyhow!("ERR time is too large"))?;
    let current_timestamp = current_unix_timestamp()? as u64;

    let duration = time
        .checked_sub(current_timestamp)
        .ok_or_else(|| anyhow::anyhow!("ERR time is in the past"))?;

    let duration = Duration::from_millis(duration);

    Ok(duration)
}

fn parse_get(body: Vec<String>) -> Result<Command, anyhow::Error> {
    let key = body
        .first()
        .ok_or_else(|| anyhow::anyhow!("ERR missing key for GET command"))?
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
        .ok_or_else(|| anyhow::anyhow!("ERR config must specify a command"))?;

    if get_cmd.to_ascii_lowercase() != "get" {
        anyhow::bail!("ERR only get commands supported for config for now");
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
        anyhow::bail!(
            "ERR Only * command supported for keys, received {}",
            key_group
        )
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
        None => anyhow::bail!("ERR Stream ID format not recognized"),
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
                .ok_or_else(|| anyhow::anyhow!("ERR Expected block length after block option"))??;
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
        anyhow::bail!("ERR Expected at least one stream");
    }

    let mut streams: Vec<XReadCommandStream> = Vec::with_capacity(num_streams);
    for i in 0..num_streams {
        let key_index = i + starting_index;
        let stream_start_index = i + starting_index + num_streams;

        let key = body
            .get(key_index)
            .ok_or_else(|| anyhow::anyhow!("ERR Unable to read stream key at index {}", key_index))?
            .to_string();

        let stream_start = body.get(stream_start_index).ok_or_else(|| {
            anyhow::anyhow!(
                "ERR Unable to read stream start at index {}",
                stream_start_index
            )
        })?;

        let stream_start = if stream_start.len() == 1 && stream_start.starts_with('$') {
            if block.is_none() {
                anyhow::bail!("ERR $ entry ID can only be used in blocking commands")
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

fn parse_increment(body: Vec<String>) -> Result<Command, anyhow::Error> {
    let key = body
        .first()
        .ok_or_else(|| anyhow::anyhow!("usage incr <key>"))?
        .to_string();

    Ok(Command::Incr(key))
}

fn parse_increment_by(body: Vec<String>) -> Result<Command, anyhow::Error> {
    let key = body
        .first()
        .ok_or_else(|| anyhow::anyhow!("usage incrby <key> <increment>"))?
        .to_string();

    let increment = body
        .get(1)
        .ok_or_else(|| anyhow::anyhow!("usage incrby <key> <increment>"))?;
    let increment = str::parse::<i64>(increment)
        .map_err(|e| not_an_integer().context(format!("Unable to parse as u64: {}", e)))?;

    Ok(Command::IncrBy(key, increment))
}

fn parse_increment_by_float(body: Vec<String>) -> Result<Command, anyhow::Error> {
    let key = body
        .first()
        .ok_or_else(|| anyhow::anyhow!("usage incrbyfloat <key> <increment"))?
        .to_string();

    let increment = body
        .get(1)
        .ok_or_else(|| anyhow::anyhow!("usage incrby <key> <increment>"))?;

    let increment = str::parse::<f64>(increment).map_err(|e| {
        anyhow::anyhow!("ERR value is not a valid float")
            .context(format!("Unable to parse as u64: {}", e))
    })?;

    Ok(Command::IncrByFloat(key, increment))
}

fn parse_decrement(body: Vec<String>) -> Result<Command, anyhow::Error> {
    let key = body
        .first()
        .ok_or_else(|| anyhow::anyhow!("usage decr <key>"))?
        .to_string();

    Ok(Command::Decr(key))
}

fn parse_decrement_by(body: Vec<String>) -> Result<Command, anyhow::Error> {
    let key = body
        .first()
        .ok_or_else(|| anyhow::anyhow!("usage incrby <key> <decrement>"))?
        .to_string();

    let decrement = body
        .get(1)
        .ok_or_else(|| anyhow::anyhow!("usage incrby <key> <decrement>"))?;
    let decrement = str::parse::<i64>(decrement)
        .map_err(|e| not_an_integer().context(format!("Unable to parse as u64: {}", e)))?;

    Ok(Command::DecrBy(key, decrement))
}

fn parse_delete(body: Vec<String>) -> Result<Command, anyhow::Error> {
    if body.is_empty() {
        anyhow::bail!("usage del <key> [key ...]")
    }

    let keys = body.iter().map(|k| k.to_string()).collect();

    Ok(Command::Del(keys))
}

fn parse_get_delete(body: Vec<String>) -> Result<Command, anyhow::Error> {
    let key = body
        .first()
        .ok_or_else(|| anyhow::anyhow!("usage getdel <key>"))?
        .to_string();

    Ok(Command::GetDel(key))
}

fn parse_getex(body: Vec<String>) -> Result<Command, anyhow::Error> {
    let mut body_iter = body.iter();

    let getex_instructions = "GETEX key [EX seconds | PX milliseconds | EXAT unix-time-seconds |
      PXAT unix-time-milliseconds | PERSIST]";

    let key = body_iter
        .next()
        .ok_or_else(|| anyhow::anyhow!("ERR Missing key: {}", getex_instructions))?
        .to_string();

    let mut expires: CommandExpiration = CommandExpiration::None;

    if let Some(item) = body_iter.next() {
        match item.to_ascii_lowercase().as_str() {
            "ex" => {
                let amount = body_iter
                    .next()
                    .ok_or_else(|| invalid_expire_time("getex"))?;
                let duration = parse_expiry(amount, 1000)?;
                expires = CommandExpiration::Expiry(duration);
            }
            "px" => {
                let amount = body_iter
                    .next()
                    .ok_or_else(|| invalid_expire_time("getex"))?;
                let duration = parse_expiry(amount, 1)?;
                expires = CommandExpiration::Expiry(duration);
            }
            "exat" => {
                let time = body_iter
                    .next()
                    .ok_or_else(|| invalid_expire_time("getex"))?;
                let duration = parse_expiry_at(time, 1000)?;
                expires = CommandExpiration::Expiry(duration);
            }
            "pxat" => {
                let time = body_iter
                    .next()
                    .ok_or_else(|| invalid_expire_time("getex"))?;
                let duration = parse_expiry_at(time, 1)?;
                expires = CommandExpiration::Expiry(duration);
            }
            "persist" => expires = CommandExpiration::Other,
            other => anyhow::bail!("unknown option: {}", other),
        }
    }

    let command = Command::GetEx(key, expires);
    Ok(command)
}

pub fn invalid_expire_time(command: &str) -> anyhow::Error {
    anyhow::anyhow!("ERR invalid expire time in '{}' command", command)
}
