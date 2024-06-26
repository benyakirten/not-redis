use std::collections::HashMap;
use std::fs;
use std::io::{Cursor, Read};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use tokio::spawn;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout, Instant};

use crate::encoding::{empty_string, okay_string};
use crate::errors::{wrong_type, wrong_type_str};
use crate::request::{self, CommandExpiration, SetOverride};
use crate::utils::current_unix_timestamp;
use crate::{encoding, transmission, utils};

// https://rdb.fnordig.de/file_format.html
#[derive(PartialEq, Debug)]
enum OpCode {
    Eof,
    SelectDB,
    ExpireTime,
    ExpireTimeMS,
    ResizeDb,
    Aux,
    Other(u8),
}

impl OpCode {
    fn from_byte(byte: u8) -> Self {
        match byte {
            0xFF => OpCode::Eof,
            0xFE => OpCode::SelectDB,
            0xFD => OpCode::ExpireTime,
            0xFC => OpCode::ExpireTimeMS,
            0xFB => OpCode::ResizeDb,
            0xFA => OpCode::Aux,
            other => OpCode::Other(other),
        }
    }
}

#[allow(dead_code)]
enum AuxField {
    RedisVersion,
    RedisBits,
    CreationTime,
    MemoryUsed,
}

#[allow(dead_code)]
#[derive(PartialEq, Debug)]
enum ValueType {
    String = 0,
    List = 1,
    Set = 2,
    SortedSet = 3,
    Hash = 4,
    Zipmap = 9,
    Ziplist = 10,
    Intset = 11,
    SortedSetZiplist = 12,
    HashmapZiplist = 13,
    ListQuicklist = 14,
}

impl ValueType {
    fn from_byte(byte: u8) -> Result<Self, anyhow::Error> {
        let value_type = match byte {
            0 => Self::String,
            1 => Self::List,
            2 => Self::Set,
            3 => Self::SortedSet,
            4 => Self::Hash,
            9 => Self::Zipmap,
            10 => Self::Ziplist,
            11 => Self::Intset,
            12 => Self::SortedSetZiplist,
            13 => Self::HashmapZiplist,
            14 => Self::ListQuicklist,
            val => anyhow::bail!("Unrecognized value type: {}", val),
        };

        Ok(value_type)
    }
}

pub struct Database(Arc<RwLock<HashMap<String, DatabaseItem>>>);

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

impl Database {
    pub fn new() -> Self {
        // If we persist data to a database, we can fetch the data on initialization
        // Create a process that runs every so often to store hashmap data in a more permanent database
        Self(Arc::new(RwLock::new(HashMap::new())))
    }

    pub fn get(&self, key: &str) -> Result<Option<String>, anyhow::Error> {
        let database = self.0.read().unwrap();
        let item = database.get(key);

        let data = match item {
            Some(DatabaseItem::Stream(_)) => anyhow::bail!(wrong_type_str()),
            Some(DatabaseItem::String(redis_string)) => Some(redis_string.data.to_string()),
            None => None,
        };

        Ok(data)
    }

    pub fn get_type(&self, key: &str) -> Option<String> {
        let database = self.0.read().unwrap();
        database.get(key).map(|v| v.data_type())
    }

    pub fn set(&self, key: String, mut value: RedisString) -> Result<(), anyhow::Error> {
        let duration = value.duration;

        if let Some(dur) = duration {
            let database = self.clone();
            let key = key.to_string();
            let join_handle = spawn(async move {
                sleep(dur).await;
                database.remove(&key);
            });

            value.set_cancellation(join_handle);
        };

        let database_item = DatabaseItem::String(value);
        self.0
            .write()
            .map_err(|e| anyhow::anyhow!("{}", e))?
            .insert(key.to_string(), database_item);

        Ok(())
    }

    fn set_item(&self, key: String, item: DatabaseItem) -> Option<DatabaseItem> {
        self.0.write().unwrap().insert(key, item)
    }

    pub fn set_value(
        &self,
        key: String,
        value: String,
        return_old_value: bool,
        overwrites: SetOverride,
        expires: CommandExpiration,
    ) -> Result<String, anyhow::Error> {
        let mut db = self.0.write().map_err(|e| anyhow::anyhow!("{}", e))?;

        let item = db.get_mut(&key);
        let item = match item {
            Some(DatabaseItem::String(redis_string)) => Some(redis_string),
            None => None,
            _ => {
                if return_old_value {
                    anyhow::bail!(wrong_type_str())
                } else {
                    None
                }
            }
        };

        let return_data = if return_old_value {
            item.as_ref().map(|i| i.data()).unwrap_or_else(empty_string)
        } else {
            okay_string()
        };

        let duration = match expires {
            CommandExpiration::None => None,
            CommandExpiration::Other => {
                if let Some(i) = &item {
                    i.duration
                } else {
                    None
                }
            }
            CommandExpiration::Expiry(duration) => Some(duration),
        };

        match (overwrites, item.is_some()) {
            (SetOverride::Normal, _)
            | (SetOverride::OnlyOverwrite, true)
            | (SetOverride::NeverOverwrite, false) => {
                let mut value = RedisString::new(value, duration);
                // TODO: Cancel old cancellation process

                if let Some(item) = item {
                    item.abort_deletion_process();
                }

                if let Some(dur) = duration {
                    let database = self.clone();
                    let key_copy = key.clone();
                    let process = spawn(async move {
                        sleep(dur).await;
                        database.remove(&key_copy);
                    });

                    value.set_cancellation(process);
                };

                db.insert(key.to_string(), DatabaseItem::String(value));
            }
            (_, true) => {
                let item = item.unwrap();

                item.abort_deletion_process();

                if let Some(dur) = duration {
                    let database = self.clone();
                    let key_copy = key.clone();
                    let process = spawn(async move {
                        sleep(dur).await;
                        database.remove(&key_copy);
                    });

                    item.set_cancellation(process);
                };
            }
            _ => {}
        };

        Ok(return_data)
    }

    pub fn add_stream(
        &self,
        command: request::XAddCommand,
        sender: Sender<transmission::Transmission>,
    ) -> Result<String, anyhow::Error> {
        let mut database = self.0.write().unwrap();

        let ms_time = match command.ms_time {
            request::XAddNumber::Autogenerate => current_unix_timestamp()?,
            request::XAddNumber::Predetermined(val) => val as u128,
        };

        match database.get_mut(&command.stream_key) {
            None => {
                let sequence_number = match (command.sequence_number, ms_time) {
                    (request::XAddNumber::Autogenerate, 0) => 1,
                    (request::XAddNumber::Autogenerate, _) => 0,
                    (request::XAddNumber::Predetermined(val), _) => val,
                };

                if ms_time == 0 && sequence_number == 0 {
                    return Err(anyhow::anyhow!(
                        "ERR The ID specified in XADD must be greater than 0-0"
                    ));
                }

                let mut max_sequence_for_ms_time = HashMap::new();
                max_sequence_for_ms_time.insert(ms_time, sequence_number);

                let inner_redis_stream = InnerRedisStream {
                    items: command.data.clone(),
                    ms_time,
                    sequence_number,
                };

                broadcast_xadd(
                    &command.stream_key,
                    ms_time,
                    sequence_number,
                    command.data,
                    sender,
                )?;

                let stream_id = inner_redis_stream.stream_id();

                let redis_stream = RedisStream(vec![inner_redis_stream]);
                let item = DatabaseItem::Stream(redis_stream);
                database.insert(command.stream_key, item);

                Ok(stream_id)
            }
            Some(database_item) => match database_item {
                DatabaseItem::Stream(ref mut existing_stream) => {
                    let latest_inner = existing_stream.0.last().ok_or_else(|| {
                        anyhow::anyhow!("Streams should always have at lest one datapoint")
                    })?;

                    let sequence_number =
                        determine_sequence_number(command.sequence_number, ms_time, latest_inner);

                    if ms_time == 0 && sequence_number == 0 {
                        return Err(anyhow::anyhow!(
                            "ERR The ID specified in XADD must be greater than 0-0"
                        ));
                    }

                    // Either the millisecond time or the sequence number
                    // must be greater than the last entry.
                    let is_okay = match ms_time {
                        ms_time if ms_time < latest_inner.ms_time => false,
                        ms_time if ms_time == latest_inner.ms_time => {
                            sequence_number > latest_inner.sequence_number
                        }
                        _ => true,
                    };

                    if !is_okay {
                        return Err(anyhow::anyhow!(
                            "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                        ));
                    }

                    let inner_redis_stream = InnerRedisStream {
                        items: command.data.clone(),
                        ms_time,
                        sequence_number,
                    };

                    // We want to transmit all the original values of the item so we have to clone them.
                    // The alternative is to clone all items for the database in the streams before the block
                    // (if present) occurs. This way we only have to clone a few items.

                    broadcast_xadd(
                        &command.stream_key,
                        ms_time,
                        sequence_number,
                        command.data,
                        sender,
                    )?;

                    let stream_id = inner_redis_stream.stream_id();
                    existing_stream.0.push(inner_redis_stream);

                    Ok(stream_id)
                }
                _ => Err(wrong_type()),
            },
        }
    }

    pub fn read_from_stream(
        &self,
        key: String,
        start: request::XRangeNumber,
        end: request::XRangeNumber,
    ) -> Result<String, anyhow::Error> {
        let database = self.0.read().unwrap();
        let stream = match database.get(&key) {
            None => return Ok(empty_string()),
            Some(item) => match &item {
                DatabaseItem::String(_) => anyhow::bail!(wrong_type_str()),
                DatabaseItem::Stream(stream) => stream,
            },
        };

        let mut inner_streams: Vec<&InnerRedisStream> = vec![];
        let mut has_started: bool = false;

        // TODO: Refactor this not to be such a mess - maybe function calls
        for entry in stream.0.iter() {
            if !has_started {
                match start {
                    request::XRangeNumber::Unspecified => {
                        has_started = true;
                    }
                    request::XRangeNumber::Specified(ms_time, sequence_number) => match entry {
                        entry if entry.ms_time < ms_time => continue,
                        // If we are at the ms_time but not yet at the sequence number then ignore.
                        entry
                            if entry.ms_time == ms_time
                                && entry.sequence_number < sequence_number =>
                        {
                            continue
                        }
                        _ => {
                            has_started = true;
                        }
                    },
                }
            }

            match end {
                request::XRangeNumber::Unspecified => {}
                request::XRangeNumber::Specified(ms_time, sequence_number) => match entry {
                    entry if entry.ms_time > ms_time => break,
                    // Exceeding the sequence_number only matters if we are already at the
                    // end's ms_time.
                    entry
                        if entry.ms_time == ms_time && entry.sequence_number > sequence_number =>
                    {
                        break
                    }
                    _ => {}
                },
            }

            inner_streams.push(entry);
        }

        let encoded = encoding::encode_stream(inner_streams.as_slice());
        Ok(encoded)
    }

    pub async fn read_from_streams(
        &self,
        block: Option<request::XReadBlock>,
        read_command_streams: Vec<request::XReadCommandStream>,
        receiver: Receiver<transmission::Transmission>,
    ) -> Result<String, anyhow::Error> {
        match block {
            None => read_streams_sync(self, read_command_streams),
            Some(request::XReadBlock::Unlimited) => {
                read_streams_until_xadd(read_command_streams, receiver).await
            }
            Some(request::XReadBlock::Limited(wait)) => {
                read_streams_after_limited_wait(wait, read_command_streams, receiver).await
            }
        }
    }

    pub fn remove(&self, key: &str) -> bool {
        self.0.write().unwrap().remove(key).is_none()
    }

    pub fn update_expiration(
        &self,
        key: &str,
        expiration: CommandExpiration,
    ) -> Result<String, anyhow::Error> {
        let mut db = self.0.write().map_err(|e| anyhow::anyhow!("{}", e))?;

        if let Some(item) = db.get_mut(key) {
            match item {
                DatabaseItem::String(item) => {
                    let data = item.data();

                    item.abort_deletion_process();

                    let duration = match expiration {
                        CommandExpiration::None => None,
                        CommandExpiration::Other => None,
                        CommandExpiration::Expiry(duration) => Some(duration),
                    };

                    if let Some(duration) = duration {
                        let database = self.clone();
                        let key = key.to_string();
                        let join_handle = spawn(async move {
                            sleep(duration).await;
                            database.remove(&key);
                        });

                        item.set_cancellation(join_handle);
                    }

                    Ok(data)
                }
                _ => anyhow::bail!(wrong_type_str()),
            }
        } else {
            Ok(empty_string())
        }
    }

    pub fn get_remove(&self, key: &str) -> Result<Option<String>, anyhow::Error> {
        let mut db = self.0.write().map_err(|e| anyhow::anyhow!("{}", e))?;

        if let Some(item) = db.get_mut(key) {
            match item {
                DatabaseItem::String(item) => {
                    let data = item.data();

                    item.abort_deletion_process();

                    db.remove(key);
                    Ok(Some(data))
                }
                _ => anyhow::bail!(wrong_type_str()),
            }
        } else {
            Ok(None)
        }
    }

    pub fn remove_multiple(&self, keys: Vec<String>) -> usize {
        let mut db = self.0.write().unwrap();
        keys.iter().fold(0, |acc, key| {
            if let Some(item) = db.get_mut(key) {
                item.clean_up();
                db.remove(key);
                acc + 1
            } else {
                acc
            }
        })
    }

    pub fn adjust_value_by_int(&self, key: &str, adjustment: i64) -> Result<String, anyhow::Error> {
        let mut db = self.0.write().unwrap();
        let value = match db.get_mut(key) {
            Some(item) => match item {
                DatabaseItem::String(redis_string) => {
                    let value = if redis_string.data.find('.').is_some() {
                        adjust_float_value_by_int(&redis_string.data, adjustment)
                    } else {
                        adjust_int_value_by_int(&redis_string.data, adjustment)
                    }?;

                    redis_string.data.clone_from(&value);

                    Ok(value)
                }
                _ => Err(anyhow::anyhow!(wrong_type_str())
                    .context(format!("Item at key {} is not a string", key))),
            },
            None => {
                let data = RedisString::new(adjustment.to_string(), None);
                db.insert(key.to_string(), DatabaseItem::String(data));
                Ok(adjustment.to_string())
            }
        }?;

        let encoded = if value.find('.').is_some() {
            encoding::bulk_string(&value)
        } else {
            encoding::encode_integer(value.parse::<i64>().unwrap())
        };
        Ok(encoded)
    }

    pub fn adjust_value_by_float(
        &self,
        key: &str,
        adjustment: f64,
    ) -> Result<String, anyhow::Error> {
        let mut db = self.0.write().unwrap();
        let value = match db.get_mut(key) {
            Some(item) => match item {
                DatabaseItem::String(redis_string) => {
                    let value = if redis_string.data.find('.').is_some() {
                        adjust_float_value_by_float(&redis_string.data, adjustment)
                    } else {
                        adjust_int_value_by_float(&redis_string.data, adjustment)
                    }?;

                    redis_string.data.clone_from(&value);

                    Ok(value)
                }
                _ => Err(anyhow::anyhow!(wrong_type_str())
                    .context(format!("Item at key {} is not a string", key))),
            },
            None => {
                let redis_string = RedisString::new(adjustment.to_string(), None);
                db.insert(key.to_string(), DatabaseItem::String(redis_string));
                Ok(adjustment.to_string())
            }
        }?;

        Ok(encoding::bulk_string(&value))
    }

    pub fn keys(&self) -> Result<Vec<String>, anyhow::Error> {
        // TODO: Figure out how to do this without cloning the keys
        let keys = {
            let lock = self.0.read().map_err(|e| anyhow::anyhow!("{}", e))?;
            lock.keys().map(|k| k.to_string()).collect()
        };

        Ok(keys)
    }

    pub fn from_config(path: PathBuf) -> Result<Self, anyhow::Error> {
        let database = Database::new();
        if !path.exists() {
            return Ok(database);
        }

        let contents = fs::read(path).context("Reading RDB file")?;
        let mut cursor = Cursor::new(contents);

        let mut magic_string: [u8; 5] = [0; 5];
        cursor
            .read_exact(&mut magic_string)
            .context("Reading magic string")?;
        let magic_string =
            String::from_utf8(magic_string.to_vec()).context("Parsing magic string into utf8")?;
        if &magic_string != "REDIS" {
            anyhow::bail!(
                "Expected REDIS magic string at beginning of RDB file, got {}",
                magic_string
            );
        }

        let mut version_number: [u8; 4] = [0; 4];
        cursor
            .read_exact(&mut version_number)
            .context("Reading version number")?;
        let version_number = String::from_utf8(version_number.to_vec())
            .context("Parsing verison number into utf8")?;
        str::parse::<usize>(&version_number)
            .context("Version number cannot be parsed as an integer")?;

        loop {
            let op_code = utils::read_next_byte(&mut cursor)?;
            match OpCode::from_byte(op_code) {
                OpCode::Aux => parse_aux(&mut cursor)?,
                OpCode::SelectDB => parse_select_db(&mut cursor)?,
                OpCode::ResizeDb => parse_resize_db(&mut cursor)?,
                OpCode::ExpireTimeMS => {
                    let database_item = parse_expire_time_ms(&mut cursor)?;
                    if let Some((key, value)) = database_item {
                        database.set_item(key, value);
                    }
                }
                OpCode::ExpireTime => {
                    let database_item = parse_expire_time_sec(&mut cursor)?;
                    if let Some((key, value)) = database_item {
                        database.set_item(key, value);
                    }
                }
                OpCode::Other(value_type_byte) => {
                    let value_type = ValueType::from_byte(value_type_byte)?;
                    let (key, value) = read_key_value_pair(value_type, None, &mut cursor)?;
                    database.set_item(key, value);
                }
                OpCode::Eof => break,
            }
        }

        Ok(database)
    }
}

impl Clone for Database {
    fn clone(&self) -> Self {
        Database(self.0.clone())
    }
}

#[derive(Debug)]
pub struct RedisString {
    data: String,
    duration: Option<Duration>,
    cancellation_process: Option<JoinHandle<()>>,
}

impl RedisString {
    pub fn new(data: String, duration: Option<Duration>) -> Self {
        Self {
            data,
            duration,
            cancellation_process: None,
        }
    }

    pub fn data(&self) -> String {
        encoding::bulk_string(&self.data)
    }

    pub fn set_cancellation(&mut self, process: JoinHandle<()>) {
        self.cancellation_process = Some(process);
    }

    pub fn abort_deletion_process(&mut self) {
        if let Some(process) = &self.cancellation_process {
            process.abort();
            self.cancellation_process = None;
        }
    }
}

#[derive(Debug)]
pub enum DatabaseItem {
    String(RedisString),
    Stream(RedisStream),
}

impl DatabaseItem {
    pub fn data_type(&self) -> String {
        let data_type = match self {
            DatabaseItem::String(_) => "string",
            DatabaseItem::Stream(_) => "stream",
        };
        encoding::bulk_string(data_type)
    }

    pub fn clean_up(&mut self) {
        match self {
            DatabaseItem::String(redis_string) => {
                redis_string.abort_deletion_process();
            }
            DatabaseItem::Stream(_) => {}
        }
    }
}

// TODO: Consider if this should be a btree
#[derive(Debug)]
pub struct RedisStream(Vec<InnerRedisStream>);

#[derive(Debug, Clone)]
pub struct RedisStreamItem {
    pub key: String,
    pub value: String,
}

impl RedisStreamItem {
    pub fn new(key: String, value: String) -> Self {
        RedisStreamItem { key, value }
    }
}

#[derive(Debug, Clone)]
pub struct InnerRedisStream {
    pub items: Vec<RedisStreamItem>,
    pub ms_time: u128,
    pub sequence_number: usize,
}

impl InnerRedisStream {
    pub fn stream_id(&self) -> String {
        format!("{}-{}", self.ms_time, self.sequence_number)
    }
}

pub struct ReadStreamItem<'a> {
    pub streams: Vec<&'a InnerRedisStream>,
    pub key: String,
}

pub struct TempReadStreamItem {
    pub streams: Vec<InnerRedisStream>,
    pub key: String,
}

#[derive(Debug)]
pub struct AuxValue {
    #[allow(dead_code)]
    key: String,
    #[allow(dead_code)]
    value: String,
}

fn parse_aux(cursor: &mut Cursor<Vec<u8>>) -> Result<(), anyhow::Error> {
    let key = encoding::decode_rdb_string(cursor)?;
    let value = encoding::decode_rdb_string(cursor)?;

    let _aux_value = AuxValue { key, value };

    Ok(())
}

fn parse_select_db(cursor: &mut Cursor<Vec<u8>>) -> Result<(), anyhow::Error> {
    let _size = encoding::decode_rdb_int(cursor)?;

    Ok(())
}

fn parse_resize_db(cursor: &mut Cursor<Vec<u8>>) -> Result<(), anyhow::Error> {
    let _hash_table_size = encoding::decode_rdb_int(cursor)?;
    let _expiry_table_size = encoding::decode_rdb_int(cursor)?;

    Ok(())
}

fn parse_expire_time_ms(
    cursor: &mut Cursor<Vec<u8>>,
) -> Result<Option<(String, DatabaseItem)>, anyhow::Error> {
    let mut expire_time_ms: [u8; 8] = [0; 8];
    cursor.read_exact(&mut expire_time_ms)?;
    let expire_time_milliseconds = u64::from_le_bytes(expire_time_ms);
    let expire_time_unix_timestamp = expire_time_milliseconds / 1000;

    read_expirable_item(expire_time_unix_timestamp, cursor)
}

fn parse_expire_time_sec(
    cursor: &mut Cursor<Vec<u8>>,
) -> Result<Option<(String, DatabaseItem)>, anyhow::Error> {
    let mut expire_time_seconds: [u8; 4] = [0; 4];
    cursor.read_exact(&mut expire_time_seconds)?;
    let expire_time_seconds = u32::from_le_bytes(expire_time_seconds);

    read_expirable_item(expire_time_seconds as u64, cursor)
}

fn read_expirable_item(
    expire_time_unix_timestamp: u64,
    cursor: &mut Cursor<Vec<u8>>,
) -> Result<Option<(String, DatabaseItem)>, anyhow::Error> {
    let item_expiration = duration_to_item_expiration(expire_time_unix_timestamp);
    let item_expires_in_future = item_expiration.is_some();

    let value_type_byte = utils::read_next_byte(cursor)?;
    let value_type = ValueType::from_byte(value_type_byte)?;

    let item_data = read_key_value_pair(value_type, item_expiration, cursor)?;

    if item_expires_in_future {
        let data = Some(item_data);
        Ok(data)
    } else {
        Ok(None)
    }
}

fn read_key_value_pair(
    value_type: ValueType,
    expire_time: Option<Duration>,
    cursor: &mut Cursor<Vec<u8>>,
) -> Result<(String, DatabaseItem), anyhow::Error> {
    let key = encoding::decode_rdb_string(cursor)?;
    let value = match value_type {
        ValueType::String => encoding::decode_rdb_string(cursor)?,
        // TODO
        _ => anyhow::bail!("{:?} value type not supported", value_type),
    };

    let redis_string = RedisString::new(value, expire_time);
    let database_item = DatabaseItem::String(redis_string);

    Ok((key, database_item))
}

fn duration_to_item_expiration(expire_time_unix_timestamp: u64) -> Option<Duration> {
    let now = SystemTime::now();
    let duration_since_epoch = now.duration_since(UNIX_EPOCH).unwrap();

    // Convert the duration to seconds and return it as u64
    let current_unicode_timestamp = duration_since_epoch.as_secs();

    match expire_time_unix_timestamp.checked_sub(current_unicode_timestamp) {
        Some(dur) => {
            let duration = Duration::from_secs(dur);
            Some(duration)
        }
        None => None,
    }
}

fn determine_sequence_number(
    num: request::XAddNumber,
    ms_time: u128,
    latest_inner: &InnerRedisStream,
) -> usize {
    if let request::XAddNumber::Predetermined(val) = num {
        return val;
    }

    let sequence_number = if latest_inner.ms_time < ms_time { 0 } else { 1 };

    if sequence_number == 0 && ms_time == 0 {
        return 1;
    }

    sequence_number
}

fn broadcast_xadd(
    key: &str,
    ms_time: u128,
    sequence_number: usize,
    data: Vec<RedisStreamItem>,
    sender: Sender<transmission::Transmission>,
) -> Result<(), anyhow::Error> {
    let transmission = transmission::XAddTransmission {
        key: key.to_string(),
        ms_time,
        sequence_number,
        data,
    };

    sender
        .send(transmission::Transmission::Xadd(transmission))
        .map_err(|e| anyhow::anyhow!("{:?}", e.to_string()))?;

    Ok(())
}

async fn read_streams_after_limited_wait(
    wait: u64,
    read_command_streams: Vec<request::XReadCommandStream>,
    mut receiver: Receiver<transmission::Transmission>,
) -> Result<String, anyhow::Error> {
    let start = Instant::now();
    let mut streams: Vec<TempReadStreamItem> = vec![];
    let wait = Duration::from_millis(wait);

    loop {
        let elapsed = start.elapsed();
        if elapsed > wait {
            break;
        }

        let result = timeout(wait - elapsed, receiver.recv()).await;
        match result {
            Ok(Err(e)) => anyhow::bail!(e),
            Err(_) => break,
            Ok(Ok(transmission)) => {
                if let transmission::Transmission::Xadd(xadd) = transmission {
                    if read_command_streams.iter().any(|s| {
                        let is_valid_key = s.key == xadd.key;
                        let is_valid_entry = stream_entry_greater_than_start(
                            xadd.ms_time,
                            xadd.sequence_number,
                            &s.start,
                        );

                        is_valid_key && is_valid_entry
                    }) {
                        let inner_redis_stream = InnerRedisStream {
                            ms_time: xadd.ms_time,
                            sequence_number: xadd.sequence_number,
                            items: xadd.data,
                        };

                        if let Some(read_stream_item) =
                            streams.iter_mut().find(|rsi| rsi.key == xadd.key)
                        {
                            read_stream_item.streams.push(inner_redis_stream)
                        } else {
                            let read_stream_item = TempReadStreamItem {
                                streams: vec![inner_redis_stream],
                                key: xadd.key,
                            };

                            streams.push(read_stream_item);
                        }
                    }
                }
            }
        }
    }

    if streams.is_empty() {
        let null_string = encoding::empty_string();
        return Ok(null_string);
    }

    let streams = streams
        .iter()
        .map(|temp| ReadStreamItem {
            streams: temp.streams.iter().collect(),
            key: temp.key.to_string(),
        })
        .collect();

    let output = encoding::encode_streams(streams);
    Ok(output)
}

async fn read_streams_until_xadd(
    read_command_streams: Vec<request::XReadCommandStream>,
    mut receiver: Receiver<transmission::Transmission>,
) -> Result<String, anyhow::Error> {
    loop {
        let result = receiver.recv().await;
        match result {
            Err(e) => anyhow::bail!(e),
            Ok(transmission) => {
                if let transmission::Transmission::Xadd(xadd) = transmission {
                    if read_command_streams.iter().any(|s| {
                        let is_valid_key = s.key == xadd.key;
                        let is_valid_entry = stream_entry_greater_than_start(
                            xadd.ms_time,
                            xadd.sequence_number,
                            &s.start,
                        );

                        is_valid_key && is_valid_entry
                    }) {
                        let inner_redis_stream = InnerRedisStream {
                            ms_time: xadd.ms_time,
                            sequence_number: xadd.sequence_number,
                            items: xadd.data,
                        };
                        let read_stream_item = ReadStreamItem {
                            key: xadd.key,
                            streams: vec![&inner_redis_stream],
                        };
                        let output = encoding::encode_streams(vec![read_stream_item]);

                        return Ok(output);
                    }
                }
            }
        }
    }
}

fn read_streams_sync(
    database: &Database,
    read_command_streams: Vec<request::XReadCommandStream>,
) -> Result<String, anyhow::Error> {
    let database = database.0.read().unwrap();

    let mut streams: Vec<ReadStreamItem> = Vec::with_capacity(read_command_streams.len());
    for command_stream in read_command_streams.iter() {
        let stream = match database.get(&command_stream.key) {
            Some(item) => match &item {
                DatabaseItem::Stream(stream) => stream,
                _ => {
                    anyhow::bail!(wrong_type_str());
                }
            },
            None => continue,
        };

        let mut inner_streams: Vec<&InnerRedisStream> = vec![];
        let mut has_started: bool = false;

        for entry in stream.0.iter() {
            if !has_started {
                has_started = stream_entry_greater_than_start(
                    entry.ms_time,
                    entry.sequence_number,
                    &command_stream.start,
                );

                if !has_started {
                    continue;
                }
            }

            inner_streams.push(entry);
        }

        let item = ReadStreamItem {
            streams: inner_streams,
            key: command_stream.key.to_string(),
        };
        streams.push(item);
    }

    let output = if streams.is_empty() {
        empty_string()
    } else {
        encoding::encode_streams(streams)
    };

    Ok(output)
}

fn stream_entry_greater_than_start(
    entry_ms_time: u128,
    entry_sequence_number: usize,
    start: &request::XReadNumber,
) -> bool {
    match start {
        request::XReadNumber::AllNewEntries => true,
        request::XReadNumber::Specified(ms_time, sequence_number) => {
            if entry_ms_time < *ms_time {
                return false;
            }
            // If we are at the ms_time but not yet at the sequence number then ignore.
            if entry_ms_time == *ms_time && entry_sequence_number <= *sequence_number {
                return false;
            }

            true
        }
    }
}

fn adjust_float_value_by_int(data: &str, amount: i64) -> Result<String, anyhow::Error> {
    let value = data
        .parse::<f64>()
        .map_err(|_| anyhow::anyhow!("ERR value is not a float or out of range"))?;

    let value = value + amount as f64;
    Ok(value.to_string())
}

fn adjust_float_value_by_float(data: &str, amount: f64) -> Result<String, anyhow::Error> {
    let value = data
        .parse::<f64>()
        .map_err(|_| anyhow::anyhow!("ERR value is not a float or out of range"))?;

    let value = value + amount;
    Ok(value.to_string())
}

fn adjust_int_value_by_int(data: &str, amount: i64) -> Result<String, anyhow::Error> {
    let value = data
        .parse::<i64>()
        .map_err(|_| anyhow::anyhow!("ERR value is not an integer or out of range"))?;

    let value = value
        .checked_add(amount)
        .ok_or_else(|| anyhow::anyhow!("ERR increment or decrement would overflow"))?;

    Ok(value.to_string())
}

fn adjust_int_value_by_float(data: &str, amount: f64) -> Result<String, anyhow::Error> {
    let value = data
        .parse::<i64>()
        .map_err(|_| anyhow::anyhow!("ERR value is not an integer or out of range"))?;

    let value = (value as f64) + amount;

    Ok(value.to_string())
}
