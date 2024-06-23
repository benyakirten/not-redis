use std::io::{Cursor, Read};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;

use crate::{encoding, utils};

use super::{Database, DatabaseItem, RedisString};

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

fn read_magic_string(cursor: &mut Cursor<Vec<u8>>) -> Result<String, anyhow::Error> {
    let mut magic_string: [u8; 5] = [0; 5];
    cursor
        .read_exact(&mut magic_string)
        .context("Reading magic string")?;
    let magic_string =
        String::from_utf8(magic_string.to_vec()).context("Parsing magic string into utf8")?;

    Ok(magic_string)
}

fn read_version_number(cursor: &mut Cursor<Vec<u8>>) -> Result<u32, anyhow::Error> {
    let mut version_number: [u8; 4] = [0; 4];
    cursor
        .read_exact(&mut version_number)
        .context("Reading version number")?;
    let version_number =
        String::from_utf8(version_number.to_vec()).context("Parsing verison number into utf8")?;
    let version_number = str::parse::<u32>(&version_number)
        .context("Version number cannot be parsed as an integer")?;

    Ok(version_number)
}

pub fn read_from_file(
    database: Database,
    mut cursor: Cursor<Vec<u8>>,
) -> Result<Database, anyhow::Error> {
    read_magic_string(&mut cursor)?;
    let version_number = read_version_number(&mut cursor)?;

    let database = match version_number {
        _ => create_database_from_version_x(database, cursor),
    }?;

    Ok(database)
}

fn create_database_from_version_x(
    database: Database,
    mut cursor: Cursor<Vec<u8>>,
) -> Result<Database, anyhow::Error> {
    loop {
        let op_code = utils::read_next_byte(&mut cursor)?;
        match OpCode::from_byte(op_code) {
            OpCode::Aux => parse_aux(&mut cursor)?,
            OpCode::SelectDB => parse_select_db(&mut cursor)?,
            OpCode::ResizeDb => parse_resize_db(&mut cursor)?,
            OpCode::ExpireTimeMS => {
                if let Some((key, value)) = parse_item_with_ms_expire_time(&mut cursor)? {
                    database.set_item(key, value);
                }
            }
            OpCode::ExpireTime => {
                if let Some((key, value)) = parse_item_with_seconds_expire_time(&mut cursor)? {
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

fn parse_item_with_ms_expire_time(
    cursor: &mut Cursor<Vec<u8>>,
) -> Result<Option<(String, DatabaseItem)>, anyhow::Error> {
    let mut expire_time_ms: [u8; 8] = [0; 8];
    cursor.read_exact(&mut expire_time_ms)?;
    let expire_time_milliseconds = u64::from_le_bytes(expire_time_ms);
    let expire_time_unix_timestamp = expire_time_milliseconds / 1000;

    read_expirable_item(expire_time_unix_timestamp, cursor)
}

fn parse_item_with_seconds_expire_time(
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
