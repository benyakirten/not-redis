use std::io::{Cursor, Read};

use anyhow::Context;

use crate::utils;

const LEADING_BYTE_LENGTH_ENCODING_BIT_MASK: u8 = 0b1100_0000;
const LEADING_BYTE_LENGTH_ENCODING_RIGHT_SHIFT: u8 = 6;
// Indicates how many bytes the special format will
const LEADING_BYTE_MINUS_LENGTH_BIT_MASK: u8 = 0b0011_1111;

pub fn encode_rdb(rdb_bytes: Vec<u8>) -> Vec<u8> {
    let mut vec: Vec<u8> = format!("${}\r\n", rdb_bytes.len()).into();
    vec.extend(rdb_bytes);
    vec
}

pub fn decode_rdb_string(cursor: &mut Cursor<Vec<u8>>) -> Result<String, anyhow::Error> {
    let val = match LengthEncoding::from_cursor(cursor)? {
        LengthEncoding::OnlyThisByte(length) => read_known_length_string(length, cursor),
        LengthEncoding::AndNextByte(length) => read_known_length_string(length, cursor),
        LengthEncoding::ReadNextFourBytes(length) => read_known_length_string(length, cursor),
        LengthEncoding::SpecialFormatEncoding(byte) => {
            let string_length_encoding = StringLengthEncoding::from_byte(byte)?;
            match string_length_encoding {
                StringLengthEncoding::EightBitInteger => read_8_bit_integer_as_string(cursor),
                StringLengthEncoding::SixteenBitInteger => read_16_bit_integer_as_string(cursor),
                StringLengthEncoding::ThirtyTwoBitInteger => read_32_bit_integer_as_string(cursor),
                StringLengthEncoding::CompressedString => read_lzf_compressed_string(cursor),
            }
        }
    }?;

    Ok(val)
}

fn read_known_length_string(
    length: usize,
    cursor: &mut Cursor<Vec<u8>>,
) -> Result<String, anyhow::Error> {
    let mut val = vec![0; length];
    cursor
        .read_exact(&mut val)
        .context("Reading known length string")?;

    let result = String::from_utf8(val.to_vec())?;

    Ok(result)
}

fn read_8_bit_integer_as_string(cursor: &mut Cursor<Vec<u8>>) -> Result<String, anyhow::Error> {
    let byte = utils::read_next_byte(cursor).context("Reading 8 bit integer as string")?;

    let value = u8::from_le_bytes([byte]);
    let value = format!("{}", value);

    Ok(value)
}

fn read_16_bit_integer_as_string(cursor: &mut Cursor<Vec<u8>>) -> Result<String, anyhow::Error> {
    let mut byte: [u8; 2] = [0; 2];
    cursor
        .read_exact(&mut byte)
        .context("Reading 16 bit integer as string")?;

    let value = u16::from_le_bytes(byte);
    let value = format!("{}", value);

    Ok(value)
}

fn read_32_bit_integer_as_string(cursor: &mut Cursor<Vec<u8>>) -> Result<String, anyhow::Error> {
    let mut byte: [u8; 4] = [0; 4];
    cursor
        .read_exact(&mut byte)
        .context("Reading 32 bit integer as string")?;

    let value = u32::from_le_bytes(byte);
    let value = format!("{}", value);

    Ok(value)
}

fn read_lzf_compressed_string(cursor: &mut Cursor<Vec<u8>>) -> Result<String, anyhow::Error> {
    let clen = read_compressed_len(cursor)?;
    let ulen = read_compressed_len(cursor)?;

    let compressed_string = read_known_length_string(clen, cursor)?;
    let decompressed = lzf::decompress(compressed_string.as_bytes(), ulen)
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    let decompressed = String::from_utf8(decompressed)?;
    Ok(decompressed)
}

fn read_compressed_len(cursor: &mut Cursor<Vec<u8>>) -> Result<usize, anyhow::Error> {
    match LengthEncoding::from_cursor(cursor)? {
        LengthEncoding::OnlyThisByte(length) => Ok(length),
        LengthEncoding::AndNextByte(length) => Ok(length),
        LengthEncoding::ReadNextFourBytes(length) => Ok(length),
        _ => anyhow::bail!("Special length not supported for compressed string"),
    }
}

#[derive(Debug)]
enum LengthEncoding {
    OnlyThisByte(usize),
    AndNextByte(usize),
    ReadNextFourBytes(usize),
    SpecialFormatEncoding(u8),
}

impl LengthEncoding {
    fn from_cursor(cursor: &mut Cursor<Vec<u8>>) -> Result<Self, anyhow::Error> {
        let byte = utils::read_next_byte(cursor).context("Reading length encoding from string")?;

        // Mask the first two bits
        let leading_bits = byte & LEADING_BYTE_LENGTH_ENCODING_BIT_MASK;
        // Right shift so we only read the first two bits.
        let leading_bits = leading_bits >> LEADING_BYTE_LENGTH_ENCODING_RIGHT_SHIFT;

        match leading_bits {
            0b00 => {
                let length = byte & LEADING_BYTE_MINUS_LENGTH_BIT_MASK;
                Ok(Self::OnlyThisByte(length as usize))
            }
            0b01 => {
                let start_length = byte & LEADING_BYTE_MINUS_LENGTH_BIT_MASK;

                let byte = utils::read_next_byte(cursor)
                    .context("Read next byte to determine length encoded size")?;

                let length = u16::from(start_length) + u16::from(byte);

                Ok(Self::AndNextByte(length as usize))
            }
            0b10 => {
                let mut size_bytes: [u8; 4] = [0; 4];
                cursor.read_exact(&mut size_bytes)?;

                let length = u32::from_le_bytes(size_bytes);

                Ok(Self::ReadNextFourBytes(length as usize))
            }
            0b11 => Ok(Self::SpecialFormatEncoding(byte)),
            _ => unreachable!(),
        }
    }
}

#[derive(Debug)]
enum StringLengthEncoding {
    EightBitInteger,
    SixteenBitInteger,
    ThirtyTwoBitInteger,
    CompressedString,
}

impl StringLengthEncoding {
    fn from_byte(byte: u8) -> Result<Self, anyhow::Error> {
        match byte & LEADING_BYTE_MINUS_LENGTH_BIT_MASK {
            0b00 => Ok(Self::EightBitInteger),
            0b01 => Ok(Self::SixteenBitInteger),
            0b10 => Ok(Self::ThirtyTwoBitInteger),
            0b11 => Ok(Self::CompressedString),
            val => anyhow::bail!("Expected 0b00 to 0b11, received {:08b}", val),
        }
    }
}

pub fn decode_rdb_int(cursor: &mut Cursor<Vec<u8>>) -> Result<usize, anyhow::Error> {
    match LengthEncoding::from_cursor(cursor)? {
        LengthEncoding::OnlyThisByte(size) => Ok(size),
        LengthEncoding::AndNextByte(size) => Ok(size),
        LengthEncoding::ReadNextFourBytes(size) => Ok(size),
        LengthEncoding::SpecialFormatEncoding(byte) => {
            let string_length_encoding = StringLengthEncoding::from_byte(byte)?;
            let integer_string = match string_length_encoding {
                StringLengthEncoding::EightBitInteger => read_8_bit_integer_as_string(cursor),
                StringLengthEncoding::SixteenBitInteger => read_16_bit_integer_as_string(cursor),
                StringLengthEncoding::ThirtyTwoBitInteger => read_32_bit_integer_as_string(cursor),
                _ => anyhow::bail!("Length special format cannot be read for rdb int"),
            }?;

            let integer: usize = str::parse(&integer_string)?;

            Ok(integer)
        }
    }
}
