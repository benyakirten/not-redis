use std::io::{Cursor, ErrorKind, Read};

use bytes::Buf;

pub struct Frame {
    pub data: Vec<String>,
    pub bytes_processed: usize,
}

pub fn read_frame(cursor: &mut Cursor<&[u8]>) -> Result<Option<Frame>, anyhow::Error> {
    let mut bytes_processed = 0;
    let (size, bytes) = match read_line(cursor) {
        Ok(data) => Ok(data),
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        error => error,
    }?;

    bytes_processed += bytes;

    let size = String::from_utf8(size)?;
    let parsed_size: usize = str::parse(&size[1..])?;

    let mut data: Vec<String> = Vec::with_capacity(parsed_size);

    data.push(size);

    for _ in 0..parsed_size * 2 {
        let (line, bytes) = read_line(cursor)?;
        let line = String::from_utf8(line)?;

        bytes_processed += bytes;

        data.push(line);
    }

    let frame = Frame {
        bytes_processed,
        data,
    };
    Ok(Some(frame))
}

// Can this be genericized to work with a tokio::net::TcpStream?
pub fn read_line(cursor: &mut Cursor<&[u8]>) -> Result<(Vec<u8>, usize), std::io::Error> {
    let mut bytes_read = 0;
    let mut data: Vec<u8> = vec![];
    let mut next_char: [u8; 1] = [0; 1];

    loop {
        cursor.read_exact(&mut next_char)?;
        bytes_read += 1;

        let char_read = *next_char.first().ok_or_else(|| {
            std::io::Error::new(ErrorKind::Other, "Expectd to read byte from cursor")
        })?;

        if char_read == b'\r' {
            cursor.advance(1);
            bytes_read += 1;
            break;
        }

        data.push(char_read);
    }

    Ok((data, bytes_read))
}

pub fn read_next_byte(cursor: &mut Cursor<Vec<u8>>) -> Result<u8, anyhow::Error> {
    let mut byte: [u8; 1] = [0; 1];
    cursor.read_exact(&mut byte)?;
    let byte = byte[0];
    Ok(byte)
}
