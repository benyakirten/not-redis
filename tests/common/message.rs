use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time;

const TIMEOUT: time::Duration = time::Duration::from_millis(500);

async fn inner_send_message(
    address: &str,
    message: &[u8],
    buffer_size: usize,
    attempt_no: u8,
    max_attempts: u8,
) -> String {
    let socket = TcpStream::connect(address).await;
    match socket {
        Ok(_) => {}
        Err(e) => {
            if attempt_no >= max_attempts {
                panic!("Exceeded max attempts to connect to {}: {}", address, e);
            }
            println!("Failed to connect to {}: {}", address, e);
            time::sleep(TIMEOUT).await;
            return Box::pin(inner_send_message(
                address,
                message,
                buffer_size,
                attempt_no + 1,
                max_attempts,
            ))
            .await;
        }
    };

    let mut socket = socket.unwrap();
    socket.write_all(message).await.unwrap();

    let mut buffer = vec![0; buffer_size];
    let read_len = socket.read(&mut buffer).await.unwrap();

    String::from_utf8(buffer[..read_len].to_vec()).unwrap()
}

pub async fn send_message(address: &str, message: &[u8], buffer_size: usize) -> String {
    inner_send_message(address, message, buffer_size, 0, 5).await
}

pub fn encode_string(s: &str) -> Vec<u8> {
    encode_array(s.split_whitespace().collect())
}

pub fn encode_array(items: Vec<&str>) -> Vec<u8> {
    let mut output = format!("*{}\r\n", items.len());
    for item in items {
        let processed = &encode_array_string_item(item);
        output.push_str(processed);
    }
    output.into()
}

pub fn encode_array_string_item(item: &str) -> String {
    format!("${}\r\n{}\r\n", item.len(), item)
}

pub fn encode_simple_string(item: &str) -> String {
    format!("+{}\r\n", item)
}

pub fn empty_string() -> String {
    "$-1\r\n".to_string()
}

pub fn encode_number(num: usize) -> String {
    format!(":{}\r\n", num)
}
