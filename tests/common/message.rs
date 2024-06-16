use not_redis::encoding::bulk_string;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time;

const TIMEOUT: time::Duration = time::Duration::from_millis(500);

async fn inner_send_message(
    address: &str,
    message: &[u8],
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
                attempt_no + 1,
                max_attempts,
            ))
            .await;
        }
    };

    let mut socket = socket.unwrap();
    socket.write_all(message).await.unwrap();

    let mut buffer = vec![0; 1024];
    let read_len = socket.read(&mut buffer).await.unwrap();

    String::from_utf8(buffer[..read_len].to_vec()).unwrap()
}

pub async fn send_message(address: &str, message: &[u8]) -> String {
    inner_send_message(address, message, 0, 5).await
}

pub fn encode_string(s: &str) -> Vec<u8> {
    encode_string_array(s.split_whitespace().collect())
}

pub fn encode_string_array(items: Vec<&str>) -> Vec<u8> {
    let mut output = format!("*{}\r\n", items.len());
    for item in items {
        let processed = &bulk_string(item);
        output.push_str(processed);
    }
    output.into()
}

pub struct StreamData<'a> {
    pub id: &'a str,
    pub items: Vec<&'a str>,
}

pub fn encode_stream_items<'a>(items: Vec<StreamData<'a>>) -> String {
    let mut output = format!("*{}\r\n", items.len());
    for item in items {
        let string_array = String::from_utf8(encode_string_array(item.items)).unwrap();
        let inner_array = format!(
            "*2\r\n${}\r\n{}\r\n{}",
            item.id.len(),
            item.id,
            string_array
        );
        output.push_str(&inner_array)
    }
    output
}
