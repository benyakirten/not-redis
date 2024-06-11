use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time;

const TIMEOUT: time::Duration = time::Duration::from_millis(500);

pub async fn send_message(address: &str, message: &[u8], buffer_size: usize) -> (Vec<u8>, usize) {
    let socket = TcpStream::connect(address).await;
    match socket {
        Ok(_) => {}
        Err(e) => {
            println!("Failed to connect to {}: {}", address, e);
            time::sleep(TIMEOUT).await;
            return Box::pin(send_message(address, message, buffer_size)).await;
        }
    };

    let mut socket = socket.unwrap();
    socket.write_all(message).await.unwrap();

    let mut buffer = vec![0; buffer_size];
    let read_len = socket.read(&mut buffer).await.unwrap();

    (buffer, read_len)
}

pub fn encode_string(s: &str) -> Vec<u8> {
    format!("*1\r\n${}\r\n{}\r\n", s.len(), s).into_bytes()
}
