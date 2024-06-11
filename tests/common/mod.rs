use std::sync::Mutex;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::time;

use not_redis::app;
use not_redis::server::{
    generate_random_sha1_hex, Address, Config, RedisServer, Replication, Server, ServerRole,
};

use not_redis::data::Database;
use not_redis::transmission::Transmission;

static PORT: Mutex<u16> = Mutex::new(6379);
const TIMEOUT: time::Duration = time::Duration::from_millis(500);

pub struct TestApp {
    pub database: Database,
    pub redis_server: RedisServer,
    pub address: Address,
    pub transmitter: broadcast::Sender<Transmission>,
    join_handle: tokio::task::JoinHandle<()>,
}

pub async fn setup() -> TestApp {
    let (tx, _) = broadcast::channel::<Transmission>(100);

    let database = Database::new();

    let port = get_available_port().await;
    let address = Address::new("127.0.0.1".into(), port);

    let role = ServerRole::Master(vec![], 0, 0);
    let replication = Replication {
        id: generate_random_sha1_hex(),
        offset: 0,
    };

    let config = Config::new(None, None);

    let settings = Server::new(config, role, address.clone(), replication);
    let redis_server = RedisServer::new(settings);

    let addr = address.name().clone();
    let db = database.clone();
    let rs = redis_server.clone();
    let transmitter = tx.clone();

    let join_handle = tokio::spawn(async move {
        app::run(&addr, db, rs, transmitter)
            .await
            .expect("Failed to run app");
    });

    *PORT.lock().unwrap() = port.checked_add(1).expect("Port overflow");

    let test_app = TestApp {
        database,
        redis_server,
        address,
        transmitter: tx,
        join_handle,
    };

    test_app
}

impl Drop for TestApp {
    fn drop(&mut self) {
        self.join_handle.abort();
    }
}

async fn get_available_port() -> u16 {
    let port = *PORT.lock().unwrap();
    for i in port..u16::MAX {
        if port_available(i).await {
            return i;
        }
    }

    panic!("No available port found between {} and {}", port, u16::MAX);
}

async fn port_available(port: u16) -> bool {
    TcpListener::bind(("127.0.0.1", port)).await.is_ok()
}

pub async fn send_message(address: &str, message: &str, buffer_size: usize) -> (Vec<u8>, usize) {
    println!("Connecting to {}", address);
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
    socket.write_all(message.as_bytes()).await.unwrap();

    let mut buffer = vec![0; buffer_size];
    let read_len = socket.read(&mut buffer).await.unwrap();

    (buffer, read_len)
}
