use core::panic;
use std::net::TcpListener;
use std::sync::Mutex;

use tokio::sync::broadcast;

use not_redis::server::{
    generate_random_sha1_hex, Address, Config, RedisServer, Replication, Server, ServerRole,
};

use not_redis::data::Database;
use not_redis::transmission::Transmission;

static PORT: Mutex<u16> = Mutex::new(6379);

pub struct TestApp {
    pub database: Database,
    pub redis_server: RedisServer,
    pub address: Address,
    pub transmitter: broadcast::Sender<Transmission>,
}

pub fn setup() -> TestApp {
    let (tx, _) = broadcast::channel::<Transmission>(100);

    let database = Database::new();

    let port = get_available_port();
    let address = Address::new("127.0.0.1".into(), port);

    let role = ServerRole::Master(vec![], 0, 0);
    let replication = Replication {
        id: generate_random_sha1_hex(),
        offset: 0,
    };

    let config = Config::new(None, None);

    let settings = Server::new(config, role, address.clone(), replication);
    let redis_server = RedisServer::new(settings);

    *PORT.lock().unwrap() = port.checked_add(1).expect("Port overflow");

    TestApp {
        database,
        redis_server,
        address,
        transmitter: tx,
    }
}

fn get_available_port() -> u16 {
    let port = *PORT.lock().unwrap();
    for i in port..u16::MAX {
        if port_available(i) {
            return i;
        }
    }

    panic!("No available port found between {} and {}", port, u16::MAX);
}

fn port_available(port: u16) -> bool {
    TcpListener::bind(("127.0.0.1", port)).is_ok()
}
