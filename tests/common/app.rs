use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Mutex;

use rand::Rng;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::time;

use not_redis::app;
use not_redis::server::{
    generate_random_sha1_hex, sync_to_master, Address, Config, RedisServer, Replication, Server,
    ServerRole,
};

use not_redis::data::Database;
use not_redis::transmission::Transmission;

const MIN_PORT: u16 = 2000;
const MAX_PORT: u16 = u16::MAX;
const TIMEOUT: time::Duration = time::Duration::from_millis(500);

pub struct TestApp {
    pub database: Database,
    pub redis_server: RedisServer,
    pub address: Address,
    pub transmitter: broadcast::Sender<Transmission>,
    join_handle: tokio::task::JoinHandle<()>,
}

enum TestAppRole {
    Master,
    Slave(Address),
}

impl TestApp {
    pub async fn with_config(config: Config) -> TestApp {
        TestApp::new(TestAppRole::Master, Some(config)).await
    }

    pub async fn master() -> TestApp {
        TestApp::new(TestAppRole::Master, None).await
    }

    pub async fn slave(address: Address) -> TestApp {
        TestApp::new(TestAppRole::Slave(address), None).await
    }

    async fn new(role: TestAppRole, config: Option<Config>) -> TestApp {
        let (tx, _) = broadcast::channel::<Transmission>(100);

        let config = config.unwrap_or_else(|| Config::new(None, None));
        let database = match (&config.dir, &config.db_file_name) {
            (Some(dir), Some(file_name)) => {
                let path = PathBuf::from(dir).join(file_name);
                Database::from_config(path).unwrap()
            }
            _ => Database::new(),
        };

        let port = get_available_port().await;
        let address = Address::new("127.0.0.1".into(), port);

        let (replication, role) = match role {
            TestAppRole::Master => master_server_role(),
            TestAppRole::Slave(master_address) => {
                sync_to_master(master_address, &address, database.clone())
                    .await
                    .expect("Failed to sync to master")
            }
        };

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

        time::sleep(TIMEOUT).await;

        println!("TestApp started on {}", address.name());

        let test_app = TestApp {
            database,
            redis_server,
            address,
            transmitter: tx,
            join_handle,
        };

        test_app
    }
}

impl Drop for TestApp {
    fn drop(&mut self) {
        self.join_handle.abort();
    }
}

async fn get_available_port() -> u16 {
    // 100 Attempts to get an available port seems generous.
    for _ in 0..100 {
        if let Some(port) = find_random_available_port().await {
            return port;
        }
    }

    panic!("No available port found");
}

async fn find_random_available_port() -> Option<u16> {
    let port = get_random_port();
    match TcpListener::bind(("127.0.0.1", port)).await {
        Ok(_) => Some(port),
        Err(_) => None,
    }
}

pub fn master_server_role() -> (Replication, ServerRole) {
    let replication = Replication {
        id: generate_random_sha1_hex(),
        offset: 0,
    };
    let role = ServerRole::Master(vec![], 0, 0);
    (replication, role)
}

fn get_random_port() -> u16 {
    let mut rng = rand::thread_rng();
    rng.gen_range(MIN_PORT..MAX_PORT)
}
