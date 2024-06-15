use std::sync::Mutex;

use tokio::net::TcpListener;
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

enum TestAppRole {
    Master,
    Slave(Replication),
}

impl TestApp {
    pub async fn master() -> TestApp {
        TestApp::new(TestAppRole::Master).await
    }

    pub async fn slave(replication: Replication) -> TestApp {
        TestApp::new(TestAppRole::Slave(replication)).await
    }

    async fn new(role: TestAppRole) -> TestApp {
        let (tx, _) = broadcast::channel::<Transmission>(100);

        let database = Database::new();

        let (role, replication) = match role {
            TestAppRole::Master => master_server_role(),
            TestAppRole::Slave(replication) => (ServerRole::Slave, replication),
        };

        let port = get_available_port().await;
        let address = Address::new("127.0.0.1".into(), port);

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

        time::sleep(TIMEOUT).await;

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
}

impl Drop for TestApp {
    fn drop(&mut self) {
        self.join_handle.abort();
    }
}

async fn get_available_port() -> u16 {
    let port = *PORT.lock().unwrap();
    for i in port..u16::MAX {
        if port_available(i + 1).await {
            return i + 1;
        }
    }

    panic!("No available port found between {} and {}", port, u16::MAX);
}

async fn port_available(port: u16) -> bool {
    TcpListener::bind(("127.0.0.1", port)).await.is_ok()
}

pub fn master_server_role() -> (ServerRole, Replication) {
    let role = ServerRole::Master(vec![], 0, 0);
    let replication = Replication {
        id: generate_random_sha1_hex(),
        offset: 0,
    };
    (role, replication)
}
