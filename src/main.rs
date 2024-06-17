use tokio::sync::broadcast;

use not_redis::app;
use not_redis::server;
use not_redis::transmission;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let (tx, _) = broadcast::channel::<transmission::Transmission>(100);
    let (database, redis_server) = server::RedisServer::from_args().await?;
    let address = redis_server.address().await;

    app::run(&address, database, redis_server, tx).await
}
