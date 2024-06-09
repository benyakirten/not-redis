use tokio::sync::broadcast;

mod app;
mod commands;
mod data;
mod encoding;
mod request;
mod server;
mod stream;
mod transmission;
mod utils;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let (tx, _) = broadcast::channel::<transmission::Transmission>(100);
    let (database, redis_server) = server::RedisServer::from_args().await?;
    let address = redis_server.address().await;

    app::run(&address, database, redis_server, tx).await
}
