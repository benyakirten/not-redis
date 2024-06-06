use tokio::net::TcpListener;
use tokio::sync::broadcast;

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

    let listener = TcpListener::bind(&address)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to bind to address {}: {}", address, e))?;
    println!("Listening on {}", address);

    while let Ok((stream, _)) = listener.accept().await {
        let database = database.clone();
        let redis_server = redis_server.clone();
        let sender = tx.clone();
        tokio::spawn(async move {
            match stream::handle_stream(stream, database, redis_server, sender).await {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Error handling stream: {}", e);
                }
            }
        });
    }

    Ok(())
}
