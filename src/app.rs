use tokio::net::TcpListener;
use tokio::sync::broadcast::Sender;

use crate::data::Database;
use crate::server::RedisServer;
use crate::{stream, transmission};

pub async fn run(
    address: &str,
    database: Database,
    redis_server: RedisServer,
    tx: Sender<transmission::Transmission>,
) -> Result<(), anyhow::Error> {
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
