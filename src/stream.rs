use std::io::Cursor;

use anyhow::Context;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::broadcast::Sender;

use crate::{commands, data, encoding, request, server, transmission, utils};

#[derive(PartialEq, Debug)]
enum CommandType {
    Psync,
    ToReplicate,
    Other,
}

pub async fn handle_stream(
    mut stream: TcpStream,
    database: data::Database,
    server: server::RedisServer,
    sender: Sender<transmission::Transmission>,
) -> Result<(), anyhow::Error> {
    let mut buf = [0; 512];

    loop {
        let bytes_read = stream.read(&mut buf).await?;
        let command = &buf[..bytes_read];

        if bytes_read == 0 {
            return Ok(());
        }

        let raw_request = String::from_utf8(command.to_vec())?
            .lines()
            .map(|s| s.to_string())
            .collect::<Vec<String>>();

        let request = request::parse_request(raw_request)?;

        let command_type = match &request {
            request::Command::Get(_) | request::Command::Set(..) => CommandType::ToReplicate,
            request::Command::Psync(..) => CommandType::Psync,
            _ => CommandType::Other,
        };

        let sender = sender.clone();
        let receiver = sender.subscribe();

        let command_responses = match request {
            request::Command::Ping(body) => commands::pong(body),
            request::Command::Echo(body) => commands::echo_response(body),
            request::Command::Get(key) => commands::get_value(&database, key),
            request::Command::Set(set_command) => commands::set_value(&database, set_command),
            request::Command::Del(keys) => commands::delete_keys(&database, keys),
            request::Command::GetDel(key) => commands::get_delete_key(&database, key),
            request::Command::GetEx(key, expiry) => {
                commands::update_expiration(&database, key, expiry)
            }
            request::Command::Info => commands::get_info(&server).await,
            request::Command::ReplConf(repl) => commands::replica_confirm(repl, 0),
            request::Command::Psync(..) => commands::perform_psync(&server).await,
            request::Command::Wait(num_replicas, timeout) => {
                commands::transmit_wait(&server, num_replicas, timeout).await
            }
            request::Command::Config(config_command) => {
                commands::view_config(&server, config_command).await
            }
            request::Command::Keys(key_group) => commands::get_keys(&database, key_group),
            request::Command::Type(key) => commands::get_type(&database, key),
            // TODO: Transmit stream to the replica.
            request::Command::Xadd(command) => commands::add_stream(&database, command, sender),
            request::Command::Xrange(command) => commands::get_stream_range(&database, command),
            request::Command::Xread(command) => {
                commands::read_streams(&database, command, receiver).await
            }
            request::Command::Incr(key) => commands::increment_value_by_int(&database, key, 1),
            request::Command::IncrBy(key, amount) => {
                commands::increment_value_by_int(&database, key, amount)
            }
            request::Command::IncrByFloat(key, amount) => {
                commands::increment_value_by_float(&database, key, amount)
            }
            request::Command::Decr(key) => commands::increment_value_by_int(&database, key, -1),
            request::Command::DecrBy(key, amount) => {
                commands::increment_value_by_int(&database, key, -amount)
            }
        }?;

        write_command_responses(&mut stream, command_responses).await?;

        match command_type {
            CommandType::Other => continue,
            CommandType::ToReplicate => server.replicate_command(command).await?,
            CommandType::Psync => {
                server.add_stream(stream).await;
                return Ok(());
            }
        }
    }
}

pub async fn handle_replica_stream(
    mut stream: TcpStream,
    database: data::Database,
) -> Result<(), anyhow::Error> {
    let mut buf = [0; 512];
    let mut bytes_received: usize = 0;

    loop {
        let bytes_read = stream.read(&mut buf).await?;
        let command = &buf[..bytes_read];

        if bytes_read == 0 {
            return Ok(());
        }

        let mut cursor = Cursor::new(command);

        loop {
            let frame = utils::read_frame(&mut cursor)?;

            let frame = match frame {
                None => break,
                Some(frame) => frame,
            };

            let request = request::parse_request(frame.data)?;

            match request {
                request::Command::Get(key) => commands::get_value(&database, key).map(|_| ()),
                request::Command::Set(command) => {
                    commands::set_value(&database, command).map(|_| ())
                }
                request::Command::Wait(..) => {
                    let response = encoding::okay_string().as_bytes().to_vec();
                    let response = vec![response];

                    write_command_responses(&mut stream, response).await?;
                    Ok(())
                }
                request::Command::ReplConf(command)
                    if command == request::ReplicationCommand::Ack =>
                {
                    let command_responses = commands::replica_confirm(command, bytes_received)?;
                    write_command_responses(&mut stream, command_responses).await?;

                    Ok(())
                }
                _ => Ok(()),
            }?;

            bytes_received += frame.bytes_processed
        }
    }
}

async fn write_command_responses(
    stream: &mut TcpStream,
    command_responses: Vec<Vec<u8>>,
) -> Result<(), anyhow::Error> {
    for response in command_responses {
        stream
            .write_all(response.as_slice())
            .await
            .context("writing to outbound stream")?;
    }

    Ok(())
}
