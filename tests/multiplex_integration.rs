use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use std::collections::{HashMap, HashSet};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::Message;

use tcp_over_websocket::protocol::{Frame, FrameType};

async fn start_tows() -> Result<(String, watch::Sender<bool>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (stop_tx, stop_rx) = watch::channel(false);
    tokio::spawn(async move {
        tcp_over_websocket::server::serve(listener, stop_rx)
            .await
            .unwrap();
    });
    Ok((format!("ws://{address}/"), stop_tx))
}

async fn connect_client(
    url: &str,
) -> Result<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>> {
    let (mut websocket, _) = tokio_tungstenite::connect_async(url).await?;
    websocket
        .send(Message::Binary(
            Frame::hello("integration-test")?.encode().into(),
        ))
        .await?;
    let ack = next_frame(&mut websocket).await?;
    assert_eq!(ack.kind, FrameType::HelloAck);
    Ok(websocket)
}

async fn next_frame<S>(websocket: &mut tokio_tungstenite::WebSocketStream<S>) -> Result<Frame>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        match websocket.next().await.context("WebSocket 提前结束")?? {
            Message::Binary(bytes) => return Frame::decode(&bytes),
            Message::Ping(payload) => websocket.send(Message::Pong(payload)).await?,
            Message::Pong(_) => {}
            other => anyhow::bail!("意外 WebSocket 消息: {other:?}"),
        }
    }
}

async fn start_echo_server() -> Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut buffer = [0_u8; 8192];
                loop {
                    let Ok(size) = stream.read(&mut buffer).await else {
                        return;
                    };
                    if size == 0 || stream.write_all(&buffer[..size]).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    Ok(address.to_string())
}

#[tokio::test]
async fn three_concurrent_streams_keep_data_isolated() -> Result<()> {
    let target = start_echo_server().await?;
    let (url, stop) = start_tows().await?;
    let mut websocket = connect_client(&url).await?;

    for id in 1..=3 {
        websocket
            .send(Message::Binary(
                Frame::new(FrameType::Open, id, target.as_bytes().to_vec())?
                    .encode()
                    .into(),
            ))
            .await?;
    }
    let mut opened = HashSet::new();
    while opened.len() < 3 {
        let frame = next_frame(&mut websocket).await?;
        assert_eq!(frame.kind, FrameType::OpenOk);
        opened.insert(frame.tunnel_id);
    }

    for id in 1..=3 {
        let payload = format!("stream-{id}").into_bytes();
        websocket
            .send(Message::Binary(
                Frame::new(FrameType::Data, id, payload)?.encode().into(),
            ))
            .await?;
    }
    let mut received = HashMap::new();
    while received.len() < 3 {
        let frame = next_frame(&mut websocket).await?;
        if frame.kind == FrameType::Data {
            received.insert(frame.tunnel_id, frame.payload);
        }
    }
    for id in 1..=3 {
        assert_eq!(received[&id], format!("stream-{id}").as_bytes());
    }
    let _ = stop.send(true);
    Ok(())
}

#[tokio::test]
async fn open_failure_does_not_break_an_existing_stream() -> Result<()> {
    let target = start_echo_server().await?;
    let temporary = TcpListener::bind("127.0.0.1:0").await?;
    let actually_unavailable = temporary.local_addr()?;
    drop(temporary);
    let (url, stop) = start_tows().await?;
    let mut websocket = connect_client(&url).await?;

    for (id, address) in [(1, target), (2, actually_unavailable.to_string())] {
        websocket
            .send(Message::Binary(
                Frame::new(FrameType::Open, id, address.into_bytes())?
                    .encode()
                    .into(),
            ))
            .await?;
    }
    let mut ok = false;
    let mut failed = false;
    while !(ok && failed) {
        let frame = next_frame(&mut websocket).await?;
        ok |= frame.kind == FrameType::OpenOk && frame.tunnel_id == 1;
        failed |= frame.kind == FrameType::OpenFail && frame.tunnel_id == 2;
    }
    websocket
        .send(Message::Binary(
            Frame::new(FrameType::Data, 1, b"still-alive".to_vec())?
                .encode()
                .into(),
        ))
        .await?;
    let echoed = next_frame(&mut websocket).await?;
    assert_eq!(echoed.payload, b"still-alive");
    let _ = stop.send(true);
    Ok(())
}

#[tokio::test]
async fn eof_half_close_keeps_the_reverse_direction_readable() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let target = listener.local_addr()?.to_string();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        stream.read_to_end(&mut request).await.unwrap();
        assert_eq!(request, b"request");
        stream.write_all(b"response-after-eof").await.unwrap();
        stream.shutdown().await.unwrap();
    });

    let (url, stop) = start_tows().await?;
    let mut websocket = connect_client(&url).await?;
    websocket
        .send(Message::Binary(
            Frame::new(FrameType::Open, 9, target.into_bytes())?
                .encode()
                .into(),
        ))
        .await?;
    assert_eq!(next_frame(&mut websocket).await?.kind, FrameType::OpenOk);
    websocket
        .send(Message::Binary(
            Frame::new(FrameType::Data, 9, b"request".to_vec())?
                .encode()
                .into(),
        ))
        .await?;
    websocket
        .send(Message::Binary(
            Frame::new(FrameType::Eof, 9, Vec::new())?.encode().into(),
        ))
        .await?;

    let mut response = Vec::new();
    loop {
        let frame = next_frame(&mut websocket).await?;
        match frame.kind {
            FrameType::Data => response.extend_from_slice(&frame.payload),
            FrameType::Eof => break,
            _ => {}
        }
    }
    assert_eq!(response, b"response-after-eof");
    let _ = stop.send(true);
    Ok(())
}

#[tokio::test]
async fn client_rejects_a_different_protocol_version() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let _hello = websocket.next().await.unwrap().unwrap();
        let mut payload = 1_u16.to_be_bytes().to_vec();
        payload.extend_from_slice(b"old-tows 0.5.0");
        let ack = Frame::new(FrameType::HelloAck, 0, payload).unwrap();
        websocket
            .send(Message::Binary(ack.encode().into()))
            .await
            .unwrap();
    });
    let (mut websocket, _) = tokio_tungstenite::connect_async(format!("ws://{address}/")).await?;
    let error = tcp_over_websocket::network::client_handshake(&mut websocket, "test-client")
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("协议版本不匹配"));
    Ok(())
}

#[tokio::test]
async fn server_rejects_clients_that_never_send_hello() -> Result<()> {
    let (url, stop) = start_tows().await?;
    let (mut websocket, _) = tokio_tungstenite::connect_async(url).await?;
    let ended = tokio::time::timeout(std::time::Duration::from_secs(6), websocket.next()).await;
    assert!(ended.is_ok(), "服务端未在 5 秒 HELLO 时限后关闭连接");
    let _ = stop.send(true);
    Ok(())
}

#[tokio::test]
async fn bulk_stream_does_not_starve_an_interactive_stream() -> Result<()> {
    let bulk_listener = TcpListener::bind("127.0.0.1:0").await?;
    let bulk_target = bulk_listener.local_addr()?.to_string();
    tokio::spawn(async move {
        let (mut stream, _) = bulk_listener.accept().await.unwrap();
        let mut trigger = [0_u8; 1];
        stream.read_exact(&mut trigger).await.unwrap();
        let chunk = vec![0x5a; 65_535];
        for _ in 0..64 {
            stream.write_all(&chunk).await.unwrap();
        }
    });
    let echo_target = start_echo_server().await?;
    let (url, stop) = start_tows().await?;
    let mut websocket = connect_client(&url).await?;
    for (id, target) in [(1, bulk_target), (2, echo_target)] {
        websocket
            .send(Message::Binary(
                Frame::new(FrameType::Open, id, target.into_bytes())?
                    .encode()
                    .into(),
            ))
            .await?;
    }
    let mut opened = HashSet::new();
    while opened.len() < 2 {
        let frame = next_frame(&mut websocket).await?;
        if frame.kind == FrameType::OpenOk {
            opened.insert(frame.tunnel_id);
        }
    }
    websocket
        .send(Message::Binary(
            Frame::new(FrameType::Data, 1, vec![1])?.encode().into(),
        ))
        .await?;
    websocket
        .send(Message::Binary(
            Frame::new(FrameType::Data, 2, b"ping".to_vec())?
                .encode()
                .into(),
        ))
        .await?;

    let mut bulk_frames = 0;
    loop {
        let frame = next_frame(&mut websocket).await?;
        if frame.kind == FrameType::Data && frame.tunnel_id == 2 {
            assert_eq!(frame.payload, b"ping");
            break;
        }
        if frame.kind == FrameType::Data && frame.tunnel_id == 1 {
            bulk_frames += 1;
        }
    }
    assert!(bulk_frames < 64, "交互流直到大流量全部发送完才得到调度");
    let _ = stop.send(true);
    Ok(())
}
