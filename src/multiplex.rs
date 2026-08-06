use anyhow::{Context, Result, anyhow};
use futures_util::SinkExt;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::{Notify, Semaphore, mpsc};
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, frame::coding::CloseCode};
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes};

use crate::protocol::Frame;

const FLOW_QUEUE_FRAMES: usize = 16;
const TOTAL_QUEUE_BYTES: usize = 4 * 1024 * 1024;
const CONTROL_QUEUE_FRAMES: usize = 256;

struct QueuedFrame {
    frame: Frame,
    _permit: tokio::sync::OwnedSemaphorePermit,
    sent: Option<tokio::sync::oneshot::Sender<()>>,
}

enum WriterCommand {
    Frame(Frame),
    FrameAndRemove(Frame),
    Register(u16, mpsc::Receiver<QueuedFrame>),
    Remove(u16),
    ProtocolClose(String),
    NormalClose,
    Raw(Message),
}

#[derive(Clone)]
pub(crate) struct WsWriter {
    commands: mpsc::Sender<WriterCommand>,
    notify: Arc<Notify>,
    total_bytes: Arc<Semaphore>,
}

pub(crate) struct FlowWriter {
    sender: mpsc::Sender<QueuedFrame>,
    notify: Arc<Notify>,
    total_bytes: Arc<Semaphore>,
}

impl WsWriter {
    pub async fn send(&self, frame: Frame) -> Result<()> {
        self.commands
            .send(WriterCommand::Frame(frame))
            .await
            .map_err(|_| anyhow!("WebSocket writer task has stopped"))
    }

    /// Drop a stream's queued frames and then send its terminal control frame atomically.
    pub async fn send_and_remove(&self, frame: Frame) -> Result<()> {
        self.commands
            .send(WriterCommand::FrameAndRemove(frame))
            .await
            .map_err(|_| anyhow!("WebSocket writer task has stopped"))
    }

    pub async fn register(&self, tunnel_id: u16) -> Result<FlowWriter> {
        let (sender, receiver) = mpsc::channel(FLOW_QUEUE_FRAMES);
        self.commands
            .send(WriterCommand::Register(tunnel_id, receiver))
            .await
            .map_err(|_| anyhow!("WebSocket writer task has stopped"))?;
        Ok(FlowWriter {
            sender,
            notify: Arc::clone(&self.notify),
            total_bytes: Arc::clone(&self.total_bytes),
        })
    }

    pub async fn remove(&self, tunnel_id: u16) {
        let _ = self.commands.send(WriterCommand::Remove(tunnel_id)).await;
    }

    pub async fn protocol_close(&self, reason: impl Into<String>) {
        let _ = self
            .commands
            .send(WriterCommand::ProtocolClose(reason.into()))
            .await;
    }

    pub async fn normal_close(&self) {
        let _ = self.commands.send(WriterCommand::NormalClose).await;
    }

    pub async fn raw(&self, message: Message) {
        let _ = self.commands.send(WriterCommand::Raw(message)).await;
    }
}

impl FlowWriter {
    pub async fn send(&self, frame: Frame) -> Result<()> {
        self.enqueue(frame, None).await
    }

    /// 等到帧确实交给 WebSocket sink，供 EOF 保证严格顺序。
    pub async fn send_flushed(&self, frame: Frame) -> Result<()> {
        let (sent, receiver) = tokio::sync::oneshot::channel();
        self.enqueue(frame, Some(sent)).await?;
        receiver
            .await
            .map_err(|_| anyhow!("WebSocket writer stopped before sending the frame"))
    }

    async fn enqueue(
        &self,
        frame: Frame,
        sent: Option<tokio::sync::oneshot::Sender<()>>,
    ) -> Result<()> {
        let bytes = frame.payload.len().max(1) as u32;
        let permit = Arc::clone(&self.total_bytes)
            .acquire_many_owned(bytes)
            .await
            .context("WebSocket control queue is closed")?;
        self.sender
            .send(QueuedFrame {
                frame,
                _permit: permit,
                sent,
            })
            .await
            .map_err(|_| anyhow!("WebSocket queue for this tunnel is closed"))?;
        self.notify.notify_one();
        Ok(())
    }
}

pub(crate) fn spawn_writer<S>(
    mut sink: futures_util::stream::SplitSink<tokio_tungstenite::WebSocketStream<S>, Message>,
) -> (WsWriter, tokio::task::JoinHandle<Result<()>>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (commands, mut command_rx) = mpsc::channel(CONTROL_QUEUE_FRAMES);
    let notify = Arc::new(Notify::new());
    let writer = WsWriter {
        commands,
        notify: Arc::clone(&notify),
        total_bytes: Arc::new(Semaphore::new(TOTAL_QUEUE_BYTES)),
    };

    let task = tokio::spawn(async move {
        let mut flows: HashMap<u16, mpsc::Receiver<QueuedFrame>> = HashMap::new();
        let mut order = VecDeque::new();

        loop {
            while let Ok(command) = command_rx.try_recv() {
                if apply_command(command, &mut flows, &mut order, &mut sink).await? {
                    return Ok(());
                }
            }

            let mut sent = false;
            let checks = order.len();
            for _ in 0..checks {
                let Some(id) = order.pop_front() else { break };
                let mut keep = true;
                if let Some(receiver) = flows.get_mut(&id) {
                    match receiver.try_recv() {
                        Ok(queued) => {
                            sink.send(Message::Binary(queued.frame.encode().into()))
                                .await
                                .context("failed to send a WebSocket data frame")?;
                            if let Some(sent) = queued.sent {
                                let _ = sent.send(());
                            }
                            sent = true;
                        }
                        Err(mpsc::error::TryRecvError::Disconnected) => keep = false,
                        Err(mpsc::error::TryRecvError::Empty) => {}
                    }
                } else {
                    keep = false;
                }
                if keep {
                    order.push_back(id);
                } else {
                    flows.remove(&id);
                }
                if sent {
                    break;
                }
            }
            if sent {
                continue;
            }

            tokio::select! {
                command = command_rx.recv() => {
                    let Some(command) = command else { return Ok(()) };
                    if apply_command(command, &mut flows, &mut order, &mut sink).await? {
                        return Ok(());
                    }
                }
                _ = notify.notified() => {}
            }
        }
    });
    (writer, task)
}

async fn apply_command<S>(
    command: WriterCommand,
    flows: &mut HashMap<u16, mpsc::Receiver<QueuedFrame>>,
    order: &mut VecDeque<u16>,
    sink: &mut futures_util::stream::SplitSink<tokio_tungstenite::WebSocketStream<S>, Message>,
) -> Result<bool>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    match command {
        WriterCommand::Frame(frame) => sink
            .send(Message::Binary(frame.encode().into()))
            .await
            .context("failed to send a WebSocket control frame")?,
        WriterCommand::FrameAndRemove(frame) => {
            flows.remove(&frame.tunnel_id);
            order.retain(|existing| *existing != frame.tunnel_id);
            sink.send(Message::Binary(frame.encode().into()))
                .await
                .context("failed to send a terminal WebSocket control frame")?;
        }
        WriterCommand::Register(id, receiver) => {
            flows.insert(id, receiver);
            order.retain(|existing| *existing != id);
            order.push_back(id);
        }
        WriterCommand::Remove(id) => {
            flows.remove(&id);
            order.retain(|existing| *existing != id);
        }
        WriterCommand::ProtocolClose(reason) => {
            let reason = truncate_close_reason(&reason);
            let _ = sink
                .send(Message::Close(Some(CloseFrame {
                    code: CloseCode::Protocol,
                    reason: Utf8Bytes::from(reason),
                })))
                .await;
            return Ok(true);
        }
        WriterCommand::NormalClose => {
            let _ = sink.send(Message::Close(None)).await;
            return Ok(true);
        }
        WriterCommand::Raw(message) => sink
            .send(message)
            .await
            .context("failed to send a WebSocket control message")?,
    }
    Ok(false)
}

fn truncate_close_reason(reason: &str) -> String {
    const MAX: usize = 123;
    if reason.len() <= MAX {
        return reason.to_string();
    }
    let mut end = MAX - 3;
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &reason[..end])
}
