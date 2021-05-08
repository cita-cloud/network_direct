use std::collections::HashMap;
use std::net::SocketAddr;

use tokio::net::TcpListener;
use tokio::net::TcpStream;

use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::interval;

use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::StreamExt;

use futures::channel::oneshot;
#[allow(unused)]
use log::{debug, info, warn};
use tokio::io::Result;
use tokio::sync::mpsc;

#[derive(Debug)]
struct Session {
    session_id: u64,
    // None means Inbound connection
    // Some means Outbound connection
    peer_addr: Option<String>,
    msg_sender: UnboundedSender<Vec<u8>>,
    close_signaler: oneshot::Sender<()>,
}

impl Session {
    fn new(
        session_id: u64,
        peer_addr: Option<String>,
        conn: TcpStream,
        net_event_sender: Sender<NetEvent>,
    ) -> Self {
        let (msg_sender, msg_receiver) = mpsc::unbounded_channel();
        let (tcp_rx, tcp_tx) = conn.into_split();
        let inflow = Self::serve_inflow(session_id, tcp_rx, net_event_sender.clone());
        let outflow = Self::serve_outflow(tcp_tx, msg_receiver);

        let (close_signaler, close_waiter) = oneshot::channel::<()>();

        tokio::spawn(async move {
            tokio::select! {
                _ = close_waiter => {
                    info!("session closed");
                }
                _ = inflow => {
                    info!("inflow end");
                }
                _ = outflow => {
                    info!("outflow end");
                }
            };
            net_event_sender
                .send(NetEvent::SessionClosed { session_id })
                .await
                .unwrap();
        });

        Self {
            session_id,
            peer_addr,
            msg_sender,
            close_signaler,
        }
    }

    async fn serve_outflow(mut tcp_tx: OwnedWriteHalf, mut net_rx: UnboundedReceiver<Vec<u8>>) {
        while let Some(data) = net_rx.recv().await {
            let payload = [&(data.len() as u64).to_be_bytes(), &data[..]].concat();
            match tcp_tx.write_all(payload.as_slice()).await {
                Ok(_) => (),
                Err(e) => warn!("Session tcp send failed: `{}`", e),
            }
        }
    }

    async fn serve_inflow(session_id: u64, mut tcp_rx: OwnedReadHalf, net_tx: Sender<NetEvent>) {
        loop {
            match tcp_rx.read_u64().await {
                Ok(len) => {
                    let mut buf = vec![0; len as usize];
                    match tcp_rx.read_exact(&mut buf).await {
                        Ok(_) => {
                            let event = NetEvent::MessageReceived {
                                session_id,
                                data: buf,
                            };
                            net_tx.send(event).await.unwrap();
                        }
                        Err(e) => {
                            warn!("Session inflow read failed: `{}`", e);
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "read inflow prefix length failed, closing session, reason: `{}`",
                        e
                    );
                    break;
                }
            }
        }
    }

    fn sender(&self) -> UnboundedSender<Vec<u8>> {
        self.msg_sender.clone()
    }
}

#[derive(Debug)]
pub enum NetEvent {
    SessionOpen {
        peer_addr: Option<String>,
        conn: TcpStream,
    },
    SessionClosed {
        session_id: u64,
    },
    MessageReceived {
        session_id: u64,
        data: Vec<u8>,
    },
    SendMessage {
        session_id: u64,
        data: Vec<u8>,
    },
    InboundConnection {
        conn: TcpStream,
    },
    OutboundConnection {
        addr: String,
    },
    BroadcastMessage {
        msg: Vec<u8>,
    },
}

#[derive(Debug)]
pub struct DirectNet {
    session_id: u64,
    sessions: HashMap<u64, Session>,
    listen_addr: SocketAddr,
    net_event_sender: mpsc::Sender<NetEvent>,
    net_event_receiver: mpsc::Receiver<NetEvent>,
    outbound_sender: mpsc::UnboundedSender<(u64, Vec<u8>)>,
}

impl DirectNet {
    pub fn new(
        listen_addr: SocketAddr,
        outbound_sender: mpsc::UnboundedSender<(u64, Vec<u8>)>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(64);
        Self {
            session_id: 0,
            sessions: HashMap::new(),
            listen_addr,
            net_event_sender: tx,
            net_event_receiver: rx,
            outbound_sender,
        }
    }

    pub async fn run(&mut self) {
        self.listen(self.listen_addr).await.unwrap();
        while let Some(event) = self.net_event_receiver.recv().await {
            self.handle_net_event(event);
        }
    }

    pub async fn listen(&self, addr: SocketAddr) -> Result<()> {
        let mut listener = TcpListenerStream::new(TcpListener::bind(addr).await?);
        let event_sender = self.net_event_sender.clone();
        tokio::spawn(async move {
            while let Some(conn) = listener.next().await {
                match conn {
                    Ok(conn) => {
                        if let Err(e) = event_sender
                            .send(NetEvent::InboundConnection { conn })
                            .await
                        {
                            warn!("net event send error: `{}`", e);
                            return;
                        }
                    }
                    Err(e) => warn!("Bad stream accepted: `{}`", e),
                }
            }
        });
        Ok(())
    }

    pub fn sender(&self) -> mpsc::Sender<NetEvent> {
        self.net_event_sender.clone()
    }

    fn next_session(&mut self) -> u64 {
        let current_id = self.session_id;
        self.session_id += 1;
        current_id
    }

    fn handle_net_event(&mut self, event: NetEvent) {
        match event {
            NetEvent::InboundConnection { conn } => {
                let event_sender = self.net_event_sender.clone();
                tokio::spawn(async move {
                    event_sender
                        .send(NetEvent::SessionOpen {
                            peer_addr: None,
                            conn,
                        })
                        .await
                        .unwrap();
                });
            }
            NetEvent::OutboundConnection { addr } => {
                let event_sender = self.net_event_sender.clone();

                let is_connected = self.sessions.values().any(|s| {
                    s.peer_addr
                        .as_ref()
                        .map(|paddr| paddr == &addr)
                        .unwrap_or(false)
                });

                if !is_connected {
                    tokio::spawn(async move {
                        let conn = connect_with_retry(&addr).await;
                        event_sender
                            .send(NetEvent::SessionOpen {
                                peer_addr: Some(addr),
                                conn,
                            })
                            .await
                            .unwrap();
                    });
                }
            }
            NetEvent::MessageReceived { session_id, data } => {
                self.outbound_sender.send((session_id, data)).unwrap();
            }
            NetEvent::SendMessage { session_id, data } => {
                if let Some(ref sess) = self.sessions.get(&session_id) {
                    if let Err(e) = sess.sender().send(data) {
                        warn!("Send msg failed: {}", e);
                    }
                }
            }
            NetEvent::BroadcastMessage { msg } => {
                for sess in self.sessions.values().filter(|s| s.peer_addr.is_some()) {
                    if let Err(e) = sess.sender().send(msg.clone()) {
                        warn!("Send msg failed: {}", e);
                    }
                }
            }
            NetEvent::SessionOpen { peer_addr, conn } => {
                let session_id = self.next_session();

                info!(
                    "new session opened, peer_addr: `{:?}`, session_id: `{}`",
                    peer_addr, session_id
                );

                let session =
                    Session::new(session_id, peer_addr, conn, self.net_event_sender.clone());
                self.sessions.insert(session_id, session);
            }
            NetEvent::SessionClosed { session_id } => {
                info!("session `{}` close", session_id);
                self.sessions.remove(&session_id);
            }
        }
    }
}

async fn connect_with_retry(addr: &str) -> TcpStream {
    let retry_secs = Duration::from_millis(500);
    let mut retry_interval = interval(retry_secs);
    loop {
        retry_interval.tick().await;
        if let Ok(stream) = TcpStream::connect(addr).await {
            return stream;
        }
        warn!(
            "try to connect `{}` failed, retry in {} ms",
            addr,
            retry_secs.as_millis()
        );
    }
}
