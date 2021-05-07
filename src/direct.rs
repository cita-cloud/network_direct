#![allow(dead_code)]
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

const MAX_ADDR_LEN: usize = 128;

#[derive(Debug)]
struct Session {
    session_id: u64,
    peer_addr: SocketAddr,
    msg_sender: UnboundedSender<Vec<u8>>,
    close_signaler: oneshot::Sender<()>,
}

impl Session {
    fn new(
        session_id: u64,
        peer_addr: SocketAddr,
        conn: TcpStream,
        net_event_sender: Sender<NetEvent>,
    ) -> Self {
        let (msg_sender, msg_receiver) = mpsc::unbounded_channel();
        let (tcp_rx, tcp_tx) = conn.into_split();
        let inflow = Self::serve_inflow(session_id, tcp_rx, net_event_sender);
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
        while let Ok(len) = tcp_rx.read_u64().await {
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
    }

    fn sender(&self) -> UnboundedSender<Vec<u8>> {
        self.msg_sender.clone()
    }
}

#[derive(Debug)]
pub enum NetEvent {
    SessionOpen {
        peer_addr: SocketAddr,
        conn: TcpStream,
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
        addr: SocketAddr,
    },
    BroadcastMessage {
        msg: Vec<u8>,
    },
    CloseNet,
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
        while let Some(msg) = self.net_event_receiver.recv().await {
            match msg {
                NetEvent::CloseNet => return,
                event => self.handle_net_event(event),
            }
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

    pub async fn send_message(&mut self, session_id: u64, msg: &[u8]) {
        let msg = msg.to_owned();
        if let Err(e) = self
            .net_event_sender
            .send(NetEvent::SendMessage {
                session_id,
                data: msg,
            })
            .await
        {
            warn!("net event send error: `{}`", e);
        }
    }

    pub async fn broadcast_message(&mut self, msg: &[u8]) {
        let msg = msg.to_owned();
        if let Err(e) = self
            .net_event_sender
            .send(NetEvent::BroadcastMessage { msg })
            .await
        {
            warn!("net event send error: `{}`", e);
        }
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
            NetEvent::InboundConnection { mut conn } => {
                let event_sender = self.net_event_sender.clone();
                tokio::spawn(async move {
                    match conn.read_u64().await {
                        Ok(len) if (len as usize) < MAX_ADDR_LEN => {
                            let mut buf = vec![0; len as usize];
                            match conn.read_exact(buf.as_mut_slice()).await {
                                Ok(_) => {
                                    let addr_str = std::str::from_utf8(buf.as_slice()).unwrap();
                                    let peer_addr = addr_str.parse::<SocketAddr>().unwrap();
                                    event_sender
                                        .send(NetEvent::SessionOpen { peer_addr, conn })
                                        .await
                                        .unwrap();
                                }
                                Err(e) => warn!(
                                    "Inbound connection read addr with len={} failed: {}",
                                    len, e
                                ),
                            }
                        }
                        Ok(bad_len) => {
                            warn!(
                                "Inbound connection recv addr len `{}` which is too long.",
                                bad_len
                            );
                        }
                        Err(e) => {
                            warn!("read peer_addr len from inbound connection failed: `{}`", e);
                        }
                    }
                });
            }
            NetEvent::OutboundConnection { addr } => {
                let event_sender = self.net_event_sender.clone();
                let listen_addr = self.listen_addr;
                tokio::spawn(async move {
                    let mut conn = connect_with_retry(addr).await;
                    let addr_bytes = listen_addr.to_string().as_bytes().to_owned();
                    let payload =
                        [&(addr_bytes.len() as u64).to_be_bytes(), &addr_bytes[..]].concat();
                    conn.write_all(payload.as_slice()).await.unwrap();
                    event_sender
                        .send(NetEvent::SessionOpen {
                            peer_addr: addr,
                            conn,
                        })
                        .await
                        .unwrap();
                });
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
                for sess in self.sessions.values() {
                    if let Err(e) = sess.sender().send(msg.clone()) {
                        warn!("Send msg failed: {}", e);
                    }
                }
            }
            NetEvent::SessionOpen { peer_addr, conn } => {
                if self
                    .sessions
                    .values()
                    .find(|&sess| sess.peer_addr == peer_addr)
                    .is_none()
                {
                    let session_id = self.next_session();
                    let session =
                        Session::new(session_id, peer_addr, conn, self.net_event_sender.clone());
                    self.sessions.insert(session_id, session);
                } else {
                    warn!("repeated connection to: `{}`", peer_addr);
                }
            }
            // CloseNet event would have been handled outside.
            NetEvent::CloseNet => unreachable!(),
        }
    }
}

async fn connect_with_retry(addr: SocketAddr) -> TcpStream {
    let mut retry_interval = interval(Duration::from_millis(500));
    loop {
        retry_interval.tick().await;
        if let Ok(stream) = TcpStream::connect(addr).await {
            return stream;
        }
    }
}
