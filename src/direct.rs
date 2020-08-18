use std::collections::HashMap;
use std::net::SocketAddr;

use tokio::net::TcpListener;
use tokio::net::TcpStream;

use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::stream::StreamExt;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::interval;

#[allow(unused)]
use log::{debug, info, warn};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct DirectNet {
    uid: Arc<AtomicU64>,
    addr_by_id: HashMap<u64, SocketAddr>,
    session_mailbox: Arc<RwLock<HashMap<u64, UnboundedSender<Vec<u8>>>>>,
    out_tx: UnboundedSender<(u64, Vec<u8>)>,
}

impl DirectNet {
    pub fn new(peers: Vec<SocketAddr>, out_tx: UnboundedSender<(u64, Vec<u8>)>) -> Self {
        let addr_by_id: HashMap<u64, SocketAddr> = peers
            .into_iter()
            .enumerate()
            .map(|(id, addr)| (id as u64, addr))
            .collect();
        let session_mailbox = HashMap::<u64, UnboundedSender<Vec<u8>>>::new();
        let session_mailbox = Arc::new(RwLock::new(session_mailbox));
        Self {
            uid: Arc::new(AtomicU64::new(1)),
            addr_by_id,
            session_mailbox,
            out_tx,
        }
    }

    pub fn start(self, listen_addr: SocketAddr) {
        tokio::spawn(async move {
            let mut listener = TcpListener::bind(listen_addr).await.unwrap();
            while let Some(stream) = listener.incoming().next().await {
                match stream {
                    Ok(stream) => {
                        let uid = self.uid.fetch_add(1, Ordering::SeqCst);
                        info!("accept new stream #{}", uid);
                        tokio::spawn(self.clone().serve(uid, stream));
                    }
                    Err(_e) => (),
                }
            }
        });
    }

    async fn serve(self, uid: u64, stream: TcpStream) {
        let (inner_tx, inner_rx) = unbounded_channel::<Vec<u8>>();
        let (net_read, net_write) = stream.into_split();
        let inflow = Self::serve_inflow(uid, net_read, self.out_tx.clone());
        let outflow = Self::serve_outflow(net_write, inner_rx);
        self.session_mailbox.write().await.insert(uid, inner_tx);
        let inflow_task = tokio::spawn(inflow);
        let outflow_task = tokio::spawn(outflow);
        tokio::spawn(async move {
            inflow_task.await.unwrap();
            self.session_mailbox.write().await.remove(&uid);
            outflow_task.await.unwrap();
        });
    }

    async fn serve_inflow(
        uid: u64,
        mut net_read: OwnedReadHalf,
        send_to: UnboundedSender<(u64, Vec<u8>)>,
    ) {
        loop {
            match net_read.read_u64().await {
                Ok(len) => {
                    let mut buf = vec![0; len as usize];
                    net_read.read_exact(buf.as_mut_slice()).await.unwrap();
                    send_to.send((uid, buf)).unwrap();
                }
                Err(_e) => break,
            }
        }
    }

    async fn serve_outflow(
        mut net_write: OwnedWriteHalf,
        mut recv_from: UnboundedReceiver<Vec<u8>>,
    ) {
        loop {
            match recv_from.recv().await {
                Some(msg) => {
                    net_write
                        .write_all(
                            [&msg.len().to_be_bytes(), msg.as_slice()]
                                .concat()
                                .as_slice(),
                        )
                        .await
                        .unwrap();
                }
                None => break,
            }
        }
    }

    pub fn send_message(&self, session_id: u64, message: &[u8]) {
        let message = message.to_owned();
        let session_mailbox = self.session_mailbox.clone();
        tokio::spawn(async move {
            if let Some(tx) = session_mailbox.read().await.get(&session_id) {
                tx.send(message).unwrap();
            } else {
                warn!("session#{} lost.", session_id);
            }
        });
    }

    pub fn broadcast_message(self, message: &[u8]) {
        self.addr_by_id.values().for_each(|&addr| {
            let this = self.clone();
            let message = message.to_owned();
            tokio::spawn(async move {
                let session_id = loop {
                    if let Ok(stream) = TcpStream::connect(addr).await {
                        let uid = this.uid.fetch_add(1, Ordering::SeqCst);
                        this.clone().serve(uid, stream).await;
                        break uid;
                    }
                    let d = Duration::from_millis(100);
                    let mut delay = interval(d);
                    delay.tick().await;
                };
                this.send_message(session_id, message.as_slice());
            });
        });
    }
}
