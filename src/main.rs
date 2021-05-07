// Copyright Rivtower Technologies LLC.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod config;
mod direct;

use clap::Clap;
use git_version::git_version;
use log::{debug, info, warn};

const GIT_VERSION: &str = git_version!(
    args = ["--tags", "--always", "--dirty=-modified"],
    fallback = "unknown"
);
const GIT_HOMEPAGE: &str = "https://github.com/rink1969/cita_ng_network";

/// network service
#[derive(Clap)]
#[clap(version = "0.1.0", author = "Rivtower Technologies.")]
struct Opts {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Clap)]
enum SubCommand {
    /// print information from git
    #[clap(name = "git")]
    GitInfo,
    /// run this service
    #[clap(name = "run")]
    Run(RunOpts),
}

/// A subcommand for run
#[derive(Clap)]
struct RunOpts {
    /// Sets grpc port of this service.
    #[clap(short = 'p', long = "port", default_value = "50000")]
    grpc_port: String,
    /// Sets path of network key file.
    /// It's not used for network_direct. Leave it here for compatibility.
    #[allow(unused)]
    #[clap(short = 'k', long = "key_file", default_value = "network-key")]
    key_file: String,
}

fn main() {
    ::std::env::set_var("RUST_BACKTRACE", "full");

    let opts: Opts = Opts::parse();

    match opts.subcmd {
        SubCommand::GitInfo => {
            println!("git version: {}", GIT_VERSION);
            println!("homepage: {}", GIT_HOMEPAGE);
        }
        SubCommand::Run(opts) => {
            // init log4rs
            log4rs::init_file("network-log4rs.yaml", Default::default()).unwrap();
            info!("grpc port of this service: {}", opts.grpc_port);
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(run(opts));
        }
    }
}

use config::NetConfig;
use direct::DirectNet;
use prost::Message;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::RwLock;
use tokio::time::interval;

use cita_cloud_proto::common::Empty;
use cita_cloud_proto::common::SimpleResponse;
use cita_cloud_proto::network::network_msg_handler_service_client::NetworkMsgHandlerServiceClient;
use cita_cloud_proto::network::{
    network_service_server::NetworkService, network_service_server::NetworkServiceServer,
    NetworkMsg, NetworkStatusResponse, RegisterInfo,
};
use tonic::{transport::Server, Request, Response, Status};

use direct::NetEvent;

async fn run(opts: RunOpts) {
    let path = "network-config.toml";
    let buffer = std::fs::read_to_string(path)
        .unwrap_or_else(|err| panic!("Error while loading config: [{}]", err));
    let config = NetConfig::new(&buffer);
    let listen_addr: SocketAddr = format!("0.0.0.0:{}", config.port).parse().unwrap();
    let peers: Vec<SocketAddr> = config
        .peers
        .into_iter()
        .map(|peer| {
            format!("{}:{}", peer.ip, peer.port)
                .parse()
                .expect("failed to parse socket address")
        })
        .collect();

    let (network_tx, network_rx) = unbounded_channel();
    let mut direct_net = DirectNet::new(listen_addr, network_tx);
    let net_event_sender = direct_net.sender();
    let dispatch_table = Arc::new(RwLock::new(HashMap::new()));

    tokio::spawn(keep_connection(peers, net_event_sender.clone()));
    tokio::spawn(run_network(network_rx, dispatch_table.clone()));
    tokio::spawn(run_grpc_server(
        opts.grpc_port,
        net_event_sender,
        dispatch_table,
    ));
    direct_net.run().await;
}

async fn keep_connection(peers: Vec<SocketAddr>, net_event_sender: mpsc::Sender<NetEvent>) {
    let mut recheck_interval = interval(Duration::from_secs(15));
    loop {
        for &addr in peers.iter() {
            let event = NetEvent::OutboundConnection { addr };
            net_event_sender.send(event).await.unwrap();
        }
        recheck_interval.tick().await;
    }
}

struct NetworkServer {
    net_event_sender: mpsc::Sender<NetEvent>,
    dispatch_table: Arc<RwLock<HashMap<String, (String, String)>>>,
}

impl NetworkServer {
    fn new(
        net_event_sender: mpsc::Sender<NetEvent>,
        dispatch_table: Arc<RwLock<HashMap<String, (String, String)>>>,
    ) -> Self {
        Self {
            net_event_sender,
            dispatch_table,
        }
    }
}

#[tonic::async_trait]
impl NetworkService for NetworkServer {
    async fn send_msg(
        &self,
        request: Request<NetworkMsg>,
    ) -> Result<Response<SimpleResponse>, Status> {
        debug!("send_msg request: {:?}", request);

        let msg = request.into_inner();
        let mut buf: Vec<u8> = Vec::new();
        if msg.encode(&mut buf).is_ok() {
            let event = NetEvent::SendMessage {
                session_id: msg.origin,
                data: buf,
            };
            if let Err(e) = self.net_event_sender.clone().send(event).await {
                warn!("NetworkServer send failed: `{}`", e);
            }
            let reply = SimpleResponse { is_success: true };
            Ok(Response::new(reply))
        } else {
            Err(Status::internal("encode msg failed"))
        }
    }

    async fn broadcast(
        &self,
        request: Request<NetworkMsg>,
    ) -> Result<Response<SimpleResponse>, Status> {
        debug!("broadcast request: {:?}", request);

        let msg = request.into_inner();
        let mut buf: Vec<u8> = Vec::new();
        if msg.encode(&mut buf).is_ok() {
            let event = NetEvent::BroadcastMessage { msg: buf };
            if let Err(e) = self.net_event_sender.clone().send(event).await {
                warn!("NetworkServer broadcast failed: `{}`", e);
            }
            let reply = SimpleResponse { is_success: true };
            Ok(Response::new(reply))
        } else {
            Err(Status::internal("encode msg failed"))
        }
    }

    async fn get_network_status(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<NetworkStatusResponse>, Status> {
        debug!("register_endpoint request: {:?}", request);

        let reply = NetworkStatusResponse { peer_count: 5 };
        Ok(Response::new(reply))
    }

    async fn register_network_msg_handler(
        &self,
        request: Request<RegisterInfo>,
    ) -> Result<Response<SimpleResponse>, Status> {
        debug!("register_network_msg_handler request: {:?}", request);

        let info = request.into_inner();
        let module_name = info.module_name;
        let hostname = info.hostname;
        let port = info.port;

        let mut dispatch_table = self.dispatch_table.write().await;
        dispatch_table.insert(module_name, (hostname, port));

        let reply = SimpleResponse { is_success: true };
        Ok(Response::new(reply))
    }
}

async fn run_grpc_server(
    port: String,
    net_event_sender: mpsc::Sender<NetEvent>,
    dispatch_table: Arc<RwLock<HashMap<String, (String, String)>>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let addr_str = format!("127.0.0.1:{}", port);
    let addr = addr_str.parse()?;
    let network_server = NetworkServer::new(net_event_sender, dispatch_table);

    Server::builder()
        .add_service(NetworkServiceServer::new(network_server))
        .serve(addr)
        .await?;
    Ok(())
}

async fn dispatch_network_msg(
    client_map: Arc<
        RwLock<HashMap<String, NetworkMsgHandlerServiceClient<tonic::transport::Channel>>>,
    >,
    port: String,
    msg: NetworkMsg,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = {
        let map = client_map.read().await;
        map.get(&port).cloned()
    };

    if client.is_none() {
        let dest_addr = format!("http://127.0.0.1:{}", port);
        let c = NetworkMsgHandlerServiceClient::connect(dest_addr).await?;
        client_map.write().await.insert(port, c.clone());
        client.replace(c);
    }

    let mut client = client.unwrap();
    let request = Request::new(msg);
    let _response = client.process_network_msg(request).await?;

    Ok(())
}

async fn run_network(
    mut network_rx: UnboundedReceiver<(u64, Vec<u8>)>,
    dispatch_table: Arc<RwLock<HashMap<String, (String, String)>>>,
) {
    let client_map = Arc::new(RwLock::new(HashMap::<
        String,
        NetworkMsgHandlerServiceClient<tonic::transport::Channel>,
    >::new()));
    loop {
        if let Some(msg) = network_rx.recv().await {
            let (sid, payload) = msg;
            debug!("received msg {:?} from {}", payload, sid);

            match NetworkMsg::decode(payload.as_slice()) {
                Ok(mut msg) => {
                    msg.origin = sid as u64;
                    let dispatch_table = dispatch_table.clone();
                    let client_map = client_map.clone();
                    tokio::spawn(async move {
                        let port = {
                            let table = dispatch_table.read().await;
                            table.get(&msg.module).cloned().map(|(_, port)| port)
                        };
                        if let Some(port) = port {
                            if let Err(e) = dispatch_network_msg(client_map, port, msg).await {
                                warn!("dispatch error: {:?}", e);
                            }
                        } else {
                            warn!("dipatch port not found");
                        }
                    });
                }
                Err(e) => {
                    warn!("network msg decode failed: {}", e);
                }
            }
        }
    }
}
