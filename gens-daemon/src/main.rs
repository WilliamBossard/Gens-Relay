use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::{broadcast, mpsc, Mutex};
use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};
use url::Url;

use ring::{aead, agreement, hkdf, rand as ring_rand};
use base64::{Engine as _, engine::general_purpose::STANDARD as b64};
use webrtc::api::APIBuilder;
use webrtc::data_channel::data_channel_message::DataChannelMessage;
use webrtc::data_channel::RTCDataChannel;
use webrtc::ice_transport::ice_candidate::{RTCIceCandidate, RTCIceCandidateInit};
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::peer_connection::RTCPeerConnection;

#[derive(Deserialize, Debug)]
#[serde(tag = "type")]
enum ServerMessage {
    #[serde(rename = "welcome")]
    Welcome { id: String },
    #[serde(rename = "peer-message")]
    PeerMessage {
        #[serde(rename = "senderId")]
        sender_id: String,
        payload: serde_json::Value,
    },
    #[serde(rename = "error")]
    Error { message: String },
    #[serde(other)]
    Unknown,
}

#[derive(Serialize, Debug)]
struct ClientAction {
    action: String,
    #[serde(rename = "targetId")]
    target_id: String,
    payload: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
enum SignalingPayload {
    #[serde(rename = "offer")]
    Offer { sdp: RTCSessionDescription, hostname: String },
    #[serde(rename = "answer")]
    Answer { sdp: RTCSessionDescription, hostname: String },
    #[serde(rename = "ice_candidate")]
    IceCandidate { candidate: RTCIceCandidateInit },
    #[serde(rename = "chat")]
    Chat { message: String },
}

#[derive(Serialize, Deserialize, Debug)]
struct DiscoveryPacket {
    id: String,
    hostname: String,
    token: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct PairingOfferPacket {
    #[serde(rename = "type")]
    packet_type: String,
    domain: String,
    token: String,
}

#[derive(Clone, Debug)]
struct PeerInfo {
    id: String,
    ip: String,
}

#[derive(Serialize)]
struct IpcEvent {
    event: String,
    #[serde(flatten)]
    data: serde_json::Value,
}

#[derive(Deserialize, Debug)]
struct IpcCommand {
    action: String,
    target: Option<String>,
    msg: Option<String>,
    path: Option<String>,
}

pub struct CryptoState {
    pub key: aead::LessSafeKey,
    pub tx_nonce: std::sync::atomic::AtomicU64,
}

#[derive(Clone)]
struct AppState {
    peers: Arc<Mutex<HashMap<String, Arc<RTCPeerConnection>>>>,
    data_channels: Arc<Mutex<HashMap<String, Arc<RTCDataChannel>>>>,
    local_peers: Arc<Mutex<HashMap<String, PeerInfo>>>,
    crypto: Arc<Mutex<HashMap<String, Arc<CryptoState>>>>,
    outbound_tx: mpsc::Sender<ClientAction>,
    hostname: String,
    ipc_tx: Arc<broadcast::Sender<String>>,
    local_id: Arc<Mutex<Option<String>>>,
}

fn get_ipc_file_path() -> std::path::PathBuf {
    let mut dir = std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."));
    if dir.file_name().and_then(|s| s.to_str()) == Some("gens-daemon") {
        dir.push("../.ipc_port");
    } else {
        dir.push(".ipc_port");
    }
    dir
}

async fn resolve_target(target: &str, state: &AppState) -> String {
    let peers = state.local_peers.lock().await;
    if let Some(info) = peers.get(target) {
        info.id.clone()
    } else {
        target.to_string()
    }
}

fn create_nonce(counter: u64) -> aead::Nonce {
    let mut bytes = [0u8; 12];
    bytes[4..12].copy_from_slice(&counter.to_be_bytes());
    aead::Nonce::assume_unique_for_key(bytes)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env_path = Path::new(".env");
    let fallback_env_path = Path::new("../.env");
    
    if env_path.exists() {
        dotenvy::from_path(env_path).ok();
    } else if fallback_env_path.exists() {
        dotenvy::from_path(fallback_env_path).ok();
    } else {
        dotenvy::dotenv().ok();
    }

    let my_hostname = env::var("NODE_NAME")
        .or_else(|_| env::var("HOSTNAME"))
        .unwrap_or_else(|_| {
            hostname::get().unwrap_or_else(|_| "Unknown".into()).to_string_lossy().to_string()
        });

    let relay_domain_opt = env::var("RELAY_DOMAIN").ok();
    let auth_token_opt = env::var("AUTH_TOKEN").ok();
    
    let is_paired = relay_domain_opt.is_some() && auth_token_opt.is_some();

    let (outbound_tx, mut outbound_rx) = mpsc::channel::<ClientAction>(100);
    let (ipc_tx, _ipc_rx) = broadcast::channel::<String>(100);

    let state = AppState {
        peers: Arc::new(Mutex::new(HashMap::new())),
        data_channels: Arc::new(Mutex::new(HashMap::new())),
        local_peers: Arc::new(Mutex::new(HashMap::new())),
        crypto: Arc::new(Mutex::new(HashMap::new())),
        outbound_tx: outbound_tx.clone(),
        hostname: my_hostname.clone(),
        ipc_tx: Arc::new(ipc_tx),
        local_id: Arc::new(Mutex::new(None)),
    };

    let (command_tx, mut command_rx) = mpsc::channel::<IpcCommand>(32);
    let ipc_tx_tcp = state.ipc_tx.clone();
    let state_for_ipc = state.clone();
    let is_paired_for_ipc = is_paired;
    
    tokio::spawn(async move {
        if let Ok(listener) = TcpListener::bind("127.0.0.1:0").await {
            let local_addr = listener.local_addr().unwrap();
            let port = local_addr.port();
            println!("[IPC] Server listening on 127.0.0.1:{}", port);
            
            let ipc_file = get_ipc_file_path();
            if let Err(e) = fs::write(&ipc_file, port.to_string()).await {
                eprintln!("[IPC] Error writing .ipc_port file: {}", e);
            }
            
            while let Ok((mut socket, _)) = listener.accept().await {
                let command_tx = command_tx.clone();
                let mut rx = ipc_tx_tcp.subscribe();
                
                if !is_paired_for_ipc {
                    let status_event = IpcEvent {
                        event: "status".to_string(),
                        data: serde_json::json!({"unpaired": true}),
                    };
                    let _ = state_for_ipc.ipc_tx.send(serde_json::to_string(&status_event).unwrap());
                } else if let Some(id) = state_for_ipc.local_id.lock().await.clone() {
                    let status_event = IpcEvent {
                        event: "status".to_string(),
                        data: serde_json::json!({"id": id, "hostname": state_for_ipc.hostname.clone()}),
                    };
                    let _ = state_for_ipc.ipc_tx.send(serde_json::to_string(&status_event).unwrap());
                }
                
                tokio::spawn(async move {
                    let (reader, mut writer) = socket.split();
                    let mut reader = BufReader::new(reader).lines();
                    loop {
                        tokio::select! {
                            Ok(event_str) = rx.recv() => {
                                let _ = writer.write_all(event_str.as_bytes()).await;
                                let _ = writer.write_all(b"\n").await;
                            }
                            result = reader.next_line() => {
                                match result {
                                    Ok(Some(line)) => {
                                        if let Ok(cmd) = serde_json::from_str::<IpcCommand>(&line) {
                                            let _ = command_tx.send(cmd).await;
                                        }
                                    }
                                    _ => break,
                                }
                            }
                        }
                    }
                });
            }
        } else {
            eprintln!("[IPC] Error: Could not start IPC server.");
        }
    });

    if !is_paired {
        println!("[Daemon] Running in Unpaired Mode.");
        let ipc_tx_udp = state.ipc_tx.clone();
        tokio::spawn(async move {
            if let Ok(socket) = UdpSocket::bind("0.0.0.0:8888").await {
                let mut buf = [0; 1024];
                loop {
                    if let Ok((len, _addr)) = socket.recv_from(&mut buf).await {
                        if let Ok(text) = std::str::from_utf8(&buf[..len]) {
                            if let Ok(packet) = serde_json::from_str::<PairingOfferPacket>(text) {
                                if packet.packet_type == "pairing_offer" {
                                    println!("[UDP] Discovered pairing offer from {}", packet.domain);
                                    let event = IpcEvent {
                                        event: "pairing_discovered".to_string(),
                                        data: serde_json::json!({"domain": packet.domain, "token": packet.token}),
                                    };
                                    let _ = ipc_tx_udp.send(serde_json::to_string(&event).unwrap());
                                }
                            }
                        }
                    }
                }
            }
        });
        
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
        }
    }

    let relay_domain = relay_domain_opt.unwrap();
    let auth_token = auth_token_opt.unwrap();

    let mut url = match Url::parse(&relay_domain) {
        Ok(u) => u,
        Err(e) => {
            eprintln!("[Daemon] Error: Invalid RELAY_DOMAIN : {}", e);
            std::process::exit(1);
        }
    };
    url.query_pairs_mut().append_pair("token", &auth_token);

    println!("[Daemon] Starting Daemon [{}]", my_hostname);
    println!("[Daemon] Connecting to {}...", relay_domain);

    let (ws_stream, _) = match connect_async(url.as_str()).await {
        Ok(res) => res,
        Err(e) => {
            eprintln!("[Daemon] Fatal error: Could not connect to signaling server : {}", e);
            std::process::exit(1);
        }
    };

    let (mut write, mut read) = ws_stream.split();
    let api = APIBuilder::new().build();
    let state_console = state.clone();

    let local_peers_clone = state.local_peers.clone();
    let ipc_tx_udp = state.ipc_tx.clone();
    let my_host_clone = my_hostname.clone();
    let auth_token_udp = auth_token.clone();
    tokio::spawn(async move {
        if let Ok(socket) = UdpSocket::bind("0.0.0.0:8888").await {
            let mut buf = [0; 1024];
            loop {
                if let Ok((len, addr)) = socket.recv_from(&mut buf).await {
                    if let Ok(text) = std::str::from_utf8(&buf[..len]) {
                        if let Ok(discovery) = serde_json::from_str::<DiscoveryPacket>(text) {
                            if discovery.token == auth_token_udp && discovery.hostname != my_host_clone {
                                let mut peers = local_peers_clone.lock().await;
                                let mut is_new = false;
                                if !peers.contains_key(&discovery.hostname) {
                                    println!("[UDP] New peer discovered: {} (ID: {}, IP: {})", discovery.hostname, discovery.id, addr.ip());
                                    is_new = true;
                                }
                                peers.insert(discovery.hostname.clone(), PeerInfo {
                                    id: discovery.id.clone(),
                                    ip: addr.ip().to_string(),
                                });
                                if is_new {
                                    let mut peer_list = Vec::new();
                                    for (host, info) in peers.iter() {
                                        peer_list.push(serde_json::json!({"hostname": host, "id": &info.id, "ip": &info.ip}));
                                    }
                                    let event = IpcEvent {
                                        event: "peers_updated".to_string(),
                                        data: serde_json::json!({"peers": peer_list}),
                                    };
                                    let _ = ipc_tx_udp.send(serde_json::to_string(&event).unwrap());
                                }
                            }
                        }
                    }
                }
            }
        }
    });

    loop {
        tokio::select! {
            Some(message_result) = read.next() => {
                match message_result {
                    Ok(message) => {
                        if let WsMessage::Text(text) = message {
                            if let Ok(server_msg) = serde_json::from_str::<ServerMessage>(&text) {
                                match server_msg {
                                    ServerMessage::Welcome { id } => {
                                        println!("[Success] Connected to network! My ID is: {}.", id);
                                        *state.local_id.lock().await = Some(id.clone());
                                        let status_event = IpcEvent {
                                            event: "status".to_string(),
                                            data: serde_json::json!({"id": id.clone(), "hostname": state.hostname.clone()}),
                                        };
                                        let _ = state.ipc_tx.send(serde_json::to_string(&status_event).unwrap());
                                        
                                        let udp_id = id.clone();
                                        let udp_host = state.hostname.clone();
                                        let udp_token = auth_token.clone();
                                        
                                        tokio::spawn(async move {
                                            let broadcast_addrs = env::var("BROADCAST_ADDR")
                                                .map(|a| vec![a])
                                                .unwrap_or_else(|_| vec![
                                                    "255.255.255.255:8888".to_string(),
                                                    "192.168.1.255:8888".to_string(),
                                                    "192.168.0.255:8888".to_string(),
                                                    "10.0.0.255:8888".to_string(),
                                                ]);
                                            if let Ok(socket) = UdpSocket::bind("0.0.0.0:0").await {
                                                if socket.set_broadcast(true).is_ok() {
                                                    let packet = serde_json::to_string(&DiscoveryPacket { 
                                                        id: udp_id, 
                                                        hostname: udp_host,
                                                        token: udp_token 
                                                    }).unwrap();
                                                    loop {
                                                        for addr in &broadcast_addrs {
                                                            let _ = socket.send_to(packet.as_bytes(), addr).await;
                                                        }
                                                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                                                    }
                                                }
                                            }
                                        });
                                    }
                                    ServerMessage::PeerMessage { sender_id, payload } => {
                                        if let Ok(sig_payload) = serde_json::from_value::<SignalingPayload>(payload.clone()) {
                                            let target_id = sender_id.clone();
                                            match sig_payload {
                                                SignalingPayload::Chat { message: _message } => {}
                                                SignalingPayload::Offer { sdp, hostname } => {
                                                    println!("[WebRTC] P2P Offer received from {} ({})", hostname, sender_id);
                                                    let pc = create_peer_connection(&api, target_id.clone(), state.clone()).await?;
                                                    pc.set_remote_description(sdp).await?;
                                                    
                                                    let answer = pc.create_answer(None).await?;
                                                    pc.set_local_description(answer.clone()).await?;
                                                    
                                                    let _ = state.outbound_tx.send(ClientAction {
                                                        action: "send".to_string(),
                                                        target_id: target_id.clone(),
                                                        payload: serde_json::to_value(SignalingPayload::Answer { sdp: answer, hostname: state.hostname.clone() }).unwrap(),
                                                    }).await;
                                                    
                                                    state.peers.lock().await.insert(target_id, pc);
                                                }
                                                SignalingPayload::Answer { sdp, hostname } => {
                                                    println!("[WebRTC] P2P Answer received from {} ({})", hostname, sender_id);
                                                    if let Some(pc) = state.peers.lock().await.get(&sender_id) {
                                                        let _ = pc.set_remote_description(sdp).await;
                                                    }
                                                }
                                                SignalingPayload::IceCandidate { candidate } => {
                                                    if let Some(pc) = state.peers.lock().await.get(&sender_id) {
                                                        let _ = pc.add_ice_candidate(candidate).await;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    ServerMessage::Error { message } => eprintln!("[Server Error] {}", message),
                                    ServerMessage::Unknown => {}
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("[Error] Connection interrupted: {}", e);
                        break;
                    }
                }
            }
            
            Some(cmd) = command_rx.recv() => {
                match cmd.action.as_str() {
                    "list" => {
                        let peers = state.local_peers.lock().await;
                        let mut peer_list = Vec::new();
                        for (host, info) in peers.iter() {
                            peer_list.push(serde_json::json!({"hostname": host, "id": &info.id, "ip": &info.ip}));
                        }
                        let event = IpcEvent {
                            event: "peers_updated".to_string(),
                            data: serde_json::json!({"peers": peer_list}),
                        };
                        let _ = state.ipc_tx.send(serde_json::to_string(&event).unwrap());
                    }
                    action @ "connect" | action @ "p2p" | action @ "sendfile" | action @ "ls" | action @ "download_req" | action @ "add_peer" => {
                        if let Some(raw_target) = cmd.target {
                            let target_id = resolve_target(&raw_target, &state_console).await;
                            
                            match action {
                                "add_peer" => {
                                    if let Some(ip) = cmd.msg {
                                        let mut peers = state.local_peers.lock().await;
                                        peers.insert(ip.clone(), PeerInfo {
                                            id: raw_target.clone(),
                                            ip: ip.clone(),
                                        });
                                        println!("[Daemon] Manual peer added: {} (IP: {})", raw_target, ip);
                                        
                                        let mut peer_list = Vec::new();
                                        for (host, info) in peers.iter() {
                                            peer_list.push(serde_json::json!({"hostname": host, "id": &info.id, "ip": &info.ip}));
                                        }
                                        let event = IpcEvent {
                                            event: "peers_updated".to_string(),
                                            data: serde_json::json!({"peers": peer_list}),
                                        };
                                        let _ = state.ipc_tx.send(serde_json::to_string(&event).unwrap());
                                    }
                                }
                                "connect" => {
                                    println!("[WebRTC] Initiating connection to {} (ID: {})...", raw_target, target_id);
                                    let pc = create_peer_connection(&api, target_id.clone(), state.clone()).await?;
                                    let data_channel = pc.create_data_channel("data", None).await?;
                                    setup_data_channel(&data_channel, target_id.clone(), state.clone()).await;
                                    
                                    state.data_channels.lock().await.insert(target_id.clone(), data_channel);
                                    
                                    let offer = pc.create_offer(None).await?;
                                    pc.set_local_description(offer.clone()).await?;
                                    
                                    let _ = outbound_tx.send(ClientAction {
                                        action: "send".to_string(),
                                        target_id: target_id.clone(),
                                        payload: serde_json::to_value(SignalingPayload::Offer { sdp: offer, hostname: state.hostname.clone() }).unwrap(),
                                    }).await;
                                    
                                    state.peers.lock().await.insert(target_id, pc);
                                }
                                "p2p" => {
                                    if let Some(msg) = cmd.msg {
                                        if let Some(dc) = state.data_channels.lock().await.get(&target_id) {
                                            if let Some(crypto) = state.crypto.lock().await.get(&target_id) {
                                                let req = serde_json::json!({"type": "chat", "msg": msg});
                                                let payload = req.to_string();
                                                let nonce_val = crypto.tx_nonce.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                                let nonce = create_nonce(nonce_val);
                                                let mut in_out = payload.into_bytes();
                                                crypto.key.seal_in_place_append_tag(nonce, aead::Aad::empty(), &mut in_out).unwrap();
                                                
                                                let enc_msg = serde_json::json!({
                                                    "type": "e2ee",
                                                    "nonce": b64.encode(nonce_val.to_be_bytes()),
                                                    "ciphertext": b64.encode(&in_out)
                                                });
                                                let _ = dc.send_text(enc_msg.to_string()).await;
                                            } else {
                                                println!("[E2EE] Error: No crypto session established with {}", target_id);
                                            }
                                        } else {
                                            println!("[Error] No P2P tunnel open with {}.", target_id);
                                        }
                                    }
                                }
                                "ls" => {
                                    if let Some(path) = cmd.path {
                                        if let Some(dc) = state.data_channels.lock().await.get(&target_id) {
                                            if let Some(crypto) = state.crypto.lock().await.get(&target_id) {
                                                let req = serde_json::json!({"type": "ls", "path": path});
                                                let payload = req.to_string();
                                                let nonce_val = crypto.tx_nonce.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                                let nonce = create_nonce(nonce_val);
                                                let mut in_out = payload.into_bytes();
                                                crypto.key.seal_in_place_append_tag(nonce, aead::Aad::empty(), &mut in_out).unwrap();
                                                
                                                let enc_msg = serde_json::json!({
                                                    "type": "e2ee",
                                                    "nonce": b64.encode(nonce_val.to_be_bytes()),
                                                    "ciphertext": b64.encode(&in_out)
                                                });
                                                let _ = dc.send_text(enc_msg.to_string()).await;
                                            }
                                        }
                                    }
                                }
                                "download_req" => {
                                    if let Some(path) = cmd.path {
                                        if let Some(dc) = state.data_channels.lock().await.get(&target_id) {
                                            if let Some(crypto) = state.crypto.lock().await.get(&target_id) {
                                                let req = serde_json::json!({"type": "download_req", "path": path});
                                                let payload = req.to_string();
                                                let nonce_val = crypto.tx_nonce.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                                let nonce = create_nonce(nonce_val);
                                                let mut in_out = payload.into_bytes();
                                                crypto.key.seal_in_place_append_tag(nonce, aead::Aad::empty(), &mut in_out).unwrap();
                                                
                                                let enc_msg = serde_json::json!({
                                                    "type": "e2ee",
                                                    "nonce": b64.encode(nonce_val.to_be_bytes()),
                                                    "ciphertext": b64.encode(&in_out)
                                                });
                                                let _ = dc.send_text(enc_msg.to_string()).await;
                                            }
                                        }
                                    }
                                }
                                "sendfile" => {
                                    if let Some(path) = cmd.path {
                                        let dc_opt = state.data_channels.lock().await.get(&target_id).cloned();
                                        let crypto_opt = state.crypto.lock().await.get(&target_id).cloned();
                                        
                                        if let (Some(dc), Some(crypto)) = (dc_opt, crypto_opt) {
                                            let ipc_tx_clone = state_console.ipc_tx.clone();
                                            tokio::spawn(async move {
                                                if let Ok(mut file) = fs::File::open(&path).await {
                                                    let meta = file.metadata().await.unwrap();
                                                    let file_name = Path::new(&path).file_name().unwrap().to_string_lossy().into_owned();
                                                    
                                                    println!("[File] Sending {} ({} bytes) to {}...", file_name, meta.len(), target_id);
                                                    
                                                    let start_req = serde_json::json!({
                                                        "type": "file_start",
                                                        "name": file_name,
                                                        "size": meta.len()
                                                    });
                                                    
                                                    let nonce_val = crypto.tx_nonce.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                                    let nonce = create_nonce(nonce_val);
                                                    let mut in_out = start_req.to_string().into_bytes();
                                                    crypto.key.seal_in_place_append_tag(nonce, aead::Aad::empty(), &mut in_out).unwrap();
                                                    let enc_msg = serde_json::json!({
                                                        "type": "e2ee",
                                                        "nonce": b64.encode(nonce_val.to_be_bytes()),
                                                        "ciphertext": b64.encode(&in_out)
                                                    });
                                                    let _ = dc.send_text(enc_msg.to_string()).await;
                                                    
                                                    let mut buf = vec![0u8; 16384];
                                                    let mut total = 0;
                                                    let mut last_progress_report = 0;
                                                    while let Ok(n) = file.read(&mut buf).await {
                                                        if n == 0 { break; }
                                                        
                                                        let c_nonce_val = crypto.tx_nonce.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                                        let c_nonce = create_nonce(c_nonce_val);
                                                        let mut chunk_in_out = buf[..n].to_vec();
                                                        crypto.key.seal_in_place_append_tag(c_nonce, aead::Aad::empty(), &mut chunk_in_out).unwrap();
                                                        
                                                        let mut final_buf = c_nonce_val.to_be_bytes().to_vec();
                                                        final_buf.extend_from_slice(&chunk_in_out);
                                                        
                                                        let chunk = bytes::Bytes::from(final_buf);
                                                        let _ = dc.send(&chunk).await;
                                                        total += n;
                                                        
                                                        if total - last_progress_report > 512 * 1024 || total == (meta.len() as usize) {
                                                            last_progress_report = total;
                                                            let progress = IpcEvent {
                                                                event: "file_progress".to_string(),
                                                                data: serde_json::json!({
                                                                    "filename": file_name,
                                                                    "bytes_sent": total,
                                                                    "total": meta.len(),
                                                                    "target": target_id,
                                                                    "direction": "send"
                                                                })
                                                            };
                                                            let _ = ipc_tx_clone.send(serde_json::to_string(&progress).unwrap());
                                                        }
                                                    }
                                                    println!("[File] {} successfully sent ({} bytes)!", file_name, total);
                                                } else {
                                                    println!("[Error] File not found: {}", path);
                                                }
                                            });
                                        } else {
                                            println!("[Error] No P2P tunnel open with {}.", target_id);
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    _ => {}
                }
            }
            
            Some(action) = outbound_rx.recv() => {
                if let Ok(json_str) = serde_json::to_string(&action) {
                    let _ = write.send(WsMessage::Text(json_str)).await;
                }
            }
        }
    }

    Ok(())
}

async fn create_peer_connection(api: &webrtc::api::API, target_id: String, state: AppState) -> Result<Arc<RTCPeerConnection>, webrtc::Error> {
    let config = RTCConfiguration {
        ice_servers: vec![RTCIceServer {
            urls: vec!["stun:stun.l.google.com:19302".to_owned()],
            ..Default::default()
        }],
        ..Default::default()
    };

    let peer_connection = Arc::new(api.new_peer_connection(config).await?);

    let outbound_tx = state.outbound_tx.clone();
    let target_clone = target_id.clone();
    peer_connection.on_ice_candidate(Box::new(move |candidate: Option<RTCIceCandidate>| {
        let outbound_tx2 = outbound_tx.clone();
        let target_clone2 = target_clone.clone();
        Box::pin(async move {
            if let Some(candidate) = candidate {
                if let Ok(init) = candidate.to_json() {
                    let action = ClientAction {
                        action: "send".to_string(),
                        target_id: target_clone2,
                        payload: serde_json::to_value(SignalingPayload::IceCandidate { candidate: init }).unwrap(),
                    };
                    let _ = outbound_tx2.send(action).await;
                }
            }
        })
    }));

    let state_clone = state.clone();
    let target_clone_dc = target_id.clone();
    peer_connection.on_data_channel(Box::new(move |data_channel: Arc<RTCDataChannel>| {
        let state2 = state_clone.clone();
        let target2 = target_clone_dc.clone();
        Box::pin(async move {
            setup_data_channel(&data_channel, target2.clone(), state2.clone()).await;
            state2.data_channels.lock().await.insert(target2, data_channel);
        })
    }));
    
    peer_connection.on_peer_connection_state_change(Box::new(move |_s: RTCPeerConnectionState| {
        Box::pin(async move {})
    }));

    Ok(peer_connection)
}

async fn setup_data_channel(data_channel: &Arc<RTCDataChannel>, target_id: String, _state: AppState) {
    let target_clone = target_id.clone();
    
    let rng = ring_rand::SystemRandom::new();
    let my_priv_key = agreement::EphemeralPrivateKey::generate(&agreement::X25519, &rng).unwrap();
    let my_pub_key = my_priv_key.compute_public_key().unwrap();
    let my_priv_key_arc = Arc::new(Mutex::new(Some(my_priv_key)));
    
    let dc_for_open = data_channel.clone();
    data_channel.on_open(Box::new(move || {
        println!("[P2P] Tunnel opened with {}! Sending Key Exchange...", target_clone);
        let pub_b64 = b64.encode(my_pub_key.as_ref());
        let kex_msg = serde_json::json!({
            "type": "key_exchange",
            "pub_key": pub_b64
        });
        
        let dc = dc_for_open.clone();
        Box::pin(async move {
            let _ = dc.send_text(kex_msg.to_string()).await;
        })
    }));

    let target_clone_msg = target_id.clone();
    let current_file: Arc<Mutex<Option<fs::File>>> = Arc::new(Mutex::new(None));
    let current_filename: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
    let dc_clone = data_channel.clone();

    let state_msg = _state.clone();
    let priv_key_ref = my_priv_key_arc.clone();
    
    data_channel.on_message(Box::new(move |msg: DataChannelMessage| {
        let target2 = target_clone_msg.clone();
        let file_mtx = current_file.clone();
        let name_mtx = current_filename.clone();
        let state3 = state_msg.clone();
        let dc = dc_clone.clone();
        let priv_key_mtx = priv_key_ref.clone();

        Box::pin(async move {
            if msg.is_string {
                if let Ok(text) = String::from_utf8(msg.data.to_vec()) {
                    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
                        let t = json.get("type").and_then(|v| v.as_str());
                        
                        if t == Some("key_exchange") {
                            if let Some(pub_b64) = json.get("pub_key").and_then(|v| v.as_str()) {
                                if let Ok(peer_pub) = b64.decode(pub_b64) {
                                    let mut priv_guard = priv_key_mtx.lock().await;
                                    if let Some(priv_key) = priv_guard.take() {
                                        let peer_pub_key = agreement::UnparsedPublicKey::new(&agreement::X25519, peer_pub);
                                        let key_res = agreement::agree_ephemeral(priv_key, &peer_pub_key, |key_material| {
                                            let salt = hkdf::Salt::new(hkdf::HKDF_SHA256, b"gens-relay");
                                            let prk = salt.extract(key_material);
                                            let info = [b"e2ee key".as_slice()];
                                            let mut okm = [0u8; 32];
                                            prk.expand(&info, &aead::CHACHA20_POLY1305).unwrap().fill(&mut okm).unwrap();
                                            let unbound = aead::UnboundKey::new(&aead::CHACHA20_POLY1305, &okm).unwrap();
                                            aead::LessSafeKey::new(unbound)
                                        });
                                        
                                        if let Ok(derived_key) = key_res {
                                            println!("[E2EE] Derived shared secret with {}", target2);
                                            let crypto = CryptoState {
                                                key: derived_key,
                                                tx_nonce: std::sync::atomic::AtomicU64::new(0),
                                            };
                                            state3.crypto.lock().await.insert(target2.clone(), Arc::new(crypto));
                                            
                                            let status_event = IpcEvent {
                                                event: "e2ee_status".to_string(),
                                                data: serde_json::json!({"target": target2, "secured": true}),
                                            };
                                            let _ = state3.ipc_tx.send(serde_json::to_string(&status_event).unwrap());
                                        }
                                    }
                                }
                            }
                            return;
                        }
                        
                        if t == Some("e2ee") {
                            if let (Some(nonce_b64), Some(cipher_b64)) = (json.get("nonce").and_then(|v| v.as_str()), json.get("ciphertext").and_then(|v| v.as_str())) {
                                if let (Ok(nonce_bytes), Ok(mut cipher_bytes)) = (b64.decode(nonce_b64), b64.decode(cipher_b64)) {
                                    if nonce_bytes.len() == 8 {
                                        let mut n_arr = [0u8; 8];
                                        n_arr.copy_from_slice(&nonce_bytes);
                                        let counter = u64::from_be_bytes(n_arr);
                                        
                                        let crypto_opt = state3.crypto.lock().await.get(&target2).cloned();
                                        if let Some(crypto) = crypto_opt {
                                            let nonce = create_nonce(counter);
                                            if let Ok(ptxt) = crypto.key.open_in_place(nonce, aead::Aad::empty(), &mut cipher_bytes) {
                                                if let Ok(dec_text) = std::str::from_utf8(ptxt) {
                                                    if let Ok(inner_json) = serde_json::from_str::<serde_json::Value>(dec_text) {
                                                        let it = inner_json.get("type").and_then(|v| v.as_str());
                                                        let mut _handled = false;
                                                        
                                                        if it == Some("file_start") {
                                                            _handled = true;
                                                            if let Some(name) = inner_json.get("name").and_then(|n| n.as_str()) {
                                                                println!("[File] Starting secure reception: {} from {}...", name, target2);
                                                                let safe_name = name.replace("..", "").replace("/", "").replace("\\", "");
                                                                let _ = fs::create_dir_all("downloads").await;
                                                                let path = format!("downloads/{}", safe_name);
                                                                
                                                                if let Ok(f) = fs::File::create(&path).await {
                                                                    *file_mtx.lock().await = Some(f);
                                                                    *name_mtx.lock().await = safe_name;
                                                                }
                                                            }
                                                        } else if it == Some("ls") {
                                                            _handled = true;
                                                            if let Some(path_str) = inner_json.get("path").and_then(|n| n.as_str()) {
                                                                let mut entries = Vec::new();
                                                                let target_path = if path_str.is_empty() {
                                                                    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
                                                                } else {
                                                                    PathBuf::from(path_str)
                                                                };
                                                                
                                                                if let Ok(mut dir) = fs::read_dir(&target_path).await {
                                                                    while let Ok(Some(entry)) = dir.next_entry().await {
                                                                        if let Ok(metadata) = entry.metadata().await {
                                                                            if let Ok(sys_time) = metadata.modified() {
                                                                                let t_sec = sys_time.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
                                                                                entries.push(serde_json::json!({
                                                                                    "name": entry.file_name().to_string_lossy(),
                                                                                    "is_dir": metadata.is_dir(),
                                                                                    "size": metadata.len(),
                                                                                    "time": t_sec
                                                                                }));
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                                
                                                                let parent = target_path.parent().map(|p| p.to_string_lossy().to_string()).unwrap_or_default();
                                                                let resp = serde_json::json!({
                                                                    "type": "ls_result",
                                                                    "path": target_path.to_string_lossy(),
                                                                    "parent": parent,
                                                                    "entries": entries
                                                                });
                                                                
                                                                let payload = resp.to_string();
                                                                let nonce_val = crypto.tx_nonce.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                                                let o_nonce = create_nonce(nonce_val);
                                                                let mut in_out = payload.into_bytes();
                                                                crypto.key.seal_in_place_append_tag(o_nonce, aead::Aad::empty(), &mut in_out).unwrap();
                                                                
                                                                let enc_msg = serde_json::json!({
                                                                    "type": "e2ee",
                                                                    "nonce": b64.encode(nonce_val.to_be_bytes()),
                                                                    "ciphertext": b64.encode(&in_out)
                                                                });
                                                                let _ = dc.send_text(enc_msg.to_string()).await;
                                                            }
                                                        } else if it == Some("ls_result") {
                                                            _handled = true;
                                                            let event = IpcEvent {
                                                                event: "ls_result".to_string(),
                                                                data: inner_json.clone(),
                                                            };
                                                            let _ = state3.ipc_tx.send(serde_json::to_string(&event).unwrap());
                                                        } else if it == Some("download_req") {
                                                            _handled = true;
                                                            if let Some(path_str) = inner_json.get("path").and_then(|n| n.as_str()) {
                                                                let path = path_str.to_string();
                                                                let target_id = target2.clone();
                                                                let dc = dc.clone();
                                                                let state_tx = state3.ipc_tx.clone();
                                                                
                                                                tokio::spawn(async move {
                                                                    if let Ok(mut file) = fs::File::open(&path).await {
                                                                        let meta = file.metadata().await.unwrap();
                                                                        let file_name = Path::new(&path).file_name().unwrap().to_string_lossy().into_owned();
                                                                        
                                                                        let start_msg = serde_json::json!({
                                                                            "type": "file_start",
                                                                            "name": file_name,
                                                                            "size": meta.len()
                                                                        });
                                                                        
                                                                        let nonce_val = crypto.tx_nonce.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                                                        let o_nonce = create_nonce(nonce_val);
                                                                        let mut in_out = start_msg.to_string().into_bytes();
                                                                        crypto.key.seal_in_place_append_tag(o_nonce, aead::Aad::empty(), &mut in_out).unwrap();
                                                                        let enc_msg = serde_json::json!({
                                                                            "type": "e2ee",
                                                                            "nonce": b64.encode(nonce_val.to_be_bytes()),
                                                                            "ciphertext": b64.encode(&in_out)
                                                                        });
                                                                        let _ = dc.send_text(enc_msg.to_string()).await;
                                                                        
                                                                        let mut buf = vec![0u8; 16384];
                                                                        let mut total = 0;
                                                                        let mut last_progress_report = 0;
                                                                        while let Ok(n) = file.read(&mut buf).await {
                                                                            if n == 0 { break; }
                                                                            
                                                                            let c_nonce_val = crypto.tx_nonce.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                                                            let c_nonce = create_nonce(c_nonce_val);
                                                                            let mut chunk_in_out = buf[..n].to_vec();
                                                                            crypto.key.seal_in_place_append_tag(c_nonce, aead::Aad::empty(), &mut chunk_in_out).unwrap();
                                                                            
                                                                            let mut final_buf = c_nonce_val.to_be_bytes().to_vec();
                                                                            final_buf.extend_from_slice(&chunk_in_out);
                                                                            
                                                                            let chunk = bytes::Bytes::from(final_buf);
                                                                            let _ = dc.send(&chunk).await;
                                                                            total += n;
                                                                            
                                                                            if total - last_progress_report > 512 * 1024 || total == (meta.len() as usize) {
                                                                                last_progress_report = total;
                                                                                let progress = IpcEvent {
                                                                                    event: "file_progress".to_string(),
                                                                                    data: serde_json::json!({
                                                                                        "filename": file_name,
                                                                                        "bytes_sent": total,
                                                                                        "total": meta.len(),
                                                                                        "target": target_id,
                                                                                        "direction": "send"
                                                                                    })
                                                                                };
                                                                                let _ = state_tx.send(serde_json::to_string(&progress).unwrap());
                                                                            }
                                                                        }
                                                                    }
                                                                });
                                                            }
                                                        } else if it == Some("chat") {
                                                            if let Some(msg_txt) = inner_json.get("msg").and_then(|v| v.as_str()) {
                                                                println!("[P2P Chat] {} : {}", target2, msg_txt);
                                                                let event = IpcEvent {
                                                                    event: "chat".to_string(),
                                                                    data: serde_json::json!({"from": target2, "msg": msg_txt}),
                                                                };
                                                                let _ = state3.ipc_tx.send(serde_json::to_string(&event).unwrap());
                                                            }
                                                        }
                                                    }
                                                }
                                            } else {
                                                eprintln!("[E2EE] Failed to decrypt incoming JSON payload");
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                let data = msg.data.to_vec();
                if data.len() >= 8 {
                    let mut n_arr = [0u8; 8];
                    n_arr.copy_from_slice(&data[..8]);
                    let counter = u64::from_be_bytes(n_arr);
                    let mut ciphertext = data[8..].to_vec();
                    
                    let crypto_opt = state3.crypto.lock().await.get(&target2).cloned();
                    if let Some(crypto) = crypto_opt {
                        let nonce = create_nonce(counter);
                        if let Ok(ptxt) = crypto.key.open_in_place(nonce, aead::Aad::empty(), &mut ciphertext) {
                            let mut file_guard = file_mtx.lock().await;
                            if let Some(ref mut file) = *file_guard {
                                let _ = file.write_all(ptxt).await;
                            }
                        } else {
                            eprintln!("[E2EE] Failed to decrypt incoming binary chunk!");
                        }
                    }
                }
            }
        })
    }));
}
