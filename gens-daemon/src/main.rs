
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::path::Path;
use std::sync::Arc;
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};
use url::Url;

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
}

#[derive(Clone, Debug)]
struct PeerInfo {
    id: String,
    ip: String,
}

#[derive(Clone)]
struct AppState {
    peers: Arc<Mutex<HashMap<String, Arc<RTCPeerConnection>>>>,
    data_channels: Arc<Mutex<HashMap<String, Arc<RTCDataChannel>>>>,
    local_peers: Arc<Mutex<HashMap<String, PeerInfo>>>,
    outbound_tx: mpsc::Sender<ClientAction>,
    hostname: String,
}

async fn resolve_target(target: &str, state: &AppState) -> String {
    let peers = state.local_peers.lock().await;
    if let Some(info) = peers.get(target) {
        info.id.clone()
    } else {
        target.to_string()
    }
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

    let relay_domain = env::var("RELAY_DOMAIN").unwrap_or_else(|_| {
        eprintln!("Erreur : RELAY_DOMAIN non défini.");
        std::process::exit(1);
    });

    let auth_token = env::var("AUTH_TOKEN").unwrap_or_else(|_| {
        eprintln!("Erreur : AUTH_TOKEN non défini.");
        std::process::exit(1);
    });

    let _ = fs::create_dir_all("downloads").await;

    let mut url = match Url::parse(&relay_domain) {
        Ok(u) => u,
        Err(e) => {
            eprintln!("Erreur : RELAY_DOMAIN invalide : {}", e);
            std::process::exit(1);
        }
    };
    url.query_pairs_mut().append_pair("token", &auth_token);

    println!(">>> Démarrage du Daemon [{}] <<<", my_hostname);
    println!("Tentative de connexion à {}...", relay_domain);

    let (ws_stream, _) = match connect_async(url.as_str()).await {
        Ok(res) => res,
        Err(e) => {
            eprintln!("Erreur fatale : Impossible de se connecter : {}", e);
            std::process::exit(1);
        }
    };

    let (mut write, mut read) = ws_stream.split();
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<ClientAction>(100);

    let state = AppState {
        peers: Arc::new(Mutex::new(HashMap::new())),
        data_channels: Arc::new(Mutex::new(HashMap::new())),
        local_peers: Arc::new(Mutex::new(HashMap::new())),
        outbound_tx: outbound_tx.clone(),
        hostname: my_hostname.clone(),
    };

    let local_peers_clone = state.local_peers.clone();
    let my_host_clone = my_hostname.clone();
    tokio::spawn(async move {
        if let Ok(socket) = UdpSocket::bind("0.0.0.0:8888").await {
            let mut buf = [0; 1024];
            loop {
                if let Ok((len, addr)) = socket.recv_from(&mut buf).await {
                    if let Ok(text) = std::str::from_utf8(&buf[..len]) {
                        if let Ok(discovery) = serde_json::from_str::<DiscoveryPacket>(text) {
                            if discovery.hostname != my_host_clone {
                                let mut peers = local_peers_clone.lock().await;
                                if !peers.contains_key(&discovery.hostname) {
                                    println!("[UDP] Nouveau pair découvert : {} (ID: {}, IP: {})", discovery.hostname, discovery.id, addr.ip());
                                }
                                peers.insert(discovery.hostname.clone(), PeerInfo {
                                    id: discovery.id.clone(),
                                    ip: addr.ip().to_string(),
                                });
                            }
                        }
                    }
                }
            }
        }
    });

    let (console_tx, mut console_rx) = mpsc::channel::<String>(32);
    let state_console = state.clone();
    tokio::spawn(async move {
        let stdin = tokio::io::stdin();
        let mut reader = BufReader::new(stdin).lines();
        println!(">>> Commandes :");
        println!(">>>   list                        (Voir l'annuaire local)");
        println!(">>>   connect <TARGET_ID/NOM>     (Initier tunnel WebRTC)");
        println!(">>>   p2p <TARGET_ID/NOM> <MSG>   (Envoyer texte via tunnel)");
        println!(">>>   sendfile <TARGET_ID/NOM> <PATH>");
        
        while let Ok(Some(line)) = reader.next_line().await {
            let line = line.trim().to_string();
            if !line.is_empty() {
                if console_tx.send(line).await.is_err() { break; }
            }
        }
    });

    let api = APIBuilder::new().build();

    loop {
        tokio::select! {
            Some(message_result) = read.next() => {
                match message_result {
                    Ok(message) => {
                        if let WsMessage::Text(text) = message {
                            if let Ok(server_msg) = serde_json::from_str::<ServerMessage>(&text) {
                                match server_msg {
                                    ServerMessage::Welcome { id } => {
                                        println!("[Succès] Connecté au réseau ! Mon ID est : {}.", id);
                                        let udp_id = id.clone();
                                        let udp_host = state.hostname.clone();
                                        tokio::spawn(async move {
                                            if let Ok(socket) = UdpSocket::bind("0.0.0.0:0").await {
                                                if socket.set_broadcast(true).is_ok() {
                                                    let packet = serde_json::to_string(&DiscoveryPacket { id: udp_id, hostname: udp_host }).unwrap();
                                                    loop {
                                                        let _ = socket.send_to(packet.as_bytes(), "255.255.255.255:8888").await;
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
                                                    println!("[WebRTC] Offre P2P reçue de {} ({})", hostname, sender_id);
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
                                                    println!("[WebRTC] Réponse P2P reçue de {} ({})", hostname, sender_id);
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
                                    ServerMessage::Error { message } => eprintln!("[Erreur Serveur] {}", message),
                                    ServerMessage::Unknown => {}
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("[Erreur] Connexion interrompue : {}", e);
                        break;
                    }
                }
            }

            Some(cmd) = console_rx.recv() => {
                let parts: Vec<&str> = cmd.splitn(3, ' ').collect();
                if !parts.is_empty() {
                    let action = parts[0];
                    match action {
                        "list" | "peers" => {
                            let peers = state.local_peers.lock().await;
                            if peers.is_empty() {
                                println!(">>> Aucun pair local détecté.");
                            } else {
                                println!(">>> Pairs sur le réseau local :");
                                for (host, info) in peers.iter() {
                                    println!("  - {} -> ID: {} (IP: {})", host, info.id, info.ip);
                                }
                            }
                        }
                        _ if parts.len() >= 2 => {
                            let raw_target = parts[1];
                            let target_id = resolve_target(raw_target, &state_console).await;
                            
                            match action {
                                "connect" => {
                                    println!("[WebRTC] Initiation de la connexion vers {} (ID: {})...", raw_target, target_id);
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
                                    if parts.len() == 3 {
                                        if let Some(dc) = state.data_channels.lock().await.get(&target_id) {
                                            let _ = dc.send_text(parts[2].to_string()).await;
                                        } else {
                                            println!("[Erreur] Aucun tunnel P2P ouvert avec {}.", target_id);
                                        }
                                    }
                                }
                                "sendfile" => {
                                    if parts.len() == 3 {
                                        let path = parts[2].to_string();
                                        let dc_opt = state.data_channels.lock().await.get(&target_id).cloned();
                                        if let Some(dc) = dc_opt {
                                            tokio::spawn(async move {
                                                if let Ok(mut file) = fs::File::open(&path).await {
                                                    let meta = file.metadata().await.unwrap();
                                                    let file_name = Path::new(&path).file_name().unwrap().to_string_lossy().into_owned();
                                                    
                                                    println!("[Fichier] Envoi de {} ({} octets) vers {}...", file_name, meta.len(), target_id);
                                                    
                                                    let start_msg = serde_json::json!({
                                                        "type": "file_start",
                                                        "name": file_name,
                                                        "size": meta.len()
                                                    });
                                                    let _ = dc.send_text(start_msg.to_string()).await;
                                                    
                                                    let mut buf = vec![0u8; 16384];
                                                    let mut total = 0;
                                                    while let Ok(n) = file.read(&mut buf).await {
                                                        if n == 0 { break; }
                                                        let chunk = bytes::Bytes::copy_from_slice(&buf[..n]);
                                                        let _ = dc.send(&chunk).await;
                                                        total += n;
                                                    }
                                                    println!("[Fichier] {} envoyé avec succès ({} octets) !", file_name, total);
                                                } else {
                                                    println!("[Erreur] Fichier introuvable : {}", path);
                                                }
                                            });
                                        } else {
                                            println!("[Erreur] Aucun tunnel P2P ouvert avec {}.", target_id);
                                        }
                                    }
                                }
                                _ => println!(">>> Commande inconnue."),
                            }
                        }
                        _ => println!(">>> Format invalide."),
                    }
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
    data_channel.on_open(Box::new(move || {
        println!("[P2P] Tunnel ouvert avec {} !", target_clone);
        Box::pin(async {})
    }));

    let target_clone_msg = target_id.clone();
    let current_file: Arc<Mutex<Option<fs::File>>> = Arc::new(Mutex::new(None));
    let current_filename: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));

    data_channel.on_message(Box::new(move |msg: DataChannelMessage| {
        let target2 = target_clone_msg.clone();
        let file_mtx = current_file.clone();
        let name_mtx = current_filename.clone();

        Box::pin(async move {
            if msg.is_string {
                if let Ok(text) = String::from_utf8(msg.data.to_vec()) {
                    let mut handled = false;
                    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
                        if json.get("type").and_then(|t| t.as_str()) == Some("file_start") {
                            handled = true;
                            if let Some(name) = json.get("name").and_then(|n| n.as_str()) {
                                println!("[Fichier] Démarrage de la réception : {} depuis {}...", name, target2);
                                let safe_name = name.replace("..", "").replace("/", "").replace("\\", "");
                                let path = format!("downloads/{}", safe_name);
                                
                                if let Ok(f) = fs::File::create(&path).await {
                                    *file_mtx.lock().await = Some(f);
                                    *name_mtx.lock().await = safe_name;
                                }
                            }
                        }
                    }
                    
                    if !handled {
                        println!("[P2P Chat] {} : {}", target2, text);
                    }
                }
            } else {
                let mut file_guard = file_mtx.lock().await;
                if let Some(ref mut file) = *file_guard {
                    let _ = file.write_all(&msg.data).await;
                }
            }
        })
    }));
}
