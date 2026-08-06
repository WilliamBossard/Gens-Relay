#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use chrono::{DateTime, Local};
use eframe::egui;
use egui_extras::{Column, TableBuilder};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;
use std::time::{Duration, UNIX_EPOCH};

#[derive(Serialize)]
struct IpcCommand {
    action: String,
    target: Option<String>,
    msg: Option<String>,
    path: Option<String>,
}

#[derive(Deserialize, Debug)]
struct IpcEvent {
    event: String,
    #[serde(flatten)]
    data: Value,
}

#[derive(Serialize, Deserialize, Default)]
struct AppPrefs {
    aliases: HashMap<String, String>,
    favorites: Vec<String>,
}

impl AppPrefs {
    fn load() -> Self {
        if let Ok(data) = fs::read_to_string("prefs.json") {
            if let Ok(prefs) = serde_json::from_str(&data) {
                return prefs;
            }
        }
        Self::default()
    }
    
    fn save(&self) {
        if let Ok(data) = serde_json::to_string_pretty(self) {
            let _ = fs::write("prefs.json", data);
        }
    }
}

fn get_ipc_file_path() -> std::path::PathBuf {
    let mut dir = std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."));
    if dir.file_name().and_then(|s| s.to_str()) == Some("gens-gui") {
        dir.push("../.ipc_port");
    } else {
        dir.push(".ipc_port");
    }
    dir
}

fn spawn_daemon() -> Option<Child> {
    if let Ok(child) = Command::new("./gens-daemon")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn() 
    {
        return Some(child);
    }
    Command::new("cargo")
        .args(&["run", "-p", "gens-daemon"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()
}

fn write_env(domain: &str, token: &str) {
    let mut path = std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."));
    if path.file_name().and_then(|s| s.to_str()) == Some("gens-gui") {
        path.push("../.env");
    } else {
        path.push(".env");
    }
    let data = format!("RELAY_DOMAIN={}\nAUTH_TOKEN={}\n", domain, token);
    let _ = fs::write(path, data);
}

fn main() {
    let mut options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1200.0, 750.0])
            .with_title("Gens-Relay P2P Pro"),
        ..Default::default()
    };
    
    options.wgpu_options.supported_backends = eframe::wgpu::Backends::all();
    
    let result = eframe::run_native(
        "Gens-Relay",
        options,
        Box::new(|cc| Ok(Box::new(MyApp::new(cc)))),
    );

    if let Err(e) = result {
        eprintln!("Fatal error launching Gens-Relay GUI: {}", e);
    }
}

#[derive(PartialEq)]
enum Tab {
    Chat,
    Files,
    FileExplorer,
}

struct LocalFileEntry {
    name: String,
    is_dir: bool,
    size: u64,
    time: u64,
}

struct MyApp {
    cmd_tx: Sender<String>,
    event_rx: Receiver<String>,
    connected: bool,
    peers: Vec<Value>,
    chat_messages: Vec<String>,
    input_message: String,
    selected_target: Option<String>,
    prefs: AppPrefs,
    my_id: Option<String>,
    my_hostname: Option<String>,
    daemon_process: Option<Child>,
    current_tab: Tab,
    file_progress: Option<(String, usize, usize)>,
    pending_file: Option<std::path::PathBuf>,
    
    unpaired: bool,
    pairing_domain_input: String,
    pairing_token_input: String,
    discovered_pairing: Option<(String, String)>,
    
    remote_current_path: String,
    remote_parent_path: String,
    remote_entries: Vec<Value>,
    
    local_current_path: std::path::PathBuf,
    local_entries: Vec<LocalFileEntry>,
    
    show_manual_peer_modal: bool,
    manual_peer_id: String,
    manual_peer_ip: String,
    
    e2ee_secured_peers: HashSet<String>,
}

impl MyApp {
    fn new(cc: &eframe::CreationContext<'_>) -> Self {
        let mut visuals = egui::Visuals::dark();
        visuals.window_fill = egui::Color32::from_rgb(30, 30, 30);
        visuals.panel_fill = egui::Color32::from_rgb(25, 25, 25);
        visuals.widgets.noninteractive.bg_fill = egui::Color32::from_rgb(35, 35, 35);
        visuals.widgets.inactive.bg_fill = egui::Color32::from_rgb(45, 45, 45);
        visuals.widgets.hovered.bg_fill = egui::Color32::from_rgb(60, 60, 60);
        visuals.widgets.active.bg_fill = egui::Color32::from_rgb(80, 80, 80);
        visuals.widgets.inactive.rounding = egui::Rounding::same(6.0);
        visuals.widgets.hovered.rounding = egui::Rounding::same(6.0);
        visuals.widgets.active.rounding = egui::Rounding::same(6.0);
        
        let mut style = egui::Style::default();
        style.spacing.item_spacing = egui::vec2(8.0, 8.0);
        style.spacing.window_margin = egui::Margin::same(12.0);
        style.visuals = visuals;
        cc.egui_ctx.set_style(style);

        let (cmd_tx, cmd_rx) = channel::<String>();
        let (event_tx, event_rx) = channel::<String>();
        let ctx_clone = cc.egui_ctx.clone();
        
        let daemon_process = spawn_daemon();

        Self::start_tcp_thread(event_tx, cmd_rx, ctx_clone);

        let mut app = Self {
            cmd_tx,
            event_rx,
            connected: false,
            peers: Vec::new(),
            chat_messages: Vec::new(),
            input_message: String::new(),
            selected_target: None,
            prefs: AppPrefs::load(),
            my_id: None,
            my_hostname: None,
            daemon_process,
            current_tab: Tab::Chat,
            file_progress: None,
            pending_file: None,
            
            unpaired: false,
            pairing_domain_input: String::new(),
            pairing_token_input: String::new(),
            discovered_pairing: None,
            
            remote_current_path: String::new(),
            remote_parent_path: String::new(),
            remote_entries: Vec::new(),
            
            local_current_path: std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from(".")),
            local_entries: Vec::new(),
            
            show_manual_peer_modal: false,
            manual_peer_id: String::new(),
            manual_peer_ip: String::new(),
            
            e2ee_secured_peers: HashSet::new(),
        };
        app.refresh_local_files();
        app
    }
    
    fn refresh_local_files(&mut self) {
        self.local_entries.clear();
        if let Ok(mut dir) = fs::read_dir(&self.local_current_path) {
            while let Some(Ok(entry)) = dir.next() {
                if let Ok(metadata) = entry.metadata() {
                    let time = metadata.modified().unwrap_or(std::time::SystemTime::now())
                        .duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
                    self.local_entries.push(LocalFileEntry {
                        name: entry.file_name().to_string_lossy().to_string(),
                        is_dir: metadata.is_dir(),
                        size: metadata.len(),
                        time,
                    });
                }
            }
        }
        self.local_entries.sort_by(|a, b| {
            b.is_dir.cmp(&a.is_dir).then(a.name.cmp(&b.name))
        });
    }
    
    fn start_tcp_thread(event_tx: Sender<String>, cmd_rx: Receiver<String>, ctx_clone: egui::Context) {
        thread::spawn(move || {
            loop {
                let ipc_file = get_ipc_file_path();
                if let Ok(port_str) = std::fs::read_to_string(&ipc_file) {
                    if let Ok(port) = port_str.trim().parse::<u16>() {
                        let addr = format!("127.0.0.1:{}", port);
                        match TcpStream::connect(&addr) {
                            Ok(mut stream) => {
                                let _ = event_tx.send("{\"event\": \"_connected\"}".to_string());
                                ctx_clone.request_repaint();
                                let _ = stream.write_all(b"{\"action\": \"list\"}\n");

                                let mut reader = BufReader::new(stream.try_clone().unwrap());
                                let event_tx_clone = event_tx.clone();
                                let ctx_clone2 = ctx_clone.clone();
                                
                                let read_thread = thread::spawn(move || {
                                    let mut line = String::new();
                                    while let Ok(bytes) = reader.read_line(&mut line) {
                                        if bytes == 0 { break; }
                                        let _ = event_tx_clone.send(line.clone());
                                        ctx_clone2.request_repaint();
                                        line.clear();
                                    }
                                    let _ = event_tx_clone.send("{\"event\": \"_disconnected\"}".to_string());
                                    ctx_clone2.request_repaint();
                                });

                                while let Ok(cmd) = cmd_rx.recv() {
                                    if stream.write_all(cmd.as_bytes()).is_err() || stream.write_all(b"\n").is_err() {
                                        break;
                                    }
                                }
                                
                                let _ = read_thread.join();
                            }
                            Err(_) => { thread::sleep(Duration::from_secs(2)); }
                        }
                    } else { thread::sleep(Duration::from_secs(2)); }
                } else { thread::sleep(Duration::from_secs(2)); }
            }
        });
    }
    
    fn send_command(&self, cmd: IpcCommand) {
        if let Ok(json) = serde_json::to_string(&cmd) {
            let _ = self.cmd_tx.send(json);
        }
    }
    
    fn restart_daemon(&mut self) {
        if let Some(mut child) = self.daemon_process.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        self.daemon_process = spawn_daemon();
    }
    
    fn format_size(size: u64) -> String {
        if size == 0 { return String::new(); }
        let kb = size / 1024;
        if kb < 1024 {
            format!("{} KB", kb)
        } else {
            format!("{:.2} MB", kb as f64 / 1024.0)
        }
    }
    
    fn format_date(ts: u64) -> String {
        if let Some(dt) = chrono::DateTime::from_timestamp(ts as i64, 0) {
            let local: DateTime<Local> = DateTime::from(dt);
            local.format("%Y-%m-%d %H:%M").to_string()
        } else {
            String::new()
        }
    }
}

impl Drop for MyApp {
    fn drop(&mut self) {
        if let Some(mut child) = self.daemon_process.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl eframe::App for MyApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        while let Ok(evt_str) = self.event_rx.try_recv() {
            if let Ok(evt) = serde_json::from_str::<IpcEvent>(&evt_str) {
                match evt.event.as_str() {
                    "_connected" => {
                        self.connected = true;
                        self.send_command(IpcCommand { action: "list".into(), target: None, msg: None, path: None });
                    }
                    "_disconnected" => {
                        self.connected = false;
                        self.unpaired = false;
                    }
                    "status" => {
                        if evt.data.get("unpaired").and_then(|v| v.as_bool()).unwrap_or(false) {
                            self.unpaired = true;
                        } else {
                            self.unpaired = false;
                            if let Some(id) = evt.data.get("id").and_then(|v| v.as_str()) {
                                self.my_id = Some(id.to_string());
                            }
                            if let Some(host) = evt.data.get("hostname").and_then(|v| v.as_str()) {
                                self.my_hostname = Some(host.to_string());
                            }
                        }
                    }
                    "e2ee_status" => {
                        if let Some(target) = evt.data.get("target").and_then(|v| v.as_str()) {
                            if evt.data.get("secured").and_then(|v| v.as_bool()).unwrap_or(false) {
                                self.e2ee_secured_peers.insert(target.to_string());
                                self.chat_messages.push(">>> 🔒 Tunnel securely established (Curve25519 + ChaCha20-Poly1305)".to_string());
                            }
                        }
                    }
                    "pairing_discovered" => {
                        if let (Some(domain), Some(token)) = (evt.data.get("domain").and_then(|v| v.as_str()), evt.data.get("token").and_then(|v| v.as_str())) {
                            self.discovered_pairing = Some((domain.to_string(), token.to_string()));
                        }
                    }
                    "peers_updated" => {
                        if let Some(peers_arr) = evt.data.get("peers").and_then(|p| p.as_array()) {
                            self.peers = peers_arr.clone();
                        }
                    }
                    "chat" => {
                        let from = evt.data.get("from").and_then(|v| v.as_str()).unwrap_or("Unknown");
                        let msg = evt.data.get("msg").and_then(|v| v.as_str()).unwrap_or("");
                        let display_name = self.prefs.aliases.get(from).cloned().unwrap_or_else(|| from.to_string());
                        self.chat_messages.push(format!("[{}] {}", display_name, msg));
                    }
                    "file_progress" => {
                        let filename = evt.data.get("filename").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        let sent = evt.data.get("bytes_sent").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
                        let total = evt.data.get("total").and_then(|v| v.as_u64()).unwrap_or(1) as usize;
                        self.file_progress = Some((filename.clone(), sent, total));
                        if sent >= total {
                            self.file_progress = None;
                            self.chat_messages.push(format!(">>> File transfer complete: {}", filename));
                            if self.current_tab == Tab::FileExplorer {
                                self.refresh_local_files();
                                if let Some(t) = &self.selected_target {
                                    self.send_command(IpcCommand { action: "ls".into(), target: Some(t.clone()), msg: None, path: Some(self.remote_current_path.clone()) });
                                }
                            }
                        }
                    }
                    "ls_result" => {
                        if let Some(path) = evt.data.get("path").and_then(|v| v.as_str()) {
                            self.remote_current_path = path.to_string();
                        }
                        if let Some(parent) = evt.data.get("parent").and_then(|v| v.as_str()) {
                            self.remote_parent_path = parent.to_string();
                        }
                        if let Some(entries) = evt.data.get("entries").and_then(|v| v.as_array()) {
                            self.remote_entries = entries.clone();
                            self.remote_entries.sort_by(|a, b| {
                                let dir_a = a.get("is_dir").and_then(|v| v.as_bool()).unwrap_or(false);
                                let dir_b = b.get("is_dir").and_then(|v| v.as_bool()).unwrap_or(false);
                                let name_a = a.get("name").and_then(|v| v.as_str()).unwrap_or("");
                                let name_b = b.get("name").and_then(|v| v.as_str()).unwrap_or("");
                                dir_b.cmp(&dir_a).then(name_a.cmp(name_b))
                            });
                        }
                    }
                    _ => {}
                }
            }
        }

        if self.unpaired {
            egui::CentralPanel::default().frame(egui::Frame::default().fill(egui::Color32::from_rgb(30, 30, 30)).inner_margin(20.0)).show(ctx, |ui| {
                ui.vertical_centered(|ui| {
                    ui.add_space(50.0);
                    ui.heading(egui::RichText::new("Gens-Relay").strong().size(30.0));
                    ui.add_space(20.0);
                    ui.label(egui::RichText::new("Your node is currently Unpaired.").color(egui::Color32::YELLOW));
                    ui.label("A Signaling Server configuration is required to continue.");
                    ui.add_space(30.0);
                    
                    if let Some((domain, token)) = self.discovered_pairing.clone() {
                        ui.group(|ui| {
                            ui.heading("Local Pairing Offer Discovered!");
                            ui.label(format!("Server Domain: {}", domain));
                            ui.add_space(10.0);
                            ui.horizontal(|ui| {
                                if ui.button("Pair with this Server").clicked() {
                                    write_env(&domain, &token);
                                    self.restart_daemon();
                                    self.discovered_pairing = None;
                                }
                                if ui.button("Dismiss").clicked() {
                                    self.discovered_pairing = None;
                                }
                            });
                        });
                        ui.add_space(30.0);
                        ui.label("OR");
                        ui.add_space(30.0);
                    }
                    
                    ui.group(|ui| {
                        ui.heading("Manual Pairing");
                        ui.add_space(10.0);
                        ui.horizontal(|ui| {
                            ui.label("Relay Domain (e.g. wss://relay.example.com):");
                            ui.add(egui::TextEdit::singleline(&mut self.pairing_domain_input).desired_width(200.0));
                        });
                        ui.horizontal(|ui| {
                            ui.label("Auth Token:");
                            ui.add(egui::TextEdit::singleline(&mut self.pairing_token_input).desired_width(200.0));
                        });
                        ui.add_space(10.0);
                        if ui.button("Save & Pair").clicked() {
                            write_env(&self.pairing_domain_input, &self.pairing_token_input);
                            self.restart_daemon();
                        }
                    });
                });
            });
            return;
        }

        if self.show_manual_peer_modal {
            egui::Window::new("Add Manual Peer")
                .collapsible(false)
                .resizable(false)
                .show(ctx, |ui| {
                    ui.label("Enter peer details to connect directly (bypassing UDP).");
                    ui.add_space(10.0);
                    ui.horizontal(|ui| {
                        ui.label("Peer ID:");
                        ui.add(egui::TextEdit::singleline(&mut self.manual_peer_id).desired_width(150.0));
                    });
                    ui.horizontal(|ui| {
                        ui.label("Peer IP Address:");
                        ui.add(egui::TextEdit::singleline(&mut self.manual_peer_ip).desired_width(150.0));
                    });
                    ui.add_space(10.0);
                    ui.horizontal(|ui| {
                        if ui.button("Add Peer").clicked() {
                            self.send_command(IpcCommand {
                                action: "add_peer".into(),
                                target: Some(self.manual_peer_id.clone()),
                                msg: Some(self.manual_peer_ip.clone()),
                                path: None,
                            });
                            self.show_manual_peer_modal = false;
                            self.manual_peer_id.clear();
                            self.manual_peer_ip.clear();
                        }
                        if ui.button("Cancel").clicked() {
                            self.show_manual_peer_modal = false;
                        }
                    });
                });
        }
        
        egui::TopBottomPanel::top("top_panel").frame(egui::Frame::default().fill(egui::Color32::from_rgb(51, 51, 51)).inner_margin(10.0)).show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.heading(egui::RichText::new("Gens-Relay").strong().size(20.0).color(egui::Color32::WHITE));
                
                ui.add_space(20.0);
                if let Some(host) = &self.my_hostname {
                    ui.label(egui::RichText::new(host).color(egui::Color32::LIGHT_GRAY));
                }
                if let Some(id) = &self.my_id {
                    ui.label(egui::RichText::new(format!("ID: {}", id)).color(egui::Color32::DARK_GRAY));
                }

                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if self.connected {
                        ui.label(egui::RichText::new("●").color(egui::Color32::GREEN));
                        ui.label("Daemon Connected");
                    } else {
                        ui.label(egui::RichText::new("●").color(egui::Color32::RED));
                        ui.label("Daemon Disconnected");
                    }
                });
            });
        });

        egui::SidePanel::left("left_panel").frame(egui::Frame::default().fill(egui::Color32::from_rgb(37, 37, 38)).inner_margin(10.0)).resizable(true).min_width(200.0).show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.heading("Directory");
                if ui.button("🔄").on_hover_text("Refresh").clicked() {
                    self.send_command(IpcCommand { action: "list".into(), target: None, msg: None, path: None });
                }
            });
            ui.add_space(10.0);
            
            if ui.button("+ Add Manual Peer").clicked() {
                self.show_manual_peer_modal = true;
            }
            ui.add_space(10.0);

            if self.peers.is_empty() {
                ui.label(egui::RichText::new("No peers detected.").italics());
            } else {
                egui::ScrollArea::vertical().show(ui, |ui| {
                    let mut sorted_peers = self.peers.clone();
                    sorted_peers.sort_by(|a, b| {
                        let id_a = a.get("id").and_then(|v| v.as_str()).unwrap_or("");
                        let id_b = b.get("id").and_then(|v| v.as_str()).unwrap_or("");
                        let fav_a = self.prefs.favorites.contains(&id_a.to_string());
                        let fav_b = self.prefs.favorites.contains(&id_b.to_string());
                        fav_b.cmp(&fav_a)
                    });

                    for peer in &sorted_peers {
                        let host = peer.get("hostname").and_then(|v| v.as_str()).unwrap_or("Unknown");
                        let id = peer.get("id").and_then(|v| v.as_str()).unwrap_or("?");
                        
                        let display_name = self.prefs.aliases.get(id).cloned().unwrap_or_else(|| host.to_string());
                        let is_fav = self.prefs.favorites.contains(&id.to_string());
                        
                        ui.horizontal(|ui| {
                            if ui.button(if is_fav { "⭐" } else { "☆" }).clicked() {
                                if is_fav {
                                    self.prefs.favorites.retain(|x| x != id);
                                } else {
                                    self.prefs.favorites.push(id.to_string());
                                }
                                self.prefs.save();
                            }
                            
                            let mut selected = self.selected_target.as_deref() == Some(id);
                            if ui.toggle_value(&mut selected, display_name).clicked() {
                                if selected {
                                    self.selected_target = Some(id.to_string());
                                }
                            }
                        });
                    }
                });
            }
        });

        egui::CentralPanel::default().frame(egui::Frame::default().fill(egui::Color32::from_rgb(30, 30, 30)).inner_margin(20.0)).show(ctx, |ui| {
            if let Some(target) = self.selected_target.clone() {
                let display_name = self.prefs.aliases.get(&target).cloned().unwrap_or_else(|| target.clone());
                
                ui.horizontal(|ui| {
                    ui.heading(format!("Peer: {}", display_name));
                    
                    let mut alias = display_name.clone();
                    if ui.add(egui::TextEdit::singleline(&mut alias).desired_width(100.0)).changed() {
                        self.prefs.aliases.insert(target.clone(), alias);
                        self.prefs.save();
                    }
                    
                    if ui.button("Connect").clicked() {
                        self.send_command(IpcCommand { action: "connect".into(), target: Some(target.clone()), msg: None, path: None });
                        self.chat_messages.push(format!(">>> WebRTC Connection Initiated..."));
                    }
                    
                    if self.e2ee_secured_peers.contains(&target) {
                        ui.add_space(20.0);
                        ui.label(egui::RichText::new("🔒 E2EE Secured").color(egui::Color32::GREEN).strong());
                    }
                });
                ui.add_space(10.0);

                ui.horizontal(|ui| {
                    ui.selectable_value(&mut self.current_tab, Tab::Chat, "P2P Chat");
                    ui.selectable_value(&mut self.current_tab, Tab::Files, "File Dropzone");
                    if ui.selectable_value(&mut self.current_tab, Tab::FileExplorer, "File Explorer (Split-View)").clicked() {
                        self.send_command(IpcCommand { action: "ls".into(), target: Some(target.clone()), msg: None, path: Some("".into()) });
                    }
                });
                ui.separator();
                
                if let Some((name, sent, total)) = &self.file_progress {
                    ui.label(format!("Transferring: {}", name));
                    let progress = if *total > 0 { *sent as f32 / *total as f32 } else { 0.0 };
                    ui.add(egui::ProgressBar::new(progress).show_percentage());
                    ui.add_space(10.0);
                }

                match self.current_tab {
                    Tab::Chat => {
                        let scroll_area = egui::ScrollArea::vertical().auto_shrink([false; 2]);
                        ui.allocate_ui(egui::vec2(ui.available_width(), ui.available_height() - 40.0), |ui| {
                            scroll_area.show(ui, |ui| {
                                for msg in &self.chat_messages {
                                    ui.label(msg);
                                }
                            });
                        });

                        ui.horizontal(|ui| {
                            let response = ui.add(egui::TextEdit::singleline(&mut self.input_message).hint_text("Write a message...").desired_width(ui.available_width() - 60.0));
                            if ui.button("Send").clicked() || (response.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter))) {
                                if !self.input_message.trim().is_empty() {
                                    self.send_command(IpcCommand { 
                                        action: "p2p".into(), 
                                        target: Some(target.clone()), 
                                        msg: Some(self.input_message.clone()), 
                                        path: None 
                                    });
                                    self.chat_messages.push(format!("[Me] {}", self.input_message));
                                    self.input_message.clear();
                                    ui.memory_mut(|m| m.request_focus(response.id));
                                }
                            }
                        });
                    }
                    Tab::Files => {
                        ui.label("Drag and drop a file here to send it.");
                        
                        let rect = ui.available_rect_before_wrap();
                        let response = ui.interact(rect, ui.id().with("drop_zone"), egui::Sense::hover());
                        
                        if response.hovered() {
                            ui.painter().rect_filled(rect, 10.0, egui::Color32::from_rgba_unmultiplied(100, 100, 255, 50));
                        }

                        let mut dropped = None;
                        ctx.input(|i| {
                            if !i.raw.dropped_files.is_empty() {
                                dropped = Some(i.raw.dropped_files[0].clone());
                            }
                        });
                        
                        if let Some(file) = dropped {
                            if let Some(path) = file.path {
                                self.pending_file = Some(path);
                            }
                        }

                        if let Some(path) = self.pending_file.clone() {
                            ui.add_space(20.0);
                            ui.group(|ui| {
                                let filename = path.file_name().unwrap_or_default().to_string_lossy().into_owned();
                                let size = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                                ui.label(format!("File ready to send: {} ({} bytes)", filename, size));
                                ui.horizontal(|ui| {
                                    let path_clone = path.clone();
                                    if ui.button("Send").clicked() {
                                        self.send_command(IpcCommand {
                                            action: "sendfile".into(),
                                            target: Some(target.clone()),
                                            msg: None,
                                            path: Some(path_clone.to_string_lossy().to_string()),
                                        });
                                        self.chat_messages.push(format!(">>> Started sending file {:?}", path_clone));
                                        self.pending_file = None;
                                        self.current_tab = Tab::Chat;
                                    }
                                    if ui.button("Cancel").clicked() {
                                        self.pending_file = None;
                                    }
                                });
                            });
                        }
                    }
                    Tab::FileExplorer => {
                        ui.columns(2, |columns| {
                            // Left Panel: Local Files
                            columns[0].vertical(|ui| {
                                ui.heading("Local PC");
                                ui.horizontal(|ui| {
                                    ui.label(egui::RichText::new(self.local_current_path.to_string_lossy()).strong());
                                    if ui.button("Refresh").clicked() {
                                        self.refresh_local_files();
                                    }
                                });
                                ui.add_space(5.0);
                                
                                let table = TableBuilder::new(ui)
                                    .striped(true)
                                    .resizable(true)
                                    .cell_layout(egui::Layout::left_to_right(egui::Align::Center))
                                    .column(Column::auto().at_least(150.0))
                                    .column(Column::auto())
                                    .column(Column::remainder())
                                    .min_scrolled_height(0.0);
                                    
                                table.header(20.0, |mut header| {
                                    header.col(|ui| { ui.strong("Name"); });
                                    header.col(|ui| { ui.strong("Size"); });
                                    header.col(|ui| { ui.strong("Date"); });
                                }).body(|mut body| {
                                    let parent = self.local_current_path.parent().map(|p| p.to_path_buf());
                                    if let Some(p) = parent {
                                        body.row(20.0, |mut row| {
                                            row.col(|ui| {
                                                if ui.button("⬆ ..").clicked() {
                                                    self.local_current_path = p;
                                                    self.refresh_local_files();
                                                }
                                            });
                                            row.col(|_| {}); row.col(|_| {});
                                        });
                                    }
                                    
                                    let entries = self.local_entries.iter().map(|e| (e.name.clone(), e.is_dir, e.size, e.time)).collect::<Vec<_>>();
                                    for entry in entries {
                                        let name = entry.0;
                                        let is_dir = entry.1;
                                        let size = entry.2;
                                        let time = entry.3;
                                        body.row(20.0, |mut row| {
                                            row.col(|ui| {
                                                let icon = if is_dir { "📁" } else { "📄" };
                                                let label = ui.selectable_label(false, format!("{} {}", icon, name));
                                                
                                                if label.double_clicked() && is_dir {
                                                    self.local_current_path.push(&name);
                                                    self.refresh_local_files();
                                                }
                                                
                                                if !is_dir && label.drag_started() {
                                                    let mut file_path = self.local_current_path.clone();
                                                    file_path.push(&name);
                                                    ctx.memory_mut(|m| {
                                                        m.data.insert_temp(egui::Id::new("drag_payload"), file_path.to_string_lossy().to_string());
                                                    });
                                                }
                                            });
                                            row.col(|ui| { if !is_dir { ui.label(Self::format_size(size)); } });
                                            row.col(|ui| { ui.label(Self::format_date(time)); });
                                        });
                                    }
                                });
                                
                                let rect = ui.available_rect_before_wrap();
                                let resp = ui.interact(rect, ui.id().with("local_drop"), egui::Sense::hover());
                                if resp.hovered() && ctx.dragged_id().is_some() {
                                    ui.painter().rect_filled(rect, 4.0, egui::Color32::from_rgba_unmultiplied(100, 255, 100, 30));
                                    if ctx.input(|i| i.pointer.any_released()) {
                                        if let Some(payload) = ctx.memory(|m| m.data.get_temp::<String>(egui::Id::new("drag_remote_payload"))) {
                                            let mut file_path = std::path::PathBuf::from(&self.remote_current_path);
                                            file_path.push(payload.clone());
                                            self.send_command(IpcCommand { action: "download_req".into(), target: Some(target.clone()), msg: None, path: Some(file_path.to_string_lossy().to_string()) });
                                            self.chat_messages.push(format!(">>> Requesting secure download for {}", payload));
                                        }
                                    }
                                }
                            });
                            
                            // Right Panel: Remote Files
                            columns[1].vertical(|ui| {
                                ui.heading("Remote PC");
                                ui.horizontal(|ui| {
                                    ui.label(egui::RichText::new(&self.remote_current_path).strong());
                                    if ui.button("Refresh").clicked() {
                                        self.send_command(IpcCommand { action: "ls".into(), target: Some(target.clone()), msg: None, path: Some(self.remote_current_path.clone()) });
                                    }
                                });
                                ui.add_space(5.0);
                                
                                let table = TableBuilder::new(ui)
                                    .striped(true)
                                    .resizable(true)
                                    .cell_layout(egui::Layout::left_to_right(egui::Align::Center))
                                    .column(Column::auto().at_least(150.0))
                                    .column(Column::auto())
                                    .column(Column::remainder())
                                    .min_scrolled_height(0.0);
                                    
                                table.header(20.0, |mut header| {
                                    header.col(|ui| { ui.strong("Name"); });
                                    header.col(|ui| { ui.strong("Size"); });
                                    header.col(|ui| { ui.strong("Date"); });
                                }).body(|mut body| {
                                    if !self.remote_parent_path.is_empty() {
                                        body.row(20.0, |mut row| {
                                            row.col(|ui| {
                                                if ui.button("⬆ ..").clicked() {
                                                    self.send_command(IpcCommand { action: "ls".into(), target: Some(target.clone()), msg: None, path: Some(self.remote_parent_path.clone()) });
                                                }
                                            });
                                            row.col(|_| {}); row.col(|_| {});
                                        });
                                    }
                                    
                                    let entries = self.remote_entries.clone();
                                    for entry in &entries {
                                        let name = entry.get("name").and_then(|v| v.as_str()).unwrap_or("");
                                        let is_dir = entry.get("is_dir").and_then(|v| v.as_bool()).unwrap_or(false);
                                        let size = entry.get("size").and_then(|v| v.as_u64()).unwrap_or(0);
                                        let time = entry.get("time").and_then(|v| v.as_u64()).unwrap_or(0);
                                        
                                        body.row(20.0, |mut row| {
                                            row.col(|ui| {
                                                let icon = if is_dir { "📁" } else { "📄" };
                                                let label = ui.selectable_label(false, format!("{} {}", icon, name));
                                                
                                                if label.double_clicked() && is_dir {
                                                    let mut new_path = std::path::PathBuf::from(&self.remote_current_path);
                                                    new_path.push(name);
                                                    self.send_command(IpcCommand { action: "ls".into(), target: Some(target.clone()), msg: None, path: Some(new_path.to_string_lossy().to_string()) });
                                                }
                                                
                                                if !is_dir && label.drag_started() {
                                                    ctx.memory_mut(|m| {
                                                        m.data.insert_temp(egui::Id::new("drag_remote_payload"), name.to_string());
                                                    });
                                                }
                                            });
                                            row.col(|ui| { if !is_dir { ui.label(Self::format_size(size)); } });
                                            row.col(|ui| { ui.label(Self::format_date(time)); });
                                        });
                                    }
                                });
                                
                                let rect = ui.available_rect_before_wrap();
                                let resp = ui.interact(rect, ui.id().with("remote_drop"), egui::Sense::hover());
                                if resp.hovered() && ctx.dragged_id().is_some() {
                                    ui.painter().rect_filled(rect, 4.0, egui::Color32::from_rgba_unmultiplied(100, 100, 255, 30));
                                    if ctx.input(|i| i.pointer.any_released()) {
                                        if let Some(payload) = ctx.memory(|m| m.data.get_temp::<String>(egui::Id::new("drag_payload"))) {
                                            self.send_command(IpcCommand {
                                                action: "sendfile".into(),
                                                target: Some(target.clone()),
                                                msg: None,
                                                path: Some(payload.clone()),
                                            });
                                            self.chat_messages.push(format!(">>> Started secure upload of {:?}", payload));
                                        }
                                    }
                                }
                            });
                        });
                    }
                }
            } else {
                ui.centered_and_justified(|ui| {
                    ui.label("Select a peer in the directory to start.");
                });
            }
        });
    }
}
