#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use eframe::egui;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;
use std::time::Duration;

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
    // Essayer de lancer gens-daemon depuis le même dossier (prod)
    if let Ok(child) = Command::new("./gens-daemon")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn() 
    {
        return Some(child);
    }
    // Fallback: cargo run (dev)
    Command::new("cargo")
        .args(&["run", "-p", "gens-daemon"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()
}

fn main() {
    let mut options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([900.0, 600.0])
            .with_title("Gens-Relay P2P"),
        ..Default::default()
    };
    
    options.wgpu_options.supported_backends = eframe::wgpu::Backends::all();
    
    let result = eframe::run_native(
        "Gens-Relay",
        options,
        Box::new(|cc| Ok(Box::new(MyApp::new(cc)))),
    );

    if let Err(e) = result {
        eprintln!("============================================================");
        eprintln!("Erreur fatale au lancement de l'interface graphique Gens-Relay.");
        eprintln!("Détails de l'erreur : {}", e);
        eprintln!("------------------------------------------------------------");
        eprintln!("Si vous êtes dans une Machine Virtuelle (VM) et que vous voyez");
        eprintln!("l'erreur 'NoSuitableAdapterFound', cela signifie que votre VM");
        eprintln!("ne dispose pas d'un support GPU matériel adéquat.");
        eprintln!("");
        eprintln!("Solutions recommandées :");
        eprintln!("  1. Exécutez l'application côté Hôte.");
        eprintln!("  2. Installez Mesa3D (llvmpipe) pour forcer le rendu logiciel CPU.");
        eprintln!("============================================================");
        
        #[cfg(windows)]
        {
            let msg = format!("Erreur GPU : {}\n\nSi vous êtes sur une VM, installez Mesa3D (llvmpipe).", e);
            use std::ffi::OsStr;
            use std::iter::once;
            use std::os::windows::ffi::OsStrExt;
            
            let wide: Vec<u16> = OsStr::new(&msg).encode_wide().chain(once(0)).collect();
            let wide_title: Vec<u16> = OsStr::new("Erreur Fatale Gens-Relay").encode_wide().chain(once(0)).collect();
            
            #[link(name = "user32")]
            unsafe extern "system" {
                fn MessageBoxW(hwnd: *mut std::ffi::c_void, lptext: *const u16, lpcaption: *const u16, utype: u32) -> i32;
            }
            
            unsafe {
                MessageBoxW(std::ptr::null_mut(), wide.as_ptr(), wide_title.as_ptr(), 0x10);
            }
        }
    }
}

#[derive(PartialEq)]
enum Tab {
    Chat,
    Files,
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
    file_progress: Option<(String, usize, usize)>, // (filename, sent, total)
    pending_file: Option<std::path::PathBuf>,
}

impl MyApp {
    fn new(cc: &eframe::CreationContext<'_>) -> Self {
        // Appliquer le thème sombre Blip style
        cc.egui_ctx.set_visuals(egui::Visuals::dark());

        let (cmd_tx, cmd_rx) = channel::<String>();
        let (event_tx, event_rx) = channel::<String>();
        let ctx_clone = cc.egui_ctx.clone();
        
        let daemon_process = spawn_daemon();

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

        Self {
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
        }
    }
    
    fn send_command(&self, cmd: IpcCommand) {
        if let Ok(json) = serde_json::to_string(&cmd) {
            let _ = self.cmd_tx.send(json);
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
                    "_disconnected" => self.connected = false,
                    "status" => {
                        if let Some(id) = evt.data.get("id").and_then(|v| v.as_str()) {
                            self.my_id = Some(id.to_string());
                        }
                        if let Some(host) = evt.data.get("hostname").and_then(|v| v.as_str()) {
                            self.my_hostname = Some(host.to_string());
                        }
                    }
                    "peers_updated" => {
                        if let Some(peers_arr) = evt.data.get("peers").and_then(|p| p.as_array()) {
                            self.peers = peers_arr.clone();
                        }
                    }
                    "chat" => {
                        let from = evt.data.get("from").and_then(|v| v.as_str()).unwrap_or("Inconnu");
                        let msg = evt.data.get("msg").and_then(|v| v.as_str()).unwrap_or("");
                        let display_name = self.prefs.aliases.get(from).cloned().unwrap_or_else(|| from.to_string());
                        self.chat_messages.push(format!("[{}] {}", display_name, msg));
                    }
                    "file_progress" => {
                        let filename = evt.data.get("filename").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        let sent = evt.data.get("bytes_sent").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
                        let total = evt.data.get("total").and_then(|v| v.as_u64()).unwrap_or(1) as usize;
                        self.file_progress = Some((filename, sent, total));
                        if sent >= total {
                            // Clear progress after short delay or instantly
                            // To keep it simple, clear it when finished
                            self.file_progress = None;
                            self.chat_messages.push(">>> Transfert de fichier terminé.".to_string());
                        }
                    }
                    _ => {}
                }
            }
        }

        egui::TopBottomPanel::top("top_panel").frame(egui::Frame::default().fill(egui::Color32::from_rgb(30, 30, 30)).inner_margin(10.0)).show(ctx, |ui| {
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
                        ui.label("Démon connecté");
                    } else {
                        ui.label(egui::RichText::new("●").color(egui::Color32::RED));
                        ui.label("Démon déconnecté");
                    }
                });
            });
        });

        egui::SidePanel::left("left_panel").frame(egui::Frame::default().fill(egui::Color32::from_rgb(40, 40, 40)).inner_margin(10.0)).resizable(true).min_width(200.0).show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.heading("Annuaire");
                if ui.button("🔄").on_hover_text("Actualiser").clicked() {
                    self.send_command(IpcCommand { action: "list".into(), target: None, msg: None, path: None });
                }
            });
            ui.add_space(10.0);

            if self.peers.is_empty() {
                ui.label(egui::RichText::new("Aucun pair détecté.").italics());
            } else {
                egui::ScrollArea::vertical().show(ui, |ui| {
                    let mut sorted_peers = self.peers.clone();
                    // Sort: favorites first
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

        egui::CentralPanel::default().frame(egui::Frame::default().fill(egui::Color32::from_rgb(50, 50, 50)).inner_margin(20.0)).show(ctx, |ui| {
            if let Some(target) = self.selected_target.clone() {
                let display_name = self.prefs.aliases.get(&target).cloned().unwrap_or_else(|| target.clone());
                
                ui.horizontal(|ui| {
                    ui.heading(format!("Chat avec {}", display_name));
                    
                    // Renaming UI
                    let mut alias = display_name.clone();
                    if ui.add(egui::TextEdit::singleline(&mut alias).desired_width(100.0)).changed() {
                        self.prefs.aliases.insert(target.clone(), alias);
                        self.prefs.save();
                    }
                    
                    if ui.button("Connecter").clicked() {
                        self.send_command(IpcCommand { action: "connect".into(), target: Some(target.clone()), msg: None, path: None });
                        self.chat_messages.push(format!(">>> Tentative de connexion WebRTC..."));
                    }
                });
                ui.add_space(10.0);

                ui.horizontal(|ui| {
                    ui.selectable_value(&mut self.current_tab, Tab::Chat, "Chat P2P");
                    ui.selectable_value(&mut self.current_tab, Tab::Files, "Fichiers");
                });
                ui.separator();

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
                            let response = ui.add(egui::TextEdit::singleline(&mut self.input_message).hint_text("Écrivez un message...").desired_width(ui.available_width() - 60.0));
                            if ui.button("Envoyer").clicked() || (response.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter))) {
                                if !self.input_message.trim().is_empty() {
                                    self.send_command(IpcCommand { 
                                        action: "p2p".into(), 
                                        target: Some(target.clone()), 
                                        msg: Some(self.input_message.clone()), 
                                        path: None 
                                    });
                                    self.chat_messages.push(format!("[Moi] {}", self.input_message));
                                    self.input_message.clear();
                                    ui.memory_mut(|m| m.request_focus(response.id));
                                }
                            }
                        });
                    }
                    Tab::Files => {
                        ui.label("Glissez-déposez un fichier ici pour l'envoyer.");
                        
                        let rect = ui.available_rect_before_wrap();
                        let response = ui.interact(rect, ui.id().with("drop_zone"), egui::Sense::hover());
                        
                        if response.hovered() {
                            ui.painter().rect_filled(rect, 10.0, egui::Color32::from_rgba_unmultiplied(100, 100, 255, 50));
                        }

                        // Handle dropped files
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
                                ui.label(format!("Fichier prêt à l'envoi : {} ({} octets)", filename, size));
                                ui.horizontal(|ui| {
                                    let path_clone = path.clone();
                                    if ui.button("Envoyer").clicked() {
                                        self.send_command(IpcCommand {
                                            action: "sendfile".into(),
                                            target: Some(target.clone()),
                                            msg: None,
                                            path: Some(path_clone.to_string_lossy().to_string()),
                                        });
                                        self.chat_messages.push(format!(">>> Démarrage de l'envoi du fichier {:?}", path_clone));
                                        self.pending_file = None;
                                        self.current_tab = Tab::Chat;
                                    }
                                    if ui.button("Annuler").clicked() {
                                        self.pending_file = None;
                                    }
                                });
                            });
                        }

                        if let Some((name, sent, total)) = &self.file_progress {
                            ui.add_space(20.0);
                            ui.label(format!("Envoi en cours : {}", name));
                            let progress = if *total > 0 { *sent as f32 / *total as f32 } else { 0.0 };
                            ui.add(egui::ProgressBar::new(progress).show_percentage());
                        }
                    }
                }
            } else {
                ui.centered_and_justified(|ui| {
                    ui.label("Sélectionnez un pair dans l'annuaire pour démarrer.");
                });
            }
        });
    }
}
