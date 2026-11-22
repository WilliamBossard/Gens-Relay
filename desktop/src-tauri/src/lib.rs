use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tauri::menu::{CheckMenuItem, Menu, MenuItem};
use tauri::tray::{MouseButton, MouseButtonState, TrayIconBuilder, TrayIconEvent};
use tauri::{AppHandle, DragDropEvent, Manager, State, WebviewUrl, WebviewWindowBuilder, WindowEvent};

#[cfg(target_os = "windows")]
use std::os::windows::process::CommandExt;

const DEFAULT_WEB_ADDR: &str = "127.0.0.1:8385";
const SINGLE_INSTANCE_ADDR: &str = "127.0.0.1:48385";

#[cfg(target_os = "windows")]
const CREATE_NO_WINDOW: u32 = 0x08000000;

#[cfg(target_os = "windows")]
mod win_mutex {
    use std::ffi::OsStr;
    use std::os::windows::ffi::OsStrExt;

    const ERROR_ALREADY_EXISTS: u32 = 183;

    #[link(name = "kernel32")]
    extern "system" {
        fn CreateMutexW(
            lp_mutex_attributes: *mut core::ffi::c_void,
            b_initial_owner: i32,
            lp_name: *const u16,
        ) -> *mut core::ffi::c_void;
        fn GetLastError() -> u32;
    }

    #[allow(dead_code)]
    pub struct NamedMutex(*mut core::ffi::c_void);
    unsafe impl Send for NamedMutex {}
    unsafe impl Sync for NamedMutex {}

    pub fn try_acquire(name: &str) -> Option<NamedMutex> {
        let wide: Vec<u16> = OsStr::new(name).encode_wide().chain(Some(0)).collect();
        unsafe {
            let h = CreateMutexW(core::ptr::null_mut(), 0, wide.as_ptr());
            if h.is_null() {
                return None;
            }
            if GetLastError() == ERROR_ALREADY_EXISTS {
                return None;
            }
            Some(NamedMutex(h))
        }
    }
}

struct DaemonState {
    child: Arc<Mutex<Option<Child>>>,
    manually_stopped: Arc<AtomicBool>,
    exiting: Arc<AtomicBool>,
    minimized_notified: Arc<AtomicBool>,
    pending_drop_paths: Arc<Mutex<Vec<String>>>,
}

fn daemon_data_dir() -> Option<PathBuf> {
    if let Ok(v) = std::env::var("GENS_DATA_DIR") {
        if !v.trim().is_empty() {
            return Some(PathBuf::from(v));
        }
    }
    #[cfg(target_os = "windows")]
    if let Ok(appdata) = std::env::var("APPDATA") {
        return Some(PathBuf::from(appdata).join("Gens-Relay"));
    }
    #[cfg(not(target_os = "windows"))]
    if let Ok(home) = std::env::var("HOME") {
        return Some(PathBuf::from(home).join(".config").join("Gens-Relay"));
    }
    None
}

fn read_daemon_web_target() -> (String, String, bool) {
    let mut addr = DEFAULT_WEB_ADDR.to_string();
    let mut tls = false;
    if let Some(dir) = daemon_data_dir() {
        if let Ok(raw) = std::fs::read_to_string(dir.join("config.json")) {
            if let Ok(cfg) = serde_json::from_str::<serde_json::Value>(&raw) {
                if let Some(val) = cfg.get("web_listen").and_then(|v| v.as_str()) {
                    if !val.trim().is_empty() {
                        addr = val.trim().to_string();
                    }
                }
                if cfg.get("web_tls").and_then(|v| v.as_bool()).unwrap_or(false) {
                    tls = true;
                }
            }
        }
    }
    let connect_addr = if let Some(rest) = addr.strip_prefix("0.0.0.0:") {
        format!("127.0.0.1:{}", rest)
    } else if let Some(rest) = addr.strip_prefix("::") {
        format!("127.0.0.1{}", rest)
    } else if addr.starts_with(':') {
        format!("127.0.0.1{}", addr)
    } else {
        addr
    };
    let scheme = if tls { "https" } else { "http" };
    let url = format!("{}://{}", scheme, connect_addr);
    (connect_addr, url, tls)
}


fn read_shutdown_token() -> Option<String> {
    let dir = daemon_data_dir()?;
    std::fs::read_to_string(dir.join("shutdown.token"))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn json_escape_str(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_else(|_| "\"\"".to_string())
}

fn show_main_window(app_handle: &AppHandle) {
    if let Some(win) = app_handle.get_webview_window("main") {
        let _ = win.unminimize();
        let _ = win.show();
        let _ = win.set_focus();
    }
}

fn collect_cli_args() -> (bool, Vec<String>) {
    let mut start_minimized = false;
    let mut paths = Vec::new();
    for arg in std::env::args().skip(1) {
        if arg == "--minimized" {
            start_minimized = true;
            continue;
        }
        if arg.starts_with('-') || arg.trim().is_empty() {
            continue;
        }
        let pb = PathBuf::from(&arg);
        if pb.exists() {
            if let Ok(canon) = std::fs::canonicalize(&pb) {
                let s = canon.to_string_lossy().trim_start_matches(r"\\?\").to_string();
                paths.push(s);
                continue;
            }
        }
        paths.push(arg);
    }
    (start_minimized, paths)
}

fn try_notify_existing_instance(paths: &[String]) -> bool {
    if let Ok(addr) = SINGLE_INSTANCE_ADDR.parse() {
        if let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(300)) {
            let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
            let _ = stream.set_write_timeout(Some(Duration::from_millis(500)));
            let msg = if paths.is_empty() {
                "GENS_SHOW\n".to_string()
            } else {
                format!("GENS_DROP_PATHS\t{}\n", paths.join("\t"))
            };
            if stream.write_all(msg.as_bytes()).is_ok() {
                let mut buf = [0u8; 8];
                if let Ok(n) = stream.read(&mut buf) {
                    if n >= 2 && &buf[..2] == b"OK" {
                        return true;
                    }
                }
            }
        }
    }
    false
}

fn show_minimized_notice(app_handle: &AppHandle) {
    if app_handle.get_webview_window("minimized-notice").is_some() {
        return;
    }
    let width = 370.0;
    let height = 92.0;

    let mut builder = WebviewWindowBuilder::new(
        app_handle,
        "minimized-notice",
        WebviewUrl::App("minimized.html".into()),
    )
    .title("Gens-Relay")
    .inner_size(width, height)
    .decorations(false)
    .resizable(false)
    .skip_taskbar(true)
    .always_on_top(true)
    .focused(false);

    if let Some(main_win) = app_handle.get_webview_window("main") {
        if let Ok(Some(monitor)) = main_win.current_monitor() {
            let scale = monitor.scale_factor();
            let mon_pos = monitor.position();
            let mon_size = monitor.size();
            let x = (mon_pos.x as f64 + mon_size.width as f64) / scale - width - 18.0;
            let y = (mon_pos.y as f64 + mon_size.height as f64) / scale - height - 62.0;
            builder = builder.position(x.max(0.0), y.max(0.0));
        }
    }

    if let Ok(notice) = builder.build() {
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(4));
            let _ = notice.destroy();
        });
    }
}

fn find_daemon_binary() -> Option<PathBuf> {
    let mut candidates = Vec::new();
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            candidates.push(dir.join("gens-daemon.exe"));
            candidates.push(dir.join("gens-daemon-x86_64-pc-windows-msvc.exe"));
            candidates.push(dir.join("gens-daemon"));
            if let Some(parent) = dir.parent() {
                candidates.push(parent.join("gens-daemon.exe"));
            }
        }
    }
    #[cfg(target_os = "windows")]
    {
        if let Ok(pf) = std::env::var("ProgramFiles") {
            candidates.push(PathBuf::from(pf).join("Gens-Relay").join("gens-daemon.exe"));
        }
        if let Ok(local) = std::env::var("LOCALAPPDATA") {
            candidates.push(
                PathBuf::from(local)
                    .join("Programs")
                    .join("Gens-Relay")
                    .join("gens-daemon.exe"),
            );
        }
    }
    candidates.extend([
        PathBuf::from("binaries/gens-daemon-x86_64-pc-windows-msvc.exe"),
        PathBuf::from("../../dist/gens-daemon.exe"),
        PathBuf::from("../dist/gens-daemon.exe"),
    ]);
    for c in candidates {
        if c.exists() {
            return Some(c);
        }
    }
    None
}

fn is_daemon_listening(addr: &str, is_tls: bool) -> bool {
    if let Ok(sock) = addr.parse() {
        if let Ok(mut stream) = TcpStream::connect_timeout(&sock, Duration::from_millis(250)) {
            if is_tls {
                return daemon_data_dir()
                    .map(|d| d.join("shutdown.token").exists())
                    .unwrap_or(true);
            }
            let _ = stream.set_read_timeout(Some(Duration::from_millis(300)));
            let _ = stream.set_write_timeout(Some(Duration::from_millis(300)));
            let req = format!(
                "GET /api/v1/status HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
                addr
            );
            if stream.write_all(req.as_bytes()).is_ok() {
                let mut buf = [0u8; 512];
                if let Ok(n) = stream.read(&mut buf) {
                    if n > 0 {
                        let resp = String::from_utf8_lossy(&buf[..n]);
                        return resp.contains("device_id")
                            || resp.contains("Gens-Relay")
                            || resp.contains("setup_required")
                            || resp.contains("Unauthorized")
                            || !resp.starts_with("HTTP/");
                    }
                }
            }
            return true;
        }
    }
    false
}

fn spawn_daemon_if_needed() -> Option<Child> {
    let (connect_addr, _, is_tls) = read_daemon_web_target();
    if is_daemon_listening(&connect_addr, is_tls) {
        return None;
    }
    let bin = find_daemon_binary()?;
    let mut cmd = Command::new(bin);
    let has_cfg = daemon_data_dir()
        .map(|d| d.join("config.json").exists())
        .unwrap_or(false);
    if !has_cfg {
        cmd.args(["--web", DEFAULT_WEB_ADDR]);
    }
    #[cfg(target_os = "windows")]
    {
        cmd.creation_flags(CREATE_NO_WINDOW);
    }
    cmd.spawn().ok()
}

#[cfg(target_os = "windows")]
fn is_autostart_enabled() -> bool {
    for val in ["Gens-RelayDaemon", "Gens-Relay"] {
        let mut cmd = Command::new("reg");
        cmd.args([
            "query",
            r"HKCU\Software\Microsoft\Windows\CurrentVersion\Run",
            "/v",
            val,
        ]);
        cmd.creation_flags(CREATE_NO_WINDOW);
        if cmd.output().map(|o| o.status.success()).unwrap_or(false) {
            return true;
        }
    }
    false
}

#[cfg(not(target_os = "windows"))]
fn is_autostart_enabled() -> bool {
    false
}

#[cfg(target_os = "windows")]
fn set_autostart_enabled(enable: bool) {
    if enable {
        if let Ok(exe) = std::env::current_exe() {
            let val = format!("\"{}\" --minimized", exe.to_string_lossy());
            let mut cmd = Command::new("reg");
            cmd.args([
                "add",
                r"HKCU\Software\Microsoft\Windows\CurrentVersion\Run",
                "/v",
                "Gens-Relay",
                "/t",
                "REG_SZ",
                "/d",
                &val,
                "/f",
            ]);
            cmd.creation_flags(CREATE_NO_WINDOW);
            let _ = cmd.output();
        }
    } else {
        for val in ["Gens-Relay", "Gens-RelayDaemon"] {
            let mut cmd = Command::new("reg");
            cmd.args([
                "delete",
                r"HKCU\Software\Microsoft\Windows\CurrentVersion\Run",
                "/v",
                val,
                "/f",
            ]);
            cmd.creation_flags(CREATE_NO_WINDOW);
            let _ = cmd.output();
        }
    }
}

#[cfg(not(target_os = "windows"))]
fn set_autostart_enabled(_enable: bool) {}

fn request_graceful_shutdown() {
    if let (Some(dir), Some(tok)) = (daemon_data_dir(), read_shutdown_token()) {
        // IPC token file works reliably even when HTTPS (web_tls: true) or Basic Auth is active.
        let _ = std::fs::write(dir.join("shutdown.req"), &tok);
    }
    let (connect_addr, _, is_tls) = read_daemon_web_target();
    if !is_tls {
        if let Ok(addr) = connect_addr.parse() {
            if let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(300)) {
                let _ = stream.set_write_timeout(Some(Duration::from_millis(300)));
                let token_hdr = read_shutdown_token()
                    .map(|t| format!("X-Gens-Shutdown-Token: {}\r\n", t))
                    .unwrap_or_default();
                let req = format!(
                    "POST /api/v1/shutdown HTTP/1.1\r\nHost: {}\r\n{}Content-Length: 0\r\nConnection: close\r\n\r\n",
                    connect_addr, token_hdr
                );
                let _ = stream.write_all(req.as_bytes());
            }
        }
    }
}

fn stop_daemon(child_arc: &Arc<Mutex<Option<Child>>>, manually_stopped: &Arc<AtomicBool>) {
    manually_stopped.store(true, Ordering::SeqCst);
    request_graceful_shutdown();

    let (connect_addr, _, is_tls) = read_daemon_web_target();
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        if let Ok(mut guard) = child_arc.lock() {
            if let Some(ref mut child) = *guard {
                if let Ok(Some(_)) = child.try_wait() {
                    *guard = None;
                    return;
                }
            } else if !is_daemon_listening(&connect_addr, is_tls) {
                return;
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    if let Ok(mut guard) = child_arc.lock() {
        if let Some(ref mut child) = *guard {
            let _ = child.kill();
            let _ = child.wait();
        }
        *guard = None;
    }
}

#[tauri::command]
fn start_daemon(state: State<'_, DaemonState>) -> Result<bool, String> {
    state.manually_stopped.store(false, Ordering::SeqCst);
    let (connect_addr, _, is_tls) = read_daemon_web_target();
    if is_daemon_listening(&connect_addr, is_tls) {
        return Ok(true);
    }
    let spawned = spawn_daemon_if_needed()
        .ok_or_else(|| "Binaire gens-daemon introuvable".to_string())?;
    if let Ok(mut guard) = state.child.lock() {
        *guard = Some(spawned);
    }
    Ok(true)
}

#[tauri::command]
fn stop_daemon_cmd(state: State<'_, DaemonState>) -> Result<bool, String> {
    stop_daemon(&state.child, &state.manually_stopped);
    Ok(true)
}

#[tauri::command]
fn take_pending_drop_paths(state: State<'_, DaemonState>) -> Vec<String> {
    if let Ok(mut guard) = state.pending_drop_paths.lock() {
        return std::mem::take(&mut *guard);
    }
    Vec::new()
}

#[tauri::command]
fn send_native_notification(title: String, body: String) -> Result<(), String> {
    #[cfg(target_os = "windows")]
    {
        let clean = |s: &str| {
            s.replace('&', "&amp;")
                .replace('<', "&lt;")
                .replace('>', "&gt;")
                .replace('\'', "''")
        };
        let t = clean(&title);
        let b = clean(&body);
        let script = format!(
            "[Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType = WindowsRuntime] > $null; \
             $xml = [Windows.Data.Xml.Dom.XmlDocument, Windows.Data.Xml.Dom.XmlDocument, ContentType = WindowsRuntime]::new(); \
             $xml.LoadXml('<toast><visual><binding template=\"ToastText02\"><text id=\"1\">{}</text><text id=\"2\">{}</text></binding></visual></toast>'); \
             $toast = [Windows.UI.Notifications.ToastNotification]::new($xml); \
             [Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier('Gens-Relay').Show($toast);",
            t, b
        );
        let mut cmd = Command::new("powershell.exe");
        cmd.args(["-NoProfile", "-NonInteractive", "-WindowStyle", "Hidden", "-Command", &script]);
        cmd.creation_flags(CREATE_NO_WINDOW);
        let _ = cmd.spawn();
    }
    #[cfg(not(target_os = "windows"))]
    let _ = (title, body);
    Ok(())
}

pub fn run() {
    let (start_minimized, initial_paths) = collect_cli_args();

    #[cfg(target_os = "windows")]
    let _single_mutex = match win_mutex::try_acquire("Local\\GensRelayDesktopInstanceMutex") {
        Some(m) => Some(m),
        None => {
            try_notify_existing_instance(&initial_paths);
            return;
        }
    };

    #[cfg(not(target_os = "windows"))]
    if try_notify_existing_instance(&initial_paths) {
        return;
    }

    let instance_listener = TcpListener::bind(SINGLE_INSTANCE_ADDR).ok();

    let child_arc = Arc::new(Mutex::new(spawn_daemon_if_needed()));
    let manually_stopped_arc = Arc::new(AtomicBool::new(false));
    let exiting_arc = Arc::new(AtomicBool::new(false));
    let minimized_notified_arc = Arc::new(AtomicBool::new(false));
    let pending_paths_arc = Arc::new(Mutex::new(initial_paths));

    tauri::Builder::default()
        .manage(DaemonState {
            child: child_arc.clone(),
            manually_stopped: manually_stopped_arc.clone(),
            exiting: exiting_arc.clone(),
            minimized_notified: minimized_notified_arc.clone(),
            pending_drop_paths: pending_paths_arc.clone(),
        })
        .invoke_handler(tauri::generate_handler![
            start_daemon,
            stop_daemon_cmd,
            take_pending_drop_paths,
            send_native_notification
        ])
        .setup(move |app| {
            if start_minimized {
                if let Some(win) = app.get_webview_window("main") {
                    let _ = win.hide();
                }
            }

            let open_item = MenuItem::with_id(app, "open", "Ouvrir Gens-Relay", true, None::<&str>)?;
            let start_daemon_item = MenuItem::with_id(app, "start_daemon", "Démarrer le daemon", true, None::<&str>)?;
            let stop_daemon_item = MenuItem::with_id(app, "stop_daemon", "Arrêter le daemon", true, None::<&str>)?;
            let autostart_item = CheckMenuItem::with_id(
                app,
                "autostart",
                "Lancer avec Windows",
                true,
                is_autostart_enabled(),
                None::<&str>,
            )?;
            let quit_ui_item = MenuItem::with_id(app, "quit_ui", "Quitter l'interface", true, None::<&str>)?;
            let stop_quit_item = MenuItem::with_id(app, "stop_quit", "Arrêter le daemon et quitter", true, None::<&str>)?;

            let menu = Menu::with_items(
                app,
                &[
                    &open_item,
                    &start_daemon_item,
                    &stop_daemon_item,
                    &autostart_item,
                    &quit_ui_item,
                    &stop_quit_item,
                ],
            )?;

            let autostart_clone = autostart_item.clone();
            let child_for_menu = child_arc.clone();
            let manual_for_menu = manually_stopped_arc.clone();
            let exiting_for_menu = exiting_arc.clone();

            let mut tray_builder = TrayIconBuilder::new()
                .menu(&menu)
                .tooltip("Gens-Relay — Sync P2P & QuickDrop")
                .show_menu_on_left_click(false)
                .on_tray_icon_event(|tray, event| {
                    if let TrayIconEvent::Click {
                        button: MouseButton::Left,
                        button_state: MouseButtonState::Up,
                        ..
                    } = event
                    {
                        show_main_window(tray.app_handle());
                    }
                })
                .on_menu_event(move |app_handle, event| match event.id.as_ref() {
                    "open" => {
                        show_main_window(app_handle);
                    }
                    "start_daemon" => {
                        manual_for_menu.store(false, Ordering::SeqCst);
                        if let Some(new_child) = spawn_daemon_if_needed() {
                            if let Ok(mut guard) = child_for_menu.lock() {
                                *guard = Some(new_child);
                            }
                        }
                    }
                    "stop_daemon" => {
                        let c = child_for_menu.clone();
                        let m = manual_for_menu.clone();
                        std::thread::spawn(move || stop_daemon(&c, &m));
                    }
                    "autostart" => {
                        if let Ok(checked) = autostart_clone.is_checked() {
                            set_autostart_enabled(checked);
                        }
                    }
                    "quit_ui" => {
                        // Leaves shared gens-daemon running for companion apps & background sync.
                        exiting_for_menu.store(true, Ordering::SeqCst);
                        app_handle.exit(0);
                    }
                    "stop_quit" => {
                        exiting_for_menu.store(true, Ordering::SeqCst);
                        stop_daemon(&child_for_menu, &manual_for_menu);
                        app_handle.exit(0);
                    }
                    _ => {}
                });

            if let Some(icon) = app.default_window_icon().cloned() {
                tray_builder = tray_builder.icon(icon);
            }
            let _tray = tray_builder.build(app)?;

            // Wait until gens-daemon Web UI responds, then navigate main window to exact Web UI
            let handle = app.handle().clone();
            std::thread::spawn(move || {
                let (connect_addr, web_url, is_tls) = read_daemon_web_target();
                let deadline = Instant::now() + Duration::from_secs(10);
                while Instant::now() < deadline {
                    if is_daemon_listening(&connect_addr, is_tls) {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
                if let Some(win) = handle.get_webview_window("main") {
                    if let Ok(url) = web_url.parse() {
                        let _ = win.navigate(url);
                    }
                }
            });

            // Watchdog: respawn owned daemon ONLY on unexpected crash (code != 0) with exponential backoff.
            // Voluntary clean shutdown (exit code 0) must NEVER auto-respawn.
            let watchdog_child = child_arc.clone();
            let watchdog_manual = manually_stopped_arc.clone();
            let watchdog_exit = exiting_arc.clone();
            std::thread::spawn(move || {
                let mut consecutive_failures: u32 = 0;
                let mut last_spawn = Instant::now();
                loop {
                    std::thread::sleep(Duration::from_secs(2));
                    if watchdog_exit.load(Ordering::SeqCst) {
                        break;
                    }
                    let mut crashed = false;
                    if let Ok(mut guard) = watchdog_child.lock() {
                        if let Some(ref mut child) = *guard {
                            match child.try_wait() {
                                Ok(Some(status)) => {
                                    *guard = None;
                                    if status.success() {
                                        watchdog_manual.store(true, Ordering::SeqCst);
                                        consecutive_failures = 0;
                                    } else if !watchdog_manual.load(Ordering::SeqCst) {
                                        crashed = true;
                                    }
                                }
                                Ok(None) => {
                                    if last_spawn.elapsed() > Duration::from_secs(30) {
                                        consecutive_failures = 0;
                                    }
                                }
                                Err(_) => {}
                            }
                        }
                    }
                    if crashed && !watchdog_exit.load(Ordering::SeqCst) && !watchdog_manual.load(Ordering::SeqCst) {
                        consecutive_failures += 1;
                        if consecutive_failures <= 5 {
                            let delay = (1u64 << (consecutive_failures - 1)).min(16);
                            std::thread::sleep(Duration::from_secs(delay));
                            if !watchdog_exit.load(Ordering::SeqCst) && !watchdog_manual.load(Ordering::SeqCst) {
                                if let Some(new_child) = spawn_daemon_if_needed() {
                                    last_spawn = Instant::now();
                                    if let Ok(mut guard) = watchdog_child.lock() {
                                        *guard = Some(new_child);
                                    }
                                }
                            }
                        }
                    }
                }
            });

            // Single-instance listener: re-opening brings existing window to front or forwards CLI paths
            if let Some(listener) = instance_listener {
                let ipc_handle = app.handle().clone();
                let ipc_exit = exiting_arc.clone();
                let ipc_pending = pending_paths_arc.clone();
                std::thread::spawn(move || {
                    for stream in listener.incoming() {
                        if ipc_exit.load(Ordering::SeqCst) {
                            break;
                        }
                        if let Ok(mut s) = stream {
                            let _ = s.set_read_timeout(Some(Duration::from_millis(500)));
                            let mut buf = [0u8; 4096];
                            if let Ok(n) = s.read(&mut buf) {
                                let msg = String::from_utf8_lossy(&buf[..n]).to_string();
                                if msg.starts_with("GENS_SHOW") {
                                    let _ = s.write_all(b"OK\n");
                                    show_main_window(&ipc_handle);
                                } else if let Some(rest) = msg.strip_prefix("GENS_DROP_PATHS\t") {
                                    let _ = s.write_all(b"OK\n");
                                    let dropped: Vec<String> = rest
                                        .trim_end_matches(['\r', '\n'])
                                        .split('\t')
                                        .filter(|p| !p.trim().is_empty())
                                        .map(|p| p.to_string())
                                        .collect();
                                    if !dropped.is_empty() {
                                        if let Ok(mut guard) = ipc_pending.lock() {
                                            guard.extend(dropped.clone());
                                        }
                                        let items: Vec<String> =
                                            dropped.iter().map(|p| json_escape_str(p)).collect();
                                        let script = format!(
                                            "window.dispatchEvent(new CustomEvent('gens-native-drop', {{ detail: [{}] }}));",
                                            items.join(",")
                                        );
                                        if let Some(win) = ipc_handle.get_webview_window("main") {
                                            let _ = win.eval(&script);
                                        }
                                    }
                                    show_main_window(&ipc_handle);
                                }
                            }
                        }
                    }
                });
            }

            Ok(())
        })
        .on_window_event(|window, event| {
            if window.label() == "main" {
                match event {
                    WindowEvent::CloseRequested { api, .. } => {
                        let _ = window.hide();
                        api.prevent_close();
                        let app_handle = window.app_handle();
                        if let Some(state) = app_handle.try_state::<DaemonState>() {
                            if !state.minimized_notified.swap(true, Ordering::SeqCst) {
                                show_minimized_notice(app_handle);
                            }
                        }
                    }
                    WindowEvent::DragDrop(DragDropEvent::Drop { paths, .. }) => {
                        if !paths.is_empty() {
                            let items: Vec<String> = paths
                                .iter()
                                .map(|p| json_escape_str(&p.to_string_lossy()))
                                .collect();
                            let script = format!(
                                "window.dispatchEvent(new CustomEvent('gens-native-drop', {{ detail: [{}] }}));",
                                items.join(",")
                            );
                            if let Some(webwin) = window.app_handle().get_webview_window("main") {
                                let _ = webwin.eval(&script);
                            }
                        }
                    }
                    _ => {}
                }
            }
        })
        .build(tauri::generate_context!())
        .expect("error while building tauri application")
        .run(|app_handle, event| {
            if let tauri::RunEvent::Exit = event {
                if let Some(state) = app_handle.try_state::<DaemonState>() {
                    state.exiting.store(true, Ordering::SeqCst);
                }
            }
        });
}
