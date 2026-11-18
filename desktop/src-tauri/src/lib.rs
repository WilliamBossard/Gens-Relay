use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tauri::menu::{CheckMenuItem, Menu, MenuItem};
use tauri::tray::{MouseButton, MouseButtonState, TrayIconBuilder, TrayIconEvent};
use tauri::{AppHandle, DragDropEvent, Manager, WebviewUrl, WebviewWindowBuilder, WindowEvent};

#[cfg(target_os = "windows")]
use std::os::windows::process::CommandExt;

const DEFAULT_WEB_ADDR: &str = "127.0.0.1:8385";
const SINGLE_INSTANCE_ADDR: &str = "127.0.0.1:48385";

#[cfg(target_os = "windows")]
const CREATE_NO_WINDOW: u32 = 0x08000000;

struct DaemonState {
    child: Arc<Mutex<Option<Child>>>,
    exiting: Arc<AtomicBool>,
    minimized_notified: Arc<AtomicBool>,
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

fn read_daemon_web_target() -> (String, String) {
    let mut addr = DEFAULT_WEB_ADDR.to_string();
    let mut tls = false;
    if let Some(dir) = daemon_data_dir() {
        if let Ok(raw) = std::fs::read_to_string(dir.join("config.json")) {
            if let Some(val) = extract_json_string(&raw, "web_listen") {
                if !val.trim().is_empty() {
                    addr = val.trim().to_string();
                }
            }
            if extract_json_bool(&raw, "web_tls").unwrap_or(false) {
                tls = true;
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
    (connect_addr, url)
}

fn find_json_value_start<'a>(raw: &'a str, key: &str) -> Option<&'a str> {
    let bytes = raw.as_bytes();
    let mut i = 0;
    let mut depth = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'{' => {
                depth += 1;
                i += 1;
            }
            b'}' => {
                depth = depth.saturating_sub(1);
                i += 1;
            }
            b'"' => {
                i += 1;
                let start = i;
                let mut escaped = false;
                while i < bytes.len() {
                    if escaped {
                        escaped = false;
                        i += 1;
                        continue;
                    }
                    if bytes[i] == b'\\' {
                        escaped = true;
                        i += 1;
                        continue;
                    }
                    if bytes[i] == b'"' {
                        break;
                    }
                    i += 1;
                }
                let tok = &raw[start..i.min(bytes.len())];
                if i < bytes.len() {
                    i += 1;
                }
                if depth == 1 && tok == key {
                    let rest = raw[i..].trim_start();
                    if let Some(after_colon) = rest.strip_prefix(':') {
                        return Some(after_colon.trim_start());
                    }
                }
            }
            _ => i += 1,
        }
    }
    None
}

fn extract_json_string(raw: &str, key: &str) -> Option<String> {
    let rest = find_json_value_start(raw, key)?;
    let mut chars = rest.chars();
    if chars.next()? != '"' {
        return None;
    }
    let mut out = String::new();
    let mut escaped = false;
    for ch in chars {
        if escaped {
            match ch {
                'n' => out.push('\n'),
                'r' => out.push('\r'),
                't' => out.push('\t'),
                other => out.push(other),
            }
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == '"' {
            return Some(out);
        } else {
            out.push(ch);
        }
    }
    None
}

fn extract_json_bool(raw: &str, key: &str) -> Option<bool> {
    let rest = find_json_value_start(raw, key)?;
    if rest.starts_with("true") {
        Some(true)
    } else if rest.starts_with("false") {
        Some(false)
    } else {
        None
    }
}

fn read_shutdown_token() -> Option<String> {
    let dir = daemon_data_dir()?;
    std::fs::read_to_string(dir.join("shutdown.token"))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn json_escape_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn show_main_window(app_handle: &AppHandle) {
    if let Some(win) = app_handle.get_webview_window("main") {
        let _ = win.unminimize();
        let _ = win.show();
        let _ = win.set_focus();
    }
}

fn try_notify_existing_instance() -> bool {
    if let Ok(addr) = SINGLE_INSTANCE_ADDR.parse() {
        if let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(300)) {
            let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
            let _ = stream.set_write_timeout(Some(Duration::from_millis(500)));
            if stream.write_all(b"GENS_SHOW\n").is_ok() {
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
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let candidates = [
                dir.join("gens-daemon.exe"),
                dir.join("gens-daemon-x86_64-pc-windows-msvc.exe"),
                dir.join("gens-daemon"),
            ];
            for c in candidates {
                if c.exists() {
                    return Some(c);
                }
            }
        }
    }
    let dev_candidates = [
        PathBuf::from("binaries/gens-daemon-x86_64-pc-windows-msvc.exe"),
        PathBuf::from("../../dist/gens-daemon.exe"),
        PathBuf::from("../dist/gens-daemon.exe"),
    ];
    for c in dev_candidates {
        if c.exists() {
            return Some(c);
        }
    }
    None
}

fn is_daemon_listening(addr: &str) -> bool {
    if let Ok(sock) = addr.parse() {
        if let Ok(mut stream) = TcpStream::connect_timeout(&sock, Duration::from_millis(250)) {
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
    let (connect_addr, _) = read_daemon_web_target();
    if is_daemon_listening(&connect_addr) {
        return None;
    }
    let bin = find_daemon_binary()?;
    let mut cmd = Command::new(bin);
    // Only pass default --web if user hasn't already persisted config.json
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
    let mut cmd = Command::new("reg");
    cmd.args([
        "query",
        r"HKCU\Software\Microsoft\Windows\CurrentVersion\Run",
        "/v",
        "Gens-Relay",
    ]);
    cmd.creation_flags(CREATE_NO_WINDOW);
    cmd.output().map(|o| o.status.success()).unwrap_or(false)
}

#[cfg(not(target_os = "windows"))]
fn is_autostart_enabled() -> bool {
    false
}

#[cfg(target_os = "windows")]
fn set_autostart_enabled(enable: bool) {
    let mut cmd = Command::new("reg");
    if enable {
        if let Ok(exe) = std::env::current_exe() {
            let val = format!("\"{}\"", exe.to_string_lossy());
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
        } else {
            return;
        }
    } else {
        cmd.args([
            "delete",
            r"HKCU\Software\Microsoft\Windows\CurrentVersion\Run",
            "/v",
            "Gens-Relay",
            "/f",
        ]);
    }
    cmd.creation_flags(CREATE_NO_WINDOW);
    let _ = cmd.output();
}

#[cfg(not(target_os = "windows"))]
fn set_autostart_enabled(_enable: bool) {}

fn request_graceful_shutdown() {
    let (connect_addr, _) = read_daemon_web_target();
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

fn stop_daemon(child_arc: &Arc<Mutex<Option<Child>>>, exiting: &Arc<AtomicBool>) {
    exiting.store(true, Ordering::SeqCst);
    request_graceful_shutdown();

    if let Ok(mut guard) = child_arc.lock() {
        if let Some(ref mut child) = *guard {
            let deadline = Instant::now() + Duration::from_secs(3);
            let mut exited = false;
            while Instant::now() < deadline {
                if let Ok(Some(_)) = child.try_wait() {
                    exited = true;
                    break;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            if !exited {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
        *guard = None;
    }
}

pub fn run() {
    if try_notify_existing_instance() {
        return;
    }
    let instance_listener = TcpListener::bind(SINGLE_INSTANCE_ADDR).ok();

    let child_arc = Arc::new(Mutex::new(spawn_daemon_if_needed()));
    let exiting_arc = Arc::new(AtomicBool::new(false));
    let minimized_notified_arc = Arc::new(AtomicBool::new(false));

    tauri::Builder::default()
        .manage(DaemonState {
            child: child_arc.clone(),
            exiting: exiting_arc.clone(),
            minimized_notified: minimized_notified_arc.clone(),
        })
        .setup(move |app| {
            let open_item = MenuItem::with_id(app, "open", "Ouvrir Gens-Relay", true, None::<&str>)?;
            let autostart_item = CheckMenuItem::with_id(
                app,
                "autostart",
                "Lancer avec Windows",
                true,
                is_autostart_enabled(),
                None::<&str>,
            )?;
            let quit_item = MenuItem::with_id(app, "quit", "Quitter", true, None::<&str>)?;

            let menu = Menu::with_items(app, &[&open_item, &autostart_item, &quit_item])?;

            let autostart_clone = autostart_item.clone();
            let child_for_menu = child_arc.clone();
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
                    "autostart" => {
                        if let Ok(checked) = autostart_clone.is_checked() {
                            set_autostart_enabled(checked);
                        }
                    }
                    "quit" => {
                        stop_daemon(&child_for_menu, &exiting_for_menu);
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
                let (connect_addr, web_url) = read_daemon_web_target();
                let deadline = Instant::now() + Duration::from_secs(10);
                while Instant::now() < deadline {
                    if is_daemon_listening(&connect_addr) {
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

            // Watchdog: restart sidecar if it crashes unexpectedly while app is running
            let watchdog_child = child_arc.clone();
            let watchdog_exit = exiting_arc.clone();
            std::thread::spawn(move || {
                loop {
                    std::thread::sleep(Duration::from_secs(5));
                    if watchdog_exit.load(Ordering::SeqCst) {
                        break;
                    }
                    let mut should_respawn = false;
                    if let Ok(mut guard) = watchdog_child.lock() {
                        if let Some(ref mut child) = *guard {
                            if let Ok(Some(_)) = child.try_wait() {
                                *guard = None;
                                should_respawn = true;
                            }
                        }
                    }
                    if should_respawn && !watchdog_exit.load(Ordering::SeqCst) {
                        if let Some(new_child) = spawn_daemon_if_needed() {
                            if let Ok(mut guard) = watchdog_child.lock() {
                                *guard = Some(new_child);
                            }
                        }
                    }
                }
            });

            // Single-instance listener: re-opening the .exe brings existing window to front
            if let Some(listener) = instance_listener {
                let ipc_handle = app.handle().clone();
                let ipc_exit = exiting_arc.clone();
                std::thread::spawn(move || {
                    for stream in listener.incoming() {
                        if ipc_exit.load(Ordering::SeqCst) {
                            break;
                        }
                        if let Ok(mut s) = stream {
                            let _ = s.set_read_timeout(Some(Duration::from_millis(300)));
                            let mut buf = [0u8; 32];
                            if let Ok(n) = s.read(&mut buf) {
                                let msg = String::from_utf8_lossy(&buf[..n]);
                                if msg.starts_with("GENS_SHOW") {
                                    let _ = s.write_all(b"OK\n");
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
                    stop_daemon(&state.child, &state.exiting);
                }
            }
        });
}
