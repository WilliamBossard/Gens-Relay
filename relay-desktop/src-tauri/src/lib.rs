use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tauri::menu::{CheckMenuItem, Menu, MenuItem};
use tauri::tray::{MouseButton, MouseButtonState, TrayIconBuilder, TrayIconEvent};
use tauri::{AppHandle, Manager, WebviewUrl, WebviewWindowBuilder, WindowEvent};

#[cfg(target_os = "windows")]
use std::os::windows::process::CommandExt;

const DEFAULT_ADMIN_ADDR: &str = "127.0.0.1:38421";
const SINGLE_INSTANCE_ADDR: &str = "127.0.0.1:48386";

#[cfg(target_os = "windows")]
const CREATE_NO_WINDOW: u32 = 0x08000000;

struct RelayState {
    child: Arc<Mutex<Option<Child>>>,
    exiting: Arc<AtomicBool>,
    minimized_notified: Arc<AtomicBool>,
}

fn relay_data_dir() -> Option<PathBuf> {
    if let Ok(v) = std::env::var("RELAY_DATA_DIR") {
        if !v.trim().is_empty() {
            return Some(PathBuf::from(v));
        }
    }
    #[cfg(target_os = "windows")]
    if let Ok(appdata) = std::env::var("APPDATA") {
        return Some(PathBuf::from(appdata).join("Gens-Relay-Server"));
    }
    #[cfg(not(target_os = "windows"))]
    if let Ok(home) = std::env::var("HOME") {
        return Some(PathBuf::from(home).join(".config").join("Gens-Relay-Server"));
    }
    None
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

fn normalize_loopback_addr(addr: &str) -> String {
    if let Some(rest) = addr.strip_prefix("0.0.0.0:") {
        format!("127.0.0.1:{}", rest)
    } else if let Some(rest) = addr.strip_prefix("::") {
        format!("127.0.0.1{}", rest)
    } else if addr.starts_with(':') {
        format!("127.0.0.1{}", addr)
    } else {
        addr.to_string()
    }
}

fn read_relay_admin_target() -> (String, String, String) {
    let mut admin_addr = DEFAULT_ADMIN_ADDR.to_string();
    let mut path_prefix = "".to_string();

    if let Some(dir) = relay_data_dir() {
        if let Ok(raw) = std::fs::read_to_string(dir.join("relay-config.json")) {
            if let Some(val) = extract_json_string(&raw, "admin_listen") {
                let trimmed = val.trim();
                if !trimmed.is_empty() && trimmed != "off" {
                    admin_addr = normalize_loopback_addr(trimmed);
                } else if trimmed == "off" || trimmed.is_empty() {
                    if let Some(la) = extract_json_string(&raw, "listen_addr") {
                        admin_addr = normalize_loopback_addr(la.trim());
                        path_prefix = "/admin".to_string();
                    }
                }
            }
        }
    }

    let url = if path_prefix.is_empty() {
        format!("http://{}", admin_addr)
    } else {
        format!("http://{}{}/", admin_addr, path_prefix)
    };
    (admin_addr, path_prefix, url)
}

fn read_shutdown_token() -> Option<String> {
    let dir = relay_data_dir()?;
    std::fs::read_to_string(dir.join("shutdown.token"))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
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
            if stream.write_all(b"GENS_RELAY_SHOW\n").is_ok() {
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
    .title("Gens-Relay Server")
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

fn find_relay_binary() -> Option<PathBuf> {
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let candidates = [
                dir.join("relay-server.exe"),
                dir.join("relay-server-x86_64-pc-windows-msvc.exe"),
                dir.join("relay-server"),
            ];
            for c in candidates {
                if c.exists() {
                    return Some(c);
                }
            }
        }
    }
    let dev_candidates = [
        PathBuf::from("binaries/relay-server-x86_64-pc-windows-msvc.exe"),
        PathBuf::from("../../dist/relay-server.exe"),
        PathBuf::from("../dist/relay-server.exe"),
    ];
    for c in dev_candidates {
        if c.exists() {
            return Some(c);
        }
    }
    None
}

fn is_relay_listening(addr: &str) -> bool {
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
                        return resp.contains("blind_relay_enabled")
                            || resp.contains("Gens-Relay-Admin")
                            || !resp.starts_with("HTTP/");
                    }
                }
            }
            return true;
        }
    }
    false
}

fn spawn_relay_if_needed() -> Option<Child> {
    let (admin_addr, _, _) = read_relay_admin_target();
    if is_relay_listening(&admin_addr) {
        return None;
    }
    let bin = find_relay_binary()?;
    let mut cmd = Command::new(bin);
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
        "Gens-Relay-Server",
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
                "Gens-Relay-Server",
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
            "Gens-Relay-Server",
            "/f",
        ]);
    }
    cmd.creation_flags(CREATE_NO_WINDOW);
    let _ = cmd.output();
}

#[cfg(not(target_os = "windows"))]
fn set_autostart_enabled(_enable: bool) {}

fn request_graceful_shutdown() {
    let (admin_addr, prefix, _) = read_relay_admin_target();
    if let Ok(addr) = admin_addr.parse() {
        if let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(300)) {
            let _ = stream.set_write_timeout(Some(Duration::from_millis(300)));
            let token_hdr = read_shutdown_token()
                .map(|t| format!("X-Gens-Shutdown-Token: {}\r\n", t))
                .unwrap_or_default();
            let req = format!(
                "POST {}/api/v1/shutdown HTTP/1.1\r\nHost: {}\r\n{}Content-Length: 0\r\nConnection: close\r\n\r\n",
                prefix, admin_addr, token_hdr
            );
            let _ = stream.write_all(req.as_bytes());
        }
    }
}

fn stop_relay(child_arc: &Arc<Mutex<Option<Child>>>, exiting: &Arc<AtomicBool>) {
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

    let child_arc = Arc::new(Mutex::new(spawn_relay_if_needed()));
    let exiting_arc = Arc::new(AtomicBool::new(false));
    let minimized_notified_arc = Arc::new(AtomicBool::new(false));

    tauri::Builder::default()
        .manage(RelayState {
            child: child_arc.clone(),
            exiting: exiting_arc.clone(),
            minimized_notified: minimized_notified_arc.clone(),
        })
        .setup(move |app| {
            let open_item = MenuItem::with_id(app, "open", "Ouvrir Admin Relais", true, None::<&str>)?;
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
                .tooltip("Gens-Relay Server — Administration")
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
                        stop_relay(&child_for_menu, &exiting_for_menu);
                        app_handle.exit(0);
                    }
                    _ => {}
                });

            if let Some(icon) = app.default_window_icon().cloned() {
                tray_builder = tray_builder.icon(icon);
            }
            let _tray = tray_builder.build(app)?;

            let handle = app.handle().clone();
            std::thread::spawn(move || {
                let (admin_addr, _, web_url) = read_relay_admin_target();
                let deadline = Instant::now() + Duration::from_secs(10);
                while Instant::now() < deadline {
                    if is_relay_listening(&admin_addr) {
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

            // Watchdog: restart relay-server sidecar ONLY on unexpected crash (code != 0) with exponential backoff.
            let watchdog_child = child_arc.clone();
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
                                        watchdog_exit.store(true, Ordering::SeqCst);
                                        consecutive_failures = 0;
                                    } else {
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
                    if crashed && !watchdog_exit.load(Ordering::SeqCst) {
                        consecutive_failures += 1;
                        if consecutive_failures <= 5 {
                            let delay = (1u64 << (consecutive_failures - 1)).min(16);
                            std::thread::sleep(Duration::from_secs(delay));
                            if !watchdog_exit.load(Ordering::SeqCst) {
                                if let Some(new_child) = spawn_relay_if_needed() {
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
                                if msg.starts_with("GENS_RELAY_SHOW") {
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
                if let WindowEvent::CloseRequested { api, .. } = event {
                    let _ = window.hide();
                    api.prevent_close();
                    let app_handle = window.app_handle();
                    if let Some(state) = app_handle.try_state::<RelayState>() {
                        if !state.minimized_notified.swap(true, Ordering::SeqCst) {
                            show_minimized_notice(app_handle);
                        }
                    }
                }
            }
        })
        .build(tauri::generate_context!())
        .expect("error while building tauri application")
        .run(|app_handle, event| {
            if let tauri::RunEvent::Exit = event {
                if let Some(state) = app_handle.try_state::<RelayState>() {
                    stop_relay(&state.child, &state.exiting);
                }
            }
        });
}
