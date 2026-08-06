use serde::{Deserialize, Serialize};
use tauri::{Emitter, State};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IpcCommand {
    pub action: String,
    pub target: Option<String>,
    pub msg: Option<String>,
    pub path: Option<String>,
}

pub struct TcpState {
    pub tx: mpsc::Sender<String>,
}

#[tauri::command]
async fn send_ipc_command(
    state: State<'_, TcpState>,
    action: String,
    target: Option<String>,
    msg: Option<String>,
    path: Option<String>,
) -> Result<(), String> {
    let cmd = IpcCommand {
        action,
        target,
        msg,
        path,
    };
    let json = serde_json::to_string(&cmd).map_err(|e| e.to_string())? + "\n";
    state.tx.send(json).await.map_err(|e| e.to_string())?;
    Ok(())
}

#[derive(Serialize)]
pub struct FileEntry {
    name: String,
    is_dir: bool,
    size: u64,
    time: u64,
}

#[derive(Serialize)]
pub struct LocalDirResult {
    path: String,
    parent: String,
    entries: Vec<FileEntry>,
}

#[tauri::command]
async fn list_local_dir(path: String) -> Result<LocalDirResult, String> {
    let target_path = if path.is_empty() || path == "." {
        std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."))
    } else {
        std::path::PathBuf::from(path)
    };

    let mut entries = Vec::new();
    if let Ok(mut dir) = tokio::fs::read_dir(&target_path).await {
        while let Ok(Some(entry)) = dir.next_entry().await {
            if let Ok(metadata) = entry.metadata().await {
                if let Ok(sys_time) = metadata.modified() {
                    let t_sec = sys_time.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
                    entries.push(FileEntry {
                        name: entry.file_name().to_string_lossy().to_string(),
                        is_dir: metadata.is_dir(),
                        size: metadata.len(),
                        time: t_sec,
                    });
                }
            }
        }
    }

    let parent = target_path.parent().map(|p| p.to_string_lossy().to_string()).unwrap_or_default();
    Ok(LocalDirResult {
        path: target_path.to_string_lossy().to_string(),
        parent,
        entries,
    })
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let (tx, mut rx) = mpsc::channel::<String>(32);

    tauri::Builder::default()
        .manage(TcpState { tx })
        .setup(|app| {
            let app_handle = app.handle().clone();

            tauri::async_runtime::spawn(async move {
                loop {
                    let mut port_str = "61879".to_string();
                    if let Ok(content) = std::fs::read_to_string("../.ipc_port") {
                        port_str = content.trim().to_string();
                    } else if let Ok(content) = std::fs::read_to_string("../../.ipc_port") {
                        port_str = content.trim().to_string();
                    }
                    
                    let addr = format!("127.0.0.1:{}", port_str);
                    match TcpStream::connect(&addr).await {
                        Ok(stream) => {
                            println!("[Tauri] Connected to gens-daemon");
                            let (reader, mut writer) = stream.into_split();
                            let mut buf_reader = BufReader::new(reader);

                            let app_handle_clone = app_handle.clone();
                            let mut read_task = tauri::async_runtime::spawn(async move {
                                let mut line = String::new();
                                loop {
                                    line.clear();
                                    match buf_reader.read_line(&mut line).await {
                                        Ok(0) => break, // EOF
                                        Ok(_) => {
                                            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&line) {
                                                let _ = app_handle_clone.emit("ipc-event", parsed);
                                            }
                                        }
                                        Err(_) => break,
                                    }
                                }
                            });

                            loop {
                                tokio::select! {
                                    Some(msg) = rx.recv() => {
                                        if writer.write_all(msg.as_bytes()).await.is_err() {
                                            break;
                                        }
                                    }
                                    _ = &mut read_task => {
                                        break;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            println!("[Tauri] Could not connect to daemon: {}. Retrying in 2s...", e);
                            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                        }
                    }
                }
            });
            Ok(())
        })
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![send_ipc_command, list_local_dir])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
