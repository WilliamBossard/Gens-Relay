export type Device = {
  device_id: string;
  name: string;
  auto_accept_drop: boolean;
  last_seen: number;
};

export type PeerStatus = {
  device_id: string;
  name: string;
  connected: boolean;
  relay_online: boolean;
  route: string;
  remote_addr?: string;
};

export type NearbyPeer = {
  device_id: string;
  name: string;
  endpoint: string;
  last_seen: number;
};

export type PairRequest = {
  device_id: string;
  direction: "incoming" | "outgoing";
  name: string;
  message?: string;
  endpoint?: string;
  created_at: number;
};

export type Folder = {
  folder_id: string;
  label: string;
  local_path: string;
  mode: "send_receive" | "send_only" | "receive_only";
  versioning: string;
  keep_versions: number;
  rescan_seconds: number;
  paused: boolean;
  ignore_patterns: string;
  peers: string[];
};

export type FolderStatus = {
  folder_id: string;
  state: string;
  local_files: number;
  local_bytes: number;
  need_files: number;
  need_bytes: number;
  remote_diverged: number;
  local_changes: number;
  rx_bps?: number;
  tx_bps?: number;
  last_scan_at: number;
  last_error?: string;
};

export type DropTransfer = {
  transfer_id: string;
  direction: "send" | "receive";
  kind?: "files" | "text";
  peer_id: string;
  peer_name: string;
  title: string;
  text_content?: string;
  dest_dir?: string;
  total_size: number;
  transferred_bytes: number;
  status: string;
  error_msg?: string;
};

export type DaemonStatus = {
  device_id: string;
  node_name: string;
  quic_port: number;
  lan_addrs: string[];
  relay_url: string;
  relay_connected: boolean;
  has_auth_token?: boolean;
  has_web_password?: boolean;
  drop_inbox_dir: string;
  max_upload_kbps: number;
  max_download_kbps: number;
  web_listen: string;
  web_tls: boolean;
  web_user: string;
};

export type LogEntry = {
  id: number;
  timestamp: number;
  message: string;
};

export type ConflictItem = {
  rel_path: string;
  original_path: string;
  size: number;
  mod_time: number;
};

export type VersionItem = {
  archive_path: string;
  orig_rel_path: string;
  size: number;
  mod_time: number;
};

export type StagedBrowserFile = {
  file: File;
  relPath: string;
};

export type LocalBrowseEntry = {
  name: string;
  path: string;
  is_dir: boolean;
  size: number;
};

export function formatBytes(bytes: number): string {
  if (!bytes) return "0 o";
  const units = ["o", "Ko", "Mo", "Go", "To"];
  let i = 0;
  let val = bytes;
  while (val >= 1024 && i < units.length - 1) {
    val /= 1024;
    i++;
  }
  return `${val.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}
