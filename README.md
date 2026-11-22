# Gens-Relay

Peer-to-peer file synchronization and instant file transfer platform written in Go, designed for Windows, Linux, and TrueNAS SCALE.

Gens-Relay combines continuous folder synchronization (bidirectional and unidirectional) with ad-hoc device-to-device transfers (**QuickDrop**) over direct **QUIC + TLS 1.3 mTLS** connections. An optional lightweight coordination server handles device presence, pairing requests, and endpoint discovery without ever storing or inspecting file data.

---

## Architecture

- **Direct P2P Data Plane (`cmd/gens-daemon`)**
  - Persistent **Ed25519** identity per machine (`cert.pem` / `key.pem`) deriving a unique `DeviceID`.
  - **QUIC with mutual TLS 1.3** (`quic-go`, UDP `:22000` by default) multiplexing control RPCs and parallel binary data streams.
  - **LAN Auto-Discovery** via UDP broadcast (`:21027`) for zero-configuration local pairing and transfers, even without internet access or a coordination server.
  - **Embedded Web UI** (`embed.FS` on `:8385`) served directly by each daemon, with optional HTTP Basic authentication for headless servers and NAS deployments.
  - **SQLite State Store** (`modernc.org/sqlite`, pure Go, `CGO_ENABLED=0`) in WAL mode.

- **Zero-Storage Coordination Server (`cmd/relay-server`)**
  - WebSocket signaling server (`:38420`, `/ws` and `/healthz`).
  - Challenge-response Ed25519 device authentication and optional shared `AUTH_TOKEN`.
  - Online presence tracking, 6-digit pairing codes, mutual pairing requests (`pair_request` / `pair_accept` / `pair_reject` with offline queuing), and NAT hole-punching coordination.
  - Optional opaque E2EE relay fallback (`--enable-relay`); never stores files (`stores_files: false`).
  - **Embedded Relay Admin Web UI** (`relay-web/`, served on `127.0.0.1:38421` by default and `/admin/` on `:38420`) with live traffic graph, connected client list, Kick/Ban controls, pair code management, live logs, and hot-reloaded settings (`relay-config.json`).

---

## Core Features

### Continuous Folder Synchronization
- **Independent Local Paths**: Peers share a logical `FolderID` while each machine maps it to its own local directory (e.g., Windows `C:\Users\...` synced to TrueNAS `/mnt/tank/...`).
- **Sync Modes**: `send_receive` (bidirectional), `send_only` (with remote override), and `receive_only` (with local revert).
- **Delta Transfers**: Files are split into adaptive **BLAKE3** blocks (256 KiB up to 16 MiB, chosen by file size via `AdaptiveBlockSize`); unchanged local blocks are reused before pulling missing blocks.
- **Fast Scanning & Live Watching**: 16-worker parallel scanner with `(size, mtime_ns)` fast-checks combined with `fsnotify` real-time watching (500 ms debounce).
- **Safety & Versioning**: Atomic temporary writes (`.gens.tmp.*`), deterministic conflict preservation (`*.sync-conflict-*`), `.gensignore` exclusion rules (with `!` negations), and configurable file versioning (`none`, `trashcan`, `simple`, `staggered` under `.gensversions/`).

### QuickDrop (Ad-Hoc Transfers)
- Send individual files or entire directory trees directly to any connected peer.
- Incoming transfers prompt the receiver to accept, pick a destination folder (or default inbox), or decline — with optional per-device auto-accept.
- Supports pause/resume via SQLite-persisted completed block tracking and configurable upload/download bandwidth limits.

### Device Pairing & Friend Requests
- **LAN Discovery**: Automatically detects unpaired devices on the local network for one-click pairing requests.
- **Direct Request**: Send a pairing request by `DeviceID` (with optional friendly message and direct `IP:Port`).
- **Short Pairing Codes**: Generate 5-minute `XXX-XXX` pairing codes via the coordination server.

---

## Repository Layout

```text
Gens-Relay/
├── cmd/
│   ├── gens-daemon/         # P2P daemon + embedded Web UI
│   └── relay-server/        # Coordination & signaling server
├── pkg/protocol/            # Ed25519 identity, signaling envelopes, QUIC wire framing
├── internal/
│   ├── chunker/             # 256 KiB–16 MiB adaptive BLAKE3 block hashing & verification
│   ├── config/              # Daemon configuration & temp cleanup
│   ├── drop/                # QuickDrop ad-hoc transfer engine
│   ├── lock/                # Atomic process lockfile with heartbeat
│   ├── p2p/                 # QUIC mTLS transport, LAN discovery, pairing requests
│   ├── relay/               # Coordination server implementation
│   ├── store/               # Pure-Go SQLite persistence
│   ├── syncer/              # Folder scanner, watcher, puller, versioner, .gensignore
│   └── webapi/              # REST API, WebSocket events & static UI serving
├── web/                     # React + TypeScript + Tailwind Web UI
├── relay-web/               # React + TypeScript Relay Admin Web UI
├── desktop/                 # Tauri Windows desktop app (daemon sidecar + tray)
├── drop-desktop/            # Tauri Windows QuickDrop app (drop sidecar + tray)
├── relay-desktop/           # Tauri Windows relay server app (relay sidecar + tray)
├── deploy/                  # Dockerfiles, Docker Compose, systemd unit, TrueNAS SCALE metadata
└── _archive_v1/             # Archived Rust / Node.js v1 prototype
```

---

## Getting Started

### Prerequisites
- **Go** 1.24+ (tested with Go 1.27)
- **Node.js** 20+ (only needed to rebuild the frontend Web UI)

### Build from Source

```bash
# 1. Build the Web UIs (required once before Go embed)
cd web && npm install && npm run build && cd ..
cd relay-web && npm install && npm run build && cd ..

# 2. Run tests
go test -count=1 ./...

# 3. Build static binaries (Windows & Linux)
# Windows (PowerShell):
$env:CGO_ENABLED="0"; $env:GOOS="windows"; $env:GOARCH="amd64"
go build -ldflags="-s -w" -o dist/gens-daemon.exe ./cmd/gens-daemon
go build -ldflags="-s -w" -o dist/relay-server.exe ./cmd/relay-server

# Linux / TrueNAS SCALE (PowerShell):
$env:CGO_ENABLED="0"; $env:GOOS="linux"; $env:GOARCH="amd64"
go build -ldflags="-s -w" -o dist/gens-daemon-linux-amd64 ./cmd/gens-daemon
go build -ldflags="-s -w" -o dist/relay-server-linux-amd64 ./cmd/relay-server
```

Or on Linux/macOS using the `Makefile`:

```bash
make build
```

> **Note (local build)**: `web/dist/` contains a placeholder `index.html` so the
> daemon binary compiles even without a JS toolchain. For the full UI, build the
> frontend first (`cd web && npm install && npm run build`), then run `go build`
> — the daemon embeds whatever is in `web/dist/` at compile time.

---

## Running

### Daemon (`gens-daemon`)

On Windows (local access only):

```powershell
.\dist\gens-daemon.exe
# Open http://127.0.0.1:8385
```

On a headless Linux VM or server (allow LAN Web UI access):

```bash
chmod +x ./gens-daemon-linux-amd64
./gens-daemon-linux-amd64 --web 0.0.0.0:8385
# Open http://<SERVER_IP>:8385 and configure Web UI credentials in Settings
```

CLI flags:
- `--data-dir <path>`: Override configuration, keys, and SQLite database directory (env: `GENS_DATA_DIR`).
- `--web <addr>`: Override Web UI listen address, e.g. `0.0.0.0:8385` (env: `GENS_WEB_LISTEN`).
- `--web-tls`: Enable HTTPS for the Web UI with an auto-generated self-signed certificate (`web-cert.pem` / `web-key.pem`, env: `GENS_WEB_TLS=true`).

### Windows Desktop App (`desktop/`)

`gens-relay-desktop.exe` wraps `gens-daemon.exe` as a silent background sidecar with a Windows System Tray icon, close-to-tray behavior, optional Windows startup toggle, and embeds the exact same Web UI:

```powershell
# Place gens-daemon.exe beside gens-relay-desktop.exe (both are output in dist/)
.\dist\gens-relay-desktop.exe
```

### Coordination Server (`relay-server`)

Headless CLI (Linux / Windows without UI wrapper):

```bash
./dist/relay-server-linux-amd64 --listen :38420 --admin-listen 0.0.0.0:38421 --admin-pass "admin-secret" --token "your-secret-token"
```

CLI flags:
- `--data-dir <path>`: Persistent config directory (`relay-config.json`, env: `RELAY_DATA_DIR`).
- `--listen <addr>`: HTTP/WebSocket listen address (env: `RELAY_LISTEN`, default `:38420`).
- `--admin-listen <addr>`: Admin Web UI listen address (env: `RELAY_ADMIN_LISTEN`, default `127.0.0.1:38421`, `'off'` to disable separate port; `/admin/` is also mounted on `--listen`).
- `--token <secret>`: Optional shared network token (env: `AUTH_TOKEN`).
- `--enable-relay`: Enable opaque encrypted relay fallback when direct P2P fails (env: `ENABLE_BLIND_RELAY=true`).
- `--max-relay-kbps <n>`: Optional Blind Relay bandwidth limit in KB/s (env: `MAX_RELAY_KBPS`).
- `--admin-user` / `--admin-pass`: Basic Auth credentials for the Relay Admin Web UI (env: `RELAY_ADMIN_USER` / `RELAY_ADMIN_PASS`).

### Windows Relay Desktop App (`relay-desktop/`)

`gens-relay-server-desktop.exe` wraps `relay-server.exe` as a silent background sidecar with a Windows System Tray icon and opens the embedded Relay Admin Web UI (`http://127.0.0.1:38421`). Both the headless `relay-server.exe` and the UI app `gens-relay-server-desktop.exe` are distributed for Windows.

---

## Deployment

- **Ubuntu / Linux VM (`systemd`)**: See [deploy/gens-daemon.service](deploy/gens-daemon.service).
- **TrueNAS SCALE / Docker**: See [deploy/docker-compose.yml](deploy/docker-compose.yml), [deploy/Dockerfile.daemon](deploy/Dockerfile.daemon), and [deploy/truenas-app/README.md](deploy/truenas-app/README.md).

Recommended firewall ports on Linux hosts:
- `8385/tcp`: Web UI (only expose if accessing remotely; enable Web UI auth in Settings)
- `22000/udp`: QUIC mTLS peer-to-peer transport
- `21027/udp`: LAN UDP discovery
- `38420/tcp`: Coordination server (only on the machine hosting `relay-server`)
