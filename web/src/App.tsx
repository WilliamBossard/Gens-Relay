import { useCallback, useEffect, useRef, useState } from "react";
import { Check, Copy, FolderSync, Link2, Send, Server, Settings, Terminal } from "lucide-react";
import { apiFetch } from "./api";
import { DevicesTab } from "./components/DevicesTab";
import { DropTab } from "./components/DropTab";
import { LogsTab } from "./components/LogsTab";
import { FolderModal, InspectorModal, PathPickerModal } from "./components/Modals";
import { SettingsFormState, SettingsTab } from "./components/SettingsTab";
import { SyncTab } from "./components/SyncTab";
import {
  ConflictItem,
  DaemonStatus,
  Device,
  DropTransfer,
  Folder,
  FolderStatus,
  LocalBrowseEntry,
  LogEntry,
  NearbyPeer,
  PairRequest,
  PeerStatus,
  VersionItem,
  formatBytes,
} from "./types";

export default function App() {
  const [tab, setTab] = useState<"sync" | "drop" | "devices" | "logs" | "settings">("sync");
  const [status, setStatus] = useState<DaemonStatus | null>(null);
  const [devices, setDevices] = useState<Device[]>([]);
  const [peerStatuses, setPeerStatuses] = useState<PeerStatus[]>([]);
  const [nearbyPeers, setNearbyPeers] = useState<NearbyPeer[]>([]);
  const [pairRequests, setPairRequests] = useState<PairRequest[]>([]);
  const [folders, setFolders] = useState<Folder[]>([]);
  const [folderStatuses, setFolderStatuses] = useState<FolderStatus[]>([]);
  const [transfers, setTransfers] = useState<DropTransfer[]>([]);
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [copiedID, setCopiedID] = useState(false);
  const [toast, setToast] = useState<{ text: string; kind: "ok" | "err" } | null>(null);

  const [inspectFolderID, setInspectFolderID] = useState<string | null>(null);
  const [conflicts, setConflicts] = useState<ConflictItem[]>([]);
  const [versions, setVersions] = useState<VersionItem[]>([]);

  const [showFolderModal, setShowFolderModal] = useState(false);
  const [editingFolder, setEditingFolder] = useState(false);
  const [fForm, setFForm] = useState<Folder>({
    folder_id: "",
    label: "",
    local_path: "",
    mode: "send_receive",
    versioning: "trashcan",
    keep_versions: 5,
    rescan_seconds: 60,
    paused: false,
    ignore_patterns: "",
    peers: [],
  });

  const [acceptAliasMap, setAcceptAliasMap] = useState<Record<string, string>>({});
  const [acceptDestMap, setAcceptDestMap] = useState<Record<string, string>>({});
  const [dropPaths, setDropPaths] = useState<string[]>([]);

  const [pickerTarget, setPickerTarget] = useState<null | "folder" | "drop" | "inbox">(null);
  const [browsePath, setBrowsePath] = useState("");
  const [browseParent, setBrowseParent] = useState("");
  const [browseEntries, setBrowseEntries] = useState<LocalBrowseEntry[]>([]);

  const cfgInitializedRef = useRef(false);
  const [cfgForm, setCfgForm] = useState<SettingsFormState>({
    node_name: "",
    relay_url: "",
    auth_token: "",
    clear_auth_token: false,
    drop_inbox_dir: "",
    max_upload_kbps: 0,
    max_download_kbps: 0,
    web_listen: "",
    web_tls: false,
    web_user: "",
    web_password: "",
    clear_web_password: false,
  });

  const toastTimerRef = useRef<number | null>(null);
  const speedMapRef = useRef<Record<string, { ts: number; bytes: number; bps: number }>>({});
  const [dropSpeedMap, setDropSpeedMap] = useState<Record<string, number>>({});

  const notify = useCallback((text: string, kind: "ok" | "err" = "ok") => {
    setToast({ text, kind });
    if (toastTimerRef.current !== null) {
      window.clearTimeout(toastTimerRef.current);
    }
    toastTimerRef.current = window.setTimeout(() => {
      setToast(null);
      toastTimerRef.current = null;
    }, 4500);
  }, []);

  const updateDropList = useCallback((list: DropTransfer[]) => {
    const now = Date.now();
    const nextSpeeds: Record<string, number> = {};
    for (const t of list) {
      if (t.status === "transferring") {
        const prev = speedMapRef.current[t.transfer_id];
        if (prev && now > prev.ts && t.transferred_bytes >= prev.bytes) {
          const dt = (now - prev.ts) / 1000;
          const instant = (t.transferred_bytes - prev.bytes) / dt;
          const smoothed = prev.bps > 0 ? prev.bps * 0.6 + instant * 0.4 : instant;
          speedMapRef.current[t.transfer_id] = { ts: now, bytes: t.transferred_bytes, bps: smoothed };
          nextSpeeds[t.transfer_id] = smoothed;
        } else {
          speedMapRef.current[t.transfer_id] = { ts: now, bytes: t.transferred_bytes, bps: prev?.bps || 0 };
          if (prev?.bps) nextSpeeds[t.transfer_id] = prev.bps;
        }
      } else {
        delete speedMapRef.current[t.transfer_id];
      }
    }
    setDropSpeedMap(nextSpeeds);
    setTransfers(list);
  }, []);

  const loadAll = useCallback(async () => {
    try {
      const [st, devs, peers, nearby, reqs, flds, fsts, drops, logList] = await Promise.all([
        apiFetch("/api/v1/status"),
        apiFetch("/api/v1/devices"),
        apiFetch("/api/v1/peers"),
        apiFetch("/api/v1/peers/nearby"),
        apiFetch("/api/v1/pair/requests"),
        apiFetch("/api/v1/folders"),
        apiFetch("/api/v1/folders/status"),
        apiFetch("/api/v1/drop"),
        apiFetch("/api/v1/logs"),
      ]);
      setStatus(st);
      setDevices(devs || []);
      setPeerStatuses(peers || []);
      setNearbyPeers(nearby || []);
      setPairRequests(reqs || []);
      setFolders(flds || []);
      setFolderStatuses(fsts || []);
      updateDropList(drops || []);
      setLogs(logList || []);
      if (!cfgInitializedRef.current) {
        cfgInitializedRef.current = true;
        setCfgForm((prev) => ({
          ...prev,
          node_name: st.node_name || "",
          relay_url: st.relay_url || "",
          drop_inbox_dir: st.drop_inbox_dir || "",
          max_upload_kbps: st.max_upload_kbps || 0,
          max_download_kbps: st.max_download_kbps || 0,
          web_listen: st.web_listen || "",
          web_tls: !!st.web_tls,
          web_user: st.web_user || "",
        }));
      }
    } catch (e) {
      console.error(e);
    }
  }, [updateDropList]);

  useEffect(() => {
    loadAll();
    let stopped = false;
    let ws: WebSocket | null = null;
    let reconnectTimer: number | null = null;
    let backoffMs = 1000;

    const connectWS = () => {
      if (stopped) return;
      const proto = window.location.protocol === "https:" ? "wss" : "ws";
      ws = new WebSocket(`${proto}://${window.location.host}/api/v1/events`);
      ws.onopen = () => {
        backoffMs = 1000;
      };
      ws.onmessage = (ev) => {
        try {
          const msg = JSON.parse(ev.data);
          if (msg.event === "peers_updated") setPeerStatuses(msg.data || []);
          if (msg.event === "nearby_updated") setNearbyPeers(msg.data || []);
          if (msg.event === "pair_requests_updated") {
            setPairRequests(msg.data || []);
            loadAll();
          }
          if (msg.event === "folders_updated") setFolderStatuses(msg.data || []);
          if (msg.event === "drop_updated") updateDropList(msg.data || []);
          if (msg.event === "log_entry" && msg.data) {
            setLogs((prev) => [...prev.slice(-499), msg.data]);
          }
        } catch {}
      };
      ws.onclose = () => {
        if (stopped) return;
        reconnectTimer = window.setTimeout(connectWS, backoffMs);
        backoffMs = Math.min(backoffMs * 2, 15000);
      };
      ws.onerror = () => {
        ws?.close();
      };
    };

    const onNativeDrop = (ev: Event) => {
      const detail = (ev as CustomEvent<string[]>).detail;
      if (Array.isArray(detail) && detail.length > 0) {
        setDropPaths((prev) => Array.from(new Set([...prev, ...detail])));
        setTab("drop");
        notify(`${detail.length} élément(s) local(aux) ajouté(s) en mode zéro-copie.`);
      }
    };
    window.addEventListener("gens-native-drop", onNativeDrop);

    connectWS();
    const timer = setInterval(loadAll, 20000);
    return () => {
      stopped = true;
      window.removeEventListener("gens-native-drop", onNativeDrop);
      if (reconnectTimer !== null) window.clearTimeout(reconnectTimer);
      ws?.close();
      clearInterval(timer);
    };
  }, [loadAll, notify, updateDropList]);

  const loadBrowse = async (p: string) => {
    try {
      const res = await apiFetch(`/api/v1/local-browse?path=${encodeURIComponent(p)}`);
      setBrowsePath(res.path || "");
      setBrowseParent(res.parent || "");
      setBrowseEntries(res.entries || []);
    } catch (e: any) {
      notify(e.message, "err");
    }
  };

  const openPicker = async (target: "folder" | "drop" | "inbox", startPath = "") => {
    setPickerTarget(target);
    await loadBrowse(startPath);
  };

  const openFolderInspector = async (folderID: string) => {
    setInspectFolderID(folderID);
    try {
      const [cList, vList] = await Promise.all([
        apiFetch(`/api/v1/folders/${encodeURIComponent(folderID)}/conflicts`),
        apiFetch(`/api/v1/folders/${encodeURIComponent(folderID)}/versions`),
      ]);
      setConflicts(cList || []);
      setVersions(vList || []);
    } catch (e: any) {
      notify(e.message, "err");
    }
  };

  const saveFolder = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      await apiFetch("/api/v1/folders", {
        method: "POST",
        body: JSON.stringify(fForm),
      });
      setShowFolderModal(false);
      await loadAll();
      notify("Dossier synchronisé enregistré.");
    } catch (err: any) {
      notify(err.message, "err");
    }
  };

  const acceptRequest = async (deviceID: string) => {
    try {
      await apiFetch(`/api/v1/pair/requests/${encodeURIComponent(deviceID)}/accept`, {
        method: "POST",
        body: JSON.stringify({ name: acceptAliasMap[deviceID] || "" }),
      });
      await loadAll();
      notify("Appareil autorisé et connecté.");
    } catch (err: any) {
      notify(err.message, "err");
    }
  };

  const rejectRequest = async (deviceID: string) => {
    try {
      await apiFetch(`/api/v1/pair/requests/${encodeURIComponent(deviceID)}/reject`, {
        method: "POST",
      });
      await loadAll();
      notify("Demande supprimée.");
    } catch (err: any) {
      notify(err.message, "err");
    }
  };

  const saveSettings = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      const res = await apiFetch("/api/v1/config", {
        method: "POST",
        body: JSON.stringify(cfgForm),
      });
      cfgInitializedRef.current = false;
      setCfgForm((p) => ({
        ...p,
        auth_token: "",
        clear_auth_token: false,
        web_password: "",
        clear_web_password: false,
      }));
      await loadAll();
      if (res?.needs_restart) {
        notify("Configuration enregistrée (redémarrage du daemon requis pour appliquer l'écoute Web / HTTPS).");
      } else {
        notify("Configuration enregistrée.");
      }
    } catch (err: any) {
      notify(err.message, "err");
    }
  };

  const connectedPeers = peerStatuses.filter((p) => p.connected);
  const pendingIncomingDrops = transfers.filter((t) => t.direction === "receive" && t.status === "pending_accept");
  const incomingRequests = pairRequests.filter((r) => r.direction === "incoming");
  const outgoingRequests = pairRequests.filter((r) => r.direction === "outgoing");

  const folderStatusMap = Object.fromEntries(folderStatuses.map((s) => [s.folder_id, s]));
  const peerStatusMap = Object.fromEntries(peerStatuses.map((p) => [p.device_id, p]));

  const totalLocalFiles = folderStatuses.reduce((acc, s) => acc + (s.local_files || 0), 0);
  const totalLocalBytes = folderStatuses.reduce((acc, s) => acc + (s.local_bytes || 0), 0);
  const totalNeedFiles = folderStatuses.reduce((acc, s) => acc + (s.need_files || 0), 0);

  return (
    <div className="min-h-screen bg-[#0b0d11] text-zinc-100 flex flex-col">
      <header className="h-14 border-b border-zinc-800/90 bg-[#10131a] px-5 flex items-center justify-between gap-4 shrink-0">
        <div className="flex items-center gap-3 min-w-0">
          <div className="w-7 h-7 rounded-md bg-blue-600 flex items-center justify-center text-xs font-bold text-white tracking-tight">
            GR
          </div>
          <div className="flex items-baseline gap-2.5 min-w-0">
            <span className="font-semibold text-sm text-zinc-100">Gens-Relay</span>
            <span className="text-xs text-zinc-500 hidden sm:inline">/</span>
            <span className="text-xs text-zinc-400 truncate hidden sm:inline">{status?.node_name || "Hôte local"}</span>
          </div>
        </div>

        <div className="flex items-center gap-3">
          {status?.device_id && (
            <button
              onClick={() => {
                navigator.clipboard.writeText(status.device_id);
                setCopiedID(true);
                setTimeout(() => setCopiedID(false), 1500);
              }}
              className="hidden md:flex items-center gap-2 px-2.5 py-1 rounded-md bg-[#161a23] border border-zinc-800 hover:border-zinc-700 text-[11px] font-mono text-zinc-300 transition"
              title="Copier le Device ID de cet appareil"
            >
              <span className="text-zinc-500">ID:</span>
              <span>{status.device_id.slice(0, 19)}...</span>
              {copiedID ? <Check size={12} className="text-emerald-400" /> : <Copy size={12} className="text-zinc-400" />}
            </button>
          )}

          <div className="flex items-center gap-2 text-xs">
            <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-[#161a23] border border-zinc-800 text-zinc-300">
              <span className={`w-1.5 h-1.5 rounded-full ${status?.relay_connected ? "bg-emerald-400" : "bg-zinc-500"}`} />
              {status?.relay_url ? (status.relay_connected ? "Relais en ligne" : "Relais hors ligne") : "LAN direct"}
            </span>
            <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-[#161a23] border border-zinc-800 text-zinc-300">
              <span className={`w-1.5 h-1.5 rounded-full ${connectedPeers.length > 0 ? "bg-blue-400" : "bg-zinc-600"}`} />
              {connectedPeers.length} pair(s) P2P
            </span>
          </div>
        </div>
      </header>

      <div className="flex-1 flex flex-col md:flex-row min-h-0">
        <aside className="md:w-56 border-b md:border-b-0 md:border-r border-zinc-800/90 bg-[#0e1117] p-3 flex md:flex-col gap-1 shrink-0 overflow-x-auto">
          {[
            { id: "sync", label: "Synchronisation", icon: FolderSync, badge: totalNeedFiles },
            { id: "drop", label: "QuickDrop", icon: Send, badge: pendingIncomingDrops.length },
            { id: "devices", label: "Appareils", icon: Server, badge: incomingRequests.length },
            { id: "logs", label: "Activité", icon: Terminal, badge: 0 },
            { id: "settings", label: "Paramètres", icon: Settings, badge: 0 },
          ].map((item) => {
            const Icon = item.icon;
            const active = tab === item.id;
            return (
              <button
                key={item.id}
                onClick={() => setTab(item.id as any)}
                className={`flex items-center justify-between gap-2 px-3 py-2 rounded-md text-xs font-medium transition whitespace-nowrap ${
                  active
                    ? "bg-zinc-800/90 text-white border border-zinc-700/80"
                    : "text-zinc-400 hover:text-zinc-200 hover:bg-zinc-800/40 border border-transparent"
                }`}
              >
                <span className="flex items-center gap-2.5">
                  <Icon size={15} className={active ? "text-blue-400" : "text-zinc-500"} />
                  {item.label}
                </span>
                {item.badge > 0 && <span className="px-1.5 py-0.5 text-[10px] rounded bg-blue-600 text-white font-semibold">{item.badge}</span>}
              </button>
            );
          })}

          <div className="mt-auto pt-4 border-t border-zinc-800/80 hidden md:block text-[11px] text-zinc-500 space-y-1 px-2">
            <div className="flex justify-between">
              <span>QUIC mTLS</span>
              <span className="font-mono text-zinc-400">:{status?.quic_port || "?"}</span>
            </div>
            <div className="flex justify-between">
              <span>Web UI</span>
              <span className="font-mono text-zinc-400 truncate max-w-[110px]">{status?.web_listen}</span>
            </div>
          </div>
        </aside>

        <div className="flex-1 flex flex-col min-w-0 overflow-y-auto">
          {incomingRequests.length > 0 && (
            <div className="bg-blue-950/35 border-b border-blue-800/50 px-6 py-3 space-y-2">
              {incomingRequests.map((req) => (
                <div key={req.device_id} className="flex flex-wrap items-center gap-3 text-xs">
                  <Link2 size={15} className="text-blue-400 shrink-0" />
                  <div className="min-w-0">
                    <span className="font-semibold text-zinc-100">{req.name}</span>
                    <span className="text-zinc-300"> demande l'autorisation de se lier à cet appareil</span>
                    {req.message && <span className="ml-2 text-zinc-400">({req.message})</span>}
                    <div className="text-[11px] font-mono text-zinc-400">{req.device_id}</div>
                  </div>
                  <div className="ml-auto flex flex-wrap items-center gap-2">
                    <input
                      type="text"
                      placeholder="Nom local (optionnel)"
                      value={acceptAliasMap[req.device_id] ?? ""}
                      onChange={(e) => setAcceptAliasMap({ ...acceptAliasMap, [req.device_id]: e.target.value })}
                      className="bg-[#0b0d11] border border-zinc-700 rounded-md px-2.5 py-1 text-xs w-40 focus:outline-none focus:border-blue-500"
                    />
                    <button onClick={() => acceptRequest(req.device_id)} className="px-3 py-1 rounded-md bg-blue-600 hover:bg-blue-500 text-xs font-medium text-white transition">
                      Autoriser
                    </button>
                    <button onClick={() => rejectRequest(req.device_id)} className="px-3 py-1 rounded-md bg-zinc-800 hover:bg-zinc-700 text-xs font-medium text-zinc-200 transition">
                      Refuser
                    </button>
                  </div>
                </div>
              ))}
            </div>
          )}

          {pendingIncomingDrops.length > 0 && (
            <div className="bg-emerald-950/30 border-b border-emerald-800/40 px-6 py-3 space-y-2">
              {pendingIncomingDrops.map((t) => (
                <div key={t.transfer_id} className="flex flex-wrap items-center gap-3 text-xs">
                  <Send size={15} className="text-emerald-400 shrink-0" />
                  <div>
                    <span className="font-semibold text-zinc-100">{t.peer_name}</span>
                    <span className="text-zinc-300"> souhaite envoyer </span>
                    <span className="font-mono text-emerald-300">{t.title}</span>
                    <span className="text-zinc-400 ml-1.5">({formatBytes(t.total_size)})</span>
                  </div>
                  <div className="ml-auto flex flex-wrap items-center gap-2">
                    <input
                      type="text"
                      placeholder={status?.drop_inbox_dir}
                      value={acceptDestMap[t.transfer_id] ?? t.dest_dir ?? status?.drop_inbox_dir ?? ""}
                      onChange={(e) => setAcceptDestMap({ ...acceptDestMap, [t.transfer_id]: e.target.value })}
                      className="bg-[#0b0d11] border border-zinc-700 rounded-md px-2.5 py-1 text-xs font-mono w-64"
                    />
                    <button
                      onClick={async () => {
                        try {
                          await apiFetch(`/api/v1/drop/${t.transfer_id}/respond`, {
                            method: "POST",
                            body: JSON.stringify({
                              accept: true,
                              dest_dir: acceptDestMap[t.transfer_id] ?? t.dest_dir ?? "",
                            }),
                          });
                          await loadAll();
                        } catch (e: any) {
                          notify(e.message, "err");
                        }
                      }}
                      className="px-3 py-1 rounded-md bg-emerald-600 hover:bg-emerald-500 text-xs font-medium text-white"
                    >
                      Accepter
                    </button>
                    <button
                      onClick={async () => {
                        await apiFetch(`/api/v1/drop/${t.transfer_id}/respond`, {
                          method: "POST",
                          body: JSON.stringify({ accept: false }),
                        });
                        await loadAll();
                      }}
                      className="px-3 py-1 rounded-md bg-zinc-800 hover:bg-zinc-700 text-xs font-medium"
                    >
                      Refuser
                    </button>
                  </div>
                </div>
              ))}
            </div>
          )}

          <main className="p-6 max-w-6xl w-full mx-auto space-y-6">
            {tab === "sync" && (
              <SyncTab
                folders={folders}
                folderStatusMap={folderStatusMap}
                devices={devices}
                peerStatusMap={peerStatusMap}
                totalLocalFiles={totalLocalFiles}
                totalLocalBytes={totalLocalBytes}
                connectedPeersCount={connectedPeers.length}
                onAddFolder={() => {
                  setEditingFolder(false);
                  setFForm({
                    folder_id: "",
                    label: "",
                    local_path: "",
                    mode: "send_receive",
                    versioning: "trashcan",
                    keep_versions: 5,
                    rescan_seconds: 60,
                    paused: false,
                    ignore_patterns: "",
                    peers: [],
                  });
                  setShowFolderModal(true);
                }}
                onEditFolder={(f) => {
                  setEditingFolder(true);
                  setFForm(f);
                  setShowFolderModal(true);
                }}
                onInspectFolder={openFolderInspector}
                onReload={loadAll}
                notify={notify}
              />
            )}

            {tab === "drop" && (
              <DropTab
                status={status}
                connectedPeers={connectedPeers}
                transfers={transfers}
                dropSpeedMap={dropSpeedMap}
                dropPaths={dropPaths}
                setDropPaths={setDropPaths}
                onOpenDropPicker={() => openPicker("drop")}
                onReload={loadAll}
                notify={notify}
              />
            )}

            {tab === "devices" && (
              <DevicesTab
                status={status}
                devices={devices}
                peerStatusMap={peerStatusMap}
                nearbyPeers={nearbyPeers}
                outgoingRequests={outgoingRequests}
                onRejectRequest={rejectRequest}
                onReload={loadAll}
                notify={notify}
              />
            )}

            {tab === "logs" && <LogsTab logs={logs} />}

            {tab === "settings" && (
              <SettingsTab
                status={status}
                cfgForm={cfgForm}
                setCfgForm={setCfgForm}
                onOpenInboxPicker={() => openPicker("inbox", cfgForm.drop_inbox_dir)}
                onSave={saveSettings}
              />
            )}
          </main>
        </div>
      </div>

      <FolderModal
        show={showFolderModal}
        editing={editingFolder}
        fForm={fForm}
        setFForm={setFForm}
        devices={devices}
        onClose={() => setShowFolderModal(false)}
        onSave={saveFolder}
        onOpenFolderPicker={(p) => openPicker("folder", p)}
      />

      <PathPickerModal
        pickerTarget={pickerTarget}
        status={status}
        browsePath={browsePath}
        setBrowsePath={setBrowsePath}
        browseParent={browseParent}
        browseEntries={browseEntries}
        onLoadBrowse={loadBrowse}
        onSelectDropPath={(p) => {
          if (!dropPaths.includes(p)) setDropPaths([...dropPaths, p]);
          setPickerTarget(null);
        }}
        onConfirmDir={() => {
          if (pickerTarget === "folder") setFForm({ ...fForm, local_path: browsePath });
          if (pickerTarget === "inbox") setCfgForm({ ...cfgForm, drop_inbox_dir: browsePath });
          setPickerTarget(null);
        }}
        onClose={() => setPickerTarget(null)}
      />

      <InspectorModal
        folderID={inspectFolderID}
        conflicts={conflicts}
        versions={versions}
        onRefresh={openFolderInspector}
        onClose={() => setInspectFolderID(null)}
        notify={notify}
      />

      {toast && (
        <div
          className={`fixed bottom-5 right-5 px-4 py-2.5 rounded-md shadow-xl text-xs font-medium z-50 border ${
            toast.kind === "err" ? "bg-rose-950/95 border-rose-700 text-rose-100" : "bg-[#161a23] border-zinc-700 text-zinc-100"
          }`}
        >
          {toast.text}
        </div>
      )}
    </div>
  );
}
