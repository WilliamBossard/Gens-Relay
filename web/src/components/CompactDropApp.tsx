import { useEffect, useRef, useState } from "react";
import {
  ArrowUpRight,
  Check,
  ClipboardCopy,
  Download,
  FolderOpen,
  Pause,
  Play,
  PowerOff,
  Send,
  Settings,
  Trash2,
  UploadCloud,
  X,
} from "lucide-react";
import { apiFetch, collectDroppedItems } from "../api";
import {
  CompanionAppsInfo,
  DaemonStatus,
  DropTransfer,
  PeerStatus,
  StagedBrowserFile,
  formatBytes,
} from "../types";

type Props = {
  status: DaemonStatus | null;
  peerStatuses: PeerStatus[];
  transfers: DropTransfer[];
  dropSpeedMap: Record<string, number>;
  dropPaths: string[];
  setDropPaths: React.Dispatch<React.SetStateAction<string[]>>;
  companionApps: CompanionAppsInfo | null;
  onReload: () => Promise<void>;
  onStopDaemon: () => Promise<void>;
  notify: (text: string, kind?: "ok" | "err") => void;
};

const STATUS_BADGE: Record<string, { label: string; cls: string }> = {
  preparing: { label: "Préparation...", cls: "bg-amber-500/15 text-amber-300" },
  queued_offline: { label: "Hors-ligne (file)", cls: "bg-amber-500/15 text-amber-300" },
  pending_accept: { label: "En attente", cls: "bg-blue-500/15 text-blue-300" },
  transferring: { label: "En cours", cls: "bg-blue-500/15 text-blue-300" },
  paused: { label: "En pause", cls: "bg-zinc-700/50 text-zinc-300" },
  completed: { label: "Terminé", cls: "bg-emerald-500/15 text-emerald-300" },
  rejected: { label: "Refusé", cls: "bg-rose-500/15 text-rose-300" },
  canceled: { label: "Annulé", cls: "bg-rose-500/15 text-rose-300" },
  failed: { label: "Échoué", cls: "bg-rose-500/15 text-rose-300" },
};

export function CompactDropApp({
  status,
  peerStatuses,
  transfers,
  dropSpeedMap,
  dropPaths,
  setDropPaths,
  companionApps,
  onReload,
  onStopDaemon,
  notify,
}: Props) {
  const [selectedPeer, setSelectedPeer] = useState("");
  const [browserFiles, setBrowserFiles] = useState<StagedBrowserFile[]>([]);
  const [textSnippet, setTextSnippet] = useState("");
  const [isDragging, setIsDragging] = useState(false);
  const [isUploading, setIsUploading] = useState(false);
  const [uploadPct, setUploadPct] = useState(0);

  const [showSettings, setShowSettings] = useState(false);
  const [inboxDraft, setInboxDraft] = useState("");
  const [nodeNameDraft, setNodeNameDraft] = useState("");
  const [confirmStop, setConfirmStop] = useState(false);
  const [stopping, setStopping] = useState(false);

  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const folderInputRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    if (status) {
      setInboxDraft((prev) => prev || status.drop_inbox_dir || "");
      setNodeNameDraft((prev) => prev || status.node_name || "");
    }
  }, [status]);

  useEffect(() => {
    if (!selectedPeer && peerStatuses.length > 0) {
      const online = peerStatuses.find((p) => p.connected);
      if (online) setSelectedPeer(online.device_id);
      else setSelectedPeer(peerStatuses[0].device_id);
    }
  }, [peerStatuses, selectedPeer]);

  useEffect(() => {
    const onPaste = (e: ClipboardEvent) => {
      const items = e.clipboardData?.items;
      if (!items) return;
      const pasted: StagedBrowserFile[] = [];
      for (let i = 0; i < items.length; i++) {
        const it = items[i];
        if (it.kind === "file") {
          const f = it.getAsFile();
          if (f) {
            const ext = f.type.split("/")[1] || "bin";
            const name = f.name && f.name !== "image.png" ? f.name : `clipboard-${Date.now()}-${i}.${ext}`;
            pasted.push({ file: f, relPath: name });
          }
        }
      }
      if (pasted.length > 0) {
        e.preventDefault();
        setBrowserFiles((prev) => [...prev, ...pasted]);
        notify(`${pasted.length} fichier(s) collé(s) via Ctrl+V.`);
        return;
      }
      const tag = (document.activeElement?.tagName || "").toUpperCase();
      if (tag !== "INPUT" && tag !== "TEXTAREA") {
        const txt = e.clipboardData?.getData("text/plain") || "";
        if (txt.trim()) {
          e.preventDefault();
          setTextSnippet((prev) => (prev ? `${prev}\n${txt}` : txt));
          notify("Texte collé.");
        }
      }
    };
    window.addEventListener("paste", onPaste);
    return () => window.removeEventListener("paste", onPaste);
  }, [notify]);

  const sendDrop = async () => {
    if (!selectedPeer) {
      notify("Sélectionne un appareil destinataire.", "err");
      return;
    }
    if (browserFiles.length === 0 && dropPaths.length === 0 && !textSnippet.trim()) {
      notify("Dépose des fichiers ou saisis un texte.", "err");
      return;
    }
    try {
      if (textSnippet.trim()) {
        await apiFetch("/api/v1/drop/send-text", {
          method: "POST",
          body: JSON.stringify({ peer_id: selectedPeer, text: textSnippet }),
        });
        setTextSnippet("");
      }
      if (browserFiles.length > 0) {
        setIsUploading(true);
        setUploadPct(0);
        const formData = new FormData();
        formData.append("peer_id", selectedPeer);
        for (const item of browserFiles) {
          formData.append("rel_path", item.relPath);
          formData.append("files", item.file, item.file.name);
        }
        await new Promise<void>((resolve, reject) => {
          const xhr = new XMLHttpRequest();
          xhr.open("POST", "/api/v1/drop/upload");
          xhr.upload.onprogress = (ev) => {
            if (ev.lengthComputable && ev.total > 0) {
              setUploadPct(Math.round((ev.loaded / ev.total) * 100));
            }
          };
          xhr.onload = () => {
            let data: any = {};
            try {
              data = JSON.parse(xhr.responseText || "{}");
            } catch {}
            if (xhr.status >= 200 && xhr.status < 300) resolve();
            else reject(new Error(data.error || `HTTP ${xhr.status}`));
          };
          xhr.onerror = () => reject(new Error("Erreur réseau lors de l'envoi"));
          xhr.send(formData);
        });
        setIsUploading(false);
        setUploadPct(0);
        setBrowserFiles([]);
      }
      if (dropPaths.length > 0) {
        await apiFetch("/api/v1/drop/send", {
          method: "POST",
          body: JSON.stringify({ peer_id: selectedPeer, paths: dropPaths }),
        });
        setDropPaths([]);
      }
      await onReload();
      notify("Envoi lancé.");
    } catch (err: any) {
      setIsUploading(false);
      notify(err?.message || "Erreur d'envoi", "err");
    }
  };

  const openFullAppOrInstall = async () => {
    if (!status?.is_loopback) {
      window.open("/", "_blank");
      return;
    }
    try {
      if (companionApps?.desktop_installed) {
        await apiFetch("/api/v1/companion-apps/desktop/launch", { method: "POST" });
        notify("Ouverture de Gens-Relay...");
      } else if (companionApps?.installer_available) {
        await apiFetch("/api/v1/companion-apps/desktop/install", { method: "POST" });
        notify("Ouverture de l'installateur Gens-Relay...");
      } else {
        window.open("/", "_blank");
      }
    } catch (e: any) {
      notify(e?.message || "Impossible d'ouvrir Gens-Relay.", "err");
    }
  };

  const saveCompactSettings = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      await apiFetch("/api/v1/config", {
        method: "POST",
        body: JSON.stringify({
          node_name: nodeNameDraft,
          drop_inbox_dir: inboxDraft,
        }),
      });
      await onReload();
      setShowSettings(false);
      notify("Paramètres enregistrés.");
    } catch (err: any) {
      notify(err?.message || "Erreur de sauvegarde", "err");
    }
  };

  const openPath = async (p: string) => {
    try {
      await apiFetch("/api/v1/open-path", {
        method: "POST",
        body: JSON.stringify({ path: p, reveal: false }),
      });
    } catch (e: any) {
      notify(e?.message || "Ouverture impossible.", "err");
    }
  };

  const pendingIncoming = transfers.filter((t) => t.direction === "receive" && t.status === "pending_accept");
  const onlineCount = peerStatuses.filter((p) => p.connected).length;

  return (
    <div className="h-screen bg-[#0b0d11] text-zinc-100 flex flex-col overflow-hidden select-none">
      {/* Compact Header */}
      <header className="h-12 px-3.5 border-b border-zinc-800/90 bg-[#10131a] flex items-center justify-between gap-2 shrink-0">
        <div className="flex items-center gap-2 min-w-0">
          <div className="w-6 h-6 rounded-md bg-blue-600 flex items-center justify-center text-[11px] font-bold text-white shrink-0">
            GD
          </div>
          <div className="min-w-0">
            <div className="text-xs font-semibold text-zinc-100 leading-tight truncate">Gens-Drop</div>
            <div className="text-[10px] text-zinc-400 flex items-center gap-1.5 truncate">
              <span className={`w-1.5 h-1.5 rounded-full ${status?.relay_connected || onlineCount > 0 ? "bg-emerald-400" : "bg-zinc-500"}`} />
              <span className="truncate">{status?.node_name || "Local"}</span>
              <span>· {onlineCount} en ligne</span>
            </div>
          </div>
        </div>

        <div className="flex items-center gap-1.5 shrink-0">
          <button
            type="button"
            onClick={openFullAppOrInstall}
            className="inline-flex items-center gap-1 px-2 py-1 rounded bg-zinc-800 hover:bg-zinc-700 border border-zinc-700/80 text-[11px] text-zinc-200 transition"
            title={
              companionApps && !companionApps.desktop_installed && status?.is_loopback
                ? "Installer Gens-Relay complet (Synchro)"
                : "Ouvrir Gens-Relay complet (Synchro)"
            }
          >
            {companionApps && !companionApps.desktop_installed && status?.is_loopback ? (
              <Download size={12} />
            ) : (
              <ArrowUpRight size={12} />
            )}
            <span>Sync</span>
          </button>
          <button
            type="button"
            onClick={() => {
              setConfirmStop(false);
              setShowSettings((v) => !v);
            }}
            className={`p-1.5 rounded border transition ${
              showSettings
                ? "bg-blue-600/20 border-blue-500/40 text-blue-300"
                : "bg-zinc-800/80 border-zinc-700/70 text-zinc-300 hover:bg-zinc-700"
            }`}
            title="Paramètres"
          >
            <Settings size={14} />
          </button>
        </div>
      </header>

      {showSettings ? (
        <div className="flex-1 overflow-y-auto p-3.5 space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="text-xs font-semibold uppercase tracking-wider text-zinc-400">Paramètres rapides</h2>
            <button type="button" onClick={() => setShowSettings(false)} className="text-xs text-blue-400">
              Retour
            </button>
          </div>

          <form onSubmit={saveCompactSettings} className="bg-[#12161d] border border-zinc-800 rounded-lg p-3.5 space-y-3">
            <div>
              <label className="block text-[11px] text-zinc-400 mb-1">Nom de l'appareil</label>
              <input
                type="text"
                value={nodeNameDraft}
                onChange={(e) => setNodeNameDraft(e.target.value)}
                className="w-full bg-[#0b0d11] border border-zinc-700 rounded px-2.5 py-1.5 text-xs"
              />
            </div>
            <div>
              <label className="block text-[11px] text-zinc-400 mb-1">Dossier de réception</label>
              <input
                type="text"
                value={inboxDraft}
                onChange={(e) => setInboxDraft(e.target.value)}
                className="w-full bg-[#0b0d11] border border-zinc-700 rounded px-2.5 py-1.5 text-xs font-mono"
              />
            </div>
            <button type="submit" className="w-full py-1.5 rounded bg-blue-600 hover:bg-blue-500 text-xs font-medium text-white">
              Enregistrer
            </button>
          </form>

          {status?.is_loopback && companionApps && (
            <div className="bg-[#12161d] border border-zinc-800 rounded-lg p-3.5 space-y-2">
              <div className="text-xs font-medium text-zinc-200">Suite Gens-Relay (Synchro dossiers)</div>
              <p className="text-[11px] text-zinc-400">
                {companionApps.desktop_installed
                  ? "Gens-Relay Desktop est installé et partage ce daemon."
                  : "Installe l'interface complète de synchronisation de dossiers en continu."}
              </p>
              <button
                type="button"
                onClick={openFullAppOrInstall}
                className="w-full py-1.5 rounded bg-zinc-800 hover:bg-zinc-700 border border-zinc-700 text-xs text-zinc-100"
              >
                {companionApps.desktop_installed ? "Ouvrir Gens-Relay Desktop" : "Installer via le Setup"}
              </button>
            </div>
          )}

          <div className="bg-rose-950/15 border border-rose-900/50 rounded-lg p-3.5 space-y-2.5">
            <div className="flex items-center gap-1.5 text-xs font-semibold text-rose-200">
              <PowerOff size={14} className="text-rose-400" /> Arrêter le daemon Gens-Relay
            </div>
            <p className="text-[11px] text-zinc-400">
              Coupe le service d'arrière-plan P2P partagé (QuickDrop & synchronisation).
            </p>
            {status?.is_loopback === false ? (
              <span className="text-[11px] text-zinc-500 italic">Disponible uniquement en accès local</span>
            ) : !confirmStop ? (
              <button
                type="button"
                onClick={() => setConfirmStop(true)}
                className="w-full py-1.5 rounded bg-rose-600/90 hover:bg-rose-500 text-xs font-medium text-white"
              >
                Arrêter le daemon
              </button>
            ) : (
              <div className="flex gap-2">
                <button
                  type="button"
                  disabled={stopping}
                  onClick={async () => {
                    setStopping(true);
                    try {
                      await onStopDaemon();
                    } finally {
                      setStopping(false);
                      setConfirmStop(false);
                    }
                  }}
                  className="flex-1 py-1.5 rounded bg-rose-600 hover:bg-rose-500 text-xs font-semibold text-white"
                >
                  {stopping ? "Arrêt..." : "Confirmer"}
                </button>
                <button
                  type="button"
                  onClick={() => setConfirmStop(false)}
                  className="flex-1 py-1.5 rounded bg-zinc-800 hover:bg-zinc-700 text-xs text-zinc-200"
                >
                  Annuler
                </button>
              </div>
            )}
          </div>
        </div>
      ) : (
        <div className="flex-1 overflow-y-auto p-3.5 space-y-3">
          {/* Pending incoming notifications */}
          {pendingIncoming.map((t) => (
            <div
              key={t.transfer_id}
              className="bg-emerald-950/30 border border-emerald-700/50 rounded-lg p-2.5 space-y-2 text-xs"
            >
              <div className="flex items-center justify-between gap-2">
                <span className="font-medium text-emerald-200 truncate">
                  {t.peer_name} envoie {t.title}
                </span>
                <span className="text-[11px] text-zinc-400 shrink-0">{formatBytes(t.total_size)}</span>
              </div>
              <div className="flex gap-2">
                <button
                  type="button"
                  onClick={() =>
                    apiFetch(`/api/v1/drop/${t.transfer_id}/respond`, {
                      method: "POST",
                      body: JSON.stringify({ accept: true, dest_dir: status?.drop_inbox_dir || "" }),
                    })
                      .then(onReload)
                      .catch((e: any) => notify(e.message, "err"))
                  }
                  className="flex-1 py-1 rounded bg-emerald-600 hover:bg-emerald-500 text-white font-medium inline-flex items-center justify-center gap-1"
                >
                  <Check size={12} /> Accepter
                </button>
                <button
                  type="button"
                  onClick={() =>
                    apiFetch(`/api/v1/drop/${t.transfer_id}/respond`, {
                      method: "POST",
                      body: JSON.stringify({ accept: false }),
                    }).then(onReload)
                  }
                  className="px-3 py-1 rounded bg-zinc-800 hover:bg-zinc-700 text-zinc-200"
                >
                  Refuser
                </button>
              </div>
            </div>
          ))}

          {/* Target device pills */}
          <div className="space-y-1.5">
            <div className="flex items-center justify-between">
              <span className="text-[11px] font-medium text-zinc-400">Destinataire</span>
              {peerStatuses.length === 0 && (
                <button type="button" onClick={openFullAppOrInstall} className="text-[11px] text-blue-400">
                  + Appairer dans Gens-Relay
                </button>
              )}
            </div>
            {peerStatuses.length > 0 && (
              <div className="flex gap-1.5 overflow-x-auto pb-1">
                {peerStatuses.map((p) => {
                  const active = selectedPeer === p.device_id;
                  return (
                    <button
                      key={p.device_id}
                      type="button"
                      onClick={() => setSelectedPeer(p.device_id)}
                      className={`inline-flex items-center gap-1.5 px-2.5 py-1.5 rounded-full text-xs whitespace-nowrap border transition ${
                        active
                          ? "bg-blue-600/20 border-blue-500 text-blue-100 font-medium"
                          : "bg-[#12161d] border-zinc-800 text-zinc-300 hover:border-zinc-700"
                      }`}
                    >
                      <span className={`w-1.5 h-1.5 rounded-full ${p.connected ? "bg-emerald-400" : "bg-zinc-500"}`} />
                      <span>{p.name || p.device_id.slice(0, 8)}</span>
                    </button>
                  );
                })}
              </div>
            )}
          </div>

          {/* Drop Zone */}
          <div
            onDragOver={(e) => {
              e.preventDefault();
              setIsDragging(true);
            }}
            onDragLeave={() => setIsDragging(false)}
            onDrop={async (e) => {
              e.preventDefault();
              setIsDragging(false);
              if (e.dataTransfer.items?.length) {
                const collected = await collectDroppedItems(e.dataTransfer.items);
                if (collected.length) setBrowserFiles((prev) => [...prev, ...collected]);
              } else if (e.dataTransfer.files?.length) {
                const list = Array.from(e.dataTransfer.files).map((file) => ({ file, relPath: file.name }));
                setBrowserFiles((prev) => [...prev, ...list]);
              }
            }}
            onClick={() => fileInputRef.current?.click()}
            className={`border border-dashed rounded-xl p-4 text-center cursor-pointer transition ${
              isDragging ? "border-blue-500 bg-blue-950/20" : "border-zinc-700 bg-[#12161d]/70 hover:border-zinc-500"
            }`}
          >
            <UploadCloud size={22} className="mx-auto text-blue-400 mb-1.5" />
            <div className="text-xs font-medium text-zinc-200">Dépose des fichiers / dossiers ou Ctrl+V</div>
            <div className="flex items-center justify-center gap-3 mt-1.5 text-[11px]">
              <span className="text-blue-400">Fichiers</span>
              <span className="text-zinc-600">·</span>
              <button
                type="button"
                onClick={(e) => {
                  e.stopPropagation();
                  folderInputRef.current?.click();
                }}
                className="text-blue-400 hover:underline"
              >
                Un dossier
              </button>
            </div>
            <input
              ref={fileInputRef}
              type="file"
              multiple
              className="hidden"
              onChange={(e) => {
                if (e.target.files?.length) {
                  setBrowserFiles((prev) => [
                    ...prev,
                    ...Array.from(e.target.files!).map((file) => ({ file, relPath: file.name })),
                  ]);
                }
                e.target.value = "";
              }}
            />
            <input
              ref={folderInputRef}
              type="file"
              // @ts-expect-error webkitdirectory
              webkitdirectory=""
              multiple
              className="hidden"
              onChange={(e) => {
                if (e.target.files?.length) {
                  setBrowserFiles((prev) => [
                    ...prev,
                    ...Array.from(e.target.files!).map((file) => ({
                      file,
                      relPath: (file as any).webkitRelativePath || file.name,
                    })),
                  ]);
                }
                e.target.value = "";
              }}
            />
          </div>

          {(browserFiles.length > 0 || dropPaths.length > 0) && (
            <div className="flex flex-wrap gap-1 max-h-20 overflow-y-auto">
              {dropPaths.map((p) => (
                <span key={p} className="text-[11px] bg-blue-950/40 border border-blue-800/60 rounded px-2 py-0.5 inline-flex items-center gap-1">
                  <span className="font-mono truncate max-w-[160px]">{p.split(/[\\/]/).pop()}</span>
                  <button type="button" onClick={() => setDropPaths(dropPaths.filter((x) => x !== p))}>
                    <X size={11} />
                  </button>
                </span>
              ))}
              {browserFiles.map((item, idx) => (
                <span key={`${item.relPath}-${idx}`} className="text-[11px] bg-zinc-800 border border-zinc-700 rounded px-2 py-0.5 inline-flex items-center gap-1">
                  <span className="font-mono truncate max-w-[160px]">{item.relPath}</span>
                  <button type="button" onClick={() => setBrowserFiles(browserFiles.filter((_, i) => i !== idx))}>
                    <X size={11} />
                  </button>
                </span>
              ))}
            </div>
          )}

          {/* Quick text + Send */}
          <div className="flex gap-2">
            <input
              type="text"
              value={textSnippet}
              onChange={(e) => setTextSnippet(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter" && (textSnippet.trim() || browserFiles.length || dropPaths.length)) {
                  e.preventDefault();
                  sendDrop();
                }
              }}
              placeholder="Ou texte / lien rapide..."
              className="flex-1 bg-[#12161d] border border-zinc-800 rounded-lg px-3 py-1.5 text-xs focus:outline-none focus:border-blue-500"
            />
            <button
              type="button"
              disabled={isUploading || !selectedPeer || (!textSnippet.trim() && browserFiles.length === 0 && dropPaths.length === 0)}
              onClick={sendDrop}
              className="inline-flex items-center gap-1.5 px-3.5 py-1.5 rounded-lg bg-blue-600 hover:bg-blue-500 disabled:opacity-50 text-xs font-medium text-white shrink-0"
            >
              <Send size={13} />
              {isUploading ? `${uploadPct}%` : "Envoyer"}
            </button>
          </div>

          {/* Transfers list */}
          <div className="space-y-1.5 pt-1">
            <div className="text-[11px] font-medium text-zinc-400">Récents & En cours</div>
            {transfers.length === 0 ? (
              <div className="text-[11px] text-zinc-500 text-center py-4 bg-[#12161d]/40 rounded-lg border border-zinc-800/60">
                Aucun transfert pour l'instant.
              </div>
            ) : (
              transfers.slice(0, 12).map((t) => {
                const pct = t.total_size > 0 ? Math.min(100, Math.round((t.transferred_bytes / t.total_size) * 100)) : 0;
                const speed = dropSpeedMap[t.transfer_id] || 0;
                const badge = STATUS_BADGE[t.status] || { label: t.status, cls: "bg-zinc-800 text-zinc-300" };
                const isText = t.kind === "text";
                return (
                  <div key={t.transfer_id} className="bg-[#12161d] border border-zinc-800/90 rounded-lg p-2.5 space-y-1.5">
                    <div className="flex items-center justify-between gap-2 text-xs">
                      <span className="truncate font-medium text-zinc-200">
                        {t.direction === "send" ? "↑" : "↓"} {t.title}
                      </span>
                      <div className="flex items-center gap-1 shrink-0">
                        <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium ${badge.cls}`}>{badge.label}</span>
                        {!isText && (t.status === "transferring" || t.status === "preparing") && (
                          <button
                            type="button"
                            onClick={() => apiFetch(`/api/v1/drop/${t.transfer_id}/pause`, { method: "POST" }).then(onReload)}
                            className="p-1 rounded hover:bg-zinc-800 text-zinc-400"
                            title="Pause"
                          >
                            <Pause size={12} />
                          </button>
                        )}
                        {!isText && (t.status === "paused" || t.status === "failed") && (
                          <button
                            type="button"
                            onClick={() => apiFetch(`/api/v1/drop/${t.transfer_id}/resume`, { method: "POST" }).then(onReload)}
                            className="p-1 rounded hover:bg-zinc-800 text-blue-400"
                            title="Reprendre"
                          >
                            <Play size={12} />
                          </button>
                        )}
                        {!isText &&
                          t.direction === "receive" &&
                          t.status === "completed" &&
                          status?.is_loopback &&
                          (t.dest_dir || status?.drop_inbox_dir) && (
                            <button
                              type="button"
                              onClick={() => openPath(t.dest_dir || status.drop_inbox_dir)}
                              className="p-1 rounded hover:bg-zinc-800 text-zinc-300"
                              title="Ouvrir le dossier"
                            >
                              <FolderOpen size={12} />
                            </button>
                          )}
                        {isText && t.text_content && (
                          <button
                            type="button"
                            onClick={() => {
                              navigator.clipboard.writeText(t.text_content || "");
                              notify("Copié !");
                            }}
                            className="p-1 rounded hover:bg-zinc-800 text-zinc-300"
                            title="Copier"
                          >
                            <ClipboardCopy size={12} />
                          </button>
                        )}
                        <button
                          type="button"
                          onClick={() => apiFetch(`/api/v1/drop/${t.transfer_id}`, { method: "DELETE" }).then(onReload)}
                          className="p-1 rounded hover:bg-zinc-800 text-zinc-500 hover:text-rose-400"
                          title="Supprimer"
                        >
                          <Trash2 size={12} />
                        </button>
                      </div>
                    </div>
                    {!isText && (
                      <>
                        <div className="flex justify-between text-[10px] text-zinc-400">
                          <span>{t.peer_name}</span>
                          <span>
                            {formatBytes(t.transferred_bytes)} / {formatBytes(t.total_size)}
                            {speed > 0 ? ` · ${formatBytes(speed)}/s` : ""}
                          </span>
                        </div>
                        <div className="w-full h-1 bg-zinc-800 rounded-full overflow-hidden">
                          <div
                            className={`h-full transition-all ${t.status === "completed" ? "bg-emerald-500" : "bg-blue-500"}`}
                            style={{ width: `${pct}%` }}
                          />
                        </div>
                      </>
                    )}
                    {t.error_msg && <div className="text-[10px] text-rose-400 truncate">{t.error_msg}</div>}
                  </div>
                );
              })
            )}
          </div>
        </div>
      )}
    </div>
  );
}
