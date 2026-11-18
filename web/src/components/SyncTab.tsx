import {
  AlertTriangle,
  ArrowDownLeft,
  ArrowLeftRight,
  ArrowUpRight,
  FolderSync,
  History,
  Pause,
  Play,
  Plus,
  RefreshCw,
  Trash2,
} from "lucide-react";
import { apiFetch } from "../api";
import { Device, Folder, FolderStatus, PeerStatus, formatBytes } from "../types";

type Props = {
  folders: Folder[];
  folderStatusMap: Record<string, FolderStatus>;
  devices: Device[];
  peerStatusMap: Record<string, PeerStatus>;
  totalLocalFiles: number;
  totalLocalBytes: number;
  connectedPeersCount: number;
  onAddFolder: () => void;
  onEditFolder: (f: Folder) => void;
  onInspectFolder: (folderID: string) => void;
  onReload: () => Promise<void>;
  notify: (text: string, kind?: "ok" | "err") => void;
};

export function SyncTab({
  folders,
  folderStatusMap,
  devices,
  peerStatusMap,
  totalLocalFiles,
  totalLocalBytes,
  connectedPeersCount,
  onAddFolder,
  onEditFolder,
  onInspectFolder,
  onReload,
  notify,
}: Props) {
  const sampleStatus = Object.values(folderStatusMap)[0];
  const rxBps = sampleStatus?.rx_bps ?? 0;
  const txBps = sampleStatus?.tx_bps ?? 0;

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <h2 className="text-lg font-semibold text-zinc-100">Dossiers synchronisés</h2>
          <p className="text-xs text-zinc-400 mt-0.5">
            Chaque machine associe un <span className="font-mono text-zinc-300">Folder ID</span> partagé à son propre répertoire local.
          </p>
        </div>
        <div className="flex items-center gap-2">
          {(rxBps > 0 || txBps > 0) && (
            <span className="text-[11px] font-mono px-2.5 py-1.5 rounded-md bg-[#12161d] border border-zinc-800 text-zinc-300">
              ↓ {formatBytes(rxBps)}/s · ↑ {formatBytes(txBps)}/s
            </span>
          )}
          <button
            onClick={onAddFolder}
            className="inline-flex items-center gap-1.5 px-3.5 py-2 rounded-md bg-blue-600 hover:bg-blue-500 text-xs font-medium text-white transition"
          >
            <Plus size={15} /> Ajouter un dossier
          </button>
        </div>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        {[
          { label: "Dossiers", value: folders.length },
          { label: "Fichiers indexés", value: totalLocalFiles },
          { label: "Volume local", value: formatBytes(totalLocalBytes) },
          { label: "Pairs connectés", value: connectedPeersCount },
        ].map((kpi) => (
          <div key={kpi.label} className="bg-[#12161d] border border-zinc-800/90 rounded-lg px-4 py-3">
            <div className="text-[11px] uppercase tracking-wider text-zinc-500">{kpi.label}</div>
            <div className="text-lg font-semibold text-zinc-100 mt-0.5">{kpi.value}</div>
          </div>
        ))}
      </div>

      {folders.length === 0 ? (
        <div className="border border-dashed border-zinc-800 rounded-lg p-12 text-center space-y-2 bg-[#12161d]/50">
          <FolderSync size={28} className="mx-auto text-zinc-600" />
          <p className="text-sm font-medium text-zinc-300">Aucun dossier configuré</p>
          <p className="text-xs text-zinc-500">
            Liez d'abord vos machines dans <strong>Appareils</strong>, puis créez un dossier synchronisé.
          </p>
        </div>
      ) : (
        <div className="space-y-3">
          {folders.map((f) => {
            const fst = folderStatusMap[f.folder_id];
            const ModeIcon =
              f.mode === "send_only" ? ArrowUpRight : f.mode === "receive_only" ? ArrowDownLeft : ArrowLeftRight;
            return (
              <div key={f.folder_id} className="bg-[#12161d] border border-zinc-800/90 rounded-lg p-4 space-y-3">
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <div>
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="font-semibold text-sm text-zinc-100">{f.label || f.folder_id}</span>
                      <span className="text-[11px] font-mono px-2 py-0.5 rounded bg-zinc-900 text-zinc-300 border border-zinc-800">
                        {f.folder_id}
                      </span>
                      <span className="inline-flex items-center gap-1 text-[11px] px-2 py-0.5 rounded bg-zinc-800/80 text-zinc-300">
                        <ModeIcon size={11} />
                        {f.mode === "send_only" ? "Envoi seul" : f.mode === "receive_only" ? "Réception seule" : "Bidirectionnel"}
                      </span>
                      <span
                        className={`inline-flex items-center gap-1.5 text-[11px] px-2 py-0.5 rounded font-medium ${
                          fst?.state === "syncing"
                            ? "bg-amber-500/10 text-amber-300 border border-amber-500/20"
                            : fst?.state === "error"
                            ? "bg-rose-500/10 text-rose-300 border border-rose-500/20"
                            : fst?.state === "paused"
                            ? "bg-zinc-800 text-zinc-400"
                            : "bg-emerald-500/10 text-emerald-300 border border-emerald-500/20"
                        }`}
                      >
                        <span
                          className={`w-1.5 h-1.5 rounded-full ${
                            fst?.state === "syncing"
                              ? "bg-amber-400 animate-pulse"
                              : fst?.state === "error"
                              ? "bg-rose-400"
                              : fst?.state === "paused"
                              ? "bg-zinc-500"
                              : "bg-emerald-400"
                          }`}
                        />
                        {fst?.state === "idle" ? "À jour" : fst?.state || "idle"}
                      </span>
                    </div>
                    <div className="text-xs font-mono text-zinc-400 mt-1">{f.local_path}</div>
                  </div>

                  <div className="flex items-center gap-1.5">
                    <button
                      onClick={async () => {
                        await apiFetch(`/api/v1/folders/${encodeURIComponent(f.folder_id)}/rescan`, { method: "POST" });
                        notify("Scan lancé.");
                      }}
                      className="p-1.5 rounded-md bg-zinc-800/80 hover:bg-zinc-700 text-zinc-300"
                      title="Scanner maintenant"
                    >
                      <RefreshCw size={14} />
                    </button>
                    <button
                      onClick={async () => {
                        await apiFetch("/api/v1/folders", {
                          method: "POST",
                          body: JSON.stringify({ ...f, paused: !f.paused }),
                        });
                        await onReload();
                      }}
                      className="p-1.5 rounded-md bg-zinc-800/80 hover:bg-zinc-700 text-zinc-300"
                      title={f.paused ? "Reprendre" : "Mettre en pause"}
                    >
                      {f.paused ? <Play size={14} /> : <Pause size={14} />}
                    </button>
                    <button
                      onClick={() => onInspectFolder(f.folder_id)}
                      className="px-2.5 py-1.5 rounded-md bg-zinc-800/80 hover:bg-zinc-700 text-xs font-medium inline-flex items-center gap-1"
                      title="Conflits et versions archivées"
                    >
                      <History size={13} /> Versions
                    </button>
                    <button
                      onClick={() => onEditFolder(f)}
                      className="px-2.5 py-1.5 rounded-md bg-zinc-800/80 hover:bg-zinc-700 text-xs font-medium"
                    >
                      Configurer
                    </button>
                    <button
                      onClick={async () => {
                        await apiFetch(`/api/v1/folders/${encodeURIComponent(f.folder_id)}`, { method: "DELETE" });
                        await onReload();
                      }}
                      className="p-1.5 rounded-md bg-zinc-800/80 hover:bg-rose-900/50 text-rose-400"
                      title="Supprimer"
                    >
                      <Trash2 size={14} />
                    </button>
                  </div>
                </div>

                <div className="flex flex-wrap items-center gap-x-6 gap-y-1.5 text-xs text-zinc-400 border-t border-zinc-800/80 pt-2.5">
                  <span>
                    Local : <strong className="text-zinc-200">{fst?.local_files ?? 0}</strong> ({formatBytes(fst?.local_bytes ?? 0)})
                  </span>
                  <span>
                    File d'attente : <strong className="text-zinc-200">{fst?.need_files ?? 0}</strong> ({formatBytes(fst?.need_bytes ?? 0)})
                  </span>
                  <span>
                    Versioning : <strong className="text-zinc-200">{f.versioning}</strong>
                  </span>
                  <span className="flex items-center gap-1.5 flex-wrap">
                    Pairs :
                    {f.peers?.length ? (
                      f.peers.map((pid) => {
                        const dev = devices.find((d) => d.device_id === pid);
                        const ps = peerStatusMap[pid];
                        return (
                          <span
                            key={pid}
                            className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded bg-[#0b0d11] border border-zinc-800 text-zinc-300"
                          >
                            <span className={`w-1.5 h-1.5 rounded-full ${ps?.connected ? "bg-emerald-400" : "bg-zinc-600"}`} />
                            {dev?.name || pid.slice(0, 9)}
                          </span>
                        );
                      })
                    ) : (
                      <span className="text-zinc-500">Aucun</span>
                    )}
                  </span>
                </div>

                {fst && fst.remote_diverged > 0 && f.mode === "send_only" && (
                  <div className="flex items-center justify-between bg-amber-950/30 border border-amber-700/40 rounded-md px-3 py-2 text-xs text-amber-200">
                    <span className="flex items-center gap-2">
                      <AlertTriangle size={14} /> {fst.remote_diverged} fichier(s) modifié(s) côté distant.
                    </span>
                    <button
                      onClick={async () => {
                        await apiFetch(`/api/v1/folders/${encodeURIComponent(f.folder_id)}/override`, { method: "POST" });
                        await onReload();
                      }}
                      className="px-2.5 py-1 rounded bg-amber-600 hover:bg-amber-500 text-white font-medium"
                    >
                      Écraser côté distant
                    </button>
                  </div>
                )}

                {fst && fst.local_changes > 0 && f.mode === "receive_only" && (
                  <div className="flex items-center justify-between bg-amber-950/30 border border-amber-700/40 rounded-md px-3 py-2 text-xs text-amber-200">
                    <span className="flex items-center gap-2">
                      <AlertTriangle size={14} /> {fst.local_changes} modification(s) locale(s) détectée(s).
                    </span>
                    <button
                      onClick={async () => {
                        await apiFetch(`/api/v1/folders/${encodeURIComponent(f.folder_id)}/revert`, { method: "POST" });
                        await onReload();
                      }}
                      className="px-2.5 py-1 rounded bg-amber-600 hover:bg-amber-500 text-white font-medium"
                    >
                      Rétablir
                    </button>
                  </div>
                )}

                {fst?.last_error && (
                  <div className="text-xs text-rose-400 bg-rose-950/30 border border-rose-800/40 rounded-md px-3 py-1.5">
                    {fst.last_error}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
