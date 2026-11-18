import { useRef, useState } from "react";
import { ClipboardCopy, FolderOpen, MessageSquare, Trash2, UploadCloud, X } from "lucide-react";
import { apiFetch, collectDroppedItems } from "../api";
import { DaemonStatus, DropTransfer, PeerStatus, StagedBrowserFile, formatBytes } from "../types";

type Props = {
  status: DaemonStatus | null;
  connectedPeers: PeerStatus[];
  transfers: DropTransfer[];
  dropSpeedMap: Record<string, number>;
  dropPaths: string[];
  setDropPaths: React.Dispatch<React.SetStateAction<string[]>>;
  onOpenDropPicker: () => void;
  onReload: () => Promise<void>;
  notify: (text: string, kind?: "ok" | "err") => void;
};

export function DropTab({
  status,
  connectedPeers,
  transfers,
  dropSpeedMap,
  dropPaths,
  setDropPaths,
  onOpenDropPicker,
  onReload,
  notify,
}: Props) {
  const [dropPeer, setDropPeer] = useState("");
  const [browserFiles, setBrowserFiles] = useState<StagedBrowserFile[]>([]);
  const [isDragging, setIsDragging] = useState(false);
  const [isUploading, setIsUploading] = useState(false);
  const [uploadPct, setUploadPct] = useState(0);
  const [dropPathInput, setDropPathInput] = useState("");
  const [textSnippet, setTextSnippet] = useState("");
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const folderInputRef = useRef<HTMLInputElement | null>(null);

  const sendDrop = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!dropPeer) {
      notify("Sélectionne un appareil destinataire en ligne.", "err");
      return;
    }

    const hostPaths = [...dropPaths];
    if (dropPathInput.trim()) {
      for (const line of dropPathInput.split("\n")) {
        const s = line.trim();
        if (s && !hostPaths.includes(s)) hostPaths.push(s);
      }
    }

    if (browserFiles.length === 0 && hostPaths.length === 0 && !textSnippet.trim()) {
      notify("Dépose des fichiers, saisis du texte ou sélectionne un chemin.", "err");
      return;
    }

    try {
      if (textSnippet.trim()) {
        await apiFetch("/api/v1/drop/send-text", {
          method: "POST",
          body: JSON.stringify({ peer_id: dropPeer, text: textSnippet }),
        });
        setTextSnippet("");
      }

      if (browserFiles.length > 0) {
        setIsUploading(true);
        setUploadPct(0);
        const formData = new FormData();
        formData.append("peer_id", dropPeer);
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
            if (xhr.status >= 200 && xhr.status < 300) {
              resolve();
            } else {
              reject(new Error(data.error || `HTTP ${xhr.status}`));
            }
          };
          xhr.onerror = () => reject(new Error("Erreur réseau lors de l'envoi"));
          xhr.send(formData);
        });
        setIsUploading(false);
        setUploadPct(0);
        setBrowserFiles([]);
      }

      if (hostPaths.length > 0) {
        await apiFetch("/api/v1/drop/send", {
          method: "POST",
          body: JSON.stringify({ peer_id: dropPeer, paths: hostPaths }),
        });
        setDropPaths([]);
        setDropPathInput("");
      }

      await onReload();
      notify("Envoi QuickDrop déclenché.");
    } catch (err: any) {
      setIsUploading(false);
      notify(err.message, "err");
    }
  };

  return (
    <div className="space-y-5">
      <div>
        <h2 className="text-lg font-semibold text-zinc-100">QuickDrop</h2>
        <p className="text-xs text-zinc-400 mt-0.5">
          Envoie des fichiers, dossiers ou messages texte directement à un appareil connecté en P2P.
        </p>
      </div>

      <div className="grid lg:grid-cols-2 gap-5">
        <form onSubmit={sendDrop} className="bg-[#12161d] border border-zinc-800/90 rounded-lg p-5 space-y-4">
          <div>
            <label className="block text-xs font-medium text-zinc-400 mb-1.5">Appareil destinataire</label>
            <select
              value={dropPeer}
              onChange={(e) => setDropPeer(e.target.value)}
              className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-2 text-sm focus:outline-none focus:border-blue-500"
            >
              <option value="">-- Sélectionner un appareil en ligne --</option>
              {connectedPeers.map((p) => (
                <option key={p.device_id} value={p.device_id}>
                  {p.name || p.device_id} ({p.route.toUpperCase()})
                </option>
              ))}
            </select>
          </div>

          {/* Drag & Drop Browser Files & Folders */}
          <div>
            <div className="flex items-center justify-between mb-1.5">
              <label className="text-xs font-medium text-zinc-400">
                Fichiers ou dossiers (glisser-déposer ou sélection)
              </label>
              <button
                type="button"
                onClick={() => folderInputRef.current?.click()}
                className="text-xs text-blue-400 hover:text-blue-300"
              >
                Choisir un dossier
              </button>
            </div>
            <div
              onDragOver={(e) => {
                e.preventDefault();
                setIsDragging(true);
              }}
              onDragLeave={() => setIsDragging(false)}
              onDrop={async (e) => {
                e.preventDefault();
                setIsDragging(false);
                if (e.dataTransfer.items && e.dataTransfer.items.length > 0) {
                  const collected = await collectDroppedItems(e.dataTransfer.items);
                  if (collected.length > 0) {
                    setBrowserFiles((prev) => [...prev, ...collected]);
                  }
                } else if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
                  const list = Array.from(e.dataTransfer.files).map((file) => ({ file, relPath: file.name }));
                  setBrowserFiles((prev) => [...prev, ...list]);
                }
              }}
              onClick={() => fileInputRef.current?.click()}
              className={`border border-dashed rounded-lg p-5 text-center cursor-pointer transition ${
                isDragging ? "border-blue-500 bg-blue-950/20" : "border-zinc-700 bg-[#0b0d11]/60 hover:border-zinc-500"
              }`}
            >
              <UploadCloud size={22} className="mx-auto text-zinc-400 mb-2" />
              <p className="text-xs text-zinc-300 font-medium">Glisse-dépose des fichiers ou un dossier entier ici</p>
              <p className="text-[11px] text-zinc-500 mt-0.5">
                Dans l'app Windows, le glisser-déposer natif envoie sans copie temporaire
              </p>
              <input
                ref={fileInputRef}
                type="file"
                multiple
                className="hidden"
                onChange={(e) => {
                  if (e.target.files && e.target.files.length > 0) {
                    const list = Array.from(e.target.files).map((file) => ({ file, relPath: file.name }));
                    setBrowserFiles((prev) => [...prev, ...list]);
                  }
                  e.target.value = "";
                }}
              />
              <input
                ref={folderInputRef}
                type="file"
                // @ts-expect-error webkitdirectory is supported in Chromium/WebView2/Firefox
                webkitdirectory=""
                multiple
                className="hidden"
                onChange={(e) => {
                  if (e.target.files && e.target.files.length > 0) {
                    const list = Array.from(e.target.files).map((file) => ({
                      file,
                      relPath: (file as any).webkitRelativePath || file.name,
                    }));
                    setBrowserFiles((prev) => [...prev, ...list]);
                  }
                  e.target.value = "";
                }}
              />
            </div>

            {browserFiles.length > 0 && (
              <div className="flex flex-wrap gap-1.5 mt-2 max-h-28 overflow-y-auto">
                {browserFiles.map((item, idx) => (
                  <span
                    key={`${item.relPath}-${idx}`}
                    className="text-xs bg-zinc-800 border border-zinc-700 rounded px-2 py-1 inline-flex items-center gap-1.5"
                  >
                    <span className="truncate max-w-[220px] font-mono text-[11px]">{item.relPath}</span>
                    <span className="text-zinc-500">({formatBytes(item.file.size)})</span>
                    <button
                      type="button"
                      onClick={() => setBrowserFiles(browserFiles.filter((_, i) => i !== idx))}
                      className="text-zinc-400 hover:text-rose-400"
                    >
                      <X size={12} />
                    </button>
                  </span>
                ))}
              </div>
            )}
          </div>

          {/* Quick Text / Clipboard Drop */}
          <div className="pt-3 border-t border-zinc-800/80">
            <label className="flex items-center gap-1.5 text-xs font-medium text-zinc-400 mb-1.5">
              <MessageSquare size={13} className="text-blue-400" /> Ou texte / lien rapide (façon presse-papiers)
            </label>
            <textarea
              rows={2}
              value={textSnippet}
              onChange={(e) => setTextSnippet(e.target.value)}
              placeholder="Colle une URL, un code ou un texte à envoyer instantanément..."
              className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-2 text-xs focus:outline-none focus:border-blue-500"
            />
          </div>

          {/* Host Local Paths */}
          <div className="pt-3 border-t border-zinc-800/80">
            <div className="flex items-center justify-between mb-1.5">
              <label className="text-xs font-medium text-zinc-400">
                Ou chemin sur l'hôte ({status?.node_name}) — zéro copie
              </label>
              <button
                type="button"
                onClick={onOpenDropPicker}
                className="inline-flex items-center gap-1 text-xs text-blue-400 hover:text-blue-300"
              >
                <FolderOpen size={13} /> Parcourir l'hôte
              </button>
            </div>

            {dropPaths.length > 0 && (
              <div className="flex flex-wrap gap-1.5 mb-2">
                {dropPaths.map((p) => (
                  <span
                    key={p}
                    className="text-xs bg-zinc-800 border border-zinc-700 rounded px-2 py-1 inline-flex items-center gap-1.5"
                  >
                    <span className="font-mono truncate max-w-[240px]">{p}</span>
                    <button
                      type="button"
                      onClick={() => setDropPaths(dropPaths.filter((x) => x !== p))}
                      className="text-zinc-400 hover:text-rose-400"
                    >
                      <X size={12} />
                    </button>
                  </span>
                ))}
              </div>
            )}

            <textarea
              rows={2}
              value={dropPathInput}
              onChange={(e) => setDropPathInput(e.target.value)}
              placeholder="Optionnel : chemins locaux présents sur la machine du daemon (1 par ligne)"
              className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-2 text-xs font-mono focus:outline-none focus:border-blue-500"
            />
          </div>

          <button
            type="submit"
            disabled={isUploading}
            className="w-full py-2.5 rounded-md bg-blue-600 hover:bg-blue-500 disabled:opacity-60 font-medium text-xs text-white transition"
          >
            {isUploading ? `Préparation du transfert (${uploadPct}%)...` : "Envoyer en P2P direct"}
          </button>
        </form>

        <div className="bg-[#12161d] border border-zinc-800/90 rounded-lg p-5 space-y-3">
          <h3 className="font-semibold text-sm text-zinc-100">Historique & Transferts actifs</h3>
          {transfers.length === 0 ? (
            <div className="py-12 text-center text-xs text-zinc-500">Aucun transfert QuickDrop.</div>
          ) : (
            <div className="space-y-2.5 max-h-[540px] overflow-y-auto pr-1">
              {transfers.map((t) => {
                const pct = t.total_size > 0 ? Math.min(100, Math.round((t.transferred_bytes / t.total_size) * 100)) : 0;
                const speedBps = dropSpeedMap[t.transfer_id] || 0;
                const etaSec =
                  speedBps > 0 && t.total_size > t.transferred_bytes
                    ? Math.ceil((t.total_size - t.transferred_bytes) / speedBps)
                    : 0;
                const isText = t.kind === "text";
                return (
                  <div key={t.transfer_id} className="bg-[#0b0d11] border border-zinc-800 rounded-md p-3 space-y-2">
                    <div className="flex items-center justify-between text-xs gap-2">
                      <span className="font-medium text-zinc-200 truncate">
                        {t.direction === "send" ? "Envoi" : "Réception"} · {isText ? "Texte · " : ""}
                        {t.title}
                      </span>
                      <div className="flex items-center gap-1.5 shrink-0">
                        <span
                          className={`px-2 py-0.5 rounded text-[11px] font-medium ${
                            t.status === "completed"
                              ? "bg-emerald-500/10 text-emerald-300"
                              : t.status === "failed" || t.status === "rejected" || t.status === "canceled"
                              ? "bg-rose-500/10 text-rose-300"
                              : "bg-blue-500/10 text-blue-300"
                          }`}
                        >
                          {t.status}
                        </span>
                        <button
                          onClick={() => apiFetch(`/api/v1/drop/${t.transfer_id}`, { method: "DELETE" }).then(onReload)}
                          className="p-1 rounded hover:bg-zinc-800 text-zinc-500 hover:text-rose-400"
                          title="Supprimer de l'historique"
                        >
                          <Trash2 size={12} />
                        </button>
                      </div>
                    </div>

                    {isText && t.text_content ? (
                      <div className="bg-[#12161d] border border-zinc-800/80 rounded p-2 text-xs text-zinc-200 flex items-start justify-between gap-2">
                        <pre className="whitespace-pre-wrap break-words font-sans flex-1 max-h-24 overflow-y-auto">
                          {t.text_content}
                        </pre>
                        <button
                          type="button"
                          onClick={() => {
                            navigator.clipboard.writeText(t.text_content || "");
                            notify("Texte copié dans le presse-papiers.");
                          }}
                          className="p-1.5 rounded bg-zinc-800 hover:bg-zinc-700 text-zinc-300 shrink-0"
                          title="Copier le texte"
                        >
                          <ClipboardCopy size={13} />
                        </button>
                      </div>
                    ) : (
                      <>
                        <div className="text-[11px] text-zinc-400 flex justify-between gap-2">
                          <span>{t.peer_name}</span>
                          <span>
                            {formatBytes(t.transferred_bytes)} / {formatBytes(t.total_size)} ({pct}%)
                            {speedBps > 0 && ` · ${formatBytes(speedBps)}/s${etaSec > 0 ? ` · ~${etaSec}s` : ""}`}
                          </span>
                        </div>
                        <div className="w-full h-1.5 bg-zinc-800 rounded-full overflow-hidden">
                          <div
                            className={`h-full transition-all ${t.status === "completed" ? "bg-emerald-500" : "bg-blue-500"}`}
                            style={{ width: `${pct}%` }}
                          />
                        </div>
                      </>
                    )}

                    {!isText &&
                      (t.status === "transferring" ||
                        t.status === "paused" ||
                        t.status === "failed" ||
                        t.status === "pending_accept") && (
                        <div className="flex gap-2 pt-1">
                          {t.direction === "send" && t.status === "transferring" && (
                            <button
                              onClick={() => apiFetch(`/api/v1/drop/${t.transfer_id}/pause`, { method: "POST" }).then(onReload)}
                              className="text-xs px-2.5 py-1 rounded bg-zinc-800 hover:bg-zinc-700"
                            >
                              Pause
                            </button>
                          )}
                          {t.direction === "send" && (t.status === "paused" || t.status === "failed") && (
                            <button
                              onClick={() => apiFetch(`/api/v1/drop/${t.transfer_id}/resume`, { method: "POST" }).then(onReload)}
                              className="text-xs px-2.5 py-1 rounded bg-blue-600 hover:bg-blue-500 text-white"
                            >
                              Reprendre
                            </button>
                          )}
                          <button
                            onClick={() => apiFetch(`/api/v1/drop/${t.transfer_id}/cancel`, { method: "POST" }).then(onReload)}
                            className="text-xs px-2.5 py-1 rounded bg-zinc-800 hover:bg-rose-900/60 text-rose-300"
                          >
                            Annuler
                          </button>
                        </div>
                      )}
                    {t.error_msg && <p className="text-[11px] text-rose-400">{t.error_msg}</p>}
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
