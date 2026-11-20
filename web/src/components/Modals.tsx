import { File as FileIcon, Folder as FolderIcon } from "lucide-react";
import { apiFetch } from "../api";
import { ConflictItem, DaemonStatus, Device, Folder, LocalBrowseEntry, VersionItem, formatBytes } from "../types";

type FolderModalProps = {
  show: boolean;
  editing: boolean;
  fForm: Folder;
  setFForm: React.Dispatch<React.SetStateAction<Folder>>;
  devices: Device[];
  onClose: () => void;
  onSave: (e: React.FormEvent) => Promise<void>;
  onOpenFolderPicker: (startPath: string) => void;
};

export function FolderModal({ show, editing, fForm, setFForm, devices, onClose, onSave, onOpenFolderPicker }: FolderModalProps) {
  if (!show) return null;
  return (
    <div className="fixed inset-0 bg-black/70 flex items-center justify-center p-4 z-40">
      <form onSubmit={onSave} className="bg-[#12161d] border border-zinc-700 rounded-lg p-5 max-w-lg w-full space-y-4 shadow-2xl">
        <h3 className="text-sm font-semibold text-zinc-100">Configurer un dossier synchronisé</h3>
        <div className="grid sm:grid-cols-2 gap-3">
          <div>
            <label className="block text-xs text-zinc-400 mb-1">Folder ID (identique sur chaque machine)</label>
            <input
              type="text"
              required
              disabled={editing}
              placeholder="ex: mes-docs"
              value={fForm.folder_id}
              onChange={(e) => setFForm({ ...fForm, folder_id: e.target.value })}
              className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-xs font-mono disabled:opacity-60"
            />
          </div>
          <div>
            <label className="block text-xs text-zinc-400 mb-1">Libellé</label>
            <input
              type="text"
              placeholder="Mes Documents"
              value={fForm.label}
              onChange={(e) => setFForm({ ...fForm, label: e.target.value })}
              className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-xs"
            />
          </div>
        </div>

        <div>
          <div className="flex justify-between mb-1">
            <label className="text-xs text-zinc-400">Chemin local sur cette machine</label>
            <button type="button" onClick={() => onOpenFolderPicker(fForm.local_path)} className="text-xs text-blue-400">
              Parcourir
            </button>
          </div>
          <input
            type="text"
            required
            placeholder="C:\Users\wboss\Documents ou /mnt/tank/Docs"
            value={fForm.local_path}
            onChange={(e) => setFForm({ ...fForm, local_path: e.target.value })}
            className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-xs font-mono"
          />
        </div>

        <div className="grid sm:grid-cols-3 gap-3">
          <div>
            <label className="block text-xs text-zinc-400 mb-1">Mode</label>
            <select
              value={fForm.mode}
              onChange={(e) => setFForm({ ...fForm, mode: e.target.value as any })}
              className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-2 py-1.5 text-xs"
            >
              <option value="send_receive">Bidirectionnel</option>
              <option value="send_only">Envoi seul</option>
              <option value="receive_only">Réception seule</option>
            </select>
          </div>
          <div>
            <label className="block text-xs text-zinc-400 mb-1">Versioning</label>
            <select
              value={fForm.versioning}
              onChange={(e) => setFForm({ ...fForm, versioning: e.target.value })}
              className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-2 py-1.5 text-xs"
            >
              <option value="trashcan">Corbeille (.gensversions)</option>
              <option value="simple">Simple (N copies)</option>
              <option value="staggered">Échelonné (Staggered)</option>
              <option value="none">Aucun</option>
            </select>
          </div>
          {fForm.versioning === "simple" && (
            <div>
              <label className="block text-xs text-zinc-400 mb-1">Versions à garder</label>
              <input
                type="number"
                min={1}
                max={100}
                value={fForm.keep_versions}
                onChange={(e) => setFForm({ ...fForm, keep_versions: Number(e.target.value) })}
                className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-2 py-1.5 text-xs"
              />
            </div>
          )}
          <div>
            <label className="block text-xs text-zinc-400 mb-1">Rescan (sec)</label>
            <input
              type="number"
              min={5}
              value={fForm.rescan_seconds}
              onChange={(e) => setFForm({ ...fForm, rescan_seconds: Number(e.target.value) })}
              className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-2 py-1.5 text-xs"
            />
          </div>
        </div>

        <div className="grid sm:grid-cols-3 gap-3">
          <div>
            <label className="block text-xs text-zinc-400 mb-1">Espace libre min. (Mo)</label>
            <input
              type="number"
              min={0}
              value={fForm.min_free_disk_mb ?? 100}
              onChange={(e) => setFForm({ ...fForm, min_free_disk_mb: Number(e.target.value) })}
              className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-2 py-1.5 text-xs"
              title="Stoppe les téléchargements si le disque passe sous ce seuil"
            />
          </div>
          <div>
            <label className="block text-xs text-zinc-400 mb-1">Rétention corbeille (j)</label>
            <input
              type="number"
              min={0}
              value={fForm.trashcan_max_days ?? 30}
              onChange={(e) => setFForm({ ...fForm, trashcan_max_days: Number(e.target.value) })}
              className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-2 py-1.5 text-xs"
              title="0 = illimité"
            />
          </div>
          <div>
            <label className="block text-xs text-zinc-400 mb-1">Quota corbeille (Mo)</label>
            <input
              type="number"
              min={0}
              value={fForm.trashcan_max_mb ?? 0}
              onChange={(e) => setFForm({ ...fForm, trashcan_max_mb: Number(e.target.value) })}
              className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-2 py-1.5 text-xs"
              title="0 = illimité"
            />
          </div>
        </div>

        <div>
          <label className="block text-xs text-zinc-400 mb-1">Appareils autorisés sur ce dossier</label>
          <div className="flex flex-wrap gap-2 bg-[#0b0d11] border border-zinc-800 rounded-md p-2.5">
            {devices.length === 0 && <span className="text-xs text-zinc-500">Aucun appareil appairé.</span>}
            {devices.map((d) => {
              const checked = fForm.peers.includes(d.device_id);
              return (
                <label key={d.device_id} className="flex items-center gap-1.5 text-xs bg-[#12161d] px-2.5 py-1 rounded border border-zinc-700 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={checked}
                    onChange={(e) => {
                      const next = e.target.checked ? [...fForm.peers, d.device_id] : fForm.peers.filter((id) => id !== d.device_id);
                      setFForm({ ...fForm, peers: next });
                    }}
                  />
                  {d.name}
                </label>
              );
            })}
          </div>
        </div>

        <div>
          <label className="block text-xs text-zinc-400 mb-1">Exclusions (.gensignore)</label>
          <textarea
            rows={3}
            placeholder={"*.tmp\nnode_modules/\n!important.tmp"}
            value={fForm.ignore_patterns}
            onChange={(e) => setFForm({ ...fForm, ignore_patterns: e.target.value })}
            className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-xs font-mono"
          />
        </div>

        <div className="flex justify-end gap-2 pt-1">
          <button type="button" onClick={onClose} className="px-3.5 py-1.5 rounded-md bg-zinc-800 text-xs">
            Annuler
          </button>
          <button type="submit" className="px-3.5 py-1.5 rounded-md bg-blue-600 hover:bg-blue-500 text-xs font-medium text-white">
            Enregistrer
          </button>
        </div>
      </form>
    </div>
  );
}

type PathPickerModalProps = {
  pickerTarget: null | "folder" | "drop" | "inbox";
  status: DaemonStatus | null;
  browsePath: string;
  setBrowsePath: (p: string) => void;
  browseParent: string;
  browseEntries: LocalBrowseEntry[];
  onLoadBrowse: (p: string) => void;
  onSelectDropPath: (p: string) => void;
  onConfirmDir: () => void;
  onClose: () => void;
};

export function PathPickerModal({
  pickerTarget,
  status,
  browsePath,
  setBrowsePath,
  browseParent,
  browseEntries,
  onLoadBrowse,
  onSelectDropPath,
  onConfirmDir,
  onClose,
}: PathPickerModalProps) {
  if (!pickerTarget) return null;
  return (
    <div className="fixed inset-0 bg-black/75 flex items-center justify-center p-4 z-50">
      <div className="bg-[#12161d] border border-zinc-700 rounded-lg p-5 max-w-lg w-full space-y-3 shadow-2xl">
        <div className="flex items-center justify-between">
          <h3 className="font-semibold text-sm text-zinc-100">Parcourir l'hôte ({status?.node_name})</h3>
          <button onClick={onClose} className="text-zinc-400 hover:text-white text-xs">
            Fermer
          </button>
        </div>
        <div className="flex gap-2">
          <button onClick={() => onLoadBrowse(browseParent)} className="px-2.5 py-1.5 rounded-md bg-zinc-800 hover:bg-zinc-700 text-xs">
            Parent
          </button>
          <input
            type="text"
            value={browsePath}
            onChange={(e) => setBrowsePath(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && onLoadBrowse(browsePath)}
            className="flex-1 bg-[#0b0d11] border border-zinc-700 rounded-md px-2.5 py-1.5 text-xs font-mono"
          />
          <button onClick={() => onLoadBrowse(browsePath)} className="px-3 py-1.5 rounded-md bg-zinc-800 text-xs">
            Ouvrir
          </button>
        </div>
        <div className="max-h-64 overflow-y-auto border border-zinc-800 rounded-md bg-[#0b0d11] divide-y divide-zinc-800/60">
          {browseEntries.map((ent) => (
            <div key={ent.path} className="flex items-center justify-between px-3 py-2 text-xs hover:bg-zinc-900">
              <button
                onClick={() => (ent.is_dir ? onLoadBrowse(ent.path) : undefined)}
                className="text-left flex-1 truncate font-mono flex items-center gap-2 text-zinc-200"
              >
                {ent.is_dir ? <FolderIcon size={14} className="text-blue-400 shrink-0" /> : <FileIcon size={14} className="text-zinc-500 shrink-0" />}
                <span className="truncate">{ent.name}</span>
              </button>
              {pickerTarget === "drop" && (
                <button onClick={() => onSelectDropPath(ent.path)} className="ml-2 px-2 py-0.5 rounded bg-blue-600 text-white text-[11px]">
                  Sélectionner
                </button>
              )}
            </div>
          ))}
        </div>
        {(pickerTarget === "folder" || pickerTarget === "inbox") && (
          <div className="flex justify-end">
            <button onClick={onConfirmDir} className="px-4 py-1.5 rounded-md bg-blue-600 hover:bg-blue-500 text-xs font-medium text-white">
              Utiliser ce dossier
            </button>
          </div>
        )}
      </div>
    </div>
  );
}

type InspectorModalProps = {
  folderID: string | null;
  conflicts: ConflictItem[];
  versions: VersionItem[];
  onRefresh: (folderID: string) => Promise<void>;
  onClose: () => void;
  notify: (text: string, kind?: "ok" | "err") => void;
};

export function InspectorModal({ folderID, conflicts, versions, onRefresh, onClose, notify }: InspectorModalProps) {
  if (!folderID) return null;
  return (
    <div className="fixed inset-0 bg-black/75 flex items-center justify-center p-4 z-50">
      <div className="bg-[#12161d] border border-zinc-700 rounded-lg p-5 max-w-2xl w-full space-y-4 shadow-2xl max-h-[85vh] overflow-y-auto">
        <div className="flex items-center justify-between">
          <h3 className="font-semibold text-sm text-zinc-100">
            Conflits & Versions archivées — <span className="font-mono text-blue-400">{folderID}</span>
          </h3>
          <button onClick={onClose} className="text-zinc-400 hover:text-white text-xs">
            Fermer
          </button>
        </div>

        <div className="space-y-2">
          <h4 className="text-xs font-semibold uppercase tracking-wider text-zinc-400">Conflits détectés ({conflicts.length})</h4>
          {conflicts.length === 0 ? (
            <p className="text-xs text-zinc-500">Aucun fichier en conflit dans ce dossier.</p>
          ) : (
            <div className="border border-zinc-800 rounded-md divide-y divide-zinc-800 bg-[#0b0d11]">
              {conflicts.map((c) => (
                <div key={c.rel_path} className="p-3 flex flex-wrap items-center justify-between gap-2 text-xs">
                  <div className="min-w-0">
                    <div className="font-mono text-amber-300 truncate">{c.rel_path}</div>
                    <div className="text-[11px] text-zinc-500">
                      Original : {c.original_path} · {formatBytes(c.size)}
                    </div>
                  </div>
                  <div className="flex gap-2">
                    <button
                      onClick={async () => {
                        await apiFetch(`/api/v1/folders/${encodeURIComponent(folderID)}/conflicts/resolve`, {
                          method: "POST",
                          body: JSON.stringify({ rel_path: c.rel_path, action: "keep_conflict" }),
                        });
                        await onRefresh(folderID);
                        notify("Version du conflit restaurée comme fichier principal.");
                      }}
                      className="px-2.5 py-1 rounded bg-blue-600 hover:bg-blue-500 text-white"
                    >
                      Garder celle-ci
                    </button>
                    <button
                      onClick={async () => {
                        await apiFetch(`/api/v1/folders/${encodeURIComponent(folderID)}/conflicts/resolve`, {
                          method: "POST",
                          body: JSON.stringify({ rel_path: c.rel_path, action: "delete" }),
                        });
                        await onRefresh(folderID);
                        notify("Fichier de conflit supprimé.");
                      }}
                      className="px-2.5 py-1 rounded bg-zinc-800 hover:bg-rose-900/60 text-rose-300"
                    >
                      Supprimer
                    </button>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        <div className="space-y-2 pt-2 border-t border-zinc-800">
          <h4 className="text-xs font-semibold uppercase tracking-wider text-zinc-400">
            Versions archivées (.gensversions) ({versions.length})
          </h4>
          {versions.length === 0 ? (
            <p className="text-xs text-zinc-500">Aucune ancienne version archivée.</p>
          ) : (
            <div className="border border-zinc-800 rounded-md divide-y divide-zinc-800 bg-[#0b0d11] max-h-60 overflow-y-auto">
              {versions.map((v) => (
                <div key={v.archive_path} className="p-3 flex flex-wrap items-center justify-between gap-2 text-xs">
                  <div className="min-w-0">
                    <div className="font-mono text-zinc-200 truncate">{v.archive_path}</div>
                    <div className="text-[11px] text-zinc-500">
                      Cible : {v.orig_rel_path} · {formatBytes(v.size)}
                    </div>
                  </div>
                  <button
                    onClick={async () => {
                      await apiFetch(`/api/v1/folders/${encodeURIComponent(folderID)}/versions/restore`, {
                        method: "POST",
                        body: JSON.stringify({ archive_path: v.archive_path }),
                      });
                      await onRefresh(folderID);
                      notify("Version restaurée.");
                    }}
                    className="px-2.5 py-1 rounded bg-emerald-600 hover:bg-emerald-500 text-white"
                  >
                    Restaurer
                  </button>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
