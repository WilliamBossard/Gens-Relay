import { useState } from "react";
import { Download, ExternalLink, Lock, PowerOff, Radio } from "lucide-react";
import { apiFetch } from "../api";
import { CompanionAppsInfo, DaemonStatus } from "../types";

export type SettingsFormState = {
  node_name: string;
  relay_url: string;
  relay_urls_text: string;
  auth_token: string;
  clear_auth_token: boolean;
  drop_inbox_dir: string;
  max_upload_kbps: number;
  max_download_kbps: number;
  log_level: string;
  log_format: string;
  enable_upnp: boolean;
  enable_compression: boolean;
  web_listen: string;
  web_tls: boolean;
  web_user: string;
  web_password: string;
  clear_web_password: boolean;
};

type Props = {
  status: DaemonStatus | null;
  cfgForm: SettingsFormState;
  setCfgForm: React.Dispatch<React.SetStateAction<SettingsFormState>>;
  companionApps: CompanionAppsInfo | null;
  onOpenInboxPicker: () => void;
  onSave: (e: React.FormEvent) => Promise<void>;
  onStopDaemon: () => Promise<void>;
  notify: (text: string, kind?: "ok" | "err") => void;
};

export function SettingsTab({
  status,
  cfgForm,
  setCfgForm,
  companionApps,
  onOpenInboxPicker,
  onSave,
  onStopDaemon,
  notify,
}: Props) {
  const [confirmStop, setConfirmStop] = useState(false);
  const [stopping, setStopping] = useState(false);

  const launchOrInstallDrop = async () => {
    if (!companionApps) return;
    try {
      if (companionApps.drop_installed) {
        await apiFetch("/api/v1/companion-apps/drop/launch", { method: "POST" });
        notify("Lancement de Gens-Drop...");
      } else {
        await apiFetch("/api/v1/companion-apps/drop/install", { method: "POST" });
        notify("Ouverture de l'installateur Gens-Relay...");
      }
    } catch (e: any) {
      notify(e?.message || "Action impossible.", "err");
    }
  };

  return (
    <div className="space-y-6 max-w-3xl">
      <form onSubmit={onSave} className="space-y-5">
        <div>
          <h2 className="text-lg font-semibold text-zinc-100">Paramètres du Daemon</h2>
          <p className="text-xs text-zinc-400 mt-0.5">
            Identité locale, serveurs relais (failover), NAT UPnP, compression Zstd et sécurité Web.
          </p>
        </div>

        <div className="bg-[#12161d] border border-zinc-800/90 rounded-lg p-5 grid sm:grid-cols-2 gap-4">
          <div>
            <label className="block text-xs font-medium text-zinc-400 mb-1">Nom de l'appareil</label>
            <input
              type="text"
              value={cfgForm.node_name}
              onChange={(e) => setCfgForm({ ...cfgForm, node_name: e.target.value })}
              className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-sm"
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-zinc-400 mb-1">Adresse d'écoute Web UI</label>
            <input
              type="text"
              value={cfgForm.web_listen}
              onChange={(e) => setCfgForm({ ...cfgForm, web_listen: e.target.value })}
              className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-sm font-mono"
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-zinc-400 mb-1">URL du relais principal</label>
            <input
              type="text"
              placeholder="ws://192.168.1.10:38420/ws"
              value={cfgForm.relay_url}
              onChange={(e) => setCfgForm({ ...cfgForm, relay_url: e.target.value })}
              className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-sm font-mono"
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-zinc-400 mb-1">Token réseau (AUTH_TOKEN)</label>
            <input
              type="password"
              placeholder={status?.has_auth_token ? "Configuré (laisser vide pour garder)" : "Aucun token"}
              value={cfgForm.auth_token}
              onChange={(e) => setCfgForm({ ...cfgForm, auth_token: e.target.value, clear_auth_token: false })}
              className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-sm"
            />
            {status?.has_auth_token && (
              <label className="inline-flex items-center gap-1.5 text-[11px] text-rose-400 mt-1.5 cursor-pointer">
                <input
                  type="checkbox"
                  checked={cfgForm.clear_auth_token}
                  onChange={(e) => setCfgForm({ ...cfgForm, clear_auth_token: e.target.checked, auth_token: "" })}
                />
                Supprimer le token réseau
              </label>
            )}
          </div>

          <div className="sm:col-span-2">
            <div className="flex items-center justify-between mb-1">
              <label className="text-xs font-medium text-zinc-400">Relais de secours / multi-relais (1 URL par ligne)</label>
              {status?.active_relay_url && (
                <span className="inline-flex items-center gap-1 text-[11px] text-emerald-400 font-mono">
                  <Radio size={11} />
                  Actif: {status.active_relay_url}
                  {status.relay_transport_mode ? ` (${status.relay_transport_mode.toUpperCase()})` : ""}
                </span>
              )}
            </div>
            <textarea
              rows={2}
              placeholder={"ws://backup-relay.exemple.com:38420/ws"}
              value={cfgForm.relay_urls_text}
              onChange={(e) => setCfgForm({ ...cfgForm, relay_urls_text: e.target.value })}
              className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-xs font-mono"
            />
          </div>

          <div className="sm:col-span-2">
            <div className="flex justify-between mb-1">
              <label className="text-xs font-medium text-zinc-400">Dossier de réception QuickDrop par défaut</label>
              <button type="button" onClick={onOpenInboxPicker} className="text-xs text-blue-400">
                Parcourir
              </button>
            </div>
            <input
              type="text"
              value={cfgForm.drop_inbox_dir}
              onChange={(e) => setCfgForm({ ...cfgForm, drop_inbox_dir: e.target.value })}
              className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-sm font-mono"
            />
          </div>

          <div>
            <label className="block text-xs font-medium text-zinc-400 mb-1">Limite Upload (Ko/s, 0 = illimité)</label>
            <input
              type="number"
              min={0}
              value={cfgForm.max_upload_kbps}
              onChange={(e) => setCfgForm({ ...cfgForm, max_upload_kbps: Number(e.target.value) })}
              className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-sm"
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-zinc-400 mb-1">Limite Download (Ko/s, 0 = illimité)</label>
            <input
              type="number"
              min={0}
              value={cfgForm.max_download_kbps}
              onChange={(e) => setCfgForm({ ...cfgForm, max_download_kbps: Number(e.target.value) })}
              className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-sm"
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-zinc-400 mb-1">Niveau de log</label>
            <select
              value={cfgForm.log_level || "info"}
              onChange={(e) => setCfgForm({ ...cfgForm, log_level: e.target.value })}
              className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-sm"
            >
              <option value="debug">debug (verbeux)</option>
              <option value="info">info</option>
              <option value="warn">warn</option>
              <option value="error">error</option>
            </select>
          </div>
          <div>
            <label className="block text-xs font-medium text-zinc-400 mb-1">Format log</label>
            <select
              value={cfgForm.log_format || "text"}
              onChange={(e) => setCfgForm({ ...cfgForm, log_format: e.target.value })}
              className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-sm"
            >
              <option value="text">text (console)</option>
              <option value="json">json (fichier)</option>
            </select>
          </div>

          <div className="sm:col-span-2 flex flex-wrap gap-6 pt-1 text-xs text-zinc-300">
            <label className="inline-flex items-center gap-2 cursor-pointer">
              <input
                type="checkbox"
                checked={cfgForm.enable_upnp}
                onChange={(e) => setCfgForm({ ...cfgForm, enable_upnp: e.target.checked })}
              />
              Ouverture automatique des ports UPnP / NAT-PMP
            </label>
            <label className="inline-flex items-center gap-2 cursor-pointer">
              <input
                type="checkbox"
                checked={cfgForm.enable_compression}
                onChange={(e) => setCfgForm({ ...cfgForm, enable_compression: e.target.checked })}
              />
              Compression Zstd adaptative à la volée
            </label>
          </div>

          <div className="sm:col-span-2 pt-3 border-t border-zinc-800 flex items-center justify-between gap-2 text-xs font-semibold text-zinc-300">
            <span className="flex items-center gap-2">
              <Lock size={13} className="text-blue-400" /> Sécurité & Authentification Web UI (recommandé sur TrueNAS / VM)
            </span>
            <label className="flex items-center gap-2 font-normal text-zinc-300 cursor-pointer">
              <input
                type="checkbox"
                checked={cfgForm.web_tls}
                onChange={(e) => setCfgForm({ ...cfgForm, web_tls: e.target.checked })}
              />
              Activer HTTPS (redémarrage requis)
            </label>
          </div>
          <div>
            <label className="block text-xs font-medium text-zinc-400 mb-1">Utilisateur</label>
            <input
              type="text"
              value={cfgForm.web_user}
              onChange={(e) => setCfgForm({ ...cfgForm, web_user: e.target.value })}
              className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-sm"
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-zinc-400 mb-1">Nouveau mot de passe</label>
            <input
              type="password"
              placeholder={status?.has_web_password ? "Configuré (laisser vide pour conserver)" : "Aucun mot de passe"}
              value={cfgForm.web_password}
              onChange={(e) => setCfgForm({ ...cfgForm, web_password: e.target.value, clear_web_password: false })}
              className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-sm"
            />
            {status?.has_web_password && (
              <label className="inline-flex items-center gap-1.5 text-[11px] text-rose-400 mt-1.5 cursor-pointer">
                <input
                  type="checkbox"
                  checked={cfgForm.clear_web_password}
                  onChange={(e) => setCfgForm({ ...cfgForm, clear_web_password: e.target.checked, web_password: "" })}
                />
                Désactiver le mot de passe Web UI
              </label>
            )}
          </div>
        </div>

        <button type="submit" className="px-5 py-2 rounded-md bg-blue-600 hover:bg-blue-500 text-xs font-medium text-white transition">
          Enregistrer
        </button>
      </form>

      {status?.is_loopback && companionApps && (
        <div className="bg-[#12161d] border border-zinc-800/90 rounded-lg p-5 flex flex-wrap items-center justify-between gap-4">
          <div className="space-y-1">
            <h3 className="text-sm font-semibold text-zinc-100">Application compagnon — Gens-Drop</h3>
            <p className="text-xs text-zinc-400">
              {companionApps.drop_installed
                ? "Gens-Drop (fenêtre compacte façon Blip) est installée et partage ce daemon."
                : "Installe l'app compacte Gens-Drop pour glisser-déposer rapidement des fichiers sans ouvrir toute l'interface."}
            </p>
          </div>
          <button
            type="button"
            onClick={launchOrInstallDrop}
            disabled={!companionApps.drop_installed && !companionApps.installer_available}
            className="inline-flex items-center gap-2 px-3.5 py-2 rounded-md bg-zinc-800 hover:bg-zinc-700 disabled:opacity-50 border border-zinc-700 text-xs font-medium text-zinc-100 transition"
          >
            {companionApps.drop_installed ? <ExternalLink size={14} /> : <Download size={14} />}
            {companionApps.drop_installed
              ? "Ouvrir Gens-Drop"
              : companionApps.installer_available
                ? "Installer Gens-Drop"
                : "Setup indisponible"}
          </button>
        </div>
      )}

      <div className="bg-rose-950/15 border border-rose-900/50 rounded-lg p-5 flex flex-wrap items-center justify-between gap-4">
        <div className="space-y-1">
          <h3 className="text-sm font-semibold text-rose-200 flex items-center gap-2">
            <PowerOff size={15} className="text-rose-400" /> Contrôle du service (gens-daemon)
          </h3>
          <p className="text-xs text-zinc-400">
            Arrêter complètement le daemon coupe toutes les connexions P2P, transferts QuickDrop et synchronisations actives (y compris pour les apps connectées).
          </p>
        </div>
        {status?.is_loopback === false ? (
          <span className="text-[11px] text-zinc-500 italic">Disponible uniquement en accès local</span>
        ) : !confirmStop ? (
          <button
            type="button"
            onClick={() => setConfirmStop(true)}
            className="px-4 py-2 rounded-md bg-rose-600/90 hover:bg-rose-500 text-xs font-medium text-white transition"
          >
            Arrêter le daemon Gens-Relay
          </button>
        ) : (
          <div className="flex items-center gap-2">
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
              className="px-3.5 py-2 rounded-md bg-rose-600 hover:bg-rose-500 text-xs font-semibold text-white"
            >
              {stopping ? "Arrêt..." : "Confirmer l'arrêt"}
            </button>
            <button
              type="button"
              onClick={() => setConfirmStop(false)}
              className="px-3 py-2 rounded-md bg-zinc-800 hover:bg-zinc-700 text-xs text-zinc-200"
            >
              Annuler
            </button>
          </div>
        )}
      </div>
    </div>
  );
}
