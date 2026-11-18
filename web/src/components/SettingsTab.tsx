import { Lock } from "lucide-react";
import { DaemonStatus } from "../types";

export type SettingsFormState = {
  node_name: string;
  relay_url: string;
  auth_token: string;
  clear_auth_token: boolean;
  drop_inbox_dir: string;
  max_upload_kbps: number;
  max_download_kbps: number;
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
  onOpenInboxPicker: () => void;
  onSave: (e: React.FormEvent) => Promise<void>;
};

export function SettingsTab({ status, cfgForm, setCfgForm, onOpenInboxPicker, onSave }: Props) {
  return (
    <form onSubmit={onSave} className="space-y-5 max-w-3xl">
      <div>
        <h2 className="text-lg font-semibold text-zinc-100">Paramètres du Daemon</h2>
        <p className="text-xs text-zinc-400 mt-0.5">Identité locale, serveur de coordination, limites de débit et sécurité Web.</p>
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
          <label className="block text-xs font-medium text-zinc-400 mb-1">URL du serveur de coordination</label>
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
  );
}
