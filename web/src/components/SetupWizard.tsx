import { useState } from "react";
import { ShieldCheck } from "lucide-react";
import { apiFetch } from "../api";
import { SetupStatus } from "../types";

type Props = {
  initialStatus: SetupStatus;
  onComplete: () => Promise<void>;
};

export function SetupWizard({ initialStatus, onComplete }: Props) {
  const [webUser, setWebUser] = useState(initialStatus.web_user || "admin");
  const [password, setPassword] = useState("");
  const [confirmPass, setConfirmPass] = useState("");
  const [nodeName, setNodeName] = useState(initialStatus.node_name || "");
  const [relayUrl, setRelayUrl] = useState(initialStatus.relay_url || "");
  const [inboxDir, setInboxDir] = useState(initialStatus.drop_inbox_dir || "");
  const [submitting, setSubmitting] = useState(false);
  const [errMsg, setErrMsg] = useState("");

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setErrMsg("");
    if (password.length < 4) {
      setErrMsg("Le mot de passe doit contenir au moins 4 caractères.");
      return;
    }
    if (password !== confirmPass) {
      setErrMsg("Les deux mots de passe ne correspondent pas.");
      return;
    }
    setSubmitting(true);
    try {
      await apiFetch("/api/v1/setup", {
        method: "POST",
        body: JSON.stringify({
          web_user: webUser.trim() || "admin",
          web_password: password,
          node_name: nodeName.trim(),
          relay_url: relayUrl.trim(),
          drop_inbox_dir: inboxDir.trim(),
        }),
      });
      await onComplete();
    } catch (err: any) {
      setErrMsg(err?.message || "Erreur lors de la configuration initiale.");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="min-h-screen bg-[#0b0d11] text-zinc-100 flex items-center justify-center p-4">
      <form
        onSubmit={submit}
        className="w-full max-w-md bg-[#12161d] border border-zinc-800 rounded-xl p-6 shadow-2xl space-y-4"
      >
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-lg bg-blue-600/15 border border-blue-500/30 flex items-center justify-center text-blue-400">
            <ShieldCheck size={20} />
          </div>
          <div>
            <h1 className="text-base font-semibold text-zinc-100">Configuration initiale — Gens-Relay</h1>
            <p className="text-xs text-zinc-400">
              Cet accès Web est exposé sur le réseau. Définis un compte administrateur avant utilisation.
            </p>
          </div>
        </div>

        {errMsg && (
          <div className="text-xs text-rose-300 bg-rose-950/40 border border-rose-800/50 rounded-md px-3 py-2">
            {errMsg}
          </div>
        )}

        <div className="space-y-3">
          <div>
            <label className="block text-xs font-medium text-zinc-400 mb-1">Nom de l'appareil / serveur</label>
            <input
              type="text"
              value={nodeName}
              onChange={(e) => setNodeName(e.target.value)}
              placeholder="TrueNAS / Mon Serveur"
              className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-sm"
            />
          </div>

          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="block text-xs font-medium text-zinc-400 mb-1">Utilisateur Web UI</label>
              <input
                type="text"
                required
                value={webUser}
                onChange={(e) => setWebUser(e.target.value)}
                className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-sm"
              />
            </div>
            <div>
              <label className="block text-xs font-medium text-zinc-400 mb-1">Dossier QuickDrop (optionnel)</label>
              <input
                type="text"
                value={inboxDir}
                onChange={(e) => setInboxDir(e.target.value)}
                className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-sm font-mono"
              />
            </div>
          </div>

          <div>
            <label className="block text-xs font-medium text-zinc-400 mb-1">Mot de passe (minimum 4 caractères)</label>
            <input
              type="password"
              required
              minLength={4}
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-sm"
            />
          </div>

          <div>
            <label className="block text-xs font-medium text-zinc-400 mb-1">Confirmer le mot de passe</label>
            <input
              type="password"
              required
              minLength={4}
              value={confirmPass}
              onChange={(e) => setConfirmPass(e.target.value)}
              className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-sm"
            />
          </div>

          <div>
            <label className="block text-xs font-medium text-zinc-400 mb-1">URL Relais (optionnel)</label>
            <input
              type="text"
              placeholder="ws://relay.exemple.com:38420/ws"
              value={relayUrl}
              onChange={(e) => setRelayUrl(e.target.value)}
              className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-sm font-mono"
            />
          </div>
        </div>

        <button
          type="submit"
          disabled={submitting}
          className="w-full py-2.5 rounded-md bg-blue-600 hover:bg-blue-500 disabled:opacity-60 text-xs font-medium text-white transition"
        >
          {submitting ? "Enregistrement..." : "Activer et sécuriser Gens-Relay"}
        </button>
      </form>
    </div>
  );
}
