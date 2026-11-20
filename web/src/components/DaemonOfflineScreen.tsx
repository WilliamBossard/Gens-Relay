import { useState } from "react";
import { Play, PowerOff, RefreshCw, WifiOff } from "lucide-react";
import { isTauriRuntime } from "../types";

type Props = {
  compact?: boolean;
  stoppedByUser: boolean;
  lastError?: string;
  onReconnect: () => Promise<void>;
  onStartDaemon?: () => Promise<void>;
};

export function DaemonOfflineScreen({ compact = false, stoppedByUser, lastError, onReconnect, onStartDaemon }: Props) {
  const [busy, setBusy] = useState<"reconnect" | "start" | null>(null);
  const [actionErr, setActionErr] = useState("");
  const inTauri = isTauriRuntime();

  const handleReconnect = async () => {
    setBusy("reconnect");
    setActionErr("");
    try {
      await onReconnect();
    } catch (e: any) {
      setActionErr(e?.message || "Le daemon ne répond pas encore.");
    } finally {
      setBusy(null);
    }
  };

  const handleStart = async () => {
    if (!onStartDaemon) return;
    setBusy("start");
    setActionErr("");
    try {
      await onStartDaemon();
    } catch (e: any) {
      setActionErr(e?.message || "Impossible de relancer le daemon.");
    } finally {
      setBusy(null);
    }
  };

  return (
    <div className="min-h-screen bg-[#0b0d11] text-zinc-100 flex items-center justify-center p-4">
      <div
        className={`w-full ${
          compact ? "max-w-sm p-5" : "max-w-md p-7"
        } bg-[#12161d] border border-zinc-800 rounded-xl shadow-2xl text-center space-y-4`}
      >
        <div
          className={`mx-auto w-12 h-12 rounded-full flex items-center justify-center ${
            stoppedByUser ? "bg-amber-500/10 text-amber-400 border border-amber-500/25" : "bg-rose-500/10 text-rose-400 border border-rose-500/25"
          }`}
        >
          {stoppedByUser ? <PowerOff size={22} /> : <WifiOff size={22} />}
        </div>

        <div className="space-y-1.5">
          <h1 className="text-base font-semibold text-zinc-100">
            {stoppedByUser ? "Daemon Gens-Relay arrêté" : "Daemon Gens-Relay hors ligne"}
          </h1>
          <p className="text-xs text-zinc-400 leading-relaxed">
            {stoppedByUser
              ? "Le service d'arrière-plan Gens-Relay a été arrêté. La synchronisation des dossiers et QuickDrop sont en pause."
              : "Impossible de joindre le daemon local ou distant. Vérifie qu'il est démarré ou relance-le."}
          </p>
        </div>

        {(actionErr || (!stoppedByUser && lastError)) && (
          <div className="text-[11px] text-rose-300 bg-rose-950/40 border border-rose-800/50 rounded-md px-3 py-2 break-words">
            {actionErr || lastError}
          </div>
        )}

        <div className="flex flex-col sm:flex-row gap-2.5 pt-1 justify-center">
          {inTauri && onStartDaemon && (
            <button
              type="button"
              disabled={busy !== null}
              onClick={handleStart}
              className="inline-flex items-center justify-center gap-2 px-4 py-2 rounded-md bg-blue-600 hover:bg-blue-500 disabled:opacity-60 text-xs font-medium text-white transition"
            >
              <Play size={14} />
              {busy === "start" ? "Démarrage..." : "Relancer le daemon"}
            </button>
          )}
          <button
            type="button"
            disabled={busy !== null}
            onClick={handleReconnect}
            className={`inline-flex items-center justify-center gap-2 px-4 py-2 rounded-md text-xs font-medium transition disabled:opacity-60 ${
              inTauri && onStartDaemon
                ? "bg-zinc-800 hover:bg-zinc-700 text-zinc-200 border border-zinc-700"
                : "bg-blue-600 hover:bg-blue-500 text-white"
            }`}
          >
            <RefreshCw size={14} className={busy === "reconnect" ? "animate-spin" : ""} />
            {busy === "reconnect" ? "Connexion..." : "Reconnecter"}
          </button>
        </div>

        <p className="text-[11px] text-zinc-500">
          {inTauri
            ? "Tu peux aussi démarrer ou arrêter le service depuis l'icône Gens-Relay dans la barre des tâches."
            : "L'interface vérifiera automatiquement si le service redevient disponible."}
        </p>
      </div>
    </div>
  );
}
