import { useState } from "react";
import { LogEntry } from "../types";

export function LogsTab({ logs }: { logs: LogEntry[] }) {
  const [logFilter, setLogFilter] = useState("");
  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold text-zinc-100">Activité & Journaux</h2>
          <p className="text-xs text-zinc-400 mt-0.5">Flux en temps réel du daemon (P2P, synchronisation, QuickDrop).</p>
        </div>
        <input
          type="text"
          placeholder="Filtrer (ex: p2p, syncer, error)..."
          value={logFilter}
          onChange={(e) => setLogFilter(e.target.value)}
          className="bg-[#12161d] border border-zinc-700 rounded-md px-3 py-1.5 text-xs w-64 focus:outline-none focus:border-blue-500"
        />
      </div>

      <div className="bg-[#080a0e] border border-zinc-800 rounded-lg p-3 font-mono text-[11px] h-[520px] overflow-y-auto space-y-1">
        {logs
          .filter((l) => !logFilter || l.message.toLowerCase().includes(logFilter.toLowerCase()))
          .slice()
          .reverse()
          .map((l) => (
            <div key={l.id} className="text-zinc-300 leading-relaxed border-b border-zinc-900/80 pb-1">
              <span className="text-zinc-500 mr-2">{new Date(l.timestamp * 1000).toLocaleTimeString()}</span>
              <span
                className={
                  l.message.toLowerCase().includes("error") || l.message.toLowerCase().includes("fatal")
                    ? "text-rose-400"
                    : l.message.includes("[p2p]")
                    ? "text-blue-300"
                    : "text-zinc-200"
                }
              >
                {l.message}
              </span>
            </div>
          ))}
        {logs.length === 0 && <div className="text-zinc-500 text-center py-8">Aucun journal enregistré.</div>}
      </div>
    </div>
  );
}
