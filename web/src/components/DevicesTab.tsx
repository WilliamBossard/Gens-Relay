import { useState } from "react";
import { Link2, Radio, ShieldCheck, Trash2, X } from "lucide-react";
import { apiFetch } from "../api";
import { DaemonStatus, Device, NearbyPeer, PairRequest, PeerStatus } from "../types";

type Props = {
  status: DaemonStatus | null;
  devices: Device[];
  peerStatusMap: Record<string, PeerStatus>;
  nearbyPeers: NearbyPeer[];
  outgoingRequests: PairRequest[];
  onRejectRequest: (deviceID: string) => Promise<void>;
  onReload: () => Promise<void>;
  notify: (text: string, kind?: "ok" | "err") => void;
};

export function DevicesTab({
  status,
  devices,
  peerStatusMap,
  nearbyPeers,
  outgoingRequests,
  onRejectRequest,
  onReload,
  notify,
}: Props) {
  const [reqDevID, setReqDevID] = useState("");
  const [reqName, setReqName] = useState("");
  const [reqAddr, setReqAddr] = useState("");
  const [reqMsg, setReqMsg] = useState("");
  const [pairCode, setPairCode] = useState<string | null>(null);
  const [redeemCode, setRedeemCode] = useState("");
  const [redeemName, setRedeemName] = useState("");

  const sendPairRequest = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      await apiFetch("/api/v1/pair/request", {
        method: "POST",
        body: JSON.stringify({
          device_id: reqDevID,
          name: reqName,
          endpoint: reqAddr,
          message: reqMsg,
        }),
      });
      setReqDevID("");
      setReqName("");
      setReqAddr("");
      setReqMsg("");
      await onReload();
      notify("Demande d'appairage envoyée.");
    } catch (err: any) {
      notify(err.message, "err");
    }
  };

  const sendNearbyRequest = async (np: NearbyPeer) => {
    try {
      await apiFetch("/api/v1/pair/request", {
        method: "POST",
        body: JSON.stringify({
          device_id: np.device_id,
          name: np.name,
          endpoint: np.endpoint,
          message: `Demande d'appairage depuis ${status?.node_name || "un pair LAN"}`,
        }),
      });
      await onReload();
      notify(`Demande d'appairage envoyée à ${np.name}.`);
    } catch (err: any) {
      notify(err.message, "err");
    }
  };

  const generatePairCode = async () => {
    try {
      const res = await apiFetch("/api/v1/pair/create", { method: "POST" });
      setPairCode(res.pair_code);
    } catch (err: any) {
      notify(err.message, "err");
    }
  };

  const submitRedeemCode = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      await apiFetch("/api/v1/pair/redeem", {
        method: "POST",
        body: JSON.stringify({ pair_code: redeemCode, name: redeemName }),
      });
      setRedeemCode("");
      setRedeemName("");
      await onReload();
      notify("Code validé, demande transmise à l'autre appareil.");
    } catch (err: any) {
      notify(err.message, "err");
    }
  };

  return (
    <div className="space-y-5">
      <div>
        <h2 className="text-lg font-semibold text-zinc-100">Appareils & Appairage</h2>
        <p className="text-xs text-zinc-400 mt-0.5">
          Envoie une demande d'appairage par Device ID, code court ou détection sur le réseau local.
        </p>
      </div>

      <div className="grid lg:grid-cols-3 gap-5">
        <div className="space-y-4">
          <form onSubmit={sendPairRequest} className="bg-[#12161d] border border-zinc-800/90 rounded-lg p-4 space-y-3">
            <h3 className="font-semibold text-xs uppercase tracking-wider text-zinc-300 flex items-center gap-2">
              <Link2 size={14} className="text-blue-400" /> Demande d'appairage
            </h3>
            <input
              type="text"
              required
              placeholder="Device ID cible (XXXX-XXXX-...)"
              value={reqDevID}
              onChange={(e) => setReqDevID(e.target.value)}
              className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-xs font-mono focus:outline-none focus:border-blue-500"
            />
            <input
              type="text"
              placeholder="Nom local (ex: TrueNAS-SCALE)"
              value={reqName}
              onChange={(e) => setReqName(e.target.value)}
              className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-xs"
            />
            <input
              type="text"
              placeholder="Note optionnelle (ex: PC Windows)"
              value={reqMsg}
              onChange={(e) => setReqMsg(e.target.value)}
              className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-xs"
            />
            <input
              type="text"
              placeholder="IP:Port LAN optionnel (sans relais)"
              value={reqAddr}
              onChange={(e) => setReqAddr(e.target.value)}
              className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-xs font-mono"
            />
            <button type="submit" className="w-full py-2 rounded-md bg-blue-600 hover:bg-blue-500 text-xs font-medium text-white transition">
              Envoyer la demande
            </button>
          </form>

          <div className="bg-[#12161d] border border-zinc-800/90 rounded-lg p-4 space-y-3">
            <h3 className="font-semibold text-xs uppercase tracking-wider text-zinc-300">Code d'appairage rapide</h3>
            <button onClick={generatePairCode} className="w-full py-1.5 rounded-md bg-zinc-800 hover:bg-zinc-700 text-xs font-medium transition">
              Générer un code (5 min)
            </button>
            {pairCode && (
              <div className="text-center py-2 bg-[#0b0d11] border border-zinc-700 rounded-md font-mono text-lg tracking-widest text-blue-400">
                {pairCode}
              </div>
            )}
            <form onSubmit={submitRedeemCode} className="space-y-2 pt-2 border-t border-zinc-800">
              <input
                type="text"
                placeholder="Code reçu (ex: 4K8-9B2)"
                value={redeemCode}
                onChange={(e) => setRedeemCode(e.target.value)}
                className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-xs font-mono"
              />
              <input
                type="text"
                placeholder="Nom de l'appareil"
                value={redeemName}
                onChange={(e) => setRedeemName(e.target.value)}
                className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-xs"
              />
              <button type="submit" className="w-full py-1.5 rounded-md bg-zinc-800 hover:bg-zinc-700 text-xs font-medium text-white">
                Valider le code
              </button>
            </form>
          </div>
        </div>

        <div className="lg:col-span-2 space-y-4">
          <div className="bg-[#12161d] border border-zinc-800/90 rounded-lg p-4 space-y-3">
            <div className="flex items-center justify-between">
              <h3 className="font-semibold text-xs uppercase tracking-wider text-zinc-300 flex items-center gap-2">
                <Radio size={14} className="text-emerald-400" /> Appareils détectés sur le LAN ({nearbyPeers.length})
              </h3>
              <span className="text-[11px] font-mono text-zinc-500">UDP :21027</span>
            </div>
            {nearbyPeers.length === 0 ? (
              <p className="text-xs text-zinc-500">Aucun nouvel appareil détecté sur le réseau local.</p>
            ) : (
              <div className="grid sm:grid-cols-2 gap-2.5">
                {nearbyPeers.map((np) => {
                  const alreadySent = outgoingRequests.some((r) => r.device_id === np.device_id);
                  return (
                    <div key={np.device_id} className="bg-[#0b0d11] border border-zinc-800 rounded-md p-3 flex items-center justify-between gap-2">
                      <div className="min-w-0">
                        <div className="text-xs font-semibold text-zinc-200 truncate">{np.name}</div>
                        <div className="text-[11px] font-mono text-zinc-500 truncate">{np.endpoint}</div>
                      </div>
                      <button
                        disabled={alreadySent}
                        onClick={() => sendNearbyRequest(np)}
                        className={`px-2.5 py-1 rounded text-xs font-medium shrink-0 ${
                          alreadySent ? "bg-zinc-800 text-zinc-500 cursor-not-allowed" : "bg-blue-600 hover:bg-blue-500 text-white"
                        }`}
                      >
                        {alreadySent ? "Envoyée" : "Appairer"}
                      </button>
                    </div>
                  );
                })}
              </div>
            )}
          </div>

          {outgoingRequests.length > 0 && (
            <div className="bg-[#12161d] border border-zinc-800/90 rounded-lg p-4 space-y-2.5">
              <h3 className="font-semibold text-xs uppercase tracking-wider text-zinc-300">
                Demandes en attente d'autorisation ({outgoingRequests.length})
              </h3>
              {outgoingRequests.map((req) => (
                <div key={req.device_id} className="bg-[#0b0d11] border border-zinc-800 rounded-md px-3.5 py-2.5 flex items-center justify-between gap-3">
                  <div className="min-w-0">
                    <div className="text-xs font-medium text-zinc-200">{req.name}</div>
                    <div className="text-[11px] font-mono text-zinc-500 truncate">{req.device_id}</div>
                  </div>
                  <div className="flex items-center gap-2">
                    <span className="text-[11px] px-2 py-0.5 rounded bg-amber-500/10 text-amber-300 border border-amber-500/20">En attente</span>
                    <button onClick={() => onRejectRequest(req.device_id)} className="p-1 rounded bg-zinc-800 hover:bg-zinc-700 text-zinc-400" title="Annuler">
                      <X size={13} />
                    </button>
                  </div>
                </div>
              ))}
            </div>
          )}

          <div className="bg-[#12161d] border border-zinc-800/90 rounded-lg p-4 space-y-2.5">
            <h3 className="font-semibold text-xs uppercase tracking-wider text-zinc-300">Appareils appairés ({devices.length})</h3>
            {devices.length === 0 ? (
              <p className="text-xs text-zinc-500">Aucun appareil appairé.</p>
            ) : (
              <div className="divide-y divide-zinc-800/80 border border-zinc-800 rounded-md bg-[#0b0d11]">
                {devices.map((d) => {
                  const ps = peerStatusMap[d.device_id];
                  return (
                    <div key={d.device_id} className="p-3.5 flex flex-wrap items-center justify-between gap-3">
                      <div className="min-w-0">
                        <div className="flex items-center gap-2">
                          <span className="font-semibold text-sm text-zinc-100">{d.name}</span>
                          <span
                            className={`inline-flex items-center gap-1.5 text-[11px] px-2 py-0.5 rounded ${
                              ps?.connected
                                ? "bg-emerald-500/10 text-emerald-300 border border-emerald-500/20"
                                : ps?.relay_online
                                ? "bg-amber-500/10 text-amber-300 border border-amber-500/20"
                                : "bg-zinc-800 text-zinc-400"
                            }`}
                          >
                            <span
                              className={`w-1.5 h-1.5 rounded-full ${
                                ps?.connected ? "bg-emerald-400" : ps?.relay_online ? "bg-amber-400" : "bg-zinc-600"
                              }`}
                            />
                            {ps?.connected ? `Connecté (${ps.route.toUpperCase()})` : ps?.relay_online ? "En ligne (relais)" : "Hors ligne"}
                          </span>
                        </div>
                        <p className="text-[11px] font-mono text-zinc-400 mt-0.5 break-all">{d.device_id}</p>
                        {ps?.remote_addr && <p className="text-[11px] text-zinc-500">{ps.remote_addr}</p>}
                      </div>

                      <div className="flex items-center gap-3">
                        <label className="flex items-center gap-1.5 text-xs text-zinc-300 cursor-pointer">
                          <input
                            type="checkbox"
                            checked={d.auto_accept_drop}
                            onChange={async (e) => {
                              await apiFetch("/api/v1/devices", {
                                method: "POST",
                                body: JSON.stringify({ ...d, auto_accept_drop: e.target.checked }),
                              });
                              await onReload();
                            }}
                          />
                          <ShieldCheck size={13} className="text-blue-400" /> Auto-accepter QuickDrop
                        </label>
                        <button
                          onClick={async () => {
                            await apiFetch(`/api/v1/devices/${encodeURIComponent(d.device_id)}`, { method: "DELETE" });
                            await onReload();
                          }}
                          className="p-1.5 rounded bg-zinc-800/80 hover:bg-rose-900/50 text-rose-400 transition"
                        >
                          <Trash2 size={13} />
                        </button>
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
