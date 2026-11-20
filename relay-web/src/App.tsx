import { useCallback, useEffect, useRef, useState } from "react";
import {
  Activity,
  Ban,
  CheckCircle2,
  KeyRound,
  LogOut,
  Radio,
  RefreshCw,
  Server,
  Settings,
  Shield,
  ShieldAlert,
  Terminal,
  Trash2,
  Users,
  Wifi,
  Zap,
} from "lucide-react";

type RelayStatus = {
  status: string;
  uptime_sec: number;
  listen_addr: string;
  admin_listen: string;
  stun_port: number;
  udp_relay_port?: number;
  online_devices: number;
  active_pair_codes: number;
  pending_queues: number;
  blind_relay_enabled: boolean;
  stores_files: boolean;
  has_auth_token: boolean;
  has_admin_password: boolean;
  trust_proxy: boolean;
  max_relay_kbps: number;
  max_clients: number;
  relay_packets_total: number;
  relay_bytes_total: number;
  udp_relay_bytes_total?: number;
  ws_relay_bytes_total?: number;
  relay_bps: number;
};

type SetupStatus = {
  needs_setup: boolean;
  is_local: boolean;
  requires_setup_token: boolean;
  admin_user?: string;
};

type ConnectedClient = {
  device_id: string;
  name: string;
  observed_addr: string;
  lan_addrs: string[];
  quic_port: number;
  connected_at: number;
  relay_rx_bytes: number;
  relay_tx_bytes: number;
  watching_count: number;
  udp_relay?: boolean;
};

type PairCodeInfo = {
  code: string;
  device_id: string;
  name: string;
  expires_at: number;
};

type PendingQueueInfo = {
  target_id: string;
  count: number;
  oldest_at: number;
};

type BanEntry = {
  device_id: string;
  reason: string;
};

type LogEntry = {
  id: number;
  timestamp: number;
  message: string;
};

type ConfigForm = {
  auth_token: string;
  clear_auth_token: boolean;
  enable_blind_relay: boolean;
  trust_proxy: boolean;
  max_relay_kbps: number;
  max_clients: number;
  admin_user: string;
  admin_password: string;
  clear_admin_password: boolean;
};

function formatBytes(bytes: number): string {
  if (!bytes) return "0 o";
  const units = ["o", "Ko", "Mo", "Go", "To"];
  let i = 0;
  let val = bytes;
  while (val >= 1024 && i < units.length - 1) {
    val /= 1024;
    i++;
  }
  return `${val.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

function formatDuration(sec: number): string {
  if (sec < 60) return `${sec}s`;
  const m = Math.floor(sec / 60);
  if (m < 60) return `${m}m ${sec % 60}s`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ${m % 60}m`;
  const d = Math.floor(h / 24);
  return `${d}j ${h % 24}h`;
}

function resolveApiBase(): string {
  const path = window.location.pathname;
  if (path.endsWith("/")) return path;
  const idx = path.lastIndexOf("/");
  return idx >= 0 ? path.slice(0, idx + 1) : "/";
}

export default function App() {
  const [tab, setTab] = useState<"overview" | "clients" | "pairing" | "logs" | "settings">("overview");
  const [setupStatus, setSetupStatus] = useState<SetupStatus | null>(null);
  const [setupUser, setSetupUser] = useState("admin");
  const [setupPass, setSetupPass] = useState("");
  const [setupPassConfirm, setSetupPassConfirm] = useState("");
  const [setupToken, setSetupToken] = useState("");
  const [setupAuthToken, setSetupAuthToken] = useState("");
  const [setupBlindRelay, setSetupBlindRelay] = useState(true);
  const [setupMaxKBps, setSetupMaxKBps] = useState(0);
  const [setupErr, setSetupErr] = useState("");
  const [setupBusy, setSetupBusy] = useState(false);

  const [status, setStatus] = useState<RelayStatus | null>(null);
  const [clients, setClients] = useState<ConnectedClient[]>([]);
  const [pairCodes, setPairCodes] = useState<PairCodeInfo[]>([]);
  const [pendingQueues, setPendingQueues] = useState<PendingQueueInfo[]>([]);
  const [bans, setBans] = useState<BanEntry[]>([]);
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [logFilter, setLogFilter] = useState("");
  const [bpsHistory, setBpsHistory] = useState<number[]>(new Array(30).fill(0));
  const [toast, setToast] = useState<{ text: string; kind: "ok" | "err" } | null>(null);
  const wsOpenRef = useRef(false);

  const [banDeviceInput, setBanDeviceInput] = useState("");
  const [banReasonInput, setBanReasonInput] = useState("");

  const cfgInitRef = useRef(false);
  const [cfgForm, setCfgForm] = useState<ConfigForm>({
    auth_token: "",
    clear_auth_token: false,
    enable_blind_relay: false,
    trust_proxy: false,
    max_relay_kbps: 0,
    max_clients: 2000,
    admin_user: "admin",
    admin_password: "",
    clear_admin_password: false,
  });

  const toastTimerRef = useRef<number | null>(null);
  const notify = (text: string, kind: "ok" | "err" = "ok") => {
    setToast({ text, kind });
    if (toastTimerRef.current !== null) window.clearTimeout(toastTimerRef.current);
    toastTimerRef.current = window.setTimeout(() => {
      setToast(null);
      toastTimerRef.current = null;
    }, 4000);
  };

  const apiBase = resolveApiBase();

  const apiFetch = useCallback(
    async (relPath: string, opts?: RequestInit) => {
      const headers: Record<string, string> = {};
      if (opts?.body) headers["Content-Type"] = "application/json";
      const res = await fetch(`${apiBase}${relPath}`, {
        ...opts,
        headers: { ...headers, ...(opts?.headers as Record<string, string>) },
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
      return data;
    },
    [apiBase]
  );

  const loadAll = useCallback(async () => {
    try {
      const ss: SetupStatus = await apiFetch("api/v1/setup/status");
      setSetupStatus(ss);
      if (ss.needs_setup) {
        if (ss.admin_user) setSetupUser((prev) => prev || ss.admin_user || "admin");
        return;
      }

      const [st, cls, pcs, pqs, banList, logList, cfg] = await Promise.all([
        apiFetch("api/v1/status"),
        apiFetch("api/v1/clients"),
        apiFetch("api/v1/pair-codes"),
        apiFetch("api/v1/pending"),
        apiFetch("api/v1/bans"),
        apiFetch("api/v1/logs"),
        apiFetch("api/v1/config"),
      ]);
      setStatus(st);
      setClients(cls || []);
      setPairCodes(pcs || []);
      setPendingQueues(pqs || []);
      setBans(banList || []);
      setLogs(logList || []);
      if (!wsOpenRef.current && typeof st?.relay_bps === "number") {
        setBpsHistory((prev) => [...prev.slice(-29), st.relay_bps]);
      }
      if (!cfgInitRef.current && cfg) {
        cfgInitRef.current = true;
        setCfgForm((prev) => ({
          ...prev,
          enable_blind_relay: !!cfg.enable_blind_relay,
          trust_proxy: !!cfg.trust_proxy,
          max_relay_kbps: cfg.max_relay_kbps || 0,
          max_clients: cfg.max_clients || 2000,
          admin_user: cfg.admin_user || "admin",
        }));
      }
    } catch (e: any) {
      if (String(e?.message || "").includes("setup_required")) {
        try {
          const ss = await apiFetch("api/v1/setup/status");
          setSetupStatus(ss);
        } catch { }
        return;
      }
      console.error(e);
    }
  }, [apiFetch]);

  useEffect(() => {
    loadAll();
    let stopped = false;
    let ws: WebSocket | null = null;
    let reconnectTimer: number | null = null;
    let backoff = 1000;

    const connectWS = () => {
      if (stopped) return;
      const proto = window.location.protocol === "https:" ? "wss" : "ws";
      ws = new WebSocket(`${proto}://${window.location.host}${apiBase}api/v1/events`);
      ws.onopen = () => {
        backoff = 1000;
        wsOpenRef.current = true;
      };
      ws.onmessage = (ev) => {
        try {
          const msg = JSON.parse(ev.data);
          if (msg.event === "status_updated" && msg.data) {
            setStatus(msg.data);
            setBpsHistory((prev) => [...prev.slice(-29), msg.data.relay_bps || 0]);
          }
          if (msg.event === "clients_updated") setClients(msg.data || []);
          if (msg.event === "log_entry" && msg.data) {
            setLogs((prev) => [...prev.slice(-499), msg.data]);
          }
        } catch { }
      };
      ws.onclose = () => {
        wsOpenRef.current = false;
        if (stopped) return;
        reconnectTimer = window.setTimeout(connectWS, backoff);
        backoff = Math.min(backoff * 2, 15000);
      };
      ws.onerror = () => ws?.close();
    };

    connectWS();
    const timer = setInterval(() => {
      if (!wsOpenRef.current) {
        loadAll();
      }
    }, 10000);
    const slowTimer = setInterval(() => {
      loadAll();
    }, 30000);
    return () => {
      stopped = true;
      if (reconnectTimer !== null) window.clearTimeout(reconnectTimer);
      ws?.close();
      clearInterval(timer);
      clearInterval(slowTimer);
    };
  }, [apiBase, loadAll]);

  const kickClient = async (deviceID: string) => {
    try {
      await apiFetch(`api/v1/clients/${encodeURIComponent(deviceID)}/kick`, { method: "POST" });
      await loadAll();
      notify(`Appareil ${deviceID.slice(0, 9)} déconnecté.`);
    } catch (e: any) {
      notify(e.message, "err");
    }
  };

  const banDevice = async (deviceID: string, reason = "Banni par l'administrateur") => {
    try {
      await apiFetch("api/v1/bans", {
        method: "POST",
        body: JSON.stringify({ device_id: deviceID, reason }),
      });
      setBanDeviceInput("");
      setBanReasonInput("");
      await loadAll();
      notify(`Appareil ${deviceID.slice(0, 9)} banni.`);
    } catch (e: any) {
      notify(e.message, "err");
    }
  };

  const unbanDevice = async (deviceID: string) => {
    try {
      await apiFetch(`api/v1/bans/${encodeURIComponent(deviceID)}`, { method: "DELETE" });
      await loadAll();
      notify(`Appareil ${deviceID.slice(0, 9)} débanni.`);
    } catch (e: any) {
      notify(e.message, "err");
    }
  };

  const revokePairCode = async (code: string) => {
    try {
      await apiFetch(`api/v1/pair-codes/${encodeURIComponent(code)}`, { method: "DELETE" });
      await loadAll();
      notify(`Code ${code} révoqué.`);
    } catch (e: any) {
      notify(e.message, "err");
    }
  };

  const saveConfig = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      await apiFetch("api/v1/config", {
        method: "POST",
        body: JSON.stringify(cfgForm),
      });
      cfgInitRef.current = false;
      setCfgForm((prev) => ({
        ...prev,
        auth_token: "",
        clear_auth_token: false,
        admin_password: "",
        clear_admin_password: false,
      }));
      await loadAll();
      notify("Configuration du relais appliquée en direct.");
    } catch (e: any) {
      notify(e.message, "err");
    }
  };

  const submitSetup = async (e: React.FormEvent) => {
    e.preventDefault();
    setSetupErr("");
    if (setupPass.length < 4) {
      setSetupErr("Le mot de passe admin doit contenir au moins 4 caractères.");
      return;
    }
    if (setupPass !== setupPassConfirm) {
      setSetupErr("Les mots de passe ne correspondent pas.");
      return;
    }
    if (setupStatus?.requires_setup_token && !setupToken.trim()) {
      setSetupErr("Le Setup Token affiché dans la console / logs Docker est requis à distance.");
      return;
    }
    setSetupBusy(true);
    try {
      await apiFetch("api/v1/setup", {
        method: "POST",
        body: JSON.stringify({
          setup_token: setupToken.trim(),
          admin_user: setupUser.trim() || "admin",
          admin_password: setupPass,
          auth_token: setupAuthToken.trim(),
          enable_blind_relay: setupBlindRelay,
          max_relay_kbps: setupMaxKBps,
        }),
      });
      window.location.reload();
    } catch (err: any) {
      setSetupErr(err?.message || "Erreur lors de la configuration.");
    } finally {
      setSetupBusy(false);
    }
  };

  if (setupStatus?.needs_setup) {
    return (
      <div className="min-h-screen bg-[#0b0d11] text-zinc-100 flex items-center justify-center p-4">
        <form
          onSubmit={submitSetup}
          className="w-full max-w-md bg-[#12161d] border border-zinc-800 rounded-xl p-6 shadow-2xl space-y-4"
        >
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-lg bg-emerald-600/15 border border-emerald-500/30 flex items-center justify-center text-emerald-400">
              <Shield size={20} />
            </div>
            <div>
              <h1 className="text-base font-semibold text-zinc-100">Configuration initiale — Relay Server</h1>
              <p className="text-xs text-zinc-400">
                Définis le compte administrateur avant d'exposer le panneau du relais.
              </p>
            </div>
          </div>

          {setupErr && (
            <div className="text-xs text-rose-300 bg-rose-950/40 border border-rose-800/50 rounded-md px-3 py-2">
              {setupErr}
            </div>
          )}

          <div className="space-y-3">
            {setupStatus.requires_setup_token && (
              <div>
                <label className="block text-xs font-medium text-amber-300 mb-1">
                  Setup Token (visible dans les logs du serveur relais)
                </label>
                <input
                  type="text"
                  required
                  placeholder="EX: A1B2C3D4..."
                  value={setupToken}
                  onChange={(e) => setSetupToken(e.target.value)}
                  className="w-full bg-[#0b0d11] border border-amber-700/60 rounded-md px-3 py-1.5 text-sm font-mono uppercase"
                />
              </div>
            )}

            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="block text-xs font-medium text-zinc-400 mb-1">Utilisateur Admin</label>
                <input
                  type="text"
                  required
                  value={setupUser}
                  onChange={(e) => setSetupUser(e.target.value)}
                  className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-sm"
                />
              </div>
              <div>
                <label className="block text-xs font-medium text-zinc-400 mb-1">Plafond relais (Ko/s, 0=illimité)</label>
                <input
                  type="number"
                  min={0}
                  value={setupMaxKBps}
                  onChange={(e) => setSetupMaxKBps(Number(e.target.value))}
                  className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-sm"
                />
              </div>
            </div>

            <div>
              <label className="block text-xs font-medium text-zinc-400 mb-1">Mot de passe Admin (min 4 car.)</label>
              <input
                type="password"
                required
                minLength={4}
                value={setupPass}
                onChange={(e) => setSetupPass(e.target.value)}
                className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-sm"
              />
            </div>
            <div>
              <label className="block text-xs font-medium text-zinc-400 mb-1">Confirmer le mot de passe</label>
              <input
                type="password"
                required
                minLength={4}
                value={setupPassConfirm}
                onChange={(e) => setSetupPassConfirm(e.target.value)}
                className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-sm"
              />
            </div>

            <div>
              <label className="block text-xs font-medium text-zinc-400 mb-1">AUTH_TOKEN réseau (optionnel)</label>
              <input
                type="password"
                placeholder="Laisser vide pour relais ouvert"
                value={setupAuthToken}
                onChange={(e) => setSetupAuthToken(e.target.value)}
                className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-sm"
              />
            </div>

            <label className="flex items-center gap-2 text-xs text-zinc-300 cursor-pointer pt-1">
              <input
                type="checkbox"
                checked={setupBlindRelay}
                onChange={(e) => setSetupBlindRelay(e.target.checked)}
              />
              Activer le Blind Relay (UDP natif + WebSocket si NAT symétrique)
            </label>
          </div>

          <button
            type="submit"
            disabled={setupBusy}
            className="w-full py-2.5 rounded-md bg-emerald-600 hover:bg-emerald-500 disabled:opacity-60 text-xs font-medium text-white transition"
          >
            {setupBusy ? "Activation..." : "Configurer et verrouiller le Relais"}
          </button>
        </form>
      </div>
    );
  }

  const maxBps = Math.max(1024, ...bpsHistory);

  return (
    <div className="min-h-screen bg-[#0b0d11] text-zinc-100 flex flex-col">
      {/* Header */}
      <header className="h-14 border-b border-zinc-800/90 bg-[#10131a] px-5 flex items-center justify-between gap-4 shrink-0">
        <div className="flex items-center gap-3">
          <div className="w-7 h-7 rounded-md bg-emerald-600 flex items-center justify-center text-xs font-bold text-white">
            RS
          </div>
          <div className="flex items-baseline gap-2.5">
            <span className="font-semibold text-sm text-zinc-100">Gens-Relay Server</span>
            <span className="text-xs text-zinc-500 hidden sm:inline">Administration Relais</span>
          </div>
        </div>

        <div className="flex items-center gap-2.5 text-xs">
          <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-[#161a23] border border-zinc-800 text-zinc-300">
            <span className={`w-1.5 h-1.5 rounded-full ${status?.blind_relay_enabled ? "bg-emerald-400" : "bg-amber-400"}`} />
            {status?.blind_relay_enabled ? "Blind Relay Actif" : "Signalisation Seule"}
          </span>
          <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-[#161a23] border border-zinc-800 text-zinc-300">
            <Users size={12} className="text-blue-400" />
            {status?.online_devices ?? 0} en ligne
          </span>
          <span className="hidden md:inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-[#161a23] border border-zinc-800 text-zinc-400 font-mono">
            Uptime: {formatDuration(status?.uptime_sec ?? 0)}
          </span>
        </div>
      </header>

      <div className="flex-1 flex flex-col md:flex-row min-h-0">
        {/* Sidebar */}
        <aside className="md:w-56 border-b md:border-b-0 md:border-r border-zinc-800/90 bg-[#0e1117] p-3 flex md:flex-col gap-1 shrink-0 overflow-x-auto">
          {[
            { id: "overview", label: "Vue d'ensemble", icon: Activity, badge: 0 },
            { id: "clients", label: "Appareils & Bans", icon: Server, badge: clients.length },
            { id: "pairing", label: "Appairage & Files", icon: KeyRound, badge: pairCodes.length },
            { id: "logs", label: "Journaux", icon: Terminal, badge: 0 },
            { id: "settings", label: "Paramètres", icon: Settings, badge: 0 },
          ].map((item) => {
            const Icon = item.icon;
            const active = tab === item.id;
            return (
              <button
                key={item.id}
                onClick={() => setTab(item.id as any)}
                className={`flex items-center justify-between gap-2 px-3 py-2 rounded-md text-xs font-medium transition whitespace-nowrap ${active
                  ? "bg-zinc-800/90 text-white border border-zinc-700/80"
                  : "text-zinc-400 hover:text-zinc-200 hover:bg-zinc-800/40 border border-transparent"
                  }`}
              >
                <span className="flex items-center gap-2.5">
                  <Icon size={15} className={active ? "text-emerald-400" : "text-zinc-500"} />
                  {item.label}
                </span>
                {item.badge > 0 && (
                  <span className="px-1.5 py-0.5 text-[10px] rounded bg-emerald-600/80 text-white font-semibold">
                    {item.badge}
                  </span>
                )}
              </button>
            );
          })}

          <div className="mt-auto pt-4 border-t border-zinc-800/80 hidden md:block text-[11px] text-zinc-500 space-y-1 px-2">
            <div className="flex justify-between">
              <span>WS / Signal</span>
              <span className="font-mono text-zinc-400">{status?.listen_addr || ":38420"}</span>
            </div>
            <div className="flex justify-between">
              <span>STUN UDP</span>
              <span className="font-mono text-zinc-400">:{status?.stun_port || "-"}</span>
            </div>
            <div className="flex justify-between">
              <span>Zero-Storage</span>
              <span className="text-emerald-400 font-medium">Actif</span>
            </div>
          </div>
        </aside>

        {/* Main */}
        <main className="flex-1 p-6 max-w-6xl w-full mx-auto space-y-6 overflow-y-auto">
          {tab === "overview" && (
            <div className="space-y-5">
              <div className="flex items-center justify-between">
                <div>
                  <h2 className="text-lg font-semibold text-zinc-100">Tableau de bord du Relais</h2>
                  <p className="text-xs text-zinc-400 mt-0.5">
                    Signalisation WebSocket, STUN UDP et relais chiffré E2EE sans stockage disque.
                  </p>
                </div>
                <button
                  onClick={loadAll}
                  className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-md bg-zinc-800 hover:bg-zinc-700 text-xs font-medium"
                >
                  <RefreshCw size={13} /> Actualiser
                </button>
              </div>

              <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
                <div className="bg-[#12161d] border border-zinc-800/90 rounded-lg p-4">
                  <div className="text-[11px] uppercase tracking-wider text-zinc-500">Appareils en ligne</div>
                  <div className="text-2xl font-semibold text-zinc-100 mt-1">{status?.online_devices ?? 0}</div>
                  <div className="text-[11px] text-zinc-500 mt-1">Max: {status?.max_clients ?? 2000}</div>
                </div>
                <div className="bg-[#12161d] border border-zinc-800/90 rounded-lg p-4">
                  <div className="text-[11px] uppercase tracking-wider text-zinc-500">Débit Blind Relay</div>
                  <div className="text-2xl font-semibold text-emerald-400 mt-1">
                    {formatBytes(status?.relay_bps ?? 0)}/s
                  </div>
                  <div className="text-[11px] text-zinc-500 mt-1">
                    UDP: {formatBytes(status?.udp_relay_bytes_total ?? 0)} · WS: {formatBytes(status?.ws_relay_bytes_total ?? 0)}
                  </div>
                </div>
                <div className="bg-[#12161d] border border-zinc-800/90 rounded-lg p-4">
                  <div className="text-[11px] uppercase tracking-wider text-zinc-500">Paquets relayés</div>
                  <div className="text-2xl font-semibold text-zinc-100 mt-1">
                    {(status?.relay_packets_total ?? 0).toLocaleString()}
                  </div>
                  <div className="text-[11px] text-zinc-500 mt-1">
                    Limite: {status?.max_relay_kbps ? `${status.max_relay_kbps} Ko/s` : "Illimitée"}
                  </div>
                </div>
                <div className="bg-[#12161d] border border-zinc-800/90 rounded-lg p-4">
                  <div className="text-[11px] uppercase tracking-wider text-zinc-500">Codes / Files</div>
                  <div className="text-2xl font-semibold text-zinc-100 mt-1">
                    {status?.active_pair_codes ?? 0} <span className="text-sm text-zinc-500">/ {status?.pending_queues ?? 0}</span>
                  </div>
                  <div className="text-[11px] text-zinc-500 mt-1">
                    STUN &amp; Relais UDP :{status?.udp_relay_port || status?.stun_port || "off"}
                  </div>
                </div>
              </div>

              {/* Live Throughput Sparkline */}
              <div className="bg-[#12161d] border border-zinc-800/90 rounded-lg p-5 space-y-3">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-wider text-zinc-300">
                    <Zap size={14} className="text-emerald-400" /> Débit Blind Relay temps réel (30 derniers échantillons)
                  </div>
                  <span className="text-xs font-mono text-zinc-400">Pic: {formatBytes(maxBps)}/s</span>
                </div>
                <div className="h-32 flex items-end gap-1 pt-2 bg-[#0b0d11] rounded-md p-3 border border-zinc-800/80">
                  {bpsHistory.map((val, idx) => {
                    const pct = Math.max(4, Math.round((val / maxBps) * 100));
                    return (
                      <div
                        key={idx}
                        title={`${formatBytes(val)}/s`}
                        className="flex-1 bg-emerald-500/70 hover:bg-emerald-400 rounded-t transition-all"
                        style={{ height: `${pct}%` }}
                      />
                    );
                  })}
                </div>
              </div>

              {/* Security Summary */}
              <div className="grid md:grid-cols-3 gap-3">
                <div className="bg-[#12161d] border border-zinc-800/90 rounded-lg p-4 flex items-center gap-3">
                  {status?.has_auth_token ? (
                    <CheckCircle2 size={18} className="text-emerald-400 shrink-0" />
                  ) : (
                    <ShieldAlert size={18} className="text-amber-400 shrink-0" />
                  )}
                  <div>
                    <div className="text-xs font-semibold text-zinc-200">Token réseau (AUTH_TOKEN)</div>
                    <div className="text-[11px] text-zinc-400">
                      {status?.has_auth_token ? "Protégé par token partagé" : "Ouvert (tout appareil Ed25519)"}
                    </div>
                  </div>
                </div>
                <div className="bg-[#12161d] border border-zinc-800/90 rounded-lg p-4 flex items-center gap-3">
                  <Shield size={18} className="text-blue-400 shrink-0" />
                  <div>
                    <div className="text-xs font-semibold text-zinc-200">Chiffrement E2EE</div>
                    <div className="text-[11px] text-zinc-400">QUIC TLS 1.3 mTLS opaque (stores_files=false)</div>
                  </div>
                </div>
                <div className="bg-[#12161d] border border-zinc-800/90 rounded-lg p-4 flex items-center gap-3">
                  <Wifi size={18} className="text-emerald-400 shrink-0" />
                  <div>
                    <div className="text-xs font-semibold text-zinc-200">Reverse Proxy (TrustProxy)</div>
                    <div className="text-[11px] text-zinc-400">
                      {status?.trust_proxy ? "X-Forwarded-For activé" : "IP directe uniquement"}
                    </div>
                  </div>
                </div>
              </div>
            </div>
          )}

          {tab === "clients" && (
            <div className="space-y-5">
              <div>
                <h2 className="text-lg font-semibold text-zinc-100">Appareils connectés & Contrôle d'accès</h2>
                <p className="text-xs text-zinc-400 mt-0.5">
                  Visualise les pairs connectés au relais, déconnecte ou bannis un DeviceID.
                </p>
              </div>

              <div className="bg-[#12161d] border border-zinc-800/90 rounded-lg p-4 space-y-3">
                <h3 className="text-xs font-semibold uppercase tracking-wider text-zinc-300">
                  Sessions actives ({clients.length})
                </h3>
                {clients.length === 0 ? (
                  <p className="text-xs text-zinc-500 py-4">Aucun appareil actuellement connecté.</p>
                ) : (
                  <div className="divide-y divide-zinc-800/80 border border-zinc-800 rounded-md bg-[#0b0d11]">
                    {clients.map((c) => (
                      <div key={c.device_id} className="p-3.5 flex flex-wrap items-center justify-between gap-3">
                        <div className="min-w-0 space-y-0.5">
                          <div className="flex items-center gap-2">
                            <span className="w-2 h-2 rounded-full bg-emerald-400" />
                            <span className="font-semibold text-sm text-zinc-100">{c.name || "Sans nom"}</span>
                            <span className="text-[11px] font-mono px-2 py-0.5 rounded bg-zinc-900 border border-zinc-800 text-zinc-300">
                              {c.observed_addr || "IP inconnue"}
                            </span>
                            <span className="text-[11px] text-zinc-500">
                              QUIC :{c.quic_port} · Watch {c.watching_count} · {c.udp_relay ? "UDP Relay" : "WS"}
                            </span>
                          </div>
                          <div className="text-[11px] font-mono text-zinc-400 break-all">{c.device_id}</div>
                          <div className="text-[11px] text-zinc-500">
                            RX Relais: {formatBytes(c.relay_rx_bytes)} · TX Relais: {formatBytes(c.relay_tx_bytes)}
                            {c.lan_addrs?.length > 0 && ` · LAN: ${c.lan_addrs.join(", ")}`}
                          </div>
                        </div>

                        <div className="flex items-center gap-2">
                          <button
                            onClick={() => kickClient(c.device_id)}
                            className="inline-flex items-center gap-1 px-2.5 py-1.5 rounded bg-zinc-800 hover:bg-zinc-700 text-xs text-zinc-200"
                            title="Déconnecter cette session"
                          >
                            <LogOut size={13} /> Kick
                          </button>
                          <button
                            onClick={() => banDevice(c.device_id)}
                            className="inline-flex items-center gap-1 px-2.5 py-1.5 rounded bg-rose-950/60 hover:bg-rose-900/70 border border-rose-800/40 text-xs text-rose-300"
                            title="Bannir ce DeviceID"
                          >
                            <Ban size={13} /> Bannir
                          </button>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>

              <div className="bg-[#12161d] border border-zinc-800/90 rounded-lg p-4 space-y-3">
                <h3 className="text-xs font-semibold uppercase tracking-wider text-zinc-300">
                  Liste noire (DeviceID bannis : {bans.length})
                </h3>
                <form
                  onSubmit={(e) => {
                    e.preventDefault();
                    if (banDeviceInput.trim()) banDevice(banDeviceInput.trim(), banReasonInput.trim() || "Banni manuellement");
                  }}
                  className="flex flex-wrap gap-2"
                >
                  <input
                    type="text"
                    placeholder="DeviceID à bannir..."
                    value={banDeviceInput}
                    onChange={(e) => setBanDeviceInput(e.target.value)}
                    className="flex-1 min-w-[220px] bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-xs font-mono"
                  />
                  <input
                    type="text"
                    placeholder="Raison (optionnel)"
                    value={banReasonInput}
                    onChange={(e) => setBanReasonInput(e.target.value)}
                    className="w-56 bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-xs"
                  />
                  <button
                    type="submit"
                    className="px-3.5 py-1.5 rounded-md bg-rose-600 hover:bg-rose-500 text-xs font-medium text-white"
                  >
                    Bannir
                  </button>
                </form>

                {bans.length > 0 && (
                  <div className="divide-y divide-zinc-800/80 border border-zinc-800 rounded-md bg-[#0b0d11]">
                    {bans.map((b) => (
                      <div key={b.device_id} className="p-3 flex items-center justify-between gap-3 text-xs">
                        <div>
                          <div className="font-mono text-zinc-200">{b.device_id}</div>
                          {b.reason && <div className="text-zinc-500 text-[11px]">{b.reason}</div>}
                        </div>
                        <button
                          onClick={() => unbanDevice(b.device_id)}
                          className="px-2.5 py-1 rounded bg-zinc-800 hover:bg-zinc-700 text-zinc-200"
                        >
                          Débannir
                        </button>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>
          )}

          {tab === "pairing" && (
            <div className="space-y-5">
              <div>
                <h2 className="text-lg font-semibold text-zinc-100">Codes d'appairage & Files d'attente</h2>
                <p className="text-xs text-zinc-400 mt-0.5">
                  Codes courts actifs (5 min) et demandes d'appairage en attente pour les appareils hors ligne.
                </p>
              </div>

              <div className="grid md:grid-cols-2 gap-5">
                <div className="bg-[#12161d] border border-zinc-800/90 rounded-lg p-4 space-y-3">
                  <h3 className="text-xs font-semibold uppercase tracking-wider text-zinc-300">
                    Codes d'appairage actifs ({pairCodes.length})
                  </h3>
                  {pairCodes.length === 0 ? (
                    <p className="text-xs text-zinc-500">Aucun code actif.</p>
                  ) : (
                    <div className="divide-y divide-zinc-800 border border-zinc-800 rounded-md bg-[#0b0d11]">
                      {pairCodes.map((pc) => (
                        <div key={pc.code} className="p-3 flex items-center justify-between gap-2 text-xs">
                          <div>
                            <div className="font-mono text-sm font-bold text-emerald-400">{pc.code}</div>
                            <div className="text-zinc-300">{pc.name}</div>
                            <div className="font-mono text-[11px] text-zinc-500">{pc.device_id}</div>
                          </div>
                          <button
                            onClick={() => revokePairCode(pc.code)}
                            className="p-1.5 rounded bg-zinc-800 hover:bg-rose-900/50 text-rose-400"
                            title="Révoquer ce code"
                          >
                            <Trash2 size={13} />
                          </button>
                        </div>
                      ))}
                    </div>
                  )}
                </div>

                <div className="bg-[#12161d] border border-zinc-800/90 rounded-lg p-4 space-y-3">
                  <h3 className="text-xs font-semibold uppercase tracking-wider text-zinc-300">
                    Messages en attente hors-ligne ({pendingQueues.length})
                  </h3>
                  {pendingQueues.length === 0 ? (
                    <p className="text-xs text-zinc-500">Aucune file d'attente active.</p>
                  ) : (
                    <div className="divide-y divide-zinc-800 border border-zinc-800 rounded-md bg-[#0b0d11]">
                      {pendingQueues.map((pq) => (
                        <div key={pq.target_id} className="p-3 flex items-center justify-between text-xs">
                          <span className="font-mono text-zinc-300 truncate">{pq.target_id}</span>
                          <span className="px-2 py-0.5 rounded bg-zinc-800 text-zinc-300 shrink-0">
                            {pq.count} msg(s)
                          </span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            </div>
          )}

          {tab === "logs" && (
            <div className="space-y-4">
              <div className="flex flex-wrap items-center justify-between gap-3">
                <div>
                  <h2 className="text-lg font-semibold text-zinc-100">Journaux du Relais</h2>
                  <p className="text-xs text-zinc-400 mt-0.5">Événements en direct du serveur de coordination.</p>
                </div>
                <input
                  type="text"
                  placeholder="Filtrer..."
                  value={logFilter}
                  onChange={(e) => setLogFilter(e.target.value)}
                  className="bg-[#12161d] border border-zinc-700 rounded-md px-3 py-1.5 text-xs w-64 focus:outline-none focus:border-emerald-500"
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
                          l.message.toLowerCase().includes("error") || l.message.toLowerCase().includes("banned")
                            ? "text-rose-400"
                            : "text-zinc-200"
                        }
                      >
                        {l.message}
                      </span>
                    </div>
                  ))}
                {logs.length === 0 && <div className="text-zinc-500 text-center py-8">Aucun journal.</div>}
              </div>
            </div>
          )}

          {tab === "settings" && (
            <form onSubmit={saveConfig} className="space-y-5 max-w-3xl">
              <div>
                <h2 className="text-lg font-semibold text-zinc-100">Paramètres du Serveur Relais</h2>
                <p className="text-xs text-zinc-400 mt-0.5">
                  Les modifications ci-dessous sont persistées dans <span className="font-mono">relay-config.json</span> et appliquées sans redémarrage.
                </p>
              </div>

              <div className="bg-[#12161d] border border-zinc-800/90 rounded-lg p-5 grid sm:grid-cols-2 gap-4">
                <label className="sm:col-span-2 flex items-center justify-between p-3 rounded-md bg-[#0b0d11] border border-zinc-800 cursor-pointer">
                  <div>
                    <div className="text-xs font-semibold text-zinc-200 flex items-center gap-2">
                      <Radio size={14} className="text-emerald-400" /> Activer le Blind Relay E2EE
                    </div>
                    <div className="text-[11px] text-zinc-400 mt-0.5">
                      Relaie les paquets QUIC chiffrés lorsque le P2P direct / NAT traversal échoue.
                    </div>
                  </div>
                  <input
                    type="checkbox"
                    checked={cfgForm.enable_blind_relay}
                    onChange={(e) => setCfgForm({ ...cfgForm, enable_blind_relay: e.target.checked })}
                  />
                </label>

                <label className="sm:col-span-2 flex items-center justify-between p-3 rounded-md bg-[#0b0d11] border border-zinc-800 cursor-pointer">
                  <div>
                    <div className="text-xs font-semibold text-zinc-200">Faire confiance au Reverse Proxy (TrustProxy)</div>
                    <div className="text-[11px] text-zinc-400 mt-0.5">
                      Lit les en-têtes X-Forwarded-For / X-Real-IP (Nginx, Caddy, Traefik).
                    </div>
                  </div>
                  <input
                    type="checkbox"
                    checked={cfgForm.trust_proxy}
                    onChange={(e) => setCfgForm({ ...cfgForm, trust_proxy: e.target.checked })}
                  />
                </label>

                <div>
                  <label className="block text-xs font-medium text-zinc-400 mb-1">
                    Nouveau Token réseau (AUTH_TOKEN)
                  </label>
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

                <div>
                  <label className="block text-xs font-medium text-zinc-400 mb-1">
                    Débit max Blind Relay (Ko/s, 0 = illimité)
                  </label>
                  <input
                    type="number"
                    min={0}
                    value={cfgForm.max_relay_kbps}
                    onChange={(e) => setCfgForm({ ...cfgForm, max_relay_kbps: Number(e.target.value) })}
                    className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-sm"
                  />
                </div>

                <div>
                  <label className="block text-xs font-medium text-zinc-400 mb-1">Clients max simultanés</label>
                  <input
                    type="number"
                    min={10}
                    max={50000}
                    value={cfgForm.max_clients}
                    onChange={(e) => setCfgForm({ ...cfgForm, max_clients: Number(e.target.value) })}
                    className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-sm"
                  />
                </div>

                <div className="sm:col-span-2 pt-3 border-t border-zinc-800 text-xs font-semibold text-zinc-300">
                  Sécurité Interface Admin Web (obligatoire hors localhost)
                </div>
                <div>
                  <label className="block text-xs font-medium text-zinc-400 mb-1">Utilisateur Admin</label>
                  <input
                    type="text"
                    value={cfgForm.admin_user}
                    onChange={(e) => setCfgForm({ ...cfgForm, admin_user: e.target.value })}
                    className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-sm"
                  />
                </div>
                <div>
                  <label className="block text-xs font-medium text-zinc-400 mb-1">Nouveau mot de passe Admin</label>
                  <input
                    type="password"
                    placeholder={status?.has_admin_password ? "Configuré (laisser vide pour garder)" : "Aucun mot de passe"}
                    value={cfgForm.admin_password}
                    onChange={(e) =>
                      setCfgForm({ ...cfgForm, admin_password: e.target.value, clear_admin_password: false })
                    }
                    className="w-full bg-[#0b0d11] border border-zinc-700 rounded-md px-3 py-1.5 text-sm"
                  />
                  {status?.has_admin_password && (
                    <label className="inline-flex items-center gap-1.5 text-[11px] text-rose-400 mt-1.5 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={cfgForm.clear_admin_password}
                        onChange={(e) =>
                          setCfgForm({ ...cfgForm, clear_admin_password: e.target.checked, admin_password: "" })
                        }
                      />
                      Supprimer le mot de passe Admin
                    </label>
                  )}
                </div>
              </div>

              <button
                type="submit"
                className="px-5 py-2 rounded-md bg-emerald-600 hover:bg-emerald-500 text-xs font-medium text-white transition"
              >
                Enregistrer et appliquer
              </button>
            </form>
          )}
        </main>
      </div>

      {toast && (
        <div
          className={`fixed bottom-5 right-5 px-4 py-2.5 rounded-md shadow-xl text-xs font-medium z-50 border ${toast.kind === "err"
            ? "bg-rose-950/95 border-rose-700 text-rose-100"
            : "bg-[#161a23] border-zinc-700 text-zinc-100"
            }`}
        >
          {toast.text}
        </div>
      )}
    </div>
  );
}
