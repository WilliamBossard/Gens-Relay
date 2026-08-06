import { useEffect, useState } from "react";
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import { Sidebar } from "./components/Sidebar";
import { FileExplorer } from "./components/FileExplorer";
import { Wifi, WifiOff } from "lucide-react";

export type PeerInfo = {
  hostname: string;
  id: string;
  ip: string;
};

export type FileEntry = {
  name: string;
  is_dir: boolean;
  size: number;
  time: string;
};

export default function App() {
  const [peers, setPeers] = useState<PeerInfo[]>([]);
  const [selectedTarget, setSelectedTarget] = useState<string | null>(null);
  const [favorites, setFavorites] = useState<string[]>([]);

  
  const [localPath, setLocalPath] = useState<string>(".");
  const [localFiles, setLocalFiles] = useState<FileEntry[]>([]);
  
  const [remotePath, setRemotePath] = useState<string>(".");
  const [remoteFiles, setRemoteFiles] = useState<FileEntry[]>([]);

  useEffect(() => {
    const storedFavs = localStorage.getItem("gens-favorites");
    if (storedFavs) setFavorites(JSON.parse(storedFavs));
  }, []);

  const toggleFavorite = (id: string) => {
    setFavorites(prev => {
      const newFavs = prev.includes(id) ? prev.filter(x => x !== id) : [...prev, id];
      localStorage.setItem("gens-favorites", JSON.stringify(newFavs));
      return newFavs;
    });
  };

  const sendCommand = async (action: string, target?: string | null, msg?: string | null, path?: string | null) => {
    try {
      await invoke("send_ipc_command", { action, target, msg, path });
    } catch (e) {
      console.error(e);
    }
  };

  const loadLocalDir = async (path: string) => {
    try {
      const res: any = await invoke("list_local_dir", { path });
      setLocalPath(res.path);
      setLocalFiles(res.entries);
    } catch (e) {
      console.error(e);
    }
  };

  useEffect(() => {
    sendCommand("list");
    loadLocalDir(".");
    sendCommand("set_download_dir", null, null, localPath);
    
    const unlisten = listen<any>("ipc-event", (event) => {
      const payload = event.payload;
      
      if (payload.event === "peers_updated") {
        setPeers(payload.peers || []);
      } else if (payload.event === "ls_result") {
        setRemotePath(payload.path || ".");
        setRemoteFiles(payload.entries || []);
      } else if (payload.event === "chat") {
        // setChatMessages(prev => [...prev, `[Remote] ${payload.msg}`]);
      } else if (payload.event === "sys") {
        // setChatMessages(prev => [...prev, `[System] ${payload.msg}`]);
      }
    });

    return () => {
      unlisten.then(f => f());
    };
  }, []);

  useEffect(() => {
    sendCommand("set_download_dir", null, null, localPath);
  }, [localPath]);

  return (
    <div className="flex h-screen w-screen bg-slate-900 text-slate-50 font-sans overflow-hidden">
      <Sidebar 
        peers={peers} 
        favorites={favorites} 
        toggleFavorite={toggleFavorite}
        selectedTarget={selectedTarget}
        setSelectedTarget={setSelectedTarget}
        sendCommand={sendCommand}
      />
      <div className="flex-1 flex flex-col min-w-0">
        <header className="h-14 bg-slate-800 border-b border-slate-700 flex items-center px-6">
          <h1 className="text-xl font-bold bg-gradient-to-r from-blue-400 to-cyan-300 bg-clip-text text-transparent">
            Gens-Relay 2.0
          </h1>
          <div className="ml-auto flex items-center gap-4 text-sm text-slate-400">
            <div className={`flex items-center gap-2 px-3 py-1.5 rounded-full text-xs font-medium border transition-colors duration-300 ${
              selectedTarget 
                ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20 shadow-[0_0_15px_rgba(16,185,129,0.1)]' 
                : 'bg-slate-800 text-slate-400 border-slate-700'
            }`}>
              {selectedTarget ? <Wifi size={14} /> : <WifiOff size={14} />}
              {selectedTarget ? `Connected to ${selectedTarget}` : 'Disconnected'}
            </div>
          </div>
        </header>
        
        <main className="flex-1 flex overflow-hidden p-4 gap-4">
          <FileExplorer 
            title="Local PC" 
            path={localPath} 
            files={localFiles} 
            isRemote={false}
            onNavigate={(newPath: string) => loadLocalDir(newPath)}
            selectedTarget={selectedTarget}
            sendCommand={sendCommand}
            localPath={localPath}
            remotePath={remotePath}
          />
          <FileExplorer 
            title={selectedTarget ? `Remote PC (${selectedTarget})` : "Remote PC (Not Connected)"} 
            path={remotePath} 
            files={remoteFiles} 
            isRemote={true}
            onNavigate={(newPath: string) => sendCommand("ls", selectedTarget, null, newPath)}
            selectedTarget={selectedTarget}
            sendCommand={sendCommand}
            localPath={localPath}
            remotePath={remotePath}
          />
        </main>
      </div>
    </div>
  );
}
