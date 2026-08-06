import { useState } from "react";
import { PeerInfo } from "../App";
import { Star, Link as LinkIcon, Users, Computer, Search, Plus } from "lucide-react";

export function Sidebar({ peers, favorites, toggleFavorite, selectedTarget, setSelectedTarget, sendCommand }: any) {
  const [manualIp, setManualIp] = useState("");

  const handleManualConnect = (e: any) => {
    e.preventDefault();
    if (manualIp) {
      sendCommand("add_peer", null, manualIp);
      setManualIp("");
    }
  };

  const sortedPeers = [...peers].sort((a, b) => {
    const favA = favorites.includes(a.id);
    const favB = favorites.includes(b.id);
    return (favB ? 1 : 0) - (favA ? 1 : 0);
  });

  return (
    <div className="w-72 bg-slate-900/95 border-r border-slate-800 flex flex-col shadow-lg z-10">
      <div className="p-5 border-b border-slate-800/80 bg-slate-900/50">
        <h2 className="text-xs font-bold text-slate-500 uppercase tracking-widest mb-3 flex items-center gap-2">
          <LinkIcon size={14} /> Manual Connect
        </h2>
        <form onSubmit={handleManualConnect} className="relative group">
          <input 
            type="text" 
            placeholder="IP (e.g. 192.168.1.5)" 
            className="w-full bg-slate-800/50 text-sm rounded-md pl-3 pr-10 py-2.5 border border-slate-700/50 text-slate-200 placeholder:text-slate-600 focus:outline-none focus:border-blue-500/50 focus:ring-1 focus:ring-blue-500/50 transition-all"
            value={manualIp}
            onChange={e => setManualIp(e.target.value)}
          />
          <button type="submit" className="absolute right-1.5 top-1.5 p-1 bg-blue-600/10 text-blue-400 hover:bg-blue-500 hover:text-white rounded transition-colors">
            <Plus size={16} />
          </button>
        </form>
      </div>
      
      <div className="p-3 flex-1 overflow-y-auto">
        <div className="px-2 pb-2 mt-2">
          <h2 className="text-xs font-bold text-slate-500 uppercase tracking-widest mb-2 flex items-center gap-2">
            <Users size={14} /> Network Peers
          </h2>
        </div>
        <div className="space-y-0.5">
          {sortedPeers.map((peer: PeerInfo) => {
            const isFav = favorites.includes(peer.id);
            const isSelected = selectedTarget === peer.id;
            return (
              <div 
                key={peer.id}
                onClick={() => setSelectedTarget(peer.id)}
                className={`group flex items-center gap-3 px-3 py-2.5 rounded-lg cursor-pointer transition-all duration-200 ${
                  isSelected 
                    ? 'bg-blue-500/10 border border-blue-500/20 shadow-[inset_0_0_12px_rgba(59,130,246,0.1)]' 
                    : 'border border-transparent hover:bg-slate-800/60 hover:border-slate-700/50'
                }`}
              >
                <button 
                  onClick={(e) => { e.stopPropagation(); toggleFavorite(peer.id); }}
                  className={`p-1 rounded transition-colors ${isFav ? 'text-amber-400 hover:bg-amber-400/10' : 'text-slate-600 hover:bg-slate-700 hover:text-slate-300'}`}
                >
                  <Star size={14} fill={isFav ? "currentColor" : "none"} strokeWidth={isFav ? 1 : 2} />
                </button>
                <div className="flex flex-col min-w-0 flex-1">
                  <span className={`text-sm truncate font-medium ${isSelected ? 'text-blue-100' : 'text-slate-300 group-hover:text-slate-200'}`}>
                    {peer.hostname}
                  </span>
                  <span className={`text-[11px] truncate mt-0.5 ${isSelected ? 'text-blue-400/80' : 'text-slate-500'}`}>
                    {peer.id}
                  </span>
                </div>
                {isSelected && (
                  <div className="w-1.5 h-1.5 rounded-full bg-blue-500 shadow-[0_0_8px_rgba(59,130,246,0.8)] animate-pulse"></div>
                )}
              </div>
            );
          })}
          {peers.length === 0 && (
            <div className="text-sm text-slate-500 text-center py-4">No peers found.</div>
          )}
        </div>
      </div>
    </div>
  );
}
