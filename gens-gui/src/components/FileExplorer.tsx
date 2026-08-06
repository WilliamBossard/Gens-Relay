import { FileEntry } from "../App";
import { Folder, File, CornerLeftUp, FolderOpen, HardDrive } from "lucide-react";

export function FileExplorer({ title, path, files, isRemote, onNavigate, selectedTarget, sendCommand }: any) {
  
  const handleDoubleClick = (file: FileEntry) => {
    if (file.is_dir) {
      const separator = path.includes("\\") ? "\\" : "/";
      const newPath = path.endsWith(separator) ? `${path}${file.name}` : `${path}${separator}${file.name}`;
      onNavigate(newPath);
    } else {
      // Transfer file
      if (isRemote) {
        if (!selectedTarget) return;
        const separator = path.includes("\\") ? "\\" : "/";
        const fullPath = path.endsWith(separator) ? `${path}${file.name}` : `${path}${separator}${file.name}`;
        sendCommand("download_req", selectedTarget, null, fullPath);
      } else {
        if (!selectedTarget) return;
        const separator = path.includes("\\") ? "\\" : "/";
        const fullPath = path.endsWith(separator) ? `${path}${file.name}` : `${path}${separator}${file.name}`;
        sendCommand("sendfile", selectedTarget, null, fullPath);
      }
    }
  };

  const handleUp = () => {
    const separator = path.includes("\\") ? "\\" : "/";
    const parts = path.split(separator).filter(Boolean);
    if (parts.length > 1 || (parts.length === 1 && !path.startsWith(separator) && !path.endsWith(":\\"))) {
      if (path.includes(":\\") && parts.length === 1) return; // Don't go above C:\
      const newPath = path.substring(0, path.lastIndexOf(separator)) || separator;
      onNavigate(newPath);
    } else if (parts.length === 1 && path.includes(":\\")) {
       onNavigate(parts[0] + "\\");
    }
  };

  const handleDragStart = (e: React.DragEvent, file: FileEntry) => {
    if (file.is_dir) {
      e.preventDefault();
      return;
    }
    const separator = path.includes("\\") ? "\\" : "/";
    const fullPath = path.endsWith(separator) ? `${path}${file.name}` : `${path}${separator}${file.name}`;
    e.dataTransfer.setData("application/json", JSON.stringify({ isRemote, fullPath, name: file.name }));
    e.dataTransfer.effectAllowed = "copy";
  };

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = "copy";
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    if (!selectedTarget) return;
    
    try {
      const data = JSON.parse(e.dataTransfer.getData("application/json"));
      if (data.isRemote === isRemote) return; // Same side drop
      
      if (data.isRemote) {
        // Dragged from Remote to Local (Download)
        sendCommand("download_req", selectedTarget, null, data.fullPath);
      } else {
        // Dragged from Local to Remote (Upload)
        sendCommand("sendfile", selectedTarget, null, data.fullPath);
      }
    } catch (err) {}
  };

  return (
    <div 
      className="flex-1 flex flex-col bg-slate-800 rounded-lg border border-slate-700 shadow-xl overflow-hidden"
      onDragOver={handleDragOver}
      onDrop={handleDrop}
    >
      <div className="h-11 bg-slate-900 border-b border-slate-700/50 flex items-center px-4 justify-between select-none">
        <h3 className="text-sm font-semibold text-slate-200 flex items-center gap-2">
          <HardDrive size={16} className="text-blue-400" />
          {title}
        </h3>
        {isRemote && !selectedTarget && <span className="text-xs font-medium px-2 py-1 bg-red-500/10 text-red-400 border border-red-500/20 rounded">No Target Selected</span>}
      </div>
      
      <div className="p-2 border-b border-slate-700/50 bg-slate-800/80 flex items-center gap-2">
        <button 
          onClick={handleUp}
          className="p-1.5 hover:bg-slate-700 active:bg-slate-600 rounded text-slate-400 hover:text-slate-100 transition-colors"
          title="Go Up"
        >
          <CornerLeftUp size={16} />
        </button>
        <div className="flex-1 flex items-center bg-slate-900/50 rounded border border-slate-700/50 px-3 py-1.5 focus-within:border-blue-500/50 focus-within:ring-1 focus-within:ring-blue-500/50 transition-all">
          <input 
            type="text" 
            value={path} 
            readOnly
            className="flex-1 bg-transparent text-sm text-slate-300 focus:outline-none w-full"
          />
        </div>
      </div>

      <div className="flex-1 overflow-auto bg-slate-900 p-2">
        <table className="w-full text-left text-sm whitespace-nowrap">
          <thead className="text-xs uppercase text-slate-500 bg-slate-800 sticky top-0">
            <tr>
              <th className="px-4 py-2 font-medium rounded-tl">Name</th>
              <th className="px-4 py-2 font-medium">Size</th>
              <th className="px-4 py-2 font-medium rounded-tr">Modified</th>
            </tr>
          </thead>
          <tbody>
            {files.map((f: FileEntry, i: number) => (
              <tr 
                key={i} 
                onDoubleClick={() => handleDoubleClick(f)}
                draggable={!f.is_dir}
                onDragStart={(e) => handleDragStart(e, f)}
                className="hover:bg-slate-800 border-b border-slate-800/50 cursor-pointer transition-colors group"
              >
                <td className="px-4 py-2 flex items-center gap-3">
                  {f.is_dir ? <Folder size={16} className="text-blue-400" /> : <File size={16} className="text-slate-400" />}
                  <span className="text-slate-200 group-hover:text-white">{f.name}</span>
                </td>
                <td className="px-4 py-2 text-slate-500">
                  {f.is_dir ? "-" : (f.size / 1024 / 1024).toFixed(2) + " MB"}
                </td>
                <td className="px-4 py-2 text-slate-500 text-xs">{f.time}</td>
              </tr>
            ))}
            {files.length === 0 && (
              <tr>
                <td colSpan={3}>
                  <div className="flex flex-col items-center justify-center py-16 text-slate-500">
                    <FolderOpen size={48} className="mb-4 opacity-40 text-blue-400" strokeWidth={1} />
                    <p className="text-sm font-medium">Empty directory</p>
                  </div>
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
