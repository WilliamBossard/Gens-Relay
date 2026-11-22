import { Bootstrap, StagedBrowserFile } from "./types";

export async function apiFetch(url: string, opts?: RequestInit) {
  const headers: Record<string, string> = {};
  if (opts?.body && !(opts.body instanceof FormData)) {
    headers["Content-Type"] = "application/json";
  }
  const res = await fetch(url, {
    ...opts,
    headers: { ...headers, ...(opts?.headers as Record<string, string>) },
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(data.error || `HTTP ${res.status}`);
  }
  return data;
}

// fetchBootstrap appelle l'endpoint agrégé GET /api/v1/bootstrap : une seule
// requête réseau remplace les 11 requêtes parallèles de lecture (status,
// devices, peers, nearby, pair-requests, folders, folder-status,
// invitations, drops, logs, companion-apps). Les endpoints individuels
// restent disponibles pour les actions POST/PUT et les refresh ciblés.
export async function fetchBootstrap(): Promise<Bootstrap> {
  return apiFetch("/api/v1/bootstrap");
}

export async function collectDroppedItems(items: DataTransferItemList): Promise<StagedBrowserFile[]> {
  const out: StagedBrowserFile[] = [];
  const walkEntry = async (entry: any, prefix: string) => {
    if (!entry) return;
    if (entry.isFile) {
      await new Promise<void>((resolve) => {
        entry.file(
          (f: File) => {
            out.push({ file: f, relPath: prefix ? `${prefix}/${f.name}` : f.name });
            resolve();
          },
          () => resolve()
        );
      });
    } else if (entry.isDirectory) {
      const reader = entry.createReader();
      const nextPrefix = prefix ? `${prefix}/${entry.name}` : entry.name;
      await new Promise<void>((resolve) => {
        const readBatch = () => {
          reader.readEntries(
            async (entries: any[]) => {
              if (!entries || entries.length === 0) {
                resolve();
                return;
              }
              for (const child of entries) {
                await walkEntry(child, nextPrefix);
              }
              readBatch();
            },
            () => resolve()
          );
        };
        readBatch();
      });
    }
  };

  for (let i = 0; i < items.length; i++) {
    const entry = (items[i] as any).webkitGetAsEntry?.();
    if (entry) {
      await walkEntry(entry, "");
    } else {
      const f = items[i].getAsFile();
      if (f) out.push({ file: f, relPath: f.name });
    }
  }
  return out;
}
