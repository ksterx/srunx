import type {
  BrowseResult,
  CommandJob,
  HistoryStats,
  LogData,
  Mount,
  ResourceSnapshot,
  SyncResult,
  Workflow,
  WorkflowCreateRequest,
} from "./types.ts";

/* ── Jobs ─────────────────────────────────────── */

export const jobs = {
  list: async (): Promise<CommandJob[]> => {
    const res = await fetch("/api/jobs");
    if (!res.ok) throw new Error("Failed to fetch jobs");
    return res.json();
  },

  get: async (jobId: number): Promise<CommandJob> => {
    const res = await fetch(`/api/jobs/${jobId}`);
    if (!res.ok) throw new Error(`Failed to fetch job ${jobId}`);
    return res.json();
  },

  cancel: async (jobId: number): Promise<void> => {
    const res = await fetch(`/api/jobs/${jobId}/cancel`, { method: "POST" });
    if (!res.ok) {
      const err = await res.json();
      throw new Error(err.detail || "Failed to cancel job");
    }
  },

  logs: async (jobId: number): Promise<LogData> => {
    const res = await fetch(`/api/jobs/${jobId}/logs`);
    if (!res.ok) throw new Error(`Failed to fetch logs for job ${jobId}`);
    return res.json();
  },
};

/* ── Workflows ────────────────────────────────── */

export const workflows = {
  list: async (): Promise<Workflow[]> => {
    const res = await fetch("/api/workflows");
    if (!res.ok) throw new Error("Failed to fetch workflows");
    return res.json();
  },

  get: async (name: string): Promise<Workflow> => {
    const res = await fetch(`/api/workflows/${encodeURIComponent(name)}`);
    if (!res.ok) throw new Error(`Failed to fetch workflow "${name}"`);
    return res.json();
  },

  upload: async (yamlContent: string, filename: string): Promise<Workflow> => {
    const res = await fetch("/api/workflows/upload", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ content: yamlContent, filename }),
    });
    if (!res.ok) {
      const err = await res.json();
      throw new Error(err.detail || "Failed to upload workflow");
    }
    return res.json();
  },

  create: async (request: WorkflowCreateRequest): Promise<Workflow> => {
    const res = await fetch("/api/workflows/create", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(request),
    });
    if (!res.ok) {
      const err = await res.json();
      throw new Error(err.detail || "Failed to create workflow");
    }
    return res.json();
  },
};

/* ── Resources ────────────────────────────────── */

export const resources = {
  snapshot: async (): Promise<ResourceSnapshot[]> => {
    const res = await fetch("/api/resources");
    if (!res.ok) throw new Error("Failed to fetch resources");
    return res.json();
  },
};

/* ── History ──────────────────────────────────── */

export const history = {
  stats: async (): Promise<HistoryStats> => {
    const res = await fetch("/api/history/stats");
    if (!res.ok) throw new Error("Failed to fetch history stats");
    return res.json();
  },
};

/* ── Files ────────────────────────────────── */

export const files = {
  mounts: async (): Promise<Mount[]> => {
    const res = await fetch("/api/files/mounts");
    if (!res.ok) throw new Error("Failed to fetch mounts");
    return res.json();
  },

  browse: async (mount: string, path: string = ""): Promise<BrowseResult> => {
    const params = new URLSearchParams({ mount });
    if (path) params.set("path", path);
    const res = await fetch(`/api/files/browse?${params}`);
    if (!res.ok) {
      const err = await res.json();
      throw new Error(err.detail || "Failed to browse files");
    }
    return res.json();
  },

  sync: async (mount: string): Promise<SyncResult> => {
    const res = await fetch("/api/files/sync", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mount }),
    });
    if (!res.ok) {
      const err = await res.json();
      throw new Error(err.detail || "Sync failed");
    }
    return res.json();
  },
};
