import type {
  BrowseResult,
  CommandJob,
  ConfigPathInfo,
  EnvVarInfo,
  HistoryStats,
  LogData,
  Mount,
  MountConfig,
  ProjectConfigResponse,
  ProjectInfo,
  ResourceSnapshot,
  SSHMountConfig,
  SSHProfile,
  SSHProfilesResponse,
  SrunxConfig,
  SyncResult,
  Workflow,
  WorkflowCreateRequest,
  WorkflowRun,
} from "./types.ts";

/* ── Helpers ─────────────────────────────────── */

type ValidationItem = { loc?: string[]; msg?: string };

/** Extract a human-readable message from a FastAPI error response body. */
function extractDetail(body: { detail?: unknown }, fallback: string): string {
  const detail = body.detail;
  if (typeof detail === "string") return detail;
  if (Array.isArray(detail)) {
    const msgs = (detail as ValidationItem[]).map((e) =>
      e.loc && e.msg ? `${e.loc.join(".")}: ${e.msg}` : JSON.stringify(e),
    );
    return msgs.join("; ");
  }
  return fallback;
}

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
    const res = await fetch(`/api/jobs/${jobId}`, { method: "DELETE" });
    if (!res.ok) {
      const err = await res.json();
      throw new Error(extractDetail(err, "Failed to cancel job"));
    }
  },

  logs: async (
    jobId: number,
    offsets?: { stdout_offset?: number; stderr_offset?: number },
  ): Promise<LogData> => {
    const params = new URLSearchParams();
    if (offsets?.stdout_offset)
      params.set("stdout_offset", String(offsets.stdout_offset));
    if (offsets?.stderr_offset)
      params.set("stderr_offset", String(offsets.stderr_offset));
    const qs = params.toString();
    const res = await fetch(`/api/jobs/${jobId}/logs${qs ? `?${qs}` : ""}`);
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
      body: JSON.stringify({ yaml: yamlContent, filename }),
    });
    if (!res.ok) {
      const err = await res.json();
      throw new Error(extractDetail(err, "Failed to upload workflow"));
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
      throw new Error(extractDetail(err, "Failed to create workflow"));
    }
    return res.json();
  },

  run: async (name: string): Promise<WorkflowRun> => {
    const res = await fetch(`/api/workflows/${encodeURIComponent(name)}/run`, {
      method: "POST",
    });
    if (!res.ok) {
      const err = await res.json();
      throw new Error(extractDetail(err, "Failed to run workflow"));
    }
    return res.json();
  },

  getRun: async (runId: string): Promise<WorkflowRun> => {
    const res = await fetch(`/api/workflows/runs/${encodeURIComponent(runId)}`);
    if (!res.ok) {
      const err = await res.json();
      throw new Error(extractDetail(err, "Failed to fetch run"));
    }
    return res.json();
  },

  delete: async (name: string): Promise<void> => {
    const res = await fetch(`/api/workflows/${encodeURIComponent(name)}`, {
      method: "DELETE",
    });
    if (!res.ok) {
      const err = await res.json();
      throw new Error(extractDetail(err, "Failed to delete workflow"));
    }
  },

  cancelRun: async (runId: string): Promise<void> => {
    const res = await fetch(
      `/api/workflows/runs/${encodeURIComponent(runId)}/cancel`,
      { method: "POST" },
    );
    if (!res.ok) {
      const err = await res.json();
      throw new Error(extractDetail(err, "Failed to cancel run"));
    }
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
      throw new Error(extractDetail(err, "Failed to browse files"));
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
      throw new Error(extractDetail(err, "Sync failed"));
    }
    return res.json();
  },

  mountsConfig: async (): Promise<MountConfig[]> => {
    const res = await fetch("/api/files/mounts/config");
    if (!res.ok) throw new Error("Failed to fetch mount configuration");
    return res.json();
  },

  addMount: async (mount: MountConfig): Promise<MountConfig> => {
    const res = await fetch("/api/files/mounts", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(mount),
    });
    if (!res.ok) {
      const err = await res.json();
      throw new Error(extractDetail(err, "Failed to add mount"));
    }
    return res.json();
  },

  removeMount: async (name: string): Promise<void> => {
    const res = await fetch(`/api/files/mounts/${encodeURIComponent(name)}`, {
      method: "DELETE",
    });
    if (!res.ok) {
      const err = await res.json();
      throw new Error(extractDetail(err, "Failed to remove mount"));
    }
  },
};

/* ── Config ──────────────────────────────────── */

export const config = {
  get: async (): Promise<SrunxConfig> => {
    const res = await fetch("/api/config");
    if (!res.ok) throw new Error("Failed to fetch config");
    return res.json();
  },

  update: async (body: SrunxConfig): Promise<SrunxConfig> => {
    const res = await fetch("/api/config", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const err = await res.json();
      throw new Error(extractDetail(err, "Failed to update config"));
    }
    return res.json();
  },

  paths: async (): Promise<ConfigPathInfo[]> => {
    const res = await fetch("/api/config/paths");
    if (!res.ok) throw new Error("Failed to fetch config paths");
    return res.json();
  },

  reset: async (): Promise<SrunxConfig> => {
    const res = await fetch("/api/config/reset", { method: "POST" });
    if (!res.ok) {
      const err = await res.json();
      throw new Error(extractDetail(err, "Failed to reset config"));
    }
    return res.json();
  },

  /* SSH Profiles */

  sshProfiles: async (): Promise<SSHProfilesResponse> => {
    const res = await fetch("/api/config/ssh/profiles");
    if (!res.ok) throw new Error("Failed to fetch SSH profiles");
    return res.json();
  },

  addSSHProfile: async (body: {
    name: string;
    hostname: string;
    username: string;
    key_filename: string;
    port?: number;
    description?: string;
    ssh_host?: string;
    proxy_jump?: string;
  }): Promise<SSHProfile> => {
    const res = await fetch("/api/config/ssh/profiles", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const err = await res.json();
      throw new Error(extractDetail(err, "Failed to add SSH profile"));
    }
    return res.json();
  },

  updateSSHProfile: async (
    name: string,
    body: Partial<SSHProfile>,
  ): Promise<SSHProfile> => {
    const res = await fetch(
      `/api/config/ssh/profiles/${encodeURIComponent(name)}`,
      {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      },
    );
    if (!res.ok) {
      const err = await res.json();
      throw new Error(extractDetail(err, "Failed to update SSH profile"));
    }
    return res.json();
  },

  deleteSSHProfile: async (name: string): Promise<void> => {
    const res = await fetch(
      `/api/config/ssh/profiles/${encodeURIComponent(name)}`,
      { method: "DELETE" },
    );
    if (!res.ok) {
      const err = await res.json();
      throw new Error(extractDetail(err, "Failed to delete SSH profile"));
    }
  },

  activateSSHProfile: async (name: string): Promise<void> => {
    const res = await fetch(
      `/api/config/ssh/profiles/${encodeURIComponent(name)}/activate`,
      { method: "POST" },
    );
    if (!res.ok) {
      const err = await res.json();
      throw new Error(extractDetail(err, "Failed to activate SSH profile"));
    }
  },

  addSSHMount: async (
    profileName: string,
    mount: SSHMountConfig,
  ): Promise<SSHMountConfig> => {
    const res = await fetch(
      `/api/config/ssh/profiles/${encodeURIComponent(profileName)}/mounts`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(mount),
      },
    );
    if (!res.ok) {
      const err = await res.json();
      throw new Error(extractDetail(err, "Failed to add mount"));
    }
    return res.json();
  },

  removeSSHMount: async (
    profileName: string,
    mountName: string,
  ): Promise<void> => {
    const res = await fetch(
      `/api/config/ssh/profiles/${encodeURIComponent(profileName)}/mounts/${encodeURIComponent(mountName)}`,
      { method: "DELETE" },
    );
    if (!res.ok) {
      const err = await res.json();
      throw new Error(extractDetail(err, "Failed to remove mount"));
    }
  },

  /* Environment Variables */

  envVars: async (): Promise<EnvVarInfo[]> => {
    const res = await fetch("/api/config/env");
    if (!res.ok) throw new Error("Failed to fetch environment variables");
    return res.json();
  },

  /* Projects (mount-based) */

  listProjects: async (): Promise<ProjectInfo[]> => {
    const res = await fetch("/api/config/projects");
    if (!res.ok) throw new Error("Failed to fetch projects");
    return res.json();
  },

  getProject: async (mountName: string): Promise<ProjectConfigResponse> => {
    const res = await fetch(
      `/api/config/projects/${encodeURIComponent(mountName)}`,
    );
    if (!res.ok) throw new Error("Failed to fetch project config");
    return res.json();
  },

  updateProject: async (
    mountName: string,
    body: SrunxConfig,
  ): Promise<ProjectConfigResponse> => {
    const res = await fetch(
      `/api/config/projects/${encodeURIComponent(mountName)}`,
      {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      },
    );
    if (!res.ok) {
      const err = await res.json();
      throw new Error(extractDetail(err, "Failed to update project config"));
    }
    return res.json();
  },

  initProject: async (mountName: string): Promise<ProjectConfigResponse> => {
    const res = await fetch(
      `/api/config/projects/${encodeURIComponent(mountName)}/init`,
      { method: "POST" },
    );
    if (!res.ok) {
      const err = await res.json();
      throw new Error(
        extractDetail(err, "Failed to initialize project config"),
      );
    }
    return res.json();
  },
};
