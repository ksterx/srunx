/* ── Job status ───────────────────────────────── */

export type JobStatus =
  | "UNKNOWN"
  | "PENDING"
  | "RUNNING"
  | "COMPLETED"
  | "FAILED"
  | "CANCELLED"
  | "TIMEOUT";

/* ── Resource / Environment sub-types ────────── */

export type JobResources = {
  nodes?: number;
  gpus_per_node?: number;
  memory_per_node?: string;
  time_limit?: string;
  partition?: string;
};

export type JobEnvironment = {
  conda?: string;
  venv?: string;
};

/* ── Job types ────────────────────────────────── */

export type CommandJob = {
  name: string;
  job_id?: number;
  status: JobStatus;
  depends_on?: string[];
  command: string[];
  resources: JobResources;
  environment?: JobEnvironment;
};

export type ShellJob = {
  name: string;
  job_id?: number;
  status: JobStatus;
  depends_on?: string[];
  script_path: string;
  resources: JobResources;
  environment?: JobEnvironment;
};

export type RunnableJob = CommandJob | ShellJob;

/* ── Workflow ─────────────────────────────────── */

export type Workflow = {
  name: string;
  jobs: RunnableJob[];
  default_project?: string | null;
};

/* ── Workflow run ─────────────────────────────── */

export type WorkflowRunStatus =
  | "syncing"
  | "submitting"
  | "running"
  | "completed"
  | "failed"
  | "cancelled";

export type WorkflowRun = {
  id: string;
  workflow_name: string;
  started_at: string;
  completed_at: string | null;
  status: WorkflowRunStatus;
  job_ids: Record<string, string>;
  job_statuses: Record<string, string>;
  error: string | null;
};

/* ── Resource snapshot ────────────────────────── */

export type ResourceSnapshot = {
  partition: string | null;
  gpus_available: number;
  gpus_in_use: number;
  total_gpus: number;
  gpu_utilization: number;
  has_available_gpus: boolean;
  nodes_total: number;
  nodes_idle: number;
  nodes_down: number;
};

/* ── History stats ────────────────────────────── */

export type HistoryStats = {
  completed: number;
};

/* ── Log data ─────────────────────────────────── */

export type LogData = {
  stdout: string;
  stderr: string;
  stdout_offset: number;
  stderr_offset: number;
};

/* ── Builder-specific types for DAG construction ─ */

export type ContainerRuntime = "pyxis" | "apptainer" | "singularity";

export type BuilderContainer = {
  runtime: ContainerRuntime;
  image: string;
  mounts: string; // comma-separated, split on save
  workdir: string;
};

export type BuilderJob = {
  id: string;
  name: string;
  command: string;
  // Resources
  nodes: number | null;
  gpus_per_node: number | null;
  ntasks_per_node: number | null;
  cpus_per_task: number | null;
  memory_per_node: string | null;
  time_limit: string | null;
  partition: string | null;
  nodelist: string | null;
  // Environment
  conda: string | null;
  venv: string | null;
  container: BuilderContainer | null;
  env_vars: string; // "KEY=VAL" per line, parsed on save
  // Job-level
  work_dir: string | null;
  log_dir: string | null;
  retry: number | null;
  retry_delay: number | null;
};

export type DependencyType = "afterok" | "after" | "afterany" | "afternotok";

/* ── File browser types ──────────────────── */

export type Mount = {
  name: string;
  remote: string; // remote path prefix
};

export type FileEntryType = "file" | "directory" | "symlink";

export type FileEntry = {
  name: string;
  type: FileEntryType;
  size?: number;
  accessible?: boolean; // for symlinks
  target_kind?: "file" | "directory"; // for symlinks
};

export type BrowseResult = {
  entries: FileEntry[];
  remote_prefix: string;
  mount_name: string;
};

export type SyncResult = {
  status: string;
  mount: string;
};

export type MountConfig = {
  name: string;
  local: string;
  remote: string;
  exclude_patterns?: string[];
};

/* ── Config types ────────────────────────────── */

export type ContainerConfig = {
  image: string;
  runtime?: string;
  mounts?: string[];
  workdir?: string;
};

export type ResourceDefaultsConfig = {
  nodes: number;
  gpus_per_node: number;
  ntasks_per_node: number;
  cpus_per_task: number;
  memory_per_node: string | null;
  time_limit: string | null;
  nodelist: string | null;
  partition: string | null;
};

export type EnvironmentDefaultsConfig = {
  conda: string | null;
  venv: string | null;
  container: ContainerConfig | null;
  env_vars: Record<string, string>;
};

export type SrunxConfig = {
  resources: ResourceDefaultsConfig;
  environment: EnvironmentDefaultsConfig;
  notifications: NotificationConfig;
  log_dir: string;
  work_dir: string | null;
};

export type NotificationConfig = {
  slack_webhook_url: string | null;
};

export type ConfigPathInfo = {
  path: string;
  exists: boolean;
  source: string;
};

/* ── SSH Profile types ──────────────────────── */

export type SSHMountConfig = {
  name: string;
  local: string;
  remote: string;
  exclude_patterns?: string[];
};

export type SSHProfile = {
  hostname: string;
  username: string;
  key_filename: string;
  port: number;
  description: string | null;
  ssh_host: string | null;
  proxy_jump: string | null;
  env_vars: Record<string, string> | null;
  mounts: SSHMountConfig[];
};

export type SSHProfilesResponse = {
  current: string | null;
  profiles: Record<string, SSHProfile>;
};

/* ── Environment variable info ──────────────── */

export type EnvVarInfo = {
  name: string;
  value: string;
  description: string;
};

/* ── Project config (mount-based) ────────────── */

export type ProjectInfo = {
  mount_name: string;
  local_path: string;
  remote_path: string;
  config_exists: boolean;
  config_path: string;
};

export type ProjectConfigResponse = {
  mount_name: string;
  local_path: string;
  config_path: string;
  exists: boolean;
  config: SrunxConfig | null;
};

/* ── Workflow create request ─────────────────── */

export type WorkflowCreateRequest = {
  name: string;
  default_project?: string | null;
  jobs: Array<{
    name: string;
    command: string[];
    depends_on: string[];
    resources?: {
      nodes?: number;
      gpus_per_node?: number;
      ntasks_per_node?: number;
      cpus_per_task?: number;
      memory_per_node?: string;
      time_limit?: string;
      partition?: string;
      nodelist?: string;
    };
    environment?: {
      conda?: string;
      venv?: string;
      container?: {
        runtime: string;
        image: string;
        mounts?: string[];
        workdir?: string;
      };
      env_vars?: Record<string, string>;
    };
    work_dir?: string;
    log_dir?: string;
    retry?: number;
    retry_delay?: number;
  }>;
};
