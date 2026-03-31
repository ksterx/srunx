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
