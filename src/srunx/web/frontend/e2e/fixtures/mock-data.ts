/* ── Mock data matching the TypeScript types ──── */

export const MOCK_JOBS = [
  {
    name: "train-resnet",
    job_id: 10001,
    status: "RUNNING",
    depends_on: [],
    command: ["python", "train.py", "--model", "resnet50"],
    resources: {
      nodes: 1,
      gpus_per_node: 4,
      cpus_per_task: 8,
      memory_per_node: "64GB",
      time_limit: "8:00:00",
      partition: "gpu",
    },
    environment: { conda: "ml_env" },
  },
  {
    name: "preprocess-data",
    job_id: 10002,
    status: "COMPLETED",
    depends_on: [],
    command: ["python", "preprocess.py"],
    resources: {
      nodes: 1,
      gpus_per_node: 0,
      cpus_per_task: 16,
      partition: "cpu",
      time_limit: "2:00:00",
    },
  },
  {
    name: "evaluate-model",
    job_id: 10003,
    status: "PENDING",
    depends_on: ["train-resnet"],
    command: ["python", "evaluate.py"],
    resources: {
      nodes: 1,
      gpus_per_node: 1,
      partition: "gpu",
      time_limit: "1:00:00",
    },
  },
  {
    name: "failed-job",
    job_id: 10004,
    status: "FAILED",
    depends_on: [],
    command: ["python", "broken.py"],
    resources: { nodes: 1, gpus_per_node: 0, partition: "cpu" },
  },
];

export const MOCK_WORKFLOWS = [
  {
    name: "ml-pipeline",
    args: { base_dir: "/data/experiments", model_name: "resnet50" },
    jobs: [
      {
        name: "preprocess",
        status: "COMPLETED",
        depends_on: [],
        outputs: { data_path: "/data/experiments/preprocessed" },
        command: ["python", "preprocess.py"],
        resources: { nodes: 1, gpus_per_node: 0 },
      },
      {
        name: "train",
        status: "RUNNING",
        depends_on: ["preprocess"],
        outputs: { model_path: "/data/experiments/models/best.pt" },
        command: ["python", "train.py"],
        resources: { nodes: 2, gpus_per_node: 4 },
      },
      {
        name: "evaluate",
        status: "PENDING",
        depends_on: ["train"],
        outputs: {},
        command: ["python", "evaluate.py"],
        resources: { nodes: 1, gpus_per_node: 1 },
      },
    ],
  },
  {
    name: "data-pipeline",
    jobs: [
      {
        name: "extract",
        status: "COMPLETED",
        depends_on: [],
        command: ["python", "extract.py"],
        resources: { nodes: 1, gpus_per_node: 0 },
      },
      {
        name: "transform",
        status: "COMPLETED",
        depends_on: ["extract"],
        command: ["python", "transform.py"],
        resources: { nodes: 1, gpus_per_node: 0 },
      },
    ],
  },
];

export const MOCK_RESOURCES = [
  {
    timestamp: new Date().toISOString(),
    partition: "gpu",
    total_gpus: 32,
    gpus_in_use: 20,
    gpus_available: 12,
    jobs_running: 5,
    nodes_total: 8,
    nodes_idle: 3,
    nodes_down: 0,
    gpu_utilization: 0.625,
    has_available_gpus: true,
  },
  {
    timestamp: new Date().toISOString(),
    partition: "cpu",
    total_gpus: 0,
    gpus_in_use: 0,
    gpus_available: 0,
    jobs_running: 12,
    nodes_total: 20,
    nodes_idle: 8,
    nodes_down: 1,
    gpu_utilization: 0,
    has_available_gpus: false,
  },
];

export const MOCK_HISTORY = [
  {
    job_id: 10001,
    job_name: "train-resnet",
    command: "python train.py",
    status: "RUNNING",
    submitted_at: "2026-03-30T06:00:00Z",
    workflow_name: "ml-pipeline",
    partition: "gpu",
    nodes: 1,
    gpus: 4,
  },
  {
    job_id: 10002,
    job_name: "preprocess-data",
    command: "python preprocess.py",
    status: "COMPLETED",
    submitted_at: "2026-03-30T05:00:00Z",
    completed_at: "2026-03-30T05:30:00Z",
    partition: "cpu",
    nodes: 1,
    gpus: 0,
  },
];

export const MOCK_STATS = {
  total: 42,
  completed: 35,
  failed: 4,
  cancelled: 3,
  avg_runtime_seconds: 3600,
};

export const MOCK_TEMPLATES = [
  {
    name: "base",
    description:
      "SLURM job template with full resource control and inter-job outputs",
    use_case: "All job types including distributed training",
  },
  {
    name: "gpu-single",
    description: "Single GPU training template",
    use_case: "Single GPU training jobs",
    user_defined: true,
  },
];

export const MOCK_TEMPLATE_DETAIL = {
  name: "base",
  description:
    "SLURM job template with full resource control and inter-job outputs",
  use_case: "All job types including distributed training",
  content:
    "#!/bin/bash\n#SBATCH --job-name={{ job_name }}\n#SBATCH --nodes={{ nodes }}\nsrun {{ command }}",
};

export const MOCK_LOGS = {
  stdout:
    "Epoch 1/10: loss=0.85 acc=0.72\nEpoch 2/10: loss=0.63 acc=0.81\nEpoch 3/10: loss=0.45 acc=0.88",
  stderr: "WARNING: GPU memory usage high\nINFO: Checkpoint saved",
  stdout_offset: 95,
  stderr_offset: 49,
};
