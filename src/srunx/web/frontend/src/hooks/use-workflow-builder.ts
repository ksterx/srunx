import { useCallback, useRef, useState } from "react";
import {
  addEdge,
  useNodesState,
  useEdgesState,
  type Node,
  type Edge,
  type OnNodesChange,
  type OnEdgesChange,
  type OnConnect,
  type Connection,
} from "@xyflow/react";
import type {
  BuilderJob,
  DependencyType,
  JobTemplate,
  RunnableJob,
  Workflow,
  WorkflowCreateRequest,
} from "../lib/types.ts";
import { computeDAGLayers } from "../lib/dag-layout.ts";

/* ── Constants ───────────────────────────────────── */

const BUILDER_NODE_TYPE = "builderNode";
const NODE_SPACING_X = 300;
const NODE_SPACING_Y = 150;
const INITIAL_X = 100;
const INITIAL_Y = 100;

/* ── Helpers ─────────────────────────────────────── */

function makeDefaultJob(id: string, name: string): BuilderJob {
  return {
    id,
    name,
    command: "",
    template: "base",
    nodes: null,
    gpus_per_node: null,
    ntasks_per_node: null,
    cpus_per_task: null,
    memory_per_node: null,
    time_limit: null,
    partition: null,
    nodelist: null,
    conda: null,
    venv: null,
    container: null,
    env_vars: "",
    outputs: "",
    work_dir: null,
    log_dir: null,
    retry: null,
    retry_delay: null,
    srun_args: null,
    launch_prefix: null,
  };
}

function computeNewNodePosition(existingNodes: Node[]): {
  x: number;
  y: number;
} {
  if (existingNodes.length === 0) {
    return { x: INITIAL_X, y: INITIAL_Y };
  }
  // Place new node to the right of the rightmost node, or below if row is full
  const maxX = Math.max(...existingNodes.map((n) => n.position.x));
  const maxY = Math.max(...existingNodes.map((n) => n.position.y));
  const nodesAtMaxY = existingNodes.filter((n) => n.position.y === maxY);
  if (nodesAtMaxY.length < 3) {
    return { x: maxX + NODE_SPACING_X, y: maxY };
  }
  return { x: INITIAL_X, y: maxY + NODE_SPACING_Y };
}

/**
 * Detect cycles using DFS with an explicit recursion-stack set.
 * Returns true if the directed graph (nodes, edges) contains a cycle.
 */
function hasCycle(nodeIds: string[], edges: Edge[]): boolean {
  const adj = new Map<string, string[]>();
  for (const id of nodeIds) {
    adj.set(id, []);
  }
  for (const edge of edges) {
    adj.get(edge.source)?.push(edge.target);
  }

  const visited = new Set<string>();
  const inStack = new Set<string>();

  function dfs(node: string): boolean {
    visited.add(node);
    inStack.add(node);
    for (const neighbor of adj.get(node) ?? []) {
      if (inStack.has(neighbor)) return true;
      if (!visited.has(neighbor) && dfs(neighbor)) return true;
    }
    inStack.delete(node);
    return false;
  }

  for (const id of nodeIds) {
    if (!visited.has(id) && dfs(id)) return true;
  }
  return false;
}

/* ── Workflow → Builder conversion ──────────────── */

function workflowJobToBuilderJob(job: RunnableJob, id: string): BuilderJob {
  // Convert outputs record to "KEY=value" per line string
  const outputsRecord =
    "outputs" in job
      ? ((job as Record<string, unknown>).outputs as
          | Record<string, string>
          | undefined)
      : undefined;
  const outputsStr = outputsRecord
    ? Object.entries(outputsRecord)
        .map(([k, v]) => `${k}=${v}`)
        .join("\n")
    : "";

  // Recover template from raw job data if available
  const rawTemplate = (job as Record<string, unknown>).template as
    | JobTemplate
    | undefined;

  return {
    id,
    name: job.name,
    command: "command" in job ? job.command.join(" ") : job.script_path,
    template: rawTemplate ?? "base",
    nodes: job.resources?.nodes ?? null,
    gpus_per_node: job.resources?.gpus_per_node ?? null,
    ntasks_per_node: null,
    cpus_per_task: null,
    memory_per_node: job.resources?.memory_per_node ?? null,
    time_limit: job.resources?.time_limit ?? null,
    partition: job.resources?.partition ?? null,
    nodelist: null,
    conda: job.environment?.conda ?? null,
    venv: job.environment?.venv ?? null,
    container: null,
    env_vars: "",
    outputs: outputsStr,
    work_dir: null,
    log_dir: null,
    retry: null,
    retry_delay: null,
    srun_args: null,
    launch_prefix: null,
  };
}

/**
 * Compute BFS-layered positions for loaded workflow jobs.
 * Returns a map from job name to {x, y} position.
 */
function computeLayeredPositions(
  jobs: RunnableJob[],
): Map<string, { x: number; y: number }> {
  const { groups } = computeDAGLayers(jobs);
  const positions = new Map<string, { x: number; y: number }>();

  for (const [layer, names] of groups) {
    names.forEach((name, i) => {
      positions.set(name, {
        x: INITIAL_X + i * NODE_SPACING_X,
        y: INITIAL_Y + layer * NODE_SPACING_Y,
      });
    });
  }

  return positions;
}

/* ── Hook ────────────────────────────────────────── */

export function useWorkflowBuilder() {
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const [errors, setErrors] = useState<string[]>([]);

  // Internal job map for O(1) lookup. Kept in a ref so updates don't cause
  // full re-renders on their own -- the ReactFlow node state drives rendering.
  const jobMapRef = useRef<Map<string, BuilderJob>>(new Map());
  const counterRef = useRef(0);

  /* ── Job management ────────────────────────────── */

  const addJob = useCallback(
    (defaultWorkDir?: string | null) => {
      counterRef.current += 1;
      const id = crypto.randomUUID();
      const name = `job_${counterRef.current}`;
      const job = makeDefaultJob(id, name);

      if (defaultWorkDir) {
        job.work_dir = defaultWorkDir;
      }

      jobMapRef.current.set(id, job);

      const position = computeNewNodePosition(nodes);
      const newNode: Node = {
        id,
        type: BUILDER_NODE_TYPE,
        position,
        data: { job },
      };

      setNodes((prev) => [...prev, newNode]);
    },
    [nodes, setNodes],
  );

  const updateJob = useCallback(
    (id: string, updates: Partial<BuilderJob>) => {
      const existing = jobMapRef.current.get(id);
      if (!existing) return;

      const updated = { ...existing, ...updates };
      jobMapRef.current.set(id, updated);

      setNodes((prev) =>
        prev.map((node) =>
          node.id === id ? { ...node, data: { job: updated } } : node,
        ),
      );
    },
    [setNodes],
  );

  const deleteSelected = useCallback(() => {
    setNodes((prev) => {
      const remaining = prev.filter((n) => !n.selected);
      const removedIds = new Set(
        prev.filter((n) => n.selected).map((n) => n.id),
      );

      // Clean up job map
      for (const id of removedIds) {
        jobMapRef.current.delete(id);
      }

      // Remove edges connected to deleted nodes
      setEdges((prevEdges) =>
        prevEdges.filter(
          (e) =>
            !e.selected &&
            !removedIds.has(e.source) &&
            !removedIds.has(e.target),
        ),
      );

      return remaining;
    });
  }, [setNodes, setEdges]);

  /* ── Edge management ───────────────────────────── */

  const onConnect: OnConnect = useCallback(
    (connection: Connection) => {
      const edge: Edge = {
        id: `e-${connection.source}-${connection.target}`,
        source: connection.source,
        target: connection.target,
        data: { depType: "afterok" as DependencyType },
      };
      setEdges((prev) => addEdge(edge, prev));
    },
    [setEdges],
  );

  const updateEdgeType = useCallback(
    (edgeId: string, depType: DependencyType) => {
      setEdges((prev) =>
        prev.map((e) =>
          e.id === edgeId ? { ...e, data: { ...e.data, depType } } : e,
        ),
      );
    },
    [setEdges],
  );

  /* ── Data access ───────────────────────────────── */

  const getJob = useCallback((id: string): BuilderJob | undefined => {
    return jobMapRef.current.get(id);
  }, []);

  /* ── Validation ────────────────────────────────── */

  const validate = useCallback((): boolean => {
    const collected: string[] = [];
    const jobs = Array.from(jobMapRef.current.values());

    // 1. Every job must have a non-empty name
    for (const job of jobs) {
      if (!job.name.trim()) {
        collected.push(`Job "${job.id}" has an empty name`);
      }
    }

    // 2. Every job must have a non-empty command
    for (const job of jobs) {
      if (!job.command.trim()) {
        collected.push(`Job "${job.name || job.id}" has an empty command`);
      }
    }

    // 3. No duplicate job names
    const nameCount = new Map<string, number>();
    for (const job of jobs) {
      const trimmed = job.name.trim();
      if (trimmed) {
        nameCount.set(trimmed, (nameCount.get(trimmed) ?? 0) + 1);
      }
    }
    for (const [name, count] of nameCount) {
      if (count > 1) {
        collected.push(
          `Duplicate job name: "${name}" (appears ${count} times)`,
        );
      }
    }

    // 4. No cycles
    const nodeIds = nodes.map((n) => n.id);
    if (hasCycle(nodeIds, edges)) {
      collected.push("Workflow contains a dependency cycle");
    }

    setErrors(collected);
    return collected.length === 0;
  }, [nodes, edges]);

  /* ── Serialization ─────────────────────────────── */

  const serialize = useCallback(
    (
      workflowName: string,
      defaultProject?: string | null,
      args?: Record<string, string>,
    ): WorkflowCreateRequest => {
      const jobEntries = nodes.map((node) => {
        const job = jobMapRef.current.get(node.id);
        if (!job) {
          throw new Error(`Job data missing for node ${node.id}`);
        }

        // Build depends_on from incoming edges (edges targeting this node)
        const incomingEdges = edges.filter((e) => e.target === node.id);
        const dependsOn = incomingEdges.map((e) => {
          const sourceJob = jobMapRef.current.get(e.source);
          const sourceName = sourceJob?.name ?? e.source;
          const depType = (e.data?.depType as DependencyType) ?? "afterok";
          return depType === "afterok"
            ? sourceName
            : `${depType}:${sourceName}`;
        });

        // Build command array by splitting on whitespace
        const command = job.command.trim().split(/\s+/).filter(Boolean);

        // Build resources, omitting null fields
        const resources: Record<string, number | string> = {};
        if (job.nodes !== null) resources.nodes = job.nodes;
        if (job.gpus_per_node !== null)
          resources.gpus_per_node = job.gpus_per_node;
        if (job.ntasks_per_node !== null)
          resources.ntasks_per_node = job.ntasks_per_node;
        if (job.cpus_per_task !== null)
          resources.cpus_per_task = job.cpus_per_task;
        if (job.memory_per_node !== null)
          resources.memory_per_node = job.memory_per_node;
        if (job.time_limit !== null) resources.time_limit = job.time_limit;
        if (job.partition !== null) resources.partition = job.partition;
        if (job.nodelist !== null) resources.nodelist = job.nodelist;

        // Build environment, omitting null/empty fields
        const environment: Record<string, unknown> = {};
        if (job.conda !== null) environment.conda = job.conda;
        if (job.venv !== null) environment.venv = job.venv;
        if (job.container) {
          const c: Record<string, unknown> = {
            runtime: job.container.runtime,
            image: job.container.image,
          };
          const mounts = job.container.mounts
            .split(",")
            .map((s) => s.trim())
            .filter(Boolean);
          if (mounts.length > 0) c.mounts = mounts;
          if (job.container.workdir) c.workdir = job.container.workdir;
          // Apptainer/Singularity-specific fields
          if (
            job.container.runtime === "apptainer" ||
            job.container.runtime === "singularity"
          ) {
            if (job.container.nv) c.nv = true;
            if (job.container.rocm) c.rocm = true;
            if (job.container.cleanenv) c.cleanenv = true;
            if (job.container.fakeroot) c.fakeroot = true;
            if (job.container.writable_tmpfs) c.writable_tmpfs = true;
            if (job.container.overlay) c.overlay = job.container.overlay;
            if (job.container.env.trim()) {
              const envMap: Record<string, string> = {};
              for (const line of job.container.env.split("\n")) {
                const eq = line.indexOf("=");
                if (eq > 0) {
                  envMap[line.slice(0, eq).trim()] = line.slice(eq + 1).trim();
                }
              }
              if (Object.keys(envMap).length > 0) c.env = envMap;
            }
          }
          environment.container = c;
        }
        if (job.env_vars.trim()) {
          const vars: Record<string, string> = {};
          for (const line of job.env_vars.split("\n")) {
            const eq = line.indexOf("=");
            if (eq > 0) {
              vars[line.slice(0, eq).trim()] = line.slice(eq + 1).trim();
            }
          }
          if (Object.keys(vars).length > 0) environment.env_vars = vars;
        }

        const entry: WorkflowCreateRequest["jobs"][number] = {
          name: job.name,
          command,
          depends_on: dependsOn,
        };

        if (job.template !== "base") {
          entry.template = job.template;
        }

        if (Object.keys(resources).length > 0) {
          entry.resources = resources;
        }
        if (Object.keys(environment).length > 0) {
          entry.environment = environment;
        }
        // Build outputs
        if (job.outputs.trim()) {
          const outputs: Record<string, string> = {};
          for (const line of job.outputs.split("\n")) {
            const eq = line.indexOf("=");
            if (eq > 0) {
              outputs[line.slice(0, eq).trim()] = line.slice(eq + 1).trim();
            }
          }
          if (Object.keys(outputs).length > 0) entry.outputs = outputs;
        }

        if (job.work_dir !== null) entry.work_dir = job.work_dir;
        if (job.log_dir !== null) entry.log_dir = job.log_dir;
        if (job.retry !== null) entry.retry = job.retry;
        if (job.retry_delay !== null) entry.retry_delay = job.retry_delay;
        if (job.srun_args !== null) entry.srun_args = job.srun_args;
        if (job.launch_prefix !== null) entry.launch_prefix = job.launch_prefix;

        return entry;
      });

      const request: WorkflowCreateRequest = {
        name: workflowName,
        jobs: jobEntries,
      };
      if (args && Object.keys(args).length > 0) {
        request.args = args;
      }
      if (defaultProject) {
        request.default_project = defaultProject;
      }
      return request;
    },
    [nodes, edges],
  );

  /* ── Load existing workflow for editing ─────────── */

  const loadWorkflow = useCallback(
    (workflow: Workflow) => {
      // Clear existing state
      jobMapRef.current.clear();

      // Build name→id mapping so we can wire up edges by job name
      const nameToId = new Map<string, string>();
      const positions = computeLayeredPositions(workflow.jobs);

      const newNodes: Node[] = [];
      let maxCounter = 0;

      for (const runnableJob of workflow.jobs) {
        const id = crypto.randomUUID();
        nameToId.set(runnableJob.name, id);

        const builderJob = workflowJobToBuilderJob(runnableJob, id);
        jobMapRef.current.set(id, builderJob);

        const pos = positions.get(runnableJob.name) ?? {
          x: INITIAL_X,
          y: INITIAL_Y,
        };

        newNodes.push({
          id,
          type: BUILDER_NODE_TYPE,
          position: pos,
          data: { job: builderJob },
        });

        maxCounter += 1;
      }

      counterRef.current = maxCounter;

      // Build edges from depends_on
      const newEdges: Edge[] = [];
      for (const runnableJob of workflow.jobs) {
        if (!runnableJob.depends_on) continue;
        const targetId = nameToId.get(runnableJob.name);
        if (!targetId) continue;

        for (const dep of runnableJob.depends_on) {
          // Parse "afternotok:jobname" or plain "jobname" (defaults to afterok)
          let depType: DependencyType = "afterok";
          let depName = dep;
          if (dep.includes(":")) {
            const colonIdx = dep.indexOf(":");
            const prefix = dep.slice(0, colonIdx);
            if (
              prefix === "afterok" ||
              prefix === "after" ||
              prefix === "afterany" ||
              prefix === "afternotok"
            ) {
              depType = prefix as DependencyType;
              depName = dep.slice(colonIdx + 1);
            }
          }

          const sourceId = nameToId.get(depName);
          if (!sourceId) continue;

          newEdges.push({
            id: `e-${sourceId}-${targetId}`,
            source: sourceId,
            target: targetId,
            data: { depType },
          });
        }
      }

      setNodes(newNodes);
      setEdges(newEdges);
    },
    [setNodes, setEdges],
  );

  return {
    // ReactFlow state
    nodes,
    edges,
    onNodesChange: onNodesChange as OnNodesChange,
    onEdgesChange: onEdgesChange as OnEdgesChange,
    onConnect,

    // Job management
    addJob,
    updateJob,
    deleteSelected,

    // Edge dep_type
    updateEdgeType,

    // Job data access
    getJob,

    // Validation
    errors,
    validate,

    // Serialization
    serialize,

    // Load existing workflow
    loadWorkflow,
  };
}
