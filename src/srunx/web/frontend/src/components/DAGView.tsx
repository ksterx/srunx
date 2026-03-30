import { useCallback, useMemo } from "react";
import {
  ReactFlow,
  Background,
  Controls,
  type Node,
  type Edge,
  type NodeProps,
  Handle,
  Position,
  BackgroundVariant,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { motion } from "framer-motion";
import type { RunnableJob, JobStatus } from "../lib/types.ts";
import { StatusBadge } from "./StatusBadge.tsx";

/* ── Status → color mapping ───────────────────── */

const STATUS_COLORS: Record<JobStatus, string> = {
  UNKNOWN: "var(--border-default)",
  PENDING: "var(--st-pending)",
  RUNNING: "var(--st-running)",
  COMPLETED: "var(--st-completed)",
  FAILED: "var(--st-failed)",
  CANCELLED: "var(--st-cancelled)",
  TIMEOUT: "var(--st-timeout)",
};

const STATUS_GLOWS: Partial<Record<JobStatus, string>> = {
  RUNNING: "0 0 20px rgba(34,211,238,0.3)",
  FAILED: "0 0 16px rgba(244,63,94,0.25)",
  COMPLETED: "0 0 16px rgba(52,211,153,0.2)",
};

/* ── Custom Node Component (circuit chip style) ── */

function JobNode({ data }: NodeProps) {
  const job = data.job as RunnableJob;
  const status = job.status;
  const borderColor = STATUS_COLORS[status];
  const glow = STATUS_GLOWS[status] ?? "none";
  const command = "command" in job ? job.command.join(" ") : job.script_path;

  return (
    <motion.div
      initial={{ opacity: 0, scale: 0.9 }}
      animate={{ opacity: 1, scale: 1 }}
      transition={{ duration: 0.3, ease: [0.16, 1, 0.3, 1] }}
      style={{
        background: "var(--bg-surface)",
        border: `1.5px solid ${borderColor}`,
        borderRadius: 8,
        padding: "12px 16px",
        minWidth: 200,
        maxWidth: 280,
        boxShadow: `var(--shadow-panel), ${glow}`,
        position: "relative",
      }}
    >
      <Handle
        type="target"
        position={Position.Top}
        style={{
          width: 8,
          height: 8,
          background: "var(--bg-overlay)",
          border: `2px solid ${borderColor}`,
          borderRadius: "50%",
        }}
      />

      {/* Header */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          marginBottom: 8,
        }}
      >
        <span
          style={{
            fontFamily: "var(--font-display)",
            fontWeight: 600,
            fontSize: "0.85rem",
            color: "var(--text-primary)",
            letterSpacing: "0.02em",
          }}
        >
          {job.name}
        </span>
        <StatusBadge status={status} size="sm" />
      </div>

      {/* Command preview */}
      <div
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.7rem",
          color: "var(--text-muted)",
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        }}
      >
        {command}
      </div>

      {/* Resource indicators */}
      {"resources" in job && job.resources && (
        <div
          style={{
            display: "flex",
            gap: 8,
            marginTop: 8,
            paddingTop: 8,
            borderTop: "1px solid var(--border-ghost)",
          }}
        >
          {job.resources.gpus_per_node && (
            <span
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "0.65rem",
                color: "var(--resource)",
                background: "var(--resource-dim)",
                padding: "1px 6px",
                borderRadius: 3,
              }}
            >
              GPU:{job.resources.gpus_per_node}
            </span>
          )}
          {job.resources.nodes && (
            <span
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "0.65rem",
                color: "var(--text-secondary)",
                background: "var(--bg-overlay)",
                padding: "1px 6px",
                borderRadius: 3,
              }}
            >
              N:{job.resources.nodes}
            </span>
          )}
          {job.resources.time_limit && (
            <span
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "0.65rem",
                color: "var(--text-muted)",
                background: "var(--bg-overlay)",
                padding: "1px 6px",
                borderRadius: 3,
              }}
            >
              {job.resources.time_limit}
            </span>
          )}
        </div>
      )}

      <Handle
        type="source"
        position={Position.Bottom}
        style={{
          width: 8,
          height: 8,
          background: "var(--bg-overlay)",
          border: `2px solid ${borderColor}`,
          borderRadius: "50%",
        }}
      />
    </motion.div>
  );
}

const nodeTypes = { jobNode: JobNode };

/* ── Layout: simple top-to-bottom layered DAG ──── */

function layoutDAG(jobs: RunnableJob[]): { nodes: Node[]; edges: Edge[] } {
  const jobMap = new Map(jobs.map((j) => [j.name, j]));

  /* Compute layers via BFS from roots */
  const layers = new Map<string, number>();
  const roots = jobs.filter((j) => !j.depends_on || j.depends_on.length === 0);

  const queue = roots.map((j) => ({ name: j.name, layer: 0 }));
  while (queue.length > 0) {
    const { name, layer } = queue.shift()!;
    const existing = layers.get(name);
    if (existing !== undefined && existing >= layer) continue;
    layers.set(name, layer);

    for (const job of jobs) {
      if (job.depends_on?.some((d) => d === name || d.includes(`:${name}`))) {
        queue.push({ name: job.name, layer: layer + 1 });
      }
    }
  }

  /* Assign missing jobs to layer 0 */
  for (const job of jobs) {
    if (!layers.has(job.name)) layers.set(job.name, 0);
  }

  /* Group by layer */
  const layerGroups = new Map<number, string[]>();
  for (const [name, layer] of layers) {
    const group = layerGroups.get(layer) ?? [];
    group.push(name);
    layerGroups.set(layer, group);
  }

  const NODE_W = 260;
  const NODE_H = 110;
  const GAP_X = 40;
  const GAP_Y = 60;

  const nodes: Node[] = [];
  for (const [layer, names] of layerGroups) {
    const totalWidth = names.length * NODE_W + (names.length - 1) * GAP_X;
    const startX = -totalWidth / 2;
    names.forEach((name, i) => {
      nodes.push({
        id: name,
        type: "jobNode",
        position: {
          x: startX + i * (NODE_W + GAP_X),
          y: layer * (NODE_H + GAP_Y),
        },
        data: { job: jobMap.get(name) },
      });
    });
  }

  const edges: Edge[] = [];
  for (const job of jobs) {
    if (!job.depends_on) continue;
    for (const dep of job.depends_on) {
      const depName = dep.includes(":") ? dep.split(":")[1] : dep;
      if (jobMap.has(depName)) {
        const isRunning =
          job.status === "RUNNING" || jobMap.get(depName)?.status === "RUNNING";
        edges.push({
          id: `${depName}->${job.name}`,
          source: depName,
          target: job.name,
          animated: isRunning,
          style: {
            stroke: isRunning ? "var(--st-running)" : "var(--border-strong)",
            strokeWidth: 2,
          },
        });
      }
    }
  }

  return { nodes, edges };
}

/* ── Main DAGView component ───────────────────── */

type DAGViewProps = {
  jobs: RunnableJob[];
  onJobClick?: (jobName: string) => void;
};

export function DAGView({ jobs, onJobClick }: DAGViewProps) {
  const { nodes, edges } = useMemo(() => layoutDAG(jobs), [jobs]);

  const handleNodeClick = useCallback(
    (_: React.MouseEvent, node: Node) => {
      onJobClick?.(node.id);
    },
    [onJobClick],
  );

  return (
    <div style={{ width: "100%", height: "100%", minHeight: 400 }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        onNodeClick={handleNodeClick}
        fitView
        fitViewOptions={{ padding: 0.2 }}
        proOptions={{ hideAttribution: true }}
        minZoom={0.3}
        maxZoom={1.5}
      >
        <Background
          variant={BackgroundVariant.Dots}
          gap={24}
          size={1}
          color="var(--border-ghost)"
        />
        <Controls />
      </ReactFlow>
    </div>
  );
}
