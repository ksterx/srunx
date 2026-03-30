import { useCallback, useMemo, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import {
  ReactFlow,
  Background,
  Controls,
  BackgroundVariant,
  Handle,
  Position,
  type Node,
  type Edge,
  type NodeProps,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { motion } from "framer-motion";
import { ArrowLeft, Plus, Save } from "lucide-react";
import { useWorkflowBuilder } from "../hooks/use-workflow-builder.ts";
import { JobPropertyPanel } from "../components/JobPropertyPanel.tsx";
import { workflows as workflowsApi } from "../lib/api.ts";
import type { BuilderJob, DependencyType } from "../lib/types.ts";

/* ── Dependency type options ──────────────────── */

const DEP_TYPE_OPTIONS: DependencyType[] = [
  "afterok",
  "after",
  "afterany",
  "afternotok",
];

/* ── Custom Node for builder context ─────────── */

function BuilderJobNode({ data }: NodeProps) {
  const job = data.job as BuilderJob;

  return (
    <motion.div
      initial={{ opacity: 0, scale: 0.9 }}
      animate={{ opacity: 1, scale: 1 }}
      transition={{ duration: 0.3, ease: [0.16, 1, 0.3, 1] }}
      style={{
        background: "var(--bg-surface)",
        border: "1.5px solid var(--st-pending)",
        borderRadius: 8,
        padding: "12px 16px",
        minWidth: 200,
        maxWidth: 280,
        boxShadow: "var(--shadow-panel)",
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
          border: "2px solid var(--st-pending)",
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
        <span
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.6rem",
            padding: "1px 6px",
            borderRadius: 3,
            background: "var(--st-pending-dim, rgba(250,204,21,0.1))",
            color: "var(--st-pending)",
            textTransform: "uppercase",
            letterSpacing: "0.04em",
          }}
        >
          draft
        </span>
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
        {job.command || "No command set"}
      </div>

      {/* Resource indicators */}
      {(job.gpus_per_node || job.nodes || job.time_limit || job.container) && (
        <div
          style={{
            display: "flex",
            gap: 8,
            marginTop: 8,
            paddingTop: 8,
            borderTop: "1px solid var(--border-ghost)",
            flexWrap: "wrap",
          }}
        >
          {job.gpus_per_node && (
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
              GPU:{job.gpus_per_node}
            </span>
          )}
          {job.nodes && (
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
              N:{job.nodes}
            </span>
          )}
          {job.time_limit && (
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
              {job.time_limit}
            </span>
          )}
          {job.container && (
            <span
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "0.65rem",
                color: "var(--accent)",
                background: "var(--accent-dim)",
                padding: "1px 6px",
                borderRadius: 3,
              }}
            >
              {job.container.runtime}
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
          border: "2px solid var(--st-pending)",
          borderRadius: "50%",
        }}
      />
    </motion.div>
  );
}

/* ── Edge dependency type selector popover ──── */

type SelectedEdge = {
  id: string;
  x: number;
  y: number;
};

type EdgeTypeSelectorProps = {
  selectedEdge: SelectedEdge;
  currentType: DependencyType;
  onSelect: (depType: DependencyType) => void;
  onClose: () => void;
};

function EdgeTypeSelector({
  selectedEdge,
  currentType,
  onSelect,
  onClose,
}: EdgeTypeSelectorProps) {
  return (
    <>
      {/* Backdrop to catch clicks outside */}
      <div
        onClick={onClose}
        style={{
          position: "fixed",
          inset: 0,
          zIndex: 99,
        }}
      />
      <motion.div
        initial={{ opacity: 0, scale: 0.9 }}
        animate={{ opacity: 1, scale: 1 }}
        transition={{ duration: 0.15 }}
        style={{
          position: "fixed",
          left: selectedEdge.x,
          top: selectedEdge.y,
          transform: "translate(-50%, -50%)",
          zIndex: 100,
          background: "var(--bg-surface)",
          border: "1px solid var(--border-default)",
          borderRadius: "var(--radius-md)",
          boxShadow: "var(--shadow-panel)",
          padding: "4px",
          display: "flex",
          flexDirection: "column",
          gap: 2,
          minWidth: 140,
        }}
      >
        <div
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.6rem",
            color: "var(--text-muted)",
            textTransform: "uppercase",
            letterSpacing: "0.06em",
            padding: "4px 8px 2px",
          }}
        >
          Dependency type
        </div>
        {DEP_TYPE_OPTIONS.map((opt) => (
          <button
            key={opt}
            onClick={() => {
              onSelect(opt);
              onClose();
            }}
            style={{
              display: "block",
              width: "100%",
              textAlign: "left",
              padding: "6px 8px",
              fontFamily: "var(--font-mono)",
              fontSize: "0.75rem",
              color:
                opt === currentType ? "var(--accent)" : "var(--text-secondary)",
              fontWeight: opt === currentType ? 600 : 400,
              background:
                opt === currentType ? "var(--bg-overlay)" : "transparent",
              border: "none",
              borderRadius: "var(--radius-sm)",
              cursor: "pointer",
              transition: "background var(--duration-fast) var(--ease-out)",
            }}
            onMouseEnter={(e) => {
              if (opt !== currentType) {
                e.currentTarget.style.background = "var(--bg-hover)";
              }
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background =
                opt === currentType ? "var(--bg-overlay)" : "transparent";
            }}
          >
            {opt}
          </button>
        ))}
      </motion.div>
    </>
  );
}

/* ── Workflow name validation ────────────────── */

const WORKFLOW_NAME_RE = /^[\w-]+$/;

/* ── WorkflowBuilder page ───────────────────── */

export function WorkflowBuilder() {
  const navigate = useNavigate();

  const {
    nodes,
    edges,
    onNodesChange,
    onEdgesChange,
    onConnect,
    addJob,
    updateJob,
    deleteSelected,
    updateEdgeType,
    getJob,
    errors,
    validate,
    serialize,
  } = useWorkflowBuilder();

  const [workflowName, setWorkflowName] = useState("");
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [selectedEdge, setSelectedEdge] = useState<SelectedEdge | null>(null);
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);

  const nodeTypes = useMemo(() => ({ builderNode: BuilderJobNode }), []);

  /* ── Selected job for property panel ──────── */

  const selectedJob = selectedNodeId ? getJob(selectedNodeId) : undefined;

  /* ── Edge click handler ───────────────────── */

  const handleEdgeClick = useCallback((event: React.MouseEvent, edge: Edge) => {
    event.stopPropagation();
    setSelectedEdge({
      id: edge.id,
      x: event.clientX,
      y: event.clientY,
    });
  }, []);

  /* ── Pane click — deselect everything ─────── */

  const handlePaneClick = useCallback(() => {
    setSelectedNodeId(null);
    setSelectedEdge(null);
  }, []);

  /* ── Node click ───────────────────────────── */

  const handleNodeClick = useCallback((_: React.MouseEvent, node: Node) => {
    setSelectedNodeId(node.id);
    setSelectedEdge(null);
  }, []);

  /* ── Keyboard shortcuts ───────────────────── */

  const handleKeyDown = useCallback(
    (event: React.KeyboardEvent) => {
      if (event.key === "Delete" || event.key === "Backspace") {
        // Don't delete when typing in an input
        const target = event.target as HTMLElement;
        if (
          target.tagName === "INPUT" ||
          target.tagName === "TEXTAREA" ||
          target.isContentEditable
        ) {
          return;
        }
        deleteSelected();
        setSelectedNodeId(null);
      }
    },
    [deleteSelected],
  );

  /* ── Save workflow ────────────────────────── */

  const handleSave = useCallback(async () => {
    setSaveError(null);

    // Validate workflow name
    const trimmedName = workflowName.trim();
    if (!trimmedName) {
      setSaveError("Workflow name is required");
      return;
    }
    if (!WORKFLOW_NAME_RE.test(trimmedName)) {
      setSaveError(
        "Workflow name must contain only letters, numbers, underscores, and hyphens",
      );
      return;
    }

    // Validate DAG structure
    if (!validate()) {
      return;
    }

    // Serialize and submit
    setSaving(true);
    try {
      const request = serialize(trimmedName);
      await workflowsApi.create(request);
      navigate(`/workflows/${encodeURIComponent(trimmedName)}`);
    } catch (err) {
      if (err instanceof Error) {
        setSaveError(err.message);
      } else {
        setSaveError("Failed to create workflow");
      }
    } finally {
      setSaving(false);
    }
  }, [workflowName, validate, serialize, navigate]);

  /* ── Edge dep type for selector ───────────── */

  const selectedEdgeDepType: DependencyType = useMemo(() => {
    if (!selectedEdge) return "afterok";
    const edge = edges.find((e) => e.id === selectedEdge.id);
    return (edge?.data?.depType as DependencyType) ?? "afterok";
  }, [selectedEdge, edges]);

  /* ── All errors (validation + save) ───────── */

  const allErrors = saveError ? [saveError, ...errors] : errors;

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
      }}
    >
      {/* ── Toolbar ──────────────────────────── */}
      <div
        style={{
          background: "var(--bg-surface)",
          borderBottom: "1px solid var(--border-default)",
          padding: "var(--sp-3) var(--sp-5)",
          display: "flex",
          alignItems: "center",
          gap: "var(--sp-3)",
          flexShrink: 0,
        }}
      >
        <Link
          to="/workflows"
          className="btn btn-ghost"
          style={{ padding: "6px 8px" }}
          title="Back to workflows"
        >
          <ArrowLeft size={16} />
        </Link>

        <input
          className="input"
          type="text"
          value={workflowName}
          onChange={(e) => setWorkflowName(e.target.value)}
          placeholder="workflow-name"
          style={{
            width: 220,
            fontFamily: "var(--font-mono)",
            fontSize: "0.85rem",
          }}
        />

        <div style={{ flex: 1 }} />

        <button className="btn btn-ghost" onClick={addJob}>
          <Plus size={14} />
          Add Job
        </button>

        <button
          className="btn btn-primary"
          onClick={handleSave}
          disabled={saving}
          style={{ opacity: saving ? 0.6 : 1 }}
        >
          <Save size={14} />
          {saving ? "Saving..." : "Save Workflow"}
        </button>
      </div>

      {/* ── Validation / save errors ─────────── */}
      {allErrors.length > 0 && (
        <div
          style={{
            padding: "var(--sp-3) var(--sp-5)",
            background: "var(--st-failed-dim)",
            border: "1px solid rgba(244,63,94,0.3)",
            borderRadius: 0,
            color: "var(--st-failed)",
            fontFamily: "var(--font-mono)",
            fontSize: "0.8rem",
            display: "flex",
            flexDirection: "column",
            gap: 4,
            flexShrink: 0,
          }}
        >
          {allErrors.map((err, i) => (
            <div key={i}>{err}</div>
          ))}
        </div>
      )}

      {/* ── Canvas + Property Panel ──────────── */}
      <div style={{ display: "flex", flex: 1, overflow: "hidden" }}>
        {/* ReactFlow canvas */}
        <div style={{ flex: 1 }}>
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            nodeTypes={nodeTypes}
            onNodeClick={handleNodeClick}
            onEdgeClick={handleEdgeClick}
            onPaneClick={handlePaneClick}
            onKeyDown={handleKeyDown}
            fitView
            fitViewOptions={{ padding: 0.2 }}
            minZoom={0.3}
            maxZoom={1.5}
            proOptions={{ hideAttribution: true }}
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

        {/* Property panel (shown when a node is selected) */}
        {selectedJob && selectedNodeId && (
          <JobPropertyPanel
            job={selectedJob}
            onUpdate={(updates) => updateJob(selectedNodeId, updates)}
            onClose={() => setSelectedNodeId(null)}
            onDelete={() => {
              deleteSelected();
              setSelectedNodeId(null);
            }}
          />
        )}
      </div>

      {/* ── Edge type selector popover ───────── */}
      {selectedEdge && (
        <EdgeTypeSelector
          selectedEdge={selectedEdge}
          currentType={selectedEdgeDepType}
          onSelect={(depType) => updateEdgeType(selectedEdge.id, depType)}
          onClose={() => setSelectedEdge(null)}
        />
      )}
    </div>
  );
}
