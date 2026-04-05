import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Link,
  useNavigate,
  useParams,
  useSearchParams,
} from "react-router-dom";
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
import { ArrowLeft, Plus, Save, Settings, Variable } from "lucide-react";
import { useWorkflowBuilder } from "../hooks/use-workflow-builder.ts";
import { JobPropertyPanel } from "../components/JobPropertyPanel.tsx";
import { MountSettings } from "../components/MountSettings.tsx";
import { workflows as workflowsApi, files as filesApi } from "../lib/api.ts";
import type { BuilderJob, DependencyType, Mount } from "../lib/types.ts";

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
  const { name: editName } = useParams<{ name?: string }>();
  const [searchParams] = useSearchParams();
  const mountFromUrl = searchParams.get("mount");
  const isEditMode = editName !== undefined;
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
    loadWorkflow,
  } = useWorkflowBuilder();

  const [workflowName, setWorkflowName] = useState("");
  const [originalName, setOriginalName] = useState<string | null>(null);
  const [defaultProject, setDefaultProject] = useState<string | null>(
    mountFromUrl,
  );
  const [mounts, setMounts] = useState<Mount[]>([]);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [selectedEdge, setSelectedEdge] = useState<SelectedEdge | null>(null);
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [loadingWorkflow, setLoadingWorkflow] = useState(isEditMode);
  const [showMountSettings, setShowMountSettings] = useState(false);
  const [showArgsEditor, setShowArgsEditor] = useState(false);
  const [argsText, setArgsText] = useState("");

  /* ── Load available mounts ─────────────────── */

  useEffect(() => {
    filesApi
      .mounts()
      .then(setMounts)
      .catch(() => {
        /* mounts are optional; ignore errors */
      });
  }, []);

  /* ── Load existing workflow in edit mode ────── */

  useEffect(() => {
    if (!editName) return;
    const nameToLoad = editName;
    let cancelled = false;

    async function fetchWorkflow() {
      try {
        if (!mountFromUrl) {
          setSaveError("No mount specified in URL");
          setLoadingWorkflow(false);
          return;
        }
        const workflow = await workflowsApi.get(nameToLoad, mountFromUrl);
        if (cancelled) return;
        setWorkflowName(workflow.name);
        setOriginalName(workflow.name);
        setDefaultProject(workflow.default_project ?? null);
        if (workflow.args && Object.keys(workflow.args).length > 0) {
          setArgsText(
            Object.entries(workflow.args)
              .map(([k, v]) => `${k}=${v}`)
              .join("\n"),
          );
        }
        loadWorkflow(workflow);
      } catch (err) {
        if (cancelled) return;
        setSaveError(
          err instanceof Error
            ? err.message
            : `Failed to load workflow "${nameToLoad}"`,
        );
      } finally {
        if (!cancelled) setLoadingWorkflow(false);
      }
    }

    fetchWorkflow();
    return () => {
      cancelled = true;
    };
  }, [editName, loadWorkflow]);

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

  /* ── Mount settings close handler ──────────── */

  const handleMountSettingsClose = useCallback(() => {
    setShowMountSettings(false);
    // Refresh mounts list after settings change
    filesApi
      .mounts()
      .then(setMounts)
      .catch(() => {
        /* ignore */
      });
  }, []);

  /* ── Default work_dir from selected mount ── */

  const defaultWorkDir = useMemo(() => {
    if (!defaultProject) return null;
    const mount = mounts.find((m) => m.name === defaultProject);
    return mount?.remote ?? null;
  }, [defaultProject, mounts]);

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
      // Parse args text into Record
      const parsedArgs: Record<string, string> = {};
      if (argsText.trim()) {
        for (const line of argsText.split("\n")) {
          const eq = line.indexOf("=");
          if (eq > 0) {
            parsedArgs[line.slice(0, eq).trim()] = line.slice(eq + 1).trim();
          }
        }
      }
      const request = serialize(
        trimmedName,
        defaultProject,
        Object.keys(parsedArgs).length > 0 ? parsedArgs : undefined,
      );

      if (!defaultProject) {
        setSaveError("A project (mount) must be selected to save a workflow");
        setSaving(false);
        return;
      }

      if (isEditMode && originalName) {
        const editMount = mountFromUrl ?? defaultProject;
        if (trimmedName !== originalName) {
          await workflowsApi.create(request);
          try {
            await workflowsApi.delete(originalName, editMount);
          } catch {
            // Old workflow deletion failed, but new one was created -- acceptable
          }
        } else {
          await workflowsApi.delete(originalName, editMount);
          await workflowsApi.create(request);
        }
      } else {
        await workflowsApi.create(request);
      }

      navigate(
        `/workflows/${encodeURIComponent(trimmedName)}?mount=${encodeURIComponent(defaultProject)}`,
      );
    } catch (err) {
      if (err instanceof Error) {
        setSaveError(err.message);
      } else {
        setSaveError("Failed to save workflow");
      }
    } finally {
      setSaving(false);
    }
  }, [
    workflowName,
    defaultProject,
    validate,
    serialize,
    navigate,
    isEditMode,
    originalName,
  ]);

  /* ── Edge dep type for selector ───────────── */

  const selectedEdgeDepType: DependencyType = useMemo(() => {
    if (!selectedEdge) return "afterok";
    const edge = edges.find((e) => e.id === selectedEdge.id);
    return (edge?.data?.depType as DependencyType) ?? "afterok";
  }, [selectedEdge, edges]);

  /* ── All errors (validation + save) ───────── */

  const allErrors = saveError ? [saveError, ...errors] : errors;

  if (loadingWorkflow) {
    return (
      <div style={{ padding: 48, textAlign: "center" }}>
        <div
          className="skeleton"
          style={{ width: 200, height: 24, margin: "0 auto" }}
        />
      </div>
    );
  }

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

        {mounts.length > 0 && (
          <select
            className="input"
            value={defaultProject ?? ""}
            onChange={(e) => setDefaultProject(e.target.value || null)}
            style={{
              width: 180,
              fontFamily: "var(--font-mono)",
              fontSize: "0.8rem",
              color: defaultProject
                ? "var(--text-primary)"
                : "var(--text-muted)",
            }}
          >
            <option value="">No default project</option>
            {mounts.map((m) => (
              <option key={m.name} value={m.name}>
                {m.name}
              </option>
            ))}
          </select>
        )}

        <button
          className="btn btn-ghost"
          onClick={() => setShowMountSettings(true)}
          title="Manage mounts"
          style={{ padding: "6px 8px" }}
        >
          <Settings size={16} />
        </button>

        <button
          className="btn btn-ghost"
          onClick={() => setShowArgsEditor((prev) => !prev)}
          title="Workflow variables (args)"
          style={{
            padding: "6px 8px",
            color: argsText.trim() ? "var(--accent)" : undefined,
          }}
        >
          <Variable size={16} />
        </button>

        <div style={{ flex: 1 }} />

        <button
          className="btn btn-ghost"
          onClick={() => addJob(defaultWorkDir)}
        >
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
          {saving
            ? "Saving..."
            : isEditMode
              ? "Update Workflow"
              : "Save Workflow"}
        </button>
      </div>

      {/* ── Args editor ────────────────────── */}
      {showArgsEditor && (
        <div
          style={{
            padding: "var(--sp-3) var(--sp-5)",
            background: "var(--bg-overlay)",
            borderBottom: "1px solid var(--border-default)",
            flexShrink: 0,
          }}
        >
          <div
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.7rem",
              color: "var(--text-muted)",
              textTransform: "uppercase",
              letterSpacing: "0.06em",
              marginBottom: 6,
            }}
          >
            Workflow Variables (args)
          </div>
          <textarea
            className="input"
            rows={3}
            value={argsText}
            onChange={(e) => setArgsText(e.target.value)}
            placeholder={
              "base_dir=/data/experiments\nmodel_name=resnet50\nbatch_size=32"
            }
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.75rem",
              resize: "vertical",
              width: "100%",
              maxWidth: 500,
            }}
          />
          <div
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.6rem",
              color: "var(--text-muted)",
              marginTop: 4,
            }}
          >
            {"Use {{ var_name }} in job commands to reference these variables"}
          </div>
        </div>
      )}

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

      {/* ── Mount settings modal ───────────────── */}
      {showMountSettings && (
        <MountSettings onClose={handleMountSettingsClose} />
      )}
    </div>
  );
}
