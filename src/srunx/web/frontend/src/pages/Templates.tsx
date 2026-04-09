import { useState, useEffect, useCallback } from "react";
import { motion } from "framer-motion";
import {
  FileCode2,
  Play,
  Loader2,
  ChevronDown,
  ChevronUp,
  Plus,
  Trash2,
  Pencil,
  X,
} from "lucide-react";
import { templates as templatesApi } from "../lib/api.ts";
import type { TemplateListItem, TemplateDetail } from "../lib/types.ts";
import { ErrorBanner } from "../components/ErrorBanner.tsx";
import { ScriptPreview } from "../components/ScriptPreview.tsx";

type ApplyForm = {
  command: string;
  job_name: string;
  nodes: string;
  gpus_per_node: string;
  memory_per_node: string;
  time_limit: string;
  partition: string;
  conda: string;
};

const EMPTY_FORM: ApplyForm = {
  command: "",
  job_name: "job",
  nodes: "1",
  gpus_per_node: "0",
  memory_per_node: "",
  time_limit: "",
  partition: "",
  conda: "",
};

type TemplateForm = {
  name: string;
  description: string;
  use_case: string;
  content: string;
};

const EMPTY_TEMPLATE_FORM: TemplateForm = {
  name: "",
  description: "",
  use_case: "",
  content:
    "#!/bin/bash\n\n#SBATCH --job-name={{ job_name }}\n#SBATCH --nodes={{ nodes }}\n\nsrun {{ command }}\n",
};

export function Templates() {
  const [items, setItems] = useState<TemplateListItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [detail, setDetail] = useState<TemplateDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [showSource, setShowSource] = useState(false);
  const [form, setForm] = useState<ApplyForm>(EMPTY_FORM);
  const [submitting, setSubmitting] = useState(false);
  const [commandError, setCommandError] = useState(false);

  // Template CRUD state
  const [showCreateDialog, setShowCreateDialog] = useState(false);
  const [editingTemplate, setEditingTemplate] = useState<string | null>(null);
  const [templateForm, setTemplateForm] =
    useState<TemplateForm>(EMPTY_TEMPLATE_FORM);
  const [saving, setSaving] = useState(false);
  const [deleting, setDeleting] = useState<string | null>(null);

  const loadList = useCallback(async () => {
    try {
      setLoading(true);
      const res = await templatesApi.list();
      setItems(res);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load templates");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadList();
  }, [loadList]);

  useEffect(() => {
    if (success) {
      const t = setTimeout(() => setSuccess(null), 4000);
      return () => clearTimeout(t);
    }
  }, [success]);

  const handleSelect = async (name: string) => {
    if (selected === name) {
      setSelected(null);
      setDetail(null);
      return;
    }
    setSelected(name);
    setDetailLoading(true);
    try {
      const d = await templatesApi.get(name);
      setDetail(d);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load template");
    } finally {
      setDetailLoading(false);
    }
  };

  const handleCreate = async () => {
    if (!templateForm.name.trim() || !templateForm.content.trim()) return;
    setSaving(true);
    setError(null);
    try {
      await templatesApi.create(templateForm);
      setSuccess(`Template "${templateForm.name}" created`);
      setShowCreateDialog(false);
      setTemplateForm(EMPTY_TEMPLATE_FORM);
      await loadList();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to create template");
    } finally {
      setSaving(false);
    }
  };

  const handleEdit = async (name: string) => {
    try {
      const d = await templatesApi.get(name);
      setEditingTemplate(name);
      setTemplateForm({
        name: d.name,
        description: d.description,
        use_case: d.use_case,
        content: d.content,
      });
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load template");
    }
  };

  const handleUpdate = async () => {
    if (!editingTemplate) return;
    setSaving(true);
    setError(null);
    try {
      await templatesApi.update(editingTemplate, {
        description: templateForm.description,
        use_case: templateForm.use_case,
        content: templateForm.content,
      });
      setSuccess(`Template "${editingTemplate}" updated`);
      setEditingTemplate(null);
      setTemplateForm(EMPTY_TEMPLATE_FORM);
      if (selected === editingTemplate) {
        const d = await templatesApi.get(editingTemplate);
        setDetail(d);
      }
      await loadList();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to update template");
    } finally {
      setSaving(false);
    }
  };

  const handleDelete = async (name: string) => {
    setDeleting(name);
    setError(null);
    try {
      await templatesApi.delete(name);
      setSuccess(`Template "${name}" deleted`);
      if (selected === name) {
        setSelected(null);
        setDetail(null);
      }
      await loadList();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to delete template");
    } finally {
      setDeleting(null);
    }
  };

  const buildPreviewRequest = () => {
    const cmd = form.command.split(/\s+/).filter(Boolean);
    return {
      name: form.job_name || "job",
      command: cmd,
      resources: {
        nodes: form.nodes ? Number(form.nodes) : 1,
        gpus_per_node: form.gpus_per_node ? Number(form.gpus_per_node) : 0,
        memory_per_node: form.memory_per_node || null,
        time_limit: form.time_limit || null,
        partition: form.partition || null,
      },
      environment: {
        conda: form.conda || null,
      },
      template_name: selected ?? undefined,
    };
  };

  const handleSubmit = async () => {
    if (!selected) return;
    if (!form.command.trim()) {
      setCommandError(true);
      return;
    }
    setSubmitting(true);
    setError(null);
    try {
      const cmd = form.command.split(/\s+/).filter(Boolean);
      const result = await templatesApi.apply(selected, {
        command: cmd,
        job_name: form.job_name || "job",
        resources: {
          nodes: form.nodes ? Number(form.nodes) : 1,
          gpus_per_node: form.gpus_per_node ? Number(form.gpus_per_node) : 0,
          memory_per_node: form.memory_per_node || null,
          time_limit: form.time_limit || null,
          partition: form.partition || null,
        },
        environment: {
          conda: form.conda || null,
        },
      });
      const jobId = result.job_id;
      setSuccess(
        `Job submitted${jobId ? ` (ID: ${jobId})` : ""} using template "${selected}"`,
      );
      setForm(EMPTY_FORM);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Submit failed");
    } finally {
      setSubmitting(false);
    }
  };

  if (loading) {
    return (
      <div style={{ padding: "var(--sp-6)" }}>
        <div className="panel skeleton" style={{ height: 300 }} />
      </div>
    );
  }

  return (
    <div
      style={{
        padding: "var(--sp-6)",
        display: "flex",
        flexDirection: "column",
        gap: "var(--sp-4)",
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
        }}
      >
        <h2 style={{ fontSize: "1.1rem", fontWeight: 600 }}>Job Templates</h2>
        <button
          className="btn btn-primary"
          onClick={() => {
            setShowCreateDialog(true);
            setTemplateForm(EMPTY_TEMPLATE_FORM);
          }}
          style={{ gap: 6 }}
        >
          <Plus size={14} />
          New Template
        </button>
      </div>

      <ErrorBanner error={error} />
      {success && (
        <motion.div
          initial={{ opacity: 0, y: -8 }}
          animate={{ opacity: 1, y: 0 }}
          style={{
            padding: "var(--sp-3) var(--sp-4)",
            background: "var(--st-completed-dim)",
            border: "1px solid rgba(52,211,153,0.3)",
            borderRadius: "var(--radius-md)",
            color: "var(--st-completed)",
            fontFamily: "var(--font-mono)",
            fontSize: "0.8rem",
          }}
        >
          {success}
        </motion.div>
      )}

      {/* Template cards */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))",
          gap: "var(--sp-4)",
        }}
      >
        {items.map((t) => {
          const isSelected = selected === t.name;
          const isUser = t.user_defined === true;
          return (
            <motion.div
              key={t.name}
              className="panel"
              onClick={() => handleSelect(t.name)}
              style={{
                cursor: "pointer",
                borderColor: isSelected ? "var(--accent)" : undefined,
              }}
              whileHover={{ scale: 1.01 }}
            >
              <div className="panel-header">
                <div
                  style={{
                    display: "flex",
                    alignItems: "center",
                    gap: "var(--sp-2)",
                    flex: 1,
                  }}
                >
                  <FileCode2 size={14} />
                  <h3
                    style={{
                      textTransform: "none",
                      letterSpacing: 0,
                      fontSize: "0.85rem",
                    }}
                  >
                    {t.name}
                  </h3>
                  {isUser && (
                    <span
                      style={{
                        fontSize: "0.6rem",
                        padding: "1px 6px",
                        borderRadius: "var(--radius-sm)",
                        background: "var(--accent-dim)",
                        color: "var(--accent)",
                        fontFamily: "var(--font-mono)",
                      }}
                    >
                      custom
                    </span>
                  )}
                </div>
                {isUser && (
                  <div
                    style={{
                      display: "flex",
                      gap: "var(--sp-1)",
                    }}
                  >
                    <button
                      className="btn btn-ghost"
                      onClick={(e) => {
                        e.stopPropagation();
                        handleEdit(t.name);
                      }}
                      style={{ padding: 4 }}
                    >
                      <Pencil size={12} />
                    </button>
                    <button
                      className="btn btn-ghost"
                      onClick={(e) => {
                        e.stopPropagation();
                        handleDelete(t.name);
                      }}
                      disabled={deleting === t.name}
                      style={{ padding: 4, color: "var(--st-failed)" }}
                    >
                      {deleting === t.name ? (
                        <Loader2 size={12} className="spin" />
                      ) : (
                        <Trash2 size={12} />
                      )}
                    </button>
                  </div>
                )}
              </div>
              <div className="panel-body">
                <p
                  style={{
                    fontSize: "0.8rem",
                    color: "var(--text-secondary)",
                    margin: 0,
                  }}
                >
                  {t.description}
                </p>
                <p
                  style={{
                    fontSize: "0.75rem",
                    color: "var(--text-muted)",
                    margin: "var(--sp-2) 0 0",
                    fontFamily: "var(--font-mono)",
                  }}
                >
                  {t.use_case}
                </p>
              </div>
            </motion.div>
          );
        })}
      </div>

      {/* Selected template detail */}
      {selected && (
        <motion.div
          className="panel"
          initial={{ opacity: 0, y: 8 }}
          animate={{ opacity: 1, y: 0 }}
        >
          <div className="panel-header">
            <h3 style={{ textTransform: "none", letterSpacing: 0 }}>
              {selected}
            </h3>
            <button
              className="btn btn-ghost"
              onClick={() => setShowSource(!showSource)}
              style={{ fontSize: "0.7rem", gap: 4 }}
            >
              {showSource ? <ChevronUp size={12} /> : <ChevronDown size={12} />}
              Template Source
            </button>
          </div>
          <div className="panel-body">
            {detailLoading && (
              <div className="skeleton" style={{ height: 100 }} />
            )}

            {/* Template source */}
            {showSource && detail && (
              <pre
                style={{
                  background: "var(--bg-base)",
                  border: "1px solid var(--border-ghost)",
                  borderRadius: "var(--radius-md)",
                  padding: "var(--sp-3) var(--sp-4)",
                  fontFamily: "var(--font-mono)",
                  fontSize: "0.7rem",
                  lineHeight: 1.6,
                  color: "var(--text-secondary)",
                  overflow: "auto",
                  maxHeight: 300,
                  marginBottom: "var(--sp-4)",
                }}
              >
                {detail.content}
              </pre>
            )}

            {/* Apply form */}
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "1fr 1fr",
                gap: "var(--sp-3)",
              }}
            >
              <div style={{ gridColumn: "1 / -1" }}>
                <label
                  style={{
                    fontSize: "0.7rem",
                    color: "var(--text-muted)",
                    textTransform: "uppercase",
                    letterSpacing: "0.08em",
                  }}
                >
                  Command <span style={{ color: "var(--st-failed)" }}>*</span>
                </label>
                <input
                  className="input"
                  placeholder="python train.py --epochs 10"
                  value={form.command}
                  onChange={(e) => {
                    setForm({ ...form, command: e.target.value });
                    if (e.target.value.trim()) setCommandError(false);
                  }}
                  style={{
                    width: "100%",
                    marginTop: 4,
                    fontFamily: "var(--font-mono)",
                    borderColor: commandError ? "var(--st-failed)" : undefined,
                  }}
                />
                {commandError && (
                  <span
                    style={{
                      color: "var(--st-failed)",
                      fontSize: "0.75rem",
                      marginTop: 4,
                      display: "block",
                    }}
                  >
                    Enter a command to preview or submit
                  </span>
                )}
              </div>
              {(
                [
                  ["job_name", "Job Name"],
                  ["nodes", "Nodes"],
                  ["gpus_per_node", "GPUs/Node"],
                  ["memory_per_node", "Memory"],
                  ["time_limit", "Time Limit"],
                  ["partition", "Partition"],
                  ["conda", "Conda Env"],
                ] as const
              ).map(([field, label]) => (
                <div key={field}>
                  <label
                    style={{
                      fontSize: "0.7rem",
                      color: "var(--text-muted)",
                      textTransform: "uppercase",
                      letterSpacing: "0.08em",
                    }}
                  >
                    {label}
                  </label>
                  <input
                    className="input"
                    value={form[field]}
                    onChange={(e) =>
                      setForm({ ...form, [field]: e.target.value })
                    }
                    style={{ width: "100%", marginTop: 4 }}
                  />
                </div>
              ))}
            </div>

            {/* Preview + Submit */}
            <div
              style={{
                display: "flex",
                alignItems: "flex-start",
                gap: "var(--sp-3)",
                marginTop: "var(--sp-4)",
                flexDirection: "column",
              }}
            >
              <ScriptPreview
                getRequest={buildPreviewRequest}
                disabled={!form.command.trim()}
                onValidationError={() => setCommandError(true)}
              />
              <button
                className="btn btn-primary"
                onClick={handleSubmit}
                disabled={submitting}
                style={{ gap: 6 }}
              >
                {submitting ? (
                  <Loader2 size={14} className="spin" />
                ) : (
                  <Play size={14} />
                )}
                Submit Job
              </button>
            </div>
          </div>
        </motion.div>
      )}
      {/* Create / Edit dialog */}
      {(showCreateDialog || editingTemplate) && (
        <div
          style={{
            position: "fixed",
            inset: 0,
            background: "rgba(0,0,0,0.5)",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            zIndex: 100,
          }}
          onClick={() => {
            setShowCreateDialog(false);
            setEditingTemplate(null);
            setTemplateForm(EMPTY_TEMPLATE_FORM);
          }}
        >
          <motion.div
            className="panel"
            initial={{ opacity: 0, scale: 0.95 }}
            animate={{ opacity: 1, scale: 1 }}
            onClick={(e) => e.stopPropagation()}
            style={{
              width: "min(640px, 90vw)",
              maxHeight: "85vh",
              overflow: "auto",
            }}
          >
            <div className="panel-header">
              <h3 style={{ textTransform: "none", letterSpacing: 0 }}>
                {editingTemplate ? `Edit: ${editingTemplate}` : "New Template"}
              </h3>
              <button
                className="btn btn-ghost"
                onClick={() => {
                  setShowCreateDialog(false);
                  setEditingTemplate(null);
                  setTemplateForm(EMPTY_TEMPLATE_FORM);
                }}
                style={{ padding: 4 }}
              >
                <X size={14} />
              </button>
            </div>
            <div
              className="panel-body"
              style={{
                display: "flex",
                flexDirection: "column",
                gap: "var(--sp-3)",
              }}
            >
              {!editingTemplate && (
                <div>
                  <label
                    style={{
                      fontSize: "0.7rem",
                      color: "var(--text-muted)",
                      textTransform: "uppercase",
                      letterSpacing: "0.08em",
                    }}
                  >
                    Name <span style={{ color: "var(--st-failed)" }}>*</span>
                  </label>
                  <input
                    className="input"
                    placeholder="my-template"
                    value={templateForm.name}
                    onChange={(e) =>
                      setTemplateForm({ ...templateForm, name: e.target.value })
                    }
                    style={{
                      width: "100%",
                      marginTop: 4,
                      fontFamily: "var(--font-mono)",
                    }}
                  />
                </div>
              )}
              <div>
                <label
                  style={{
                    fontSize: "0.7rem",
                    color: "var(--text-muted)",
                    textTransform: "uppercase",
                    letterSpacing: "0.08em",
                  }}
                >
                  Description
                </label>
                <input
                  className="input"
                  placeholder="What this template does"
                  value={templateForm.description}
                  onChange={(e) =>
                    setTemplateForm({
                      ...templateForm,
                      description: e.target.value,
                    })
                  }
                  style={{ width: "100%", marginTop: 4 }}
                />
              </div>
              <div>
                <label
                  style={{
                    fontSize: "0.7rem",
                    color: "var(--text-muted)",
                    textTransform: "uppercase",
                    letterSpacing: "0.08em",
                  }}
                >
                  Use Case
                </label>
                <input
                  className="input"
                  placeholder="e.g. Single GPU training jobs"
                  value={templateForm.use_case}
                  onChange={(e) =>
                    setTemplateForm({
                      ...templateForm,
                      use_case: e.target.value,
                    })
                  }
                  style={{ width: "100%", marginTop: 4 }}
                />
              </div>
              <div>
                <label
                  style={{
                    fontSize: "0.7rem",
                    color: "var(--text-muted)",
                    textTransform: "uppercase",
                    letterSpacing: "0.08em",
                  }}
                >
                  Template Content (Jinja2){" "}
                  <span style={{ color: "var(--st-failed)" }}>*</span>
                </label>
                <textarea
                  className="input"
                  value={templateForm.content}
                  onChange={(e) =>
                    setTemplateForm({
                      ...templateForm,
                      content: e.target.value,
                    })
                  }
                  style={{
                    width: "100%",
                    marginTop: 4,
                    fontFamily: "var(--font-mono)",
                    fontSize: "0.75rem",
                    minHeight: 240,
                    resize: "vertical",
                    lineHeight: 1.6,
                  }}
                />
              </div>
              <div
                style={{
                  display: "flex",
                  gap: "var(--sp-2)",
                  justifyContent: "flex-end",
                }}
              >
                <button
                  className="btn btn-ghost"
                  onClick={() => {
                    setShowCreateDialog(false);
                    setEditingTemplate(null);
                    setTemplateForm(EMPTY_TEMPLATE_FORM);
                  }}
                >
                  Cancel
                </button>
                <button
                  className="btn btn-primary"
                  onClick={editingTemplate ? handleUpdate : handleCreate}
                  disabled={
                    saving ||
                    (!editingTemplate && !templateForm.name.trim()) ||
                    !templateForm.content.trim()
                  }
                  style={{ gap: 6 }}
                >
                  {saving && <Loader2 size={14} className="spin" />}
                  {editingTemplate ? "Update" : "Create"}
                </button>
              </div>
            </div>
          </motion.div>
        </div>
      )}
    </div>
  );
}
