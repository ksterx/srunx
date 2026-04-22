"""Canonical entry point for rendering workflow submission scripts.

Previously the render path forked between ``src/srunx/web/routers/workflows.py``
(mount-aware dry-run / submit for Web non-sweep) and
``src/srunx/web/ssh_adapter.py::SlurmSSHAdapter.run`` (mount-agnostic sweep
cell render). This module unifies them: Web submission (both non-sweep
and sweep) and MCP sweep call :func:`render_workflow_for_submission`;
MCP non-sweep still renders via the local ``Slurm`` path. The helper
(a) resolves mount information into the ``Job`` rows before render,
(b) delegates to the existing :func:`srunx.models.render_job_script` for
the template substitution, and (c) returns a :class:`RenderedWorkflow`
with per-job ``script_text`` + metadata.

The render itself stays mount-agnostic — ``Job.work_dir`` /
``Job.log_dir`` (Phase 2 render parity fields) are the single source of
truth for path fields. Translation from local → remote paths happens
*before* the template is invoked; the template never sees mount info.
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from srunx.models import (
    Job,
    RunnableJobType,
    ShellJob,
    Workflow,
    render_job_script,
    render_shell_job_script,
)
from srunx.runner import WorkflowRunner
from srunx.template import get_template_path


@dataclass(frozen=True)
class SubmissionRenderContext:
    """Context that determines how paths in the workflow YAML are resolved.

    Attributes:
        mount_name: Selected mount (Web request's ``?mount=<name>`` query).
            ``None`` means "no mount translation" (e.g. local CLI path).
            Even if ``mounts`` is populated, translation is only performed
            when ``mount_name`` is explicitly set — this keeps the
            semantics unambiguous.
        mounts: Full mounts registry (typically from the configured SSH
            profile). Used to look up the selected mount + translate
            absolute local paths that happen to fall under ``mount.local``
            to the equivalent ``mount.remote`` path. Tuple of
            :class:`~srunx.ssh.core.config.MountConfig`-compatible objects
            (duck-typed on ``.name``, ``.local``, ``.remote``).
        default_work_dir: Optional override for :attr:`Job.work_dir` when
            the job doesn't declare one. For SSH submission this is the
            selected ``mount.remote``; for local CLI it is typically
            ``None`` (renderer keeps its existing ``os.getcwd()`` / config
            fallback).
    """

    mount_name: str | None = None
    mounts: tuple[Any, ...] = field(default_factory=tuple)
    default_work_dir: str | None = None


@dataclass(frozen=True)
class RenderedJob:
    """One job's render output + metadata needed by the submit layer.

    Attributes:
        job: Mount-resolved :class:`Job` (or :class:`ShellJob`) whose
            ``work_dir`` / ``log_dir`` have been rewritten according to
            the :class:`SubmissionRenderContext`. Original ``Workflow``
            passed in is **not** mutated — this is a fresh copy produced
            via ``model_copy(update=...)``.
        script_text: The rendered ``#SBATCH`` script content (UTF-8).
        script_filename: Suggested filename for uploading / writing the
            script, e.g. ``"train.slurm"``. Derived from the job name so
            it stays stable across runs and matches the existing
            ``render_job_script`` output convention.
    """

    job: RunnableJobType
    script_text: str
    script_filename: str


@dataclass(frozen=True)
class RenderedWorkflow:
    """Full render output for one workflow submission."""

    workflow: Workflow
    jobs: tuple[RenderedJob, ...]


def render_workflow_for_submission(
    yaml_path: str | Path,
    *,
    args_override: dict[str, Any] | None = None,
    context: SubmissionRenderContext | None = None,
    single_job: str | None = None,
) -> RenderedWorkflow:
    """Canonical render entry used by Web non-sweep, Web sweep, and MCP.

    Steps (in order):

    1. Load YAML + apply ``args_override`` + resolve ``exports`` /
       ``deps`` (delegates to :meth:`WorkflowRunner.from_yaml`).
    2. For each :class:`Job` (and :class:`ShellJob`), apply ``context``
       path normalization via :func:`_normalize_paths_for_mount`:

       - Fill in ``context.default_work_dir`` when ``Job.work_dir`` is
         missing.
       - Rewrite absolute local paths under ``mount.local`` to the
         equivalent ``mount.remote`` path.
       - Leave relative paths alone (resolved at execution time against
         the working directory set by ``--chdir``).
    3. Render each :class:`Job`'s script via :func:`render_job_script`.
       The canonical render path honours ``Job.template`` (Phase 2 render
       metadata) when no caller-supplied template override is wanted;
       otherwise falls back to the packaged ``base`` template resolved
       via :func:`srunx.template.get_template_path`.
    4. Return a :class:`RenderedWorkflow` with the mount-resolved Jobs
       and their rendered script texts.

    Args:
        yaml_path: Path to the workflow YAML file.
        args_override: Optional ``args`` overrides forwarded to
            :meth:`WorkflowRunner.from_yaml` (merged over the YAML
            ``args`` section before Jinja evaluation).
        context: :class:`SubmissionRenderContext` controlling mount /
            default-path resolution. When ``None`` no translation is
            performed and each job's ``work_dir`` / ``log_dir`` are used
            verbatim — this preserves the local CLI semantics bit-for-bit.
        single_job: Optional filter; when set, :meth:`from_yaml` loads
            only the target job and its transitive dependencies, then
            this function renders **only** the target (no dependency
            siblings) so the returned tuple has exactly one entry.

    Returns:
        :class:`RenderedWorkflow` containing the workflow + per-job
        :class:`RenderedJob` entries in YAML declaration order.

    Raises:
        FileNotFoundError: If the YAML file does not exist.
        WorkflowValidationError: If workflow validation fails.
        jinja2.TemplateError: If template rendering fails.
    """
    # --- 1. Load the workflow (args + deps are resolved here). ---
    runner = WorkflowRunner.from_yaml(
        yaml_path,
        args_override=args_override,
        single_job=single_job,
    )
    workflow = runner.workflow

    # --- 2. Apply mount / default path normalization to each job. ---
    resolved_jobs: list[RunnableJobType] = []
    for job in workflow.jobs:
        if single_job is not None and job.name != single_job:
            continue
        resolved = (
            _normalize_paths_for_mount(job, context) if context is not None else job
        )
        resolved_jobs.append(resolved)

    # Rebuild the Workflow with the resolved jobs so downstream callers
    # that iterate ``rendered.workflow.jobs`` see the same objects as
    # ``rendered.jobs[i].job``. The underlying ``Workflow.__init__``
    # accepts a jobs list and re-runs dependency checks on ``.add`` only,
    # so constructing directly is fine here.
    resolved_workflow = Workflow(name=workflow.name, jobs=resolved_jobs)

    # --- 3. Render each job's SLURM script. ---
    rendered_entries: list[RenderedJob] = []
    with tempfile.TemporaryDirectory() as tmpdir:
        for job in resolved_jobs:
            script_text = _render_one(job, Path(tmpdir))
            rendered_entries.append(
                RenderedJob(
                    job=job,
                    script_text=script_text,
                    script_filename=f"{job.name}.slurm",
                )
            )

    return RenderedWorkflow(
        workflow=resolved_workflow,
        jobs=tuple(rendered_entries),
    )


# ---------------------------------------------------------------------------
# Public helpers (Batch 2a: used by SSH executor for per-job normalization)
# ---------------------------------------------------------------------------


def normalize_job_for_submission(
    job: RunnableJobType,
    context: SubmissionRenderContext | None,
) -> RunnableJobType:
    """Public entry for mount-aware path normalization on a single job.

    Thin alias around :func:`_normalize_paths_for_mount` so callers that
    render a single job at submission time (e.g. :class:`SlurmSSHAdapter.run`)
    can apply the same ``work_dir`` / ``log_dir`` translation as the
    full-workflow :func:`render_workflow_for_submission` path without
    reaching into private module state.

    When ``context`` is ``None`` the job is returned unchanged — matches
    the ``context is None`` branch in :func:`render_workflow_for_submission`
    so the "no translation" semantics stay identical across entry points.
    """
    if context is None:
        return job
    return _normalize_paths_for_mount(job, context)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _render_one(job: RunnableJobType, tmpdir: Path) -> str:
    """Render a single job to a SLURM script string.

    Delegates to :func:`render_job_script` (for :class:`Job`) or
    :func:`render_shell_job_script` (for :class:`ShellJob`). Template
    resolution honours ``Job.template`` when present, else the packaged
    ``base`` template.
    """
    if isinstance(job, Job):
        template_path = job.template if job.template else get_template_path("base")
        rendered_path = render_job_script(
            template_path,
            job,
            output_dir=tmpdir,
        )
    elif isinstance(job, ShellJob):
        # ShellJob uses its own script path as the "template" (existing
        # semantics in ``SlurmSSHAdapter.run``).
        rendered_path = render_shell_job_script(
            job.script_path,
            job,
            output_dir=tmpdir,
        )
    else:  # pragma: no cover — Pydantic union guard
        raise TypeError(f"Unsupported job type: {type(job).__name__}")

    return Path(rendered_path).read_text(encoding="utf-8")


def _normalize_paths_for_mount(
    job: RunnableJobType,
    context: SubmissionRenderContext,
) -> RunnableJobType:
    """Return a Job (or ShellJob) with mount-resolved ``work_dir`` / ``log_dir``.

    Rules:

    1. ``work_dir``:

       - If empty / ``None`` → ``context.default_work_dir`` (if set);
         otherwise leave unchanged.
       - If absolute path under any ``context.mounts[i].local`` **and**
         ``context.mount_name`` is set → rewrite to ``mount.remote`` +
         relative tail.
       - If absolute but not under any mount → leave unchanged (user
         knows what they're doing).
       - If relative → leave unchanged (interpreted relative to cwd at
         run time).

    2. ``log_dir``:

       - If empty / ``None`` → leave unchanged (template falls back).
       - Absolute local under a mount → translate to remote.
       - Relative → leave unchanged (relative to work_dir).

    Only :class:`Job` carries ``work_dir`` / ``log_dir``; :class:`ShellJob`
    is returned as-is.
    """
    if not isinstance(job, Job):
        return job

    updates: dict[str, str] = {}

    # --- work_dir ---
    new_work_dir = _resolve_work_dir(job.work_dir, context)
    if new_work_dir is not None and new_work_dir != job.work_dir:
        updates["work_dir"] = new_work_dir

    # --- log_dir ---
    new_log_dir = _resolve_log_dir(job.log_dir, context)
    if new_log_dir is not None and new_log_dir != job.log_dir:
        updates["log_dir"] = new_log_dir

    if not updates:
        return job
    return job.model_copy(update=updates)


def _resolve_work_dir(
    current: str | None,
    context: SubmissionRenderContext,
) -> str | None:
    """Compute the effective ``work_dir`` under *context*.

    Returns ``None`` when no change is needed (caller short-circuits).
    """
    # Missing → fill in default when available.
    if not current:
        if context.default_work_dir:
            return context.default_work_dir
        return None

    # Relative → leave alone.
    if not _is_absolute(current):
        return None

    # Absolute → try mount translation (only when mount_name is set).
    translated = _translate_abs_path(current, context)
    return translated  # may be None (no match / no mount selected)


def _resolve_log_dir(
    current: str | None,
    context: SubmissionRenderContext,
) -> str | None:
    """Compute the effective ``log_dir`` under *context*.

    ``log_dir`` differs from ``work_dir`` in two ways:

    * No ``default_*`` injection — the template already has a sensible
      fallback (``%x_%j.log`` in the cwd when ``log_dir`` is empty).
    * Empty / missing stays empty.
    """
    if not current:
        return None
    if not _is_absolute(current):
        return None
    return _translate_abs_path(current, context)


def _is_absolute(path: str) -> bool:
    """Return True if *path* is a POSIX-style absolute path.

    We intentionally use ``startswith('/')`` rather than
    :meth:`Path.is_absolute` because the render targets a remote SLURM
    host (always POSIX semantics), and running on a Windows dev machine
    should still classify ``/opt/foo`` as absolute.
    """
    return path.startswith("/")


def _translate_abs_path(
    path: str,
    context: SubmissionRenderContext,
) -> str | None:
    """Translate an absolute local path to its remote equivalent.

    Only performs translation when ``context.mount_name`` is set — the
    mount registry alone is not enough to disambiguate (multiple mounts
    may share ancestor prefixes). Returns ``None`` when the path is not
    under the selected mount.

    The translation preserves any trailing relative segment:
    ``/users/foo/project/logs`` under a mount of
    (local=``/users/foo/project``, remote=``/home/u/project``) becomes
    ``/home/u/project/logs``.
    """
    if context.mount_name is None:
        return None

    mount = _find_mount_by_name(context.mounts, context.mount_name)
    if mount is None:
        return None

    try:
        local_root = Path(mount.local).resolve()
        candidate = Path(path).resolve()
    except (OSError, RuntimeError):
        # ``resolve()`` can raise on broken symlinks in strict mode (we
        # don't pass strict=True) or on invalid chars; fall back to the
        # raw string comparison below.
        local_root = Path(mount.local)
        candidate = Path(path)

    try:
        rel = candidate.relative_to(local_root)
    except ValueError:
        return None

    # Use PurePosixPath semantics for the remote join so Windows dev
    # machines don't emit backslashes.
    rel_posix = rel.as_posix()
    remote_root = mount.remote.rstrip("/")
    if rel_posix in ("", "."):
        return remote_root
    return f"{remote_root}/{rel_posix}"


def _find_mount_by_name(
    mounts: tuple[Any, ...],
    name: str,
) -> Any | None:
    """Linear lookup in the mount registry. Returns ``None`` if absent.

    Duck-typed on ``.name`` so callers can pass either the real
    :class:`~srunx.ssh.core.config.MountConfig` or a lightweight test
    double.
    """
    for m in mounts:
        if getattr(m, "name", None) == name:
            return m
    return None
