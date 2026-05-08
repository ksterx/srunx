"""Canonical entry point for rendering workflow submission scripts.

Previously the render path forked between ``src/srunx/web/routers/workflows.py``
(mount-aware dry-run / submit for Web non-sweep) and
``src/srunx/web/ssh_adapter.py::SlurmSSHAdapter.run`` (mount-agnostic sweep
cell render). This module unifies them: Web submission (both non-sweep
and sweep) and MCP sweep call :func:`render_workflow_for_submission`;
MCP non-sweep still renders via the local ``Slurm`` path. The helper
(a) resolves mount information into the ``Job`` rows before render,
(b) delegates to the existing :func:`srunx.domain.render_job_script` for
the template substitution, and (c) returns a :class:`RenderedWorkflow`
with per-job ``script_text`` + metadata.

The render itself stays mount-agnostic — ``Job.work_dir`` /
``Job.log_dir`` (Phase 2 render parity fields) are the single source of
truth for path fields. Translation from local → remote paths happens
*before* the template is invoked; the template never sees mount info.
"""

from __future__ import annotations

import shlex
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import jinja2
from rich.console import Console
from rich.syntax import Syntax

from srunx.common.logging import get_logger
from srunx.domain import Job, JobEnvironment, RunnableJobType, ShellJob, Workflow
from srunx.runtime.templates import get_template_path

# NOTE: ``srunx.runtime.workflow.runner`` is imported lazily inside
# :func:`render_workflow_for_submission` to avoid a circular import.
# ``runner`` imports ``srunx.callbacks`` which (via ``srunx.__init__``)
# resolves ``srunx.domain`` — now a shim that re-exports from this
# module. Proper fix lands in Phase 5 (#161) when callbacks move out
# of the import hot path.

logger = get_logger(__name__)
console = Console()


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
    allow_in_place: bool = False
    """Permission flag for the IN_PLACE submission path.

    Workflow Phase 2 (#135): :meth:`SlurmSSHAdapter.run` only takes
    the IN_PLACE shortcut (sbatch the user's mount-resident script
    verbatim, skipping the temp upload) when this flag is True,
    AND the source bytes equal the rendered bytes, AND the source
    sits under one of the adapter's mounts.

    Default ``False`` so callers that haven't grabbed the per-mount
    sync lock (Web ``/api/workflows/run`` today, MCP sweep cells)
    can't accidentally race a concurrent rsync. The CLI workflow
    runner — which holds the lock for the lifetime of the run via
    :func:`srunx.cli.workflow.mounts._hold_workflow_mounts` — flips this
    to ``True`` when constructing the context. Closes Codex
    blocker #3 on PR #141."""

    locked_mount_names: tuple[str, ...] = ()
    """Names of mounts the caller is currently holding the sync lock for.

    Defence-in-depth for the sweep IN_PLACE path (#143). The CLI
    workflow runner already aggregates mounts across every sweep
    cell via :func:`collect_touched_mounts_across_cells` so the
    lock-set should contain every mount any cell can touch. This
    field is the safety-net: if a buggy / racy cell renders to a
    mount we somehow missed, the SSH adapter rejects the IN_PLACE
    path with a "mount X not locked" error instead of silently
    racing rsync. Empty tuple = no enforcement (preserves all
    pre-#143 callers verbatim, including non-sweep workflows where
    the lock-set is computed from a single base render and the
    safety net would just add noise)."""


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
       via :func:`srunx.runtime.templates.get_template_path`.
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
    from srunx.runtime.workflow.runner import WorkflowRunner

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


# ---------------------------------------------------------------------------
# SLURM script rendering helpers (moved from :mod:`srunx.domain` in Phase 3).
# ---------------------------------------------------------------------------


def _render_base_script(
    template_path: Path | str,
    template_vars: dict,
    output_filename: str,
    output_dir: Path | str | None = None,
    verbose: bool = False,
) -> str:
    """Base function for rendering SLURM scripts from templates.

    Args:
        template_path: Path to the Jinja template file.
        template_vars: Variables to pass to the template.
        output_filename: Name of the output file.
        output_dir: Directory where the generated script will be saved.
        verbose: Whether to print the rendered content.

    Returns:
        Path to the generated SLURM batch script.

    Raises:
        FileNotFoundError: If the template file does not exist.
        jinja2.TemplateError: If template rendering fails.
    """
    template_file = Path(template_path)
    if not template_file.is_file():
        raise FileNotFoundError(f"Template file '{template_path}' not found")

    with open(template_file, encoding="utf-8") as f:
        template_content = f.read()

    # ``keep_trailing_newline=True`` so the rendered output preserves
    # the source file's trailing ``\n`` instead of silently stripping
    # it (Jinja's default). Workflow Phase 2's IN_PLACE eligibility
    # check relies on ``rendered_bytes == source_bytes``; with the
    # default behaviour, every script ending in ``\n`` (i.e. every
    # POSIX-conforming shell script) compared as different and the
    # in-place path was effectively dead. Codex blocker #2 on PR #141.
    template = jinja2.Template(
        template_content,
        undefined=jinja2.StrictUndefined,
        keep_trailing_newline=True,
    )

    # Debug: log template variables
    logger.debug(f"Template variables: {template_vars}")

    rendered_content = template.render(template_vars)

    if verbose:
        console.print(
            Syntax(rendered_content, "bash", theme="monokai", line_numbers=True)
        )

    # Generate output file
    if output_dir is not None:
        output_path = Path(output_dir) / output_filename
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(rendered_content)

        return str(output_path)

    else:
        logger.info("`output_dir` is not specified, rendered content is not saved")
        return ""


def render_job_script(
    template_path: Path | str,
    job: Job,
    output_dir: Path | str | None = None,
    verbose: bool = False,
    extra_srun_args: str | None = None,
    extra_launch_prefix: str | None = None,
) -> str:
    """Render a SLURM job script from a template.

    Args:
        template_path: Path to the Jinja template file.
        job: Job configuration.
        output_dir: Directory where the generated script will be saved.
        verbose: Whether to print the rendered content.
        extra_srun_args: Additional srun flags to append after auto-generated ones.
        extra_launch_prefix: Additional launch prefix to append after auto-generated ones.

    Returns:
        Path to the generated SLURM batch script.

    Raises:
        FileNotFoundError: If the template file does not exist.
        jinja2.TemplateError: If template rendering fails.
    """
    # Prepare template variables.
    #
    # ``list[str]`` commands are rendered with ``shlex.join`` so shell
    # metacharacters stay quoted. The previous ``" ".join(...)`` silently
    # collapsed commands like ``["bash", "-c", "echo X; sleep 5; echo Y"]``
    # into ``bash -c echo X; sleep 5; echo Y`` — the third argument's shell
    # metacharacters escaped their intended ``-c`` payload and got
    # reinterpreted by the outer shell. ``shlex.join`` preserves the
    # original token boundaries via minimal single-quote wrapping.
    if isinstance(job.command, str):
        command_str = job.command
    else:
        command_str = shlex.join(job.command or [])
    environment_setup, srun_args, launch_prefix = _build_environment_setup(
        job.environment
    )
    # Fallback to Job-level metadata when explicit args absent.
    # Explicit args always win to preserve existing Web non-sweep path
    # (which passes extras from raw YAML directly).
    effective_extra_srun_args = (
        extra_srun_args if extra_srun_args is not None else job.srun_args
    )
    effective_extra_launch_prefix = (
        extra_launch_prefix if extra_launch_prefix is not None else job.launch_prefix
    )
    # Merge user-specified extras with auto-generated values
    if effective_extra_srun_args:
        srun_args = f"{srun_args} {effective_extra_srun_args}".strip()
    if effective_extra_launch_prefix:
        launch_prefix = f"{launch_prefix} {effective_extra_launch_prefix}".strip()

    template_vars = {
        "job_name": job.name,
        "command": command_str,
        "log_dir": job.log_dir,
        "work_dir": job.work_dir,
        "environment_setup": environment_setup,
        "srun_args": srun_args,
        "launch_prefix": launch_prefix,
        "container": job.environment.container,
        **job.resources.model_dump(),
    }

    return _render_base_script(
        template_path=template_path,
        template_vars=template_vars,
        output_filename=f"{job.name}.slurm",
        output_dir=output_dir,
        verbose=verbose,
    )


def _build_environment_setup(
    environment: JobEnvironment,
) -> tuple[str, str, str]:
    """Build environment setup script.

    Returns:
        A 3-tuple of (env_setup_lines, srun_args, launch_prefix).
        - env_setup_lines: Shell setup including env vars, conda/venv activation,
          and container prelude (if any).
        - srun_args: Flags passed to srun (Pyxis uses this).
        - launch_prefix: Command wrapper (Apptainer uses this).
    """
    from srunx.containers import get_runtime

    setup_lines: list[str] = []

    # 1. Environment variables (single-quoted to prevent shell injection)
    for key, value in environment.env_vars.items():
        escaped_value = str(value).replace("'", "'\\''")
        setup_lines.append(f"export {key}='{escaped_value}'")

    # 2. Conda/venv activation (independent of container)
    if environment.conda:
        home_dir = Path.home()
        escaped_conda = environment.conda.replace("'", "'\\''")
        setup_lines.extend(
            [
                f"source {str(home_dir)}/miniconda3/bin/activate",
                "conda deactivate",
                f"conda activate '{escaped_conda}'",
            ]
        )
    elif environment.venv:
        escaped_venv = environment.venv.replace("'", "'\\''")
        setup_lines.append(f"source '{escaped_venv}'/bin/activate")

    # 3. Container setup (independent of conda/venv)
    # Only process container if it has an image — a runtime-only container
    # (no image) is not actionable and would generate broken commands.
    srun_args = ""
    launch_prefix = ""
    if environment.container and environment.container.image:
        runtime = get_runtime(environment.container.runtime)
        spec = runtime.build_launch_spec(environment.container)
        if spec.prelude:
            setup_lines.append(spec.prelude)
        srun_args = spec.srun_args
        launch_prefix = spec.launch_prefix

    return "\n".join(setup_lines), srun_args, launch_prefix


def render_shell_job_script(
    template_path: Path | str,
    job: ShellJob,
    output_dir: Path | str | None = None,
    verbose: bool = False,
) -> str:
    """Render a SLURM shell job script from a template.

    Args:
        template_path: Path to the Jinja template file.
        job: ShellJob configuration.
        output_dir: Directory where the generated script will be saved.
        verbose: Whether to print the rendered content.

    Returns:
        Path to the generated SLURM batch script.

    Raises:
        FileNotFoundError: If the template file does not exist.
        jinja2.TemplateError: If template rendering fails.
    """
    template_file = Path(template_path)
    output_filename = f"{template_file.stem}.slurm"

    return _render_base_script(
        template_path=template_path,
        template_vars=job.script_vars,
        output_filename=output_filename,
        output_dir=output_dir,
        verbose=verbose,
    )
