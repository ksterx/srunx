"""Non-sweep workflow submission.

Drives the ``POST /api/workflows/{name}/run`` happy path when no
``sweep`` spec is present:

1. Resolve the profile + render the workflow via the canonical
   :func:`~srunx.runtime.rendering.render_workflow_for_submission` helper.
2. Hold the per-mount sync lock across the entire BFS submit via
   :func:`_submission_common.hold_workflow_mounts_web`.
3. BFS-submit each job in topological order, writing
   ``jobs`` / ``job_state_transitions`` / ``workflow_run_jobs`` rows
   atomically.
4. Open a ``kind='workflow_run'`` watch (plus subscription when
   ``notify=true``) so ``ActiveWatchPoller`` can drive aggregate
   transitions.

The class takes a ``profile_resolver`` callable so the router can
forward its own module-level ``_get_current_profile`` — patching that
attribute in tests continues to short-circuit the resolution.
"""

from __future__ import annotations

import functools
import sqlite3
from collections import deque
from collections.abc import Callable
from pathlib import Path
from typing import Any

import anyio
from fastapi import HTTPException, Request

from srunx.common.logging import get_logger
from srunx.domain import Job, ShellJob, Workflow
from srunx.observability.storage.connection import transaction
from srunx.observability.storage.repositories.base import now_iso
from srunx.observability.storage.repositories.job_state_transitions import (
    JobStateTransitionRepository,
)
from srunx.observability.storage.repositories.jobs import JobRepository
from srunx.observability.storage.repositories.watches import WatchRepository
from srunx.observability.storage.repositories.workflow_run_jobs import (
    WorkflowRunJobRepository,
)
from srunx.observability.storage.repositories.workflow_runs import WorkflowRunRepository
from srunx.runtime.rendering import (
    RenderedWorkflow,
    SubmissionRenderContext,
    render_workflow_for_submission,
)
from srunx.runtime.sweep.state_service import WorkflowRunStateService
from srunx.runtime.workflow.runner import WorkflowRunner
from srunx.slurm.ssh import SlurmSSHAdapter

from ..schemas.workflows import WorkflowRunRequest
from ._submission_common import (
    MountSyncFailedError,
    build_submission_context,
    enforce_shell_script_roots,
    hold_workflow_mounts_web,
    reject_python_prefix_web,
)

logger = get_logger(__name__)


def filter_workflow_jobs(
    workflow: Workflow,
    from_job: str | None,
    to_job: str | None,
    single_job: str | None,
) -> list[Job | ShellJob]:
    """Filter workflow jobs based on execution control parameters."""
    all_jobs = {job.name: job for job in workflow.jobs}

    if single_job:
        if single_job not in all_jobs:
            raise HTTPException(422, f"Job '{single_job}' not found in workflow")
        job = all_jobs[single_job]
        # Create a copy-like job with no dependencies for standalone execution
        return [job]

    names = [job.name for job in workflow.jobs]

    start_idx = 0
    end_idx = len(names)

    if from_job:
        if from_job not in all_jobs:
            raise HTTPException(422, f"Job '{from_job}' not found in workflow")
        start_idx = names.index(from_job)

    if to_job:
        if to_job not in all_jobs:
            raise HTTPException(422, f"Job '{to_job}' not found in workflow")
        end_idx = names.index(to_job) + 1

    if from_job and to_job and start_idx >= end_idx:
        raise HTTPException(
            422,
            f"from_job '{from_job}' must appear before to_job '{to_job}' in the workflow",
        )

    selected_names = set(names[start_idx:end_idx])
    return [job for job in workflow.jobs if job.name in selected_names]


def render_workflow(
    yaml_path: Path,
    *,
    submission_context: SubmissionRenderContext,
    args_override: dict[str, Any] | None = None,
    single_job: str | None = None,
) -> RenderedWorkflow:
    """Thin wrapper around :func:`render_workflow_for_submission`."""
    return render_workflow_for_submission(
        yaml_path,
        args_override=args_override,
        context=submission_context,
        single_job=single_job,
    )


def resolve_in_place_target(
    job: Job | ShellJob,
    rendered_text: str,
    profile: Any,
) -> tuple[str, str] | None:
    """Decide whether *job* qualifies for the in-place sbatch path.

    Workflow Phase 2 (#135): only :class:`ShellJob` instances whose
    ``script_path`` resolves under one of the SSH profile's mount
    ``local`` roots, and whose Jinja-rendered bytes still equal the
    on-disk source bytes, can run the user's file verbatim on the
    cluster. Anything else (``Job`` with ``command``, ShellJob outside
    every mount, or rendered output that diverged from source) must
    fall back to the legacy temp-upload path so the rendered artifact
    actually reaches the cluster.

    Returns ``(remote_path, submit_cwd)`` when the in-place path is
    safe, otherwise ``None``. ``submit_cwd`` is the script's parent
    directory on the remote so relative paths inside the user's
    ``#SBATCH`` directives resolve as they would on a head-node
    ``sbatch ./script.sh``. Same ``parent_remote or remote_script``
    fallback the single-job /api/jobs path uses.

    Path security: the workflow's ``_enforce_shell_script_roots``
    guard already rejected scripts outside every allowed root before
    render, so reaching here means the path is safe to translate.
    The mount lookup is a longest-prefix match via
    :func:`resolve_mount_for_path` so nested mounts pick the deepest
    owner.
    """
    from srunx.runtime.submission_plan import (
        render_text_matches_source,
        resolve_mount_for_path,
        translate_local_to_remote,
    )

    if profile is None or not isinstance(job, ShellJob):
        return None

    script_attr = getattr(job, "script_path", None)
    if not script_attr:
        return None

    try:
        source_path = Path(script_attr)
    except (TypeError, ValueError):
        return None

    mount = resolve_mount_for_path(source_path, profile)
    if mount is None:
        return None

    if not render_text_matches_source(rendered_text, source_path):
        return None

    remote_path = translate_local_to_remote(source_path, mount)
    parent_remote, _, _ = remote_path.rpartition("/")
    submit_cwd = parent_remote or remote_path
    return remote_path, submit_cwd


async def submit_jobs_bfs(
    workflow: Workflow,
    scripts: dict[str, str],
    run_opts: WorkflowRunRequest,
    adapter: SlurmSSHAdapter,
    *,
    conn: sqlite3.Connection,
    run_id: int,
    profile: Any = None,
    unrendered_jobs_by_name: dict[str, Job | ShellJob] | None = None,
) -> dict[str, str]:
    """Submit jobs in topological order via BFS, returning {name: slurm_id}.

    Each successful submit is persisted atomically:

    1. ``jobs`` row via :meth:`JobRepository.record_submission` (with
       ``submission_source='workflow'`` + ``workflow_run_id``).
    2. Seed ``job_state_transitions`` with ``PENDING`` so the active
       watch poller's first observation produces a real transition.
    3. Link to the workflow via :meth:`WorkflowRunJobRepository.create`.

    On sbatch failure we record a membership row with ``job_id=None``
    so the response can still reflect the attempted job set, then
    raise.
    """
    job_repo = JobRepository(conn)
    wrj_repo = WorkflowRunJobRepository(conn)
    transition_repo = JobStateTransitionRepository(conn)

    filtered_names = {job.name for job in workflow.jobs}
    job_map: dict[str, Job | ShellJob] = {job.name: job for job in workflow.jobs}
    dependents: dict[str, list[str]] = {job.name: [] for job in workflow.jobs}
    in_degree: dict[str, int] = {
        job.name: len(
            [d for d in job.parsed_dependencies if d.job_name in filtered_names]
        )
        for job in workflow.jobs
    }

    for job in workflow.jobs:
        for dep in job.parsed_dependencies:
            if dep.job_name in filtered_names:
                dependents[dep.job_name].append(job.name)

    queue: deque[str] = deque(
        job.name for job in workflow.jobs if in_degree[job.name] == 0
    )
    submitted: dict[str, str] = {}

    while queue:
        current_name = queue.popleft()
        current_job = job_map[current_name]

        dep_parts: list[str] = []
        if not run_opts.single_job:
            for dep in current_job.parsed_dependencies:
                if dep.job_name in submitted:
                    parent_id = submitted[dep.job_name]
                    dep_parts.append(f"{dep.dep_type}:{parent_id}")
        dependency = ",".join(dep_parts) if dep_parts else None

        depends_on = [
            d.job_name
            for d in current_job.parsed_dependencies
            if d.job_name in filtered_names
        ]

        # The rendered ``current_job`` may have its ``script_path``
        # already translated to remote form by
        # ``_normalize_paths_for_mount``, which makes the in-place
        # mount lookup miss. Use the unrendered job (with its original
        # local path) when the caller threaded one through. See #150
        # root cause.
        in_place_job = (
            unrendered_jobs_by_name.get(current_name, current_job)
            if unrendered_jobs_by_name is not None
            else current_job
        )
        in_place = resolve_in_place_target(in_place_job, scripts[current_name], profile)

        try:
            if in_place is not None:
                remote_path, submit_cwd = in_place

                def _in_place_submit(
                    rp: str = remote_path,
                    cwd: str = submit_cwd,
                    n: str = current_name,
                    d: str | None = dependency,
                ) -> int:
                    submitted_obj = adapter.submit_remote_sbatch(
                        rp,
                        submit_cwd=cwd,
                        job_name=n,
                        dependency=d,
                    )
                    if submitted_obj is None or submitted_obj.job_id is None:
                        raise RuntimeError("remote sbatch returned no job_id")
                    return int(submitted_obj.job_id)

                slurm_id = await anyio.to_thread.run_sync(_in_place_submit)
            else:
                result = await anyio.to_thread.run_sync(
                    lambda s=scripts[current_name],  # type: ignore[misc]
                    n=current_name,
                    d=dependency: adapter.submit_job(s, job_name=n, dependency=d)
                )
                slurm_id = int(result["job_id"])
            submitted[current_name] = str(slurm_id)
        except Exception as exc:
            # R3: record a membership row with ``job_id=None`` so the
            # GET /runs/{id} response still shows the failed node.
            # Best-effort — a write failure must not mask the original
            # sbatch exception.
            try:

                def _record_failed(
                    jname: str = current_name,
                    deps: list[str] = depends_on,
                ) -> None:
                    wrj_repo.create(
                        workflow_run_id=run_id,
                        job_name=jname,
                        depends_on=deps or None,
                        job_id=None,
                    )

                await anyio.to_thread.run_sync(_record_failed)
            except Exception:
                logger.debug(
                    "Failed to record membership for the failed job",
                    exc_info=True,
                )
            raise HTTPException(
                status_code=502,
                detail=f"sbatch failed for '{current_name}': {exc}",
            ) from exc

        # R1: persist the three related rows atomically. On autocommit
        # connections (isolation_level=None) each ``execute`` would
        # otherwise commit on its own — a mid-sequence failure would
        # leave e.g. the jobs row inserted with no transition or
        # membership to match it, breaking poller dedup downstream.
        wf_scheduler_key = adapter.scheduler_key
        if wf_scheduler_key.startswith("ssh:"):
            wf_transport_type = "ssh"
            wf_profile_name: str | None = wf_scheduler_key[len("ssh:") :]
        else:
            wf_transport_type = "local"
            wf_profile_name = None

        def _persist(
            jid: int = slurm_id,
            jname: str = current_name,
            job_obj: Job | ShellJob = current_job,
            deps: list[str] = depends_on,
            tt: str = wf_transport_type,
            pn: str | None = wf_profile_name,
            sk: str = wf_scheduler_key,
        ) -> None:
            resources = getattr(job_obj, "resources", None)
            environment = getattr(job_obj, "environment", None)
            command_val = getattr(job_obj, "command", None)
            with transaction(conn, "IMMEDIATE"):
                job_repo.record_submission(
                    job_id=jid,
                    name=jname,
                    status="PENDING",
                    submission_source="workflow",
                    transport_type=tt,  # type: ignore[arg-type]
                    profile_name=pn,
                    scheduler_key=sk,
                    workflow_run_id=run_id,
                    command=command_val if isinstance(command_val, list) else None,
                    nodes=getattr(resources, "nodes", None) if resources else None,
                    gpus_per_node=(
                        getattr(resources, "gpus_per_node", None) if resources else None
                    ),
                    memory_per_node=(
                        getattr(resources, "memory_per_node", None)
                        if resources
                        else None
                    ),
                    time_limit=(
                        getattr(resources, "time_limit", None) if resources else None
                    ),
                    partition=(
                        getattr(resources, "partition", None) if resources else None
                    ),
                    nodelist=(
                        getattr(resources, "nodelist", None) if resources else None
                    ),
                    conda=(
                        getattr(environment, "conda", None) if environment else None
                    ),
                    venv=(getattr(environment, "venv", None) if environment else None),
                    env_vars=(
                        getattr(environment, "env_vars", None) if environment else None
                    ),
                )
                transition_repo.insert(
                    job_id=jid,
                    from_status=None,
                    to_status="PENDING",
                    source="webhook",
                    scheduler_key=sk,
                )
                wrj_repo.create(
                    workflow_run_id=run_id,
                    job_name=jname,
                    depends_on=deps or None,
                    job_id=jid,
                    scheduler_key=sk,
                )

        await anyio.to_thread.run_sync(_persist)

        for dep_name in dependents[current_name]:
            in_degree[dep_name] -= 1
            if in_degree[dep_name] == 0:
                queue.append(dep_name)

    return submitted


class WorkflowSubmissionService:
    """Submit a non-sweep workflow end-to-end.

    :param profile_resolver: Zero-arg callable → ``ServerProfile | None``.
        Router passes its ``_get_current_profile`` so test patches stay
        effective.
    :param workflow_runner_cls: :class:`WorkflowRunner` class. Router
        passes its module-level import so
        ``patch('srunx.web.routers.workflows.WorkflowRunner', ...)``
        reaches the ``from_yaml`` call.
    """

    def __init__(
        self,
        *,
        profile_resolver: Callable[[], Any],
        terminal_statuses: frozenset[str],
        allowed_presets: tuple[str, ...],
        workflow_runner_cls: Any = WorkflowRunner,
    ) -> None:
        self._profile_resolver = profile_resolver
        self._terminal = terminal_statuses
        self._allowed_presets = allowed_presets
        self._runner_cls = workflow_runner_cls

    async def run(
        self,
        *,
        name: str,
        mount: str,
        yaml_path: Path,
        run_opts: WorkflowRunRequest,
        request: Request,
        adapter: SlurmSSHAdapter,
        conn: sqlite3.Connection,
    ) -> dict[str, Any]:
        """Handle the non-sweep branch of ``POST /{name}/run``.

        Caller has already resolved the YAML path (via
        :meth:`WorkflowStorageService.find_yaml`), validated the name
        regex, and confirmed ``run_opts.sweep is None``.
        """
        # Validate preset up-front — before mounting, rendering, and
        # sbatching. Deferring this check until post-submit means a
        # bogus preset returns 422 with jobs already queued.
        if run_opts.notify and run_opts.preset not in self._allowed_presets:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"Invalid preset '{run_opts.preset}'. "
                    f"Allowed: {self._allowed_presets}"
                ),
            )

        # R7: sanitize structured sweep / args_override payloads.
        if run_opts.args_override:
            reject_python_prefix_web(run_opts.args_override, source="args_override")

        # Load and optionally filter workflow. ``self._runner_cls`` is
        # the router module's ``WorkflowRunner`` attribute so test
        # patches on that attribute replace the ``from_yaml`` target.
        runner_cls = self._runner_cls
        runner = await anyio.to_thread.run_sync(
            lambda: runner_cls.from_yaml(
                yaml_path,
                args_override=run_opts.args_override or None,
            )
        )
        workflow = runner.workflow
        if run_opts.from_job or run_opts.to_job or run_opts.single_job:
            filtered_jobs = filter_workflow_jobs(
                workflow,
                run_opts.from_job,
                run_opts.to_job,
                run_opts.single_job,
            )
            workflow = Workflow(name=workflow.name, jobs=filtered_jobs)

        run_repo = WorkflowRunRepository(conn)
        watch_repo = WatchRepository(conn)

        # Create run record (skip for dry runs)
        run_id: int | None = None
        if not run_opts.dry_run:
            run_id = await anyio.to_thread.run_sync(
                lambda: run_repo.create(
                    workflow_name=name,
                    yaml_path=str(yaml_path),
                    args=runner.args or None,
                    triggered_by="web",
                )
            )

        terminal = self._terminal

        def _fail(reason: str) -> None:
            if run_id is None:
                return
            # Route through WorkflowRunStateService so a status_changed
            # event is emitted (subscribers of the auto-created
            # workflow_run watch then receive a Slack-etc. delivery).
            # Read the current status fresh — the poller may have
            # already advanced the row before the failure fires.
            with transaction(conn, "IMMEDIATE"):
                latest = run_repo.get(run_id)
                if latest is None or latest.status in terminal:
                    return
                WorkflowRunStateService.update(
                    conn=conn,
                    workflow_run_id=run_id,
                    from_status=latest.status,
                    to_status="failed",
                    error=reason,
                    completed_at=now_iso(),
                )

        # Resolve the SSH profile up-front so render + the shell-script
        # guard see the same mount registry the lock-and-submit path
        # will use.
        profile = await anyio.to_thread.run_sync(self._profile_resolver)

        # Phase 2: Render scripts via the canonical helper. Mount
        # translation and template resolution live in
        # :mod:`srunx.runtime.rendering` so Web non-sweep, Web sweep, and MCP
        # share identical semantics.
        submission_context = build_submission_context(mount, profile)
        shell_check_workflow = (
            Workflow(
                name=runner.workflow.name,
                jobs=[j for j in runner.workflow.jobs if j.name == run_opts.single_job],
            )
            if run_opts.single_job
            else runner.workflow
        )
        try:
            await anyio.to_thread.run_sync(
                lambda: enforce_shell_script_roots(
                    shell_check_workflow,
                    mount,
                    profile,
                    profile_resolver=self._profile_resolver,
                )
            )
            rendered = await anyio.to_thread.run_sync(
                lambda: render_workflow(
                    yaml_path,
                    submission_context=submission_context,
                    args_override=run_opts.args_override or None,
                    single_job=run_opts.single_job,
                )
            )
        except HTTPException:
            # 403 shell-script-root violation: propagate without _fail
            # side effects (no jobs queued, no cluster state to roll
            # back).
            raise
        except Exception as exc:
            reason = f"Script rendering failed: {exc}"
            await anyio.to_thread.run_sync(functools.partial(_fail, reason))
            raise HTTPException(status_code=500, detail=reason) from exc

        # When ``from_job`` / ``to_job`` are set the canonical helper
        # doesn't prune; apply the existing filter over the rendered
        # result. The ``single_job`` case is already handled inside the
        # helper.
        if run_opts.from_job or run_opts.to_job:
            filtered_names = {
                j.name
                for j in filter_workflow_jobs(
                    rendered.workflow,
                    run_opts.from_job,
                    run_opts.to_job,
                    None,
                )
            }
            rendered_jobs = tuple(
                rj for rj in rendered.jobs if rj.job.name in filtered_names
            )
        else:
            rendered_jobs = rendered.jobs

        submission_workflow = Workflow(
            name=rendered.workflow.name,
            jobs=[rj.job for rj in rendered_jobs],
        )
        scripts: dict[str, str] = {rj.job.name: rj.script_text for rj in rendered_jobs}

        # Phase 3: Dry run early return
        if run_opts.dry_run:
            job_names_in_wf = {job.name for job in submission_workflow.jobs}
            return {
                "dry_run": True,
                "jobs": [
                    {
                        "name": job.name,
                        "script": scripts.get(job.name, ""),
                        "depends_on": [
                            d.job_name
                            for d in job.parsed_dependencies
                            if d.job_name in job_names_in_wf
                        ],
                        "resources": job.resources.model_dump()
                        if isinstance(job, Job)
                        else {},
                    }
                    for job in submission_workflow.jobs
                ],
                "execution_order": [job.name for job in submission_workflow.jobs],
            }

        # Phase 4: Submit + persist + link + seed transition.
        assert run_id is not None
        try:
            # Pass the UNRENDERED workflow to the lock-acquisition
            # helper so ``collect_touched_mounts`` sees the original
            # local script_paths (mount.local form) rather than the
            # post-render remote form.
            async with hold_workflow_mounts_web(
                runner.workflow, runner, sync_required=True
            ) as locked_profile:
                unrendered_by_name: dict[str, Job | ShellJob] = {
                    j.name: j for j in runner.workflow.jobs
                }
                try:
                    await submit_jobs_bfs(
                        submission_workflow,
                        scripts,
                        run_opts,
                        adapter,
                        conn=conn,
                        run_id=run_id,
                        profile=locked_profile,
                        unrendered_jobs_by_name=unrendered_by_name,
                    )
                except HTTPException as exc:
                    reason = (
                        f"Submission failed: {exc.detail}"
                        if isinstance(exc.detail, str)
                        else "Submission failed"
                    )

                    # R2: cancel any jobs that were already accepted by
                    # sbatch before the failure.
                    def _load_orphan_ids() -> list[int]:
                        memberships = WorkflowRunJobRepository(conn).list_by_run(run_id)
                        return [m.job_id for m in memberships if m.job_id is not None]

                    orphan_ids = await anyio.to_thread.run_sync(_load_orphan_ids)
                    for jid in orphan_ids:
                        try:
                            await anyio.to_thread.run_sync(
                                lambda x=jid: adapter.cancel_job(int(x))  # type: ignore[misc]
                            )
                        except Exception:
                            logger.warning(
                                "Failed to cancel orphan SLURM job %s during "
                                "workflow-run rollback",
                                jid,
                                exc_info=True,
                            )

                    await anyio.to_thread.run_sync(functools.partial(_fail, reason))
                    raise
        except MountSyncFailedError as exc:
            # Lock-acquisition failures land here (raised from
            # ``hold_workflow_mounts_web`` before the body runs). Mark
            # the run as failed with the sync-failure reason.
            reason = f"Mount sync failed: {exc}"
            await anyio.to_thread.run_sync(functools.partial(_fail, reason))
            raise HTTPException(status_code=502, detail=reason) from exc

        # Phase 5: open the workflow_run watch so the poller can drive
        # status transitions going forward.
        def _open_watch() -> int | None:
            new_watch_id = watch_repo.create(
                kind="workflow_run",
                target_ref=f"workflow_run:{run_id}",
            )
            if run_opts.notify and run_opts.endpoint_id is not None:
                from srunx.observability.storage.repositories.endpoints import (
                    EndpointRepository,
                )
                from srunx.observability.storage.repositories.subscriptions import (
                    SubscriptionRepository,
                )

                endpoint = EndpointRepository(conn).get(run_opts.endpoint_id)
                if endpoint is None or endpoint.disabled_at is not None:
                    # Non-fatal: the watch still exists, the run is
                    # open, the user just won't get external
                    # notifications.
                    logger.warning(
                        "workflow_run %s: requested endpoint_id=%s not usable "
                        "(missing or disabled); skipping subscription",
                        run_id,
                        run_opts.endpoint_id,
                    )
                    return new_watch_id
                SubscriptionRepository(conn).create(
                    watch_id=new_watch_id,
                    endpoint_id=run_opts.endpoint_id,
                    preset=run_opts.preset,
                )
            return new_watch_id

        await anyio.to_thread.run_sync(_open_watch)

        def _load_final() -> dict[str, Any]:
            # Local import to avoid a service↔service cycle (query
            # service imports models; submission service imports
            # repositories directly).
            from .workflow_run_query import build_run_response

            final_run = run_repo.get(run_id)  # type: ignore[arg-type]
            if final_run is None:
                return {"id": str(run_id), "status": "pending"}
            return build_run_response(conn, final_run)

        return await anyio.to_thread.run_sync(_load_final)
