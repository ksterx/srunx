"""Workflow file CRUD + serialization.

Owns the ``<mount.local>/.srunx/workflows/`` directory convention and all
YAML ↔ dict translation. :class:`WorkflowStorageService` takes a
zero-arg ``profile_resolver`` callable so the router can pass its own
module-level ``_get_current_profile`` — tests that patch
``srunx.web.routers.workflows._get_current_profile`` then resolve the
patched callable at every method call.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import anyio
import yaml
from fastapi import HTTPException

from srunx.common.exceptions import WorkflowValidationError
from srunx.domain import Job, JobEnvironment, JobResource, ShellJob, Workflow
from srunx.runtime.workflow.runner import WorkflowRunner

from ._submission_common import (
    ensure_workflow_dir,
    find_yaml,
    reject_python_prefix_in_yaml_args,
    reject_python_prefix_web,
    workflow_dir,
)


class WorkflowStorageService:
    """Filesystem-backed workflow store.

    :param profile_resolver: Zero-arg callable returning the active
        :class:`ServerProfile` (or ``None``). The router passes its own
        module-level ``_get_current_profile`` so ``unittest.mock.patch``
        targets on that attribute stay effective.
    """

    def __init__(self, profile_resolver: Callable[[], Any]) -> None:
        self._profile_resolver = profile_resolver

    # ── Path helpers (thin delegates so service consumers don't need
    # to thread the resolver through every call site) ──────────────

    def workflow_dir(self, mount_name: str) -> Path:
        return workflow_dir(mount_name, self._profile_resolver)

    def ensure_workflow_dir(self, mount_name: str) -> Path:
        return ensure_workflow_dir(mount_name, self._profile_resolver)

    def find_yaml(self, name: str, mount_name: str) -> Path:
        return find_yaml(name, mount_name, self._profile_resolver)

    # ── Serialization ───────────────────────────────────────────────

    @staticmethod
    def serialize_workflow(
        runner: WorkflowRunner,
        raw_yaml_jobs: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        wf = runner.workflow
        # Build lookup for extra YAML fields not stored in Job model
        raw_by_name: dict[str, dict[str, Any]] = {}
        if raw_yaml_jobs:
            for rj in raw_yaml_jobs:
                raw_by_name[rj.get("name", "")] = rj

        jobs: list[dict[str, Any]] = []
        for job in wf.jobs:
            d: dict[str, Any] = {
                "name": job.name,
                "job_id": job.job_id,
                "status": job._status.value,
                "depends_on": job.depends_on,
                "exports": job.exports,
            }
            raw_job = raw_by_name.get(job.name, {})
            if raw_job.get("template"):
                d["template"] = raw_job["template"]
            if hasattr(job, "command"):
                cmd = job.command  # type: ignore[union-attr]
                d["command"] = [cmd] if isinstance(cmd, str) else cmd
                d["resources"] = {
                    "nodes": job.resources.nodes,  # type: ignore[union-attr]
                    "gpus_per_node": job.resources.gpus_per_node,  # type: ignore[union-attr]
                    "partition": job.resources.partition,  # type: ignore[union-attr]
                    "time_limit": job.resources.time_limit,  # type: ignore[union-attr]
                }
            elif hasattr(job, "script_path"):
                d["script_path"] = job.script_path  # type: ignore[union-attr]
                d["command"] = []
                d["resources"] = {}
            else:
                d["command"] = []
                d["resources"] = {}
            jobs.append(d)
        result: dict[str, Any] = {"name": wf.name, "jobs": jobs}
        if runner.args:
            result["args"] = runner.args
        if runner.default_project:
            result["default_project"] = runner.default_project
        return result

    @staticmethod
    def workflow_to_yaml(
        name: str,
        jobs_data: list[dict[str, Any]],
        default_project: str | None = None,
        args: dict[str, Any] | None = None,
    ) -> str:
        """Serialize a workflow to YAML compatible with
        ``WorkflowRunner.from_yaml``.

        Only includes non-default / non-None resource and environment
        fields so the resulting file stays clean.
        """
        serialized_jobs: list[dict[str, Any]] = []
        for jd in jobs_data:
            entry: dict[str, Any] = {
                "name": jd["name"],
                "command": jd["command"],
            }

            depends = jd.get("depends_on", [])
            if depends:
                entry["depends_on"] = depends

            exports = jd.get("exports", {})
            if exports:
                entry["exports"] = exports

            # Resources — only include non-None values
            raw_res = jd.get("resources") or {}
            resources = {k: v for k, v in raw_res.items() if v is not None}
            if resources:
                entry["resources"] = resources

            # Environment — only include non-None values
            raw_env = jd.get("environment") or {}
            environment = {k: v for k, v in raw_env.items() if v is not None}
            if environment:
                entry["environment"] = environment

            # Job-level optional fields
            if jd.get("template"):
                entry["template"] = jd["template"]
            if jd.get("work_dir"):
                entry["work_dir"] = jd["work_dir"]
            if jd.get("log_dir"):
                entry["log_dir"] = jd["log_dir"]
            if jd.get("retry") is not None:
                entry["retry"] = jd["retry"]
            if jd.get("retry_delay") is not None:
                entry["retry_delay"] = jd["retry_delay"]
            if jd.get("srun_args"):
                entry["srun_args"] = jd["srun_args"]
            if jd.get("launch_prefix"):
                entry["launch_prefix"] = jd["launch_prefix"]

            serialized_jobs.append(entry)

        doc: dict[str, Any] = {"name": name}
        if default_project:
            doc["default_project"] = default_project
        if args:
            doc["args"] = args
        doc["jobs"] = serialized_jobs
        return yaml.dump(doc, default_flow_style=False, sort_keys=False)

    @staticmethod
    def validate_and_build_workflow(data: dict[str, Any]) -> Workflow:
        """Construct and validate a Workflow from a plain dict.

        Builds Job instances with JobResource / JobEnvironment, then
        runs cycle-detection via ``Workflow.validate()``.  Raises on any
        Pydantic or workflow-level validation failure.
        """
        name: str = data["name"]
        jobs_data: list[dict[str, Any]] = data.get("jobs", [])

        jobs: list[Job | ShellJob] = []
        for jd in jobs_data:
            resource = JobResource.model_validate(jd.get("resources") or {})
            environment = JobEnvironment.model_validate(jd.get("environment") or {})
            job_kwargs: dict[str, Any] = {
                "name": jd["name"],
                "command": jd["command"],
                "depends_on": jd.get("depends_on", []),
                "exports": jd.get("exports", {}),
                "resources": resource,
                "environment": environment,
            }
            # Always pass work_dir and log_dir explicitly to prevent Job's
            # default_factory from calling os.getcwd() (wrong for the web
            # server) or defaulting to "logs" (meaningless on a remote
            # SLURM host). Empty strings are falsy and skipped by
            # workflow_to_yaml and the SLURM template (#SBATCH --chdir is
            # only emitted when truthy).
            job_kwargs["work_dir"] = jd.get("work_dir") or ""
            job_kwargs["log_dir"] = jd.get("log_dir") or ""
            if jd.get("retry") is not None:
                job_kwargs["retry"] = jd["retry"]
            if jd.get("retry_delay") is not None:
                job_kwargs["retry_delay"] = jd["retry_delay"]
            job = Job(**job_kwargs)
            jobs.append(job)

        workflow = Workflow(name=name, jobs=jobs)
        workflow.validate()
        return workflow

    # ── Router-facing async operations ──────────────────────────────

    async def list_workflows(self, mount: str) -> list[dict[str, Any]]:
        d = self.workflow_dir(mount)
        if not d.exists():
            return []

        results: list[dict[str, Any]] = []
        for p in sorted(d.glob("*.y*ml")):
            try:

                def _load(_p: Path = p) -> tuple[WorkflowRunner, list[dict[str, Any]]]:
                    import yaml as _yaml

                    runner = WorkflowRunner.from_yaml(_p)
                    raw = _yaml.safe_load(_p.read_text(encoding="utf-8"))
                    return runner, raw.get("jobs", [])

                runner, raw_jobs = await anyio.to_thread.run_sync(_load)
                results.append(self.serialize_workflow(runner, raw_yaml_jobs=raw_jobs))
            except Exception:
                continue
        return results

    async def get(self, name: str, mount: str) -> dict[str, Any]:
        yaml_path = self.find_yaml(name, mount)
        try:

            def _load() -> tuple[WorkflowRunner, list[dict[str, Any]]]:
                import yaml as _yaml

                runner = WorkflowRunner.from_yaml(yaml_path)
                raw = _yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
                return runner, raw.get("jobs", [])

            runner, raw_jobs = await anyio.to_thread.run_sync(_load)
            return self.serialize_workflow(runner, raw_yaml_jobs=raw_jobs)
        except Exception as e:
            raise HTTPException(status_code=502, detail=str(e)) from e

    async def delete(self, name: str, mount: str) -> dict[str, str]:
        yaml_path = self.find_yaml(name, mount)  # raises 404 if not found
        await anyio.to_thread.run_sync(lambda: yaml_path.unlink())
        return {"status": "deleted", "name": name}

    async def upload(
        self,
        yaml_content: str,
        filename: str,
        mount_name: str,
        *,
        safe_name_re: Any,
    ) -> dict[str, Any]:
        if not yaml_content or not filename or not mount_name:
            raise HTTPException(
                status_code=422, detail="'yaml', 'filename', and 'mount' are required"
            )

        reject_python_prefix_in_yaml_args(yaml_content)

        if len(yaml_content) > 1_000_000:
            raise HTTPException(
                status_code=413, detail="YAML content exceeds 1MB limit"
            )

        safe_filename = Path(filename).name
        name = Path(safe_filename).stem
        if not safe_name_re.match(name):
            raise HTTPException(
                status_code=422,
                detail="Filename must be alphanumeric with hyphens/underscores only",
            )

        d = self.ensure_workflow_dir(mount_name)
        dest = d / safe_filename
        dest.write_text(yaml_content)

        try:
            runner = await anyio.to_thread.run_sync(
                lambda: WorkflowRunner.from_yaml(dest)
            )
            await anyio.to_thread.run_sync(runner.workflow.validate)
            return self.serialize_workflow(runner)
        except Exception as e:
            dest.unlink(missing_ok=True)
            raise HTTPException(status_code=422, detail=str(e)) from e

    async def create(
        self,
        body: Any,  # WorkflowCreateRequest (loosely typed to avoid import cycle)
        *,
        reserved_names: frozenset[str],
    ) -> dict[str, Any]:
        """Create a new workflow from a structured JSON payload.

        Validates all jobs via Pydantic model construction, checks for
        dependency cycles, serializes to YAML, and persists to disk.
        """
        name = body.name
        mount_name = body.default_project
        if not mount_name:
            raise HTTPException(
                status_code=422,
                detail="A mount (default_project) is required to save a workflow",
            )

        if name in reserved_names:
            raise HTTPException(
                status_code=422,
                detail=f"Workflow name '{name}' is reserved",
            )

        d = self.ensure_workflow_dir(mount_name)
        if not body.overwrite:
            for ext in (".yaml", ".yml"):
                if (d / f"{name}{ext}").exists():
                    raise HTTPException(
                        status_code=409,
                        detail=f"Workflow '{name}' already exists",
                    )

        # Reject python: args from web for security (shared guard).
        reject_python_prefix_web(body.args, source="args")

        jobs_raw: list[dict[str, Any]] = [
            j.model_dump(exclude_none=True) for j in body.jobs
        ]

        data: dict[str, Any] = {"name": name, "jobs": jobs_raw}

        try:
            self.validate_and_build_workflow(data)
        except WorkflowValidationError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except Exception as exc:
            from pydantic import ValidationError as _VE

            if isinstance(exc, _VE):
                errors = [
                    {"loc": list(e["loc"]), "msg": e["msg"], "type": e["type"]}
                    for e in exc.errors()
                ]
                raise HTTPException(status_code=422, detail=errors) from exc
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        yaml_content = self.workflow_to_yaml(
            name,
            jobs_raw,
            default_project=body.default_project,
            args=body.args or None,
        )
        dest = d / f"{name}.yaml"
        await anyio.to_thread.run_sync(lambda: dest.write_text(yaml_content))

        runner = await anyio.to_thread.run_sync(lambda: WorkflowRunner.from_yaml(dest))
        return self.serialize_workflow(runner)
