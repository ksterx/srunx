"""Workflow runner for executing YAML-defined workflows with SLURM"""

import ast
import datetime
import math
import re
import time
from collections import defaultdict
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from textwrap import dedent
from typing import Any, Self

import jinja2
import yaml  # type: ignore

from srunx.callbacks import Callback
from srunx.client import Slurm
from srunx.exceptions import WorkflowValidationError
from srunx.logging import get_logger
from srunx.models import (
    DependencyType,
    Job,
    JobEnvironment,
    JobResource,
    JobStatus,
    RunnableJobType,
    ShellJob,
    Workflow,
)

logger = get_logger(__name__)


def _safe_eval(code: str, local_vars: dict[str, Any]) -> Any:
    """Evaluate a Python expression safely.

    Only allows:
    - Literal values (strings, numbers, lists, dicts, tuples, booleans, None)
    - Simple function calls from a small allowlist (datetime.date, math.ceil, etc.)
    - Variable references from local_vars

    This does NOT use eval()/exec() — it parses the AST and interprets it directly,
    making sandbox escape via __class__/__subclasses__ impossible.
    """
    tree = ast.parse(code.strip(), mode="eval")
    return _eval_node(tree.body, local_vars)


def _safe_exec(code: str, local_vars: dict[str, Any]) -> dict[str, Any]:
    """Execute simple Python assignments safely.

    Only supports: `result = <expression>` style assignments.
    """
    tree = ast.parse(code.strip(), mode="exec")
    ns = dict(local_vars)
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name):
                ns[target.id] = _eval_node(node.value, ns)
            else:
                raise ValueError(f"Unsupported assignment target: {ast.dump(target)}")
        elif isinstance(node, ast.Expr):
            _eval_node(node.value, ns)
        else:
            raise ValueError(
                f"Unsupported statement in python: arg: {ast.dump(node)}. "
                "Only simple assignments (result = ...) are allowed."
            )
    return ns


# Allowlisted callable functions for python: args
_SAFE_CALLABLES: dict[str, Any] = {
    "int": int,
    "float": float,
    "str": str,
    "bool": bool,
    "list": list,
    "dict": dict,
    "tuple": tuple,
    "len": len,
    "abs": abs,
    "round": round,
    "min": min,
    "max": max,
    "sum": sum,
    "sorted": sorted,
    "range": range,
    "format": format,
    "repr": repr,
}

# Allowlisted module attributes
_SAFE_MODULES: dict[str, Any] = {
    "datetime": datetime,
    "math": math,
}


def _eval_node(node: ast.AST, local_vars: dict[str, Any]) -> Any:  # noqa: C901, PLR0911, PLR0912
    """Recursively evaluate an AST node in a safe context."""
    # Literals
    if isinstance(node, ast.Constant):
        return node.value

    # Variable lookup
    if isinstance(node, ast.Name):
        if node.id in local_vars:
            return local_vars[node.id]
        if node.id in _SAFE_CALLABLES:
            return _SAFE_CALLABLES[node.id]
        if node.id in _SAFE_MODULES:
            return _SAFE_MODULES[node.id]
        if node.id in ("True", "False", "None"):
            return {"True": True, "False": False, "None": None}[node.id]
        raise NameError(f"Name '{node.id}' is not allowed in python: args")

    # Attribute access (only on allowlisted modules)
    if isinstance(node, ast.Attribute):
        obj = _eval_node(node.value, local_vars)
        # Only allow attribute access on allowlisted modules and their results
        if obj in _SAFE_MODULES.values() or type(obj).__module__ in (
            "datetime",
            "math",
        ):
            return getattr(obj, node.attr)
        raise AttributeError(f"Attribute access on {type(obj).__name__} is not allowed")

    # Function calls
    if isinstance(node, ast.Call):
        func = _eval_node(node.func, local_vars)
        args = [_eval_node(a, local_vars) for a in node.args]
        kwargs = {
            kw.arg: _eval_node(kw.value, local_vars) for kw in node.keywords if kw.arg
        }
        return func(*args, **kwargs)

    # Subscript (e.g., args['x'], list[0])
    if isinstance(node, ast.Subscript):
        obj = _eval_node(node.value, local_vars)
        key = _eval_node(node.slice, local_vars)
        return obj[key]

    # Binary ops
    if isinstance(node, ast.BinOp):
        left = _eval_node(node.left, local_vars)
        right = _eval_node(node.right, local_vars)
        ops = {
            ast.Add: lambda a, b: a + b,
            ast.Sub: lambda a, b: a - b,
            ast.Mult: lambda a, b: a * b,
            ast.Div: lambda a, b: a / b,
            ast.FloorDiv: lambda a, b: a // b,
            ast.Mod: lambda a, b: a % b,
            ast.Pow: lambda a, b: a**b,
        }
        op_func = ops.get(type(node.op))
        if op_func:
            return op_func(left, right)
        raise ValueError(f"Unsupported operator: {type(node.op).__name__}")

    # Unary ops
    if isinstance(node, ast.UnaryOp):
        operand = _eval_node(node.operand, local_vars)
        if isinstance(node.op, ast.USub):
            return -operand
        if isinstance(node.op, ast.Not):
            return not operand
        raise ValueError(f"Unsupported unary operator: {type(node.op).__name__}")

    # Compare ops
    if isinstance(node, ast.Compare):
        left = _eval_node(node.left, local_vars)
        for op, comparator in zip(node.ops, node.comparators, strict=True):
            right = _eval_node(comparator, local_vars)
            cmp_ops = {
                ast.Eq: lambda a, b: a == b,
                ast.NotEq: lambda a, b: a != b,
                ast.Lt: lambda a, b: a < b,
                ast.LtE: lambda a, b: a <= b,
                ast.Gt: lambda a, b: a > b,
                ast.GtE: lambda a, b: a >= b,
                ast.In: lambda a, b: a in b,
                ast.NotIn: lambda a, b: a not in b,
            }
            op_func = cmp_ops.get(type(op))
            if not op_func:
                raise ValueError(f"Unsupported comparison: {type(op).__name__}")
            if not op_func(left, right):
                return False
            left = right
        return True

    # Boolean ops
    if isinstance(node, ast.BoolOp):
        if isinstance(node.op, ast.And):
            return all(_eval_node(v, local_vars) for v in node.values)
        if isinstance(node.op, ast.Or):
            return any(_eval_node(v, local_vars) for v in node.values)

    # Ternary (a if cond else b)
    if isinstance(node, ast.IfExp):
        if _eval_node(node.test, local_vars):
            return _eval_node(node.body, local_vars)
        return _eval_node(node.orelse, local_vars)

    # List/Tuple/Set/Dict comprehensions and literals
    if isinstance(node, ast.List):
        return [_eval_node(e, local_vars) for e in node.elts]
    if isinstance(node, ast.Tuple):
        return tuple(_eval_node(e, local_vars) for e in node.elts)
    if isinstance(node, ast.Set):
        return {_eval_node(e, local_vars) for e in node.elts}
    if isinstance(node, ast.Dict):
        return {
            _eval_node(k, local_vars): _eval_node(v, local_vars)
            for k, v in zip(node.keys, node.values, strict=True)
            if k is not None
        }

    # f-string (JoinedStr)
    if isinstance(node, ast.JoinedStr):
        parts = []
        for v in node.values:
            if isinstance(v, ast.Constant):
                parts.append(str(v.value))
            elif isinstance(v, ast.FormattedValue):
                parts.append(str(_eval_node(v.value, local_vars)))
            else:
                parts.append(str(_eval_node(v, local_vars)))
        return "".join(parts)

    raise ValueError(
        f"Unsupported expression in python: arg: {ast.dump(node)}. "
        "Only literals, simple function calls, and basic operations are allowed."
    )


def _has_python_prefix(value: str) -> bool:
    """Check if a string value has a 'python:' prefix (case-insensitive)."""
    return value.lstrip().lower().startswith("python:")


def _strip_python_prefix(value: str) -> str:
    """Strip the 'python:' prefix (case-insensitive) and leading whitespace."""
    stripped = value.lstrip()
    # Remove the prefix preserving the original case length
    return stripped[len("python:") :].lstrip()


_JINJA_VAR_RE = re.compile(r"\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}")


def _find_jinja_refs(text: str) -> set[str]:
    """Find all Jinja2 variable names referenced in *text*."""
    return set(_JINJA_VAR_RE.findall(text))


def _dependency_closure(jobs_data: list[dict[str, Any]], target: str) -> set[str]:
    """Return *target* plus every job it transitively depends on."""
    name_to_deps: dict[str, list[str]] = {
        jd["name"]: list(jd.get("depends_on") or []) for jd in jobs_data if "name" in jd
    }
    closure: set[str] = set()
    stack = [target]
    while stack:
        node = stack.pop()
        if node in closure:
            continue
        if node not in name_to_deps:
            continue
        closure.add(node)
        stack.extend(name_to_deps[node])
    return closure


class _DepsNamespace:
    """Jinja-friendly wrapper for ``{{ deps.<job>.<key> }}`` access.

    Plain dicts shadow user-declared keys with built-in methods — e.g.
    ``{{ deps.a.items }}`` resolves to ``dict.items`` rather than the
    user's export named ``items``. This wrapper forces attribute access
    to go through dict lookup, wraps nested dict values recursively, and
    raises ``AttributeError`` for missing keys so ``StrictUndefined``
    can surface a clear error.
    """

    __slots__ = ("_data",)

    def __init__(self, data: dict[str, Any]):
        self._data = data

    def __getattr__(self, name: str) -> Any:
        try:
            value = self._data[name]
        except KeyError as exc:
            raise AttributeError(
                f"'deps' has no entry '{name}'. "
                "Check that it is listed in this job's 'depends_on' "
                "and that the referenced key is declared in the parent's 'exports'."
            ) from exc
        if isinstance(value, dict):
            return _DepsNamespace(value)
        return value

    def __getitem__(self, name: str) -> Any:
        return self.__getattr__(name)

    def __contains__(self, name: str) -> bool:
        return name in self._data


def _find_required_variables(jobs_yaml: str, args: dict[str, Any]) -> set[str]:
    """Return the set of arg keys that the jobs YAML transitively depends on.

    Starts from variables directly referenced in the YAML, then walks
    through each arg value to discover transitive dependencies.
    """

    # Build a dependency graph: key -> set of keys it depends on
    graph: dict[str, set[str]] = {}
    for key, value in args.items():
        deps: set[str] = set()
        if isinstance(value, str):
            deps = _find_jinja_refs(value) & args.keys()
        graph[key] = deps

    # Seed: variables referenced directly in the jobs section
    seeds = _find_jinja_refs(jobs_yaml) & args.keys()

    # Expand seeds transitively
    required: set[str] = set()
    queue = list(seeds)
    while queue:
        var = queue.pop()
        if var in required:
            continue
        required.add(var)
        queue.extend(graph.get(var, set()) - required)

    return required


def _eval_python_var(code_raw: str, evaluated: dict[str, Any]) -> Any:
    """Evaluate a single python: variable value."""
    code = jinja2.Template(code_raw, undefined=jinja2.DebugUndefined).render(
        **evaluated
    )
    code = dedent(code).lstrip()
    try:
        return _safe_eval(code, {"args": evaluated})
    except SyntaxError:
        ns = _safe_exec(code, {"args": evaluated})
        return ns.get("result")


def _evaluate_variables(args: dict[str, Any], required: set[str]) -> dict[str, Any]:
    """Evaluate all *required* args in topological order.

    Uses ``graphlib.TopologicalSorter`` instead of ad-hoc while loops.
    Python-prefixed values are evaluated via ``_safe_eval``/``_safe_exec``;
    Jinja-only values are rendered via ``jinja2.Template``.
    """
    from graphlib import CycleError, TopologicalSorter

    # Partition into python vs plain, only keeping required keys
    plain: dict[str, Any] = {}
    python: dict[str, str] = {}
    for key in required:
        if key not in args:
            continue
        value = args[key]
        if isinstance(value, str) and _has_python_prefix(value):
            python[key] = _strip_python_prefix(value)
        else:
            plain[key] = value

    # Build dependency graph for ALL required variables
    all_vars = {**plain, **{k: args[k] for k in python}}
    graph: dict[str, set[str]] = {}
    for key, value in all_vars.items():
        if isinstance(value, str):
            graph[key] = _find_jinja_refs(value) & required
        elif key in python:
            # For python vars, scan the code for variable name references
            code = python[key]
            deps = set()
            for other in required:
                if other != key and other in code:
                    deps.add(other)
            graph[key] = deps
        else:
            graph[key] = set()

    # Topological sort
    try:
        sorter = TopologicalSorter(graph)
        order = list(sorter.static_order())
    except CycleError:
        logger.warning("Circular variable dependencies detected, using fallback order")
        order = list(required)

    # Evaluate in order
    evaluated: dict[str, Any] = {}
    for key in order:
        if key not in required or key not in args:
            continue

        if key in python:
            try:
                evaluated[key] = _eval_python_var(python[key], evaluated)
            except Exception as e:
                logger.warning(f"Failed to evaluate python variable '{key}': {e}")
        elif isinstance(plain.get(key), str):
            value = plain[key]
            refs = _find_jinja_refs(value)
            if refs:
                tmpl = jinja2.Template(value, undefined=jinja2.DebugUndefined)
                evaluated[key] = tmpl.render(**evaluated)
            else:
                evaluated[key] = value
        else:
            evaluated[key] = plain.get(key)

    return evaluated


def _transition_workflow_run(
    workflow_run_id: int,
    from_status: str,
    to_status: str,
    *,
    error: str | None = None,
) -> None:
    """Best-effort ``workflow_runs`` status transition via the state service.

    Opens a short ``BEGIN IMMEDIATE`` TX on a fresh connection so that
    :class:`WorkflowRunStateService` (which refuses to open its own TX)
    can emit the ``workflow_run.status_changed`` event and — when the
    run belongs to a sweep — fan out sweep aggregation atomically.

    Fails closed: any exception is logged at debug and swallowed, so a
    DB outage never takes down the primary workflow flow.
    """
    try:
        from srunx.db.connection import init_db, open_connection, transaction
        from srunx.db.repositories.base import now_iso
        from srunx.sweep.state_service import WorkflowRunStateService

        completed_at = (
            now_iso() if to_status in {"completed", "failed", "cancelled"} else None
        )
        init_db(delete_legacy=True)
        conn = open_connection()
        try:
            with transaction(conn, "IMMEDIATE"):
                WorkflowRunStateService.update(
                    conn=conn,
                    workflow_run_id=workflow_run_id,
                    from_status=from_status,
                    to_status=to_status,
                    error=error,
                    completed_at=completed_at,
                )
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001 — best-effort
        logger.debug(f"_transition_workflow_run failed: {exc}")


class WorkflowRunner:
    """Runner for executing workflows defined in YAML with dynamic job scheduling.

    Jobs are executed as soon as their dependencies are satisfied,
    rather than waiting for entire dependency levels to complete.
    """

    def __init__(
        self,
        workflow: Workflow,
        callbacks: Sequence[Callback] | None = None,
        args: dict[str, Any] | None = None,
        default_project: str | None = None,
    ) -> None:
        """Initialize workflow runner.

        Args:
            workflow: Workflow to execute.
            callbacks: List of callbacks for job notifications.
            args: Template variables from the YAML args section.
            default_project: Default project (mount name) for file syncing.
        """
        self.workflow = workflow
        self.slurm = Slurm(callbacks=callbacks)
        self.callbacks = callbacks or []
        self.args = args or {}
        self.default_project = default_project

    @classmethod
    def from_yaml(
        cls,
        yaml_path: str | Path,
        callbacks: Sequence[Callback] | None = None,
        single_job: str | None = None,
        *,
        args_override: dict[str, Any] | None = None,
    ) -> Self:
        """Load and validate a workflow from a YAML file.

        Args:
            yaml_path: Path to the YAML workflow definition file.
            callbacks: List of callbacks for job notifications.
            single_job: If specified, only load and process this job.
            args_override: Optional mapping merged over the YAML ``args``
                section before Jinja evaluation. Override entries win on
                key collision; keys absent from the YAML are added.

        Returns:
            WorkflowRunner instance with loaded workflow.

        Raises:
            FileNotFoundError: If the YAML file doesn't exist.
            yaml.YAMLError: If the YAML is malformed.
            WorkflowValidationError: If the workflow structure is invalid.
        """
        yaml_file = Path(yaml_path)
        if not yaml_file.exists():
            raise FileNotFoundError(f"Workflow file not found: {yaml_path}")

        with open(yaml_file, encoding="utf-8") as f:
            data = yaml.safe_load(f)

        name = data.get("name", "unnamed")
        args = {**(data.get("args") or {}), **(args_override or {})}
        default_project = data.get("default_project")
        jobs_data = data.get("jobs", [])

        # For `single_job`, restrict rendering to the target and its
        # transitive dependencies so unrelated broken jobs don't block
        # a targeted re-run. Without single_job, render the full DAG.
        if single_job:
            if not any(jd.get("name") == single_job for jd in jobs_data):
                raise WorkflowValidationError(
                    f"Job '{single_job}' not found in workflow"
                )
            closure_names = _dependency_closure(jobs_data, single_job)
            render_input = [jd for jd in jobs_data if jd.get("name") in closure_names]
        else:
            render_input = jobs_data

        rendered_jobs_data = cls._render_jobs_with_args_and_deps(render_input, args)

        if single_job:
            rendered_jobs_data = [
                jd for jd in rendered_jobs_data if jd.get("name") == single_job
            ]

        jobs = []
        for job_data in rendered_jobs_data:
            job = cls.parse_job(job_data)
            jobs.append(job)
        return cls(
            workflow=Workflow(name=name, jobs=jobs),
            callbacks=callbacks,
            args=args,
            default_project=default_project,
        )

    @staticmethod
    def _render_jobs_with_args_and_deps(
        jobs_data: list[dict[str, Any]], args: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Render Jinja templates per-job in dependency order.

        Each job is rendered with `{**args, 'deps': <DepsNamespace>}`
        where ``deps`` exposes the already-rendered ``exports`` of every
        predecessor listed in the job's ``depends_on``. Uses
        StrictUndefined so typos in ``deps.X.Y`` fail fast at load time.

        Reject legacy ``outputs:`` keys explicitly rather than silently
        dropping them, so stale YAML surfaces a clear error instead of
        producing empty exports and a broken ``deps.X.Y`` resolution.
        """
        from graphlib import CycleError, TopologicalSorter

        for jd in jobs_data:
            if "outputs" in jd:
                raise WorkflowValidationError(
                    f"Job '{jd.get('name', '?')}' uses the removed 'outputs' key. "
                    "Rename to 'exports' and update consumers to reference the value "
                    "as '{{ deps.<job_name>.<key> }}' (load-time resolution). "
                    "See CHANGELOG migration guide."
                )

        # Evaluate args up front (supports `python:` prefix).
        if args:
            jobs_yaml = yaml.dump(jobs_data, default_flow_style=False)
            required = _find_required_variables(jobs_yaml, args)
            evaluated_args = _evaluate_variables(args, required)
        else:
            evaluated_args = {}

        name_to_data = {j["name"]: j for j in jobs_data if "name" in j}
        name_to_deps = {
            name: set(jd.get("depends_on", []) or []) & name_to_data.keys()
            for name, jd in name_to_data.items()
        }

        try:
            order = list(TopologicalSorter(name_to_deps).static_order())
        except CycleError as e:
            raise WorkflowValidationError(f"Circular job dependency: {e}") from e

        rendered: dict[str, dict[str, Any]] = {}
        for job_name in order:
            raw = name_to_data[job_name]
            deps_ctx = _DepsNamespace(
                {
                    dep: rendered[dep].get("exports", {}) or {}
                    for dep in name_to_deps[job_name]
                    if dep in rendered
                }
            )
            context = {**evaluated_args, "deps": deps_ctx}

            job_yaml = yaml.dump(raw, default_flow_style=False)
            try:
                template = jinja2.Template(job_yaml, undefined=jinja2.StrictUndefined)
                rendered_yaml = template.render(**context)
            except jinja2.TemplateError as e:
                raise WorkflowValidationError(
                    f"Failed to render job '{job_name}': {e}"
                ) from e
            rendered[job_name] = yaml.safe_load(rendered_yaml)

        return [rendered[j["name"]] for j in jobs_data if j.get("name") in rendered]

    def get_independent_jobs(self) -> list[RunnableJobType]:
        """Get all jobs that are independent of any other job."""
        independent_jobs = []
        for job in self.workflow.jobs:
            if not job.depends_on:
                independent_jobs.append(job)
        return independent_jobs

    def _get_jobs_to_execute(
        self,
        from_job: str | None = None,
        to_job: str | None = None,
        single_job: str | None = None,
    ) -> list[RunnableJobType]:
        """Determine which jobs to execute based on the execution control options.

        Args:
            from_job: Start execution from this job (inclusive)
            to_job: Stop execution at this job (inclusive)
            single_job: Execute only this specific job

        Returns:
            List of jobs to execute.

        Raises:
            WorkflowValidationError: If specified jobs are not found.
        """
        all_jobs = self.workflow.jobs
        job_names = {job.name for job in all_jobs}

        # Validate job names exist
        if single_job and single_job not in job_names:
            raise WorkflowValidationError(f"Job '{single_job}' not found in workflow")
        if from_job and from_job not in job_names:
            raise WorkflowValidationError(f"Job '{from_job}' not found in workflow")
        if to_job and to_job not in job_names:
            raise WorkflowValidationError(f"Job '{to_job}' not found in workflow")

        # Single job execution - return just that job
        if single_job:
            return [job for job in all_jobs if job.name == single_job]

        # Full workflow execution - return all jobs
        if not from_job and not to_job:
            return all_jobs

        # Partial execution - determine job range
        jobs_to_execute = []

        if from_job and to_job:
            # Execute from from_job to to_job (inclusive)
            start_idx = None
            end_idx = None
            for i, job in enumerate(all_jobs):
                if job.name == from_job:
                    start_idx = i
                if job.name == to_job:
                    end_idx = i

            if start_idx is not None and end_idx is not None:
                if start_idx <= end_idx:
                    jobs_to_execute = all_jobs[start_idx : end_idx + 1]
                else:
                    # Handle reverse order - get all jobs between them
                    jobs_to_execute = all_jobs[end_idx : start_idx + 1]
            else:
                jobs_to_execute = all_jobs

        elif from_job:
            # Execute from from_job to end
            start_idx = None
            for i, job in enumerate(all_jobs):
                if job.name == from_job:
                    start_idx = i
                    break
            if start_idx is not None:
                jobs_to_execute = all_jobs[start_idx:]
            else:
                jobs_to_execute = all_jobs

        elif to_job:
            # Execute from beginning to to_job
            end_idx = None
            for i, job in enumerate(all_jobs):
                if job.name == to_job:
                    end_idx = i
                    break
            if end_idx is not None:
                jobs_to_execute = all_jobs[: end_idx + 1]
            else:
                jobs_to_execute = all_jobs

        return jobs_to_execute

    def run(
        self,
        from_job: str | None = None,
        to_job: str | None = None,
        single_job: str | None = None,
        *,
        workflow_run_id: int | None = None,
    ) -> dict[str, RunnableJobType]:
        """Run a workflow with dynamic job scheduling.

        Jobs are executed as soon as their dependencies are satisfied.

        Args:
            from_job: Start execution from this job (inclusive), ignoring dependencies
            to_job: Stop execution at this job (inclusive)
            single_job: Execute only this specific job, ignoring all dependencies
            workflow_run_id: Pre-materialized ``workflow_runs`` row id to
                attach this run to. When ``None`` (default), the runner
                creates one itself via ``create_cli_workflow_run``. The
                sweep orchestrator passes a non-None id so all cells of
                the sweep share materialised ``workflow_runs`` rows.

        Returns:
            Dictionary mapping job names to completed Job instances.
        """
        # Get the jobs to execute based on options
        jobs_to_execute = self._get_jobs_to_execute(from_job, to_job, single_job)

        # Persist a ``workflow_runs`` row so CLI-submitted jobs share
        # the same identity model the Web UI uses. Without this,
        # ``srunx report --workflow`` (which JOINs jobs to workflow_runs
        # on ``workflow_run_id``) returns zero rows for every CLI run.
        # Best-effort: a DB outage must not block the workflow itself.
        if workflow_run_id is None:
            from srunx.db.cli_helpers import create_cli_workflow_run

            workflow_run_id = create_cli_workflow_run(
                workflow_name=self.workflow.name,
                args=self.args or None,
            )
        if workflow_run_id is not None:
            # Flip from the default ``pending`` to ``running`` up-front so
            # ``workflow_runs`` reflects the live state; the final
            # completed/failed transition is recorded at the exit points
            # below. Terminal-status is cheap to re-mark, so a missed
            # update here is not fatal.
            _transition_workflow_run(workflow_run_id, "pending", "running")

        # Log execution plan
        if single_job:
            logger.info(f"🚀 Executing single job: {single_job}")
        elif from_job or to_job:
            job_range = []
            if from_job:
                job_range.append(f"from {from_job}")
            if to_job:
                job_range.append(f"to {to_job}")
            logger.info(
                f"🚀 Executing workflow {self.workflow.name} ({' '.join(job_range)}) - {len(jobs_to_execute)} jobs"
            )
        else:
            logger.info(
                f"🚀 Starting Workflow {self.workflow.name} with {len(jobs_to_execute)} jobs"
            )

        for callback in self.callbacks:
            callback.on_workflow_started(self.workflow)

        # Track jobs to execute and results
        all_jobs = jobs_to_execute.copy()
        results: dict[str, RunnableJobType] = {}
        running_futures: dict[str, Any] = {}

        # For partial execution, we need to handle dependencies differently
        ignore_dependencies = from_job is not None

        def _show_job_logs_on_failure(job: RunnableJobType) -> None:
            """Show job logs when a job fails."""
            try:
                if not job.job_id:
                    logger.warning("No job ID available for log retrieval")
                    return

                log_info = self.slurm.get_job_output_detailed(job.job_id, job.name)

                found_files = log_info.get("found_files", [])
                output = log_info.get("output", "")
                error = log_info.get("error", "")
                primary_log = log_info.get("primary_log")
                slurm_log_dir = log_info.get("slurm_log_dir")
                searched_dirs = log_info.get("searched_dirs", [])

                # Ensure types are correct
                if not isinstance(found_files, list):
                    found_files = []
                if not isinstance(output, str):
                    output = ""
                if not isinstance(error, str):
                    error = ""
                if not isinstance(searched_dirs, list):
                    searched_dirs = []

                if not found_files:
                    logger.error("❌ No log files found")
                    logger.info(f"📁 Searched in: {', '.join(searched_dirs)}")
                    if slurm_log_dir:
                        logger.info(f"💡 SLURM_LOG_DIR: {slurm_log_dir}")
                    else:
                        logger.info("💡 SLURM_LOG_DIR not set")
                    return

                logger.info(f"📁 Found {len(found_files)} log file(s)")
                for log_file in found_files:
                    logger.info(f"  📄 {log_file}")

                if output:
                    logger.error("📋 Job output:")
                    # Truncate very long output
                    lines = output.split("\n")
                    max_lines = 50
                    if len(lines) > max_lines:
                        truncated_output = "\n".join(lines[-max_lines:])
                        logger.error(
                            f"{truncated_output}\n... (showing last {max_lines} lines of {len(lines)} total)"
                        )
                    else:
                        logger.error(output)

                if error:
                    logger.error("❌ Error output:")
                    logger.error(error)

                if primary_log:
                    logger.info(f"💡 Full log available at: {primary_log}")

            except Exception as e:
                logger.warning(f"Failed to retrieve job logs: {e}")

        def execute_job(job: RunnableJobType) -> RunnableJobType:
            """Execute a single job."""
            logger.info(f"⚡ {'SUBMITTED':<12} Job {job.name:<12}")

            try:
                result = self.slurm.run(
                    job,
                    workflow_name=self.workflow.name,
                    workflow_run_id=workflow_run_id,
                )
                return result
            except Exception as e:
                # Show SLURM logs when job fails
                if hasattr(job, "job_id") and job.job_id:
                    _show_job_logs_on_failure(job)
                raise

        def execute_job_with_retry(job: RunnableJobType) -> RunnableJobType:
            """Execute a job with retry logic."""
            while True:
                try:
                    result = execute_job(job)

                    # If job completed successfully, reset retry count and return
                    if result.status == JobStatus.COMPLETED:
                        job.reset_retry()
                        return result

                    # If job failed and can be retried
                    if result.status == JobStatus.FAILED and job.can_retry():
                        job.increment_retry()
                        retry_msg = f"(retry {job.retry_count}/{job.retry})"
                        logger.warning(
                            f"⚠️  Job {job.name} failed, retrying {retry_msg}"
                        )

                        # Wait before retrying
                        if job.retry_delay > 0:
                            logger.info(
                                f"⏳ Waiting {job.retry_delay}s before retry..."
                            )
                            time.sleep(job.retry_delay)

                        # Reset job_id for retry
                        job.job_id = None
                        job.status = JobStatus.PENDING
                        continue

                    # Job failed and no more retries, or job cancelled/timeout
                    # Show logs on final failure
                    if result.status == JobStatus.FAILED:
                        _show_job_logs_on_failure(result)
                    return result

                except Exception as e:
                    # Handle job submission/execution errors
                    if job.can_retry():
                        job.increment_retry()
                        retry_msg = f"(retry {job.retry_count}/{job.retry})"
                        logger.warning(
                            f"⚠️  Job {job.name} error: {e}, retrying {retry_msg}"
                        )

                        if job.retry_delay > 0:
                            logger.info(
                                f"⏳ Waiting {job.retry_delay}s before retry..."
                            )
                            time.sleep(job.retry_delay)

                        # Reset job state for retry
                        job.job_id = None
                        job.status = JobStatus.PENDING
                        continue
                    else:
                        # No more retries, re-raise the exception
                        raise

        # Special handling for single job execution - completely ignore all dependencies
        if single_job is not None:
            # Execute only the single job without any dependency processing
            single_job_obj = next(job for job in all_jobs if job.name == single_job)

            try:
                result = execute_job_with_retry(single_job_obj)
                results[single_job] = result

                if result.status == JobStatus.FAILED:
                    logger.error(f"❌ Job {single_job} failed")
                    if workflow_run_id is not None:
                        _transition_workflow_run(
                            workflow_run_id,
                            "running",
                            "failed",
                            error=f"Job {single_job} failed",
                        )
                    raise RuntimeError(f"Job {single_job} failed")

                logger.success(f"🎉 Job {single_job} completed!!")

                if workflow_run_id is not None:
                    _transition_workflow_run(workflow_run_id, "running", "completed")

                for callback in self.callbacks:
                    callback.on_workflow_completed(self.workflow)

                return results

            except Exception as e:
                logger.error(f"❌ Job {single_job} failed: {e}")
                if workflow_run_id is not None:
                    _transition_workflow_run(
                        workflow_run_id, "running", "failed", error=str(e)
                    )
                raise

        # Build reverse dependency map for efficient lookups (only for jobs we're executing)
        dependents = defaultdict(set)
        job_names_to_execute = {job.name for job in all_jobs}

        for job in all_jobs:
            if not ignore_dependencies:
                # Normal dependency handling
                for parsed_dep in job.parsed_dependencies:
                    dependents[parsed_dep.job_name].add(job.name)
            else:
                # For partial execution, only consider dependencies within the execution set
                for parsed_dep in job.parsed_dependencies:
                    if parsed_dep.job_name in job_names_to_execute:
                        dependents[parsed_dep.job_name].add(job.name)

        def on_job_started(job_name: str) -> list[str]:
            """Handle job start and return newly ready job names (for 'after' dependencies)."""
            # Build current job status map
            job_statuses = {}
            for job in all_jobs:
                job_statuses[job.name] = job.status
            # Mark the started job as RUNNING (or whatever status it should be)
            job_statuses[job_name] = JobStatus.RUNNING

            # Find newly ready jobs that depend on this job starting
            newly_ready = []
            for dependent_name in dependents[job_name]:
                dependent_job = next(
                    (j for j in all_jobs if j.name == dependent_name), None
                )
                if dependent_job is None:
                    continue

                if dependent_job.status == JobStatus.PENDING:
                    # Check if this job has "after" dependency on the started job
                    has_after_dep = any(
                        dep.job_name == job_name
                        and dep.dep_type == DependencyType.AFTER.value
                        for dep in dependent_job.parsed_dependencies
                    )

                    if has_after_dep:
                        if ignore_dependencies:
                            partial_job_statuses = {
                                name: status
                                for name, status in job_statuses.items()
                                if name in job_names_to_execute
                            }
                            deps_satisfied = dependent_job.dependencies_satisfied(
                                partial_job_statuses
                            )
                        else:
                            deps_satisfied = dependent_job.dependencies_satisfied(
                                job_statuses
                            )

                        if deps_satisfied:
                            newly_ready.append(dependent_name)

            return newly_ready

        def on_job_complete(job_name: str, result: RunnableJobType) -> list[str]:
            """Handle job completion and return newly ready job names."""
            results[job_name] = result

            # Build current job status map
            job_statuses = {}
            for job in all_jobs:
                job_statuses[job.name] = job.status
            # Update the completed job's status
            job_statuses[job_name] = result.status

            # Find newly ready jobs
            newly_ready = []
            for dependent_name in dependents[job_name]:
                dependent_job = next(
                    (j for j in all_jobs if j.name == dependent_name), None
                )
                if dependent_job is None:
                    continue

                if dependent_job.status == JobStatus.PENDING:
                    if ignore_dependencies:
                        # For partial execution, only check dependencies within our execution set
                        partial_job_statuses = {
                            name: status
                            for name, status in job_statuses.items()
                            if name in job_names_to_execute
                        }
                        deps_satisfied = dependent_job.dependencies_satisfied(
                            partial_job_statuses
                        )
                    else:
                        # Normal dependency checking with new interface
                        deps_satisfied = dependent_job.dependencies_satisfied(
                            job_statuses
                        )

                    if deps_satisfied:
                        newly_ready.append(dependent_name)

            return newly_ready

        # Execute workflow with ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=8) as executor:
            # Submit initial ready jobs
            if ignore_dependencies:
                # For partial execution, start with all jobs (dependencies are ignored or filtered)
                initial_jobs = all_jobs
            else:
                # Normal execution - start with independent jobs or jobs whose dependencies are satisfied
                initial_jobs = []
                job_statuses = {job.name: job.status for job in all_jobs}

                for job in all_jobs:
                    if not job.parsed_dependencies:
                        # Jobs with no dependencies
                        initial_jobs.append(job)
                    else:
                        # Check if dependencies are already satisfied
                        if job.dependencies_satisfied(job_statuses):
                            initial_jobs.append(job)

            for job in initial_jobs:
                future = executor.submit(execute_job_with_retry, job)
                running_futures[job.name] = future

                # Check for jobs that should start immediately after this job starts
                newly_ready_on_start = on_job_started(job.name)
                for ready_name in newly_ready_on_start:
                    if ready_name not in running_futures:
                        ready_job = next(j for j in all_jobs if j.name == ready_name)
                        new_future = executor.submit(execute_job_with_retry, ready_job)
                        running_futures[ready_name] = new_future

            # Process completed jobs and schedule new ones
            while running_futures:
                # Check for completed futures
                completed = []
                for job_name, future in list(running_futures.items()):
                    if future.done():
                        completed.append((job_name, future))
                        del running_futures[job_name]

                if not completed:
                    time.sleep(0.1)  # Brief sleep to avoid busy waiting
                    continue

                # Handle completed jobs
                for job_name, future in completed:
                    try:
                        result = future.result()
                        newly_ready_names = on_job_complete(job_name, result)

                        # Schedule newly ready jobs
                        for ready_name in newly_ready_names:
                            if ready_name not in running_futures:
                                ready_job = next(
                                    j for j in all_jobs if j.name == ready_name
                                )
                                new_future = executor.submit(
                                    execute_job_with_retry, ready_job
                                )
                                running_futures[ready_name] = new_future

                                # Check for jobs that should start immediately after this job starts
                                newly_ready_on_start = on_job_started(ready_name)
                                for start_ready_name in newly_ready_on_start:
                                    if start_ready_name not in running_futures:
                                        start_ready_job = next(
                                            j
                                            for j in all_jobs
                                            if j.name == start_ready_name
                                        )
                                        start_future = executor.submit(
                                            execute_job_with_retry, start_ready_job
                                        )
                                        running_futures[start_ready_name] = start_future

                    except Exception as e:
                        logger.error(f"❌ Job {job_name} failed: {e}")
                        raise

        # Verify all jobs completed successfully
        failed_jobs = [j.name for j in all_jobs if j.status == JobStatus.FAILED]
        incomplete_jobs = [
            j.name
            for j in all_jobs
            if j.status not in [JobStatus.COMPLETED, JobStatus.FAILED]
        ]

        if failed_jobs:
            logger.error(f"❌ Jobs failed: {failed_jobs}")
            if workflow_run_id is not None:
                _transition_workflow_run(
                    workflow_run_id,
                    "running",
                    "failed",
                    error=f"Jobs failed: {failed_jobs}",
                )
            raise RuntimeError(f"Workflow execution failed: {failed_jobs}")

        if incomplete_jobs:
            logger.error(f"❌ Jobs did not complete: {incomplete_jobs}")
            if workflow_run_id is not None:
                _transition_workflow_run(
                    workflow_run_id,
                    "running",
                    "failed",
                    error=f"Jobs did not complete: {incomplete_jobs}",
                )
            raise RuntimeError(f"Workflow execution incomplete: {incomplete_jobs}")

        logger.success(f"🎉 Workflow {self.workflow.name} completed!!")

        if workflow_run_id is not None:
            _transition_workflow_run(workflow_run_id, "running", "completed")

        for callback in self.callbacks:
            callback.on_workflow_completed(self.workflow)

        return results

    def execute_from_yaml(self, yaml_path: str | Path) -> dict[str, RunnableJobType]:
        """Load and execute a workflow from YAML file.

        Args:
            yaml_path: Path to YAML workflow file.

        Returns:
            Dictionary mapping job names to completed Job instances.
        """
        logger.info(f"Loading workflow from {yaml_path}")
        runner = self.from_yaml(yaml_path)
        return runner.run()

    @staticmethod
    def parse_job(data: dict[str, Any]) -> RunnableJobType:
        # Check for conflicting job types
        has_shell_fields = data.get("script_path") or data.get("path")
        has_command = data.get("command")

        if has_shell_fields and has_command:
            raise WorkflowValidationError(
                "Job cannot have both shell script fields (script_path/path) and 'command'"
            )

        base = {
            "name": data["name"],
            "depends_on": data.get("depends_on", []),
            "exports": data.get("exports", {}),
            "retry": data.get("retry", 0),
            "retry_delay": data.get("retry_delay", 60),
        }

        # Handle ShellJob (script_path or path)
        if data.get("script_path"):
            shell_job_data = {
                **base,
                "script_path": data["script_path"],
                "script_vars": data.get("script_vars", {}),
            }
            return ShellJob.model_validate(shell_job_data)

        if data.get("path"):
            return ShellJob.model_validate({**base, "script_path": data["path"]})

        # Handle regular Job (command)
        if not has_command:
            raise WorkflowValidationError(
                "Job must have either 'command' or 'script_path'"
            )

        resource = JobResource.model_validate(data.get("resources", {}))
        environment = JobEnvironment.model_validate(data.get("environment", {}))

        job_data = {
            **base,
            "command": data["command"],
            "resources": resource,
            "environment": environment,
        }
        if data.get("log_dir"):
            job_data["log_dir"] = data["log_dir"]
        if data.get("work_dir"):
            job_data["work_dir"] = data["work_dir"]

        return Job.model_validate(job_data)


def run_workflow_from_file(
    yaml_path: str | Path, single_job: str | None = None
) -> dict[str, RunnableJobType]:
    """Convenience function to run workflow from YAML file.

    Args:
        yaml_path: Path to YAML workflow file.
        single_job: If specified, only run this job.

    Returns:
        Dictionary mapping job names to completed Job instances.
    """
    runner = WorkflowRunner.from_yaml(yaml_path, single_job=single_job)
    return runner.run(single_job=single_job)
