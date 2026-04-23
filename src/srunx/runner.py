"""Backward-compat shim. Canonical home: :mod:`srunx.runtime.workflow`.

Re-exports the public ``WorkflowRunner`` / ``run_workflow_from_file`` plus
every private helper consumed by the test suite through this legacy
path. External code should migrate to the canonical modules
(:mod:`srunx.runtime.workflow.runner`, :mod:`srunx.runtime.workflow.safe_eval`,
:mod:`srunx.runtime.workflow.loader`, :mod:`srunx.runtime.workflow.transitions`).
"""

from srunx.runtime.workflow.loader import (  # noqa: F401
    _dependency_closure,
    _DepsNamespace,
    _eval_python_var,
    _evaluate_variables,
    _find_jinja_refs,
    _find_required_variables,
    _has_python_prefix,
    _strip_python_prefix,
)
from srunx.runtime.workflow.runner import (
    Slurm,  # noqa: F401 — tests monkeypatch ``srunx.runner.Slurm``
    WorkflowRunner,
    run_workflow_from_file,
)
from srunx.runtime.workflow.safe_eval import (  # noqa: F401
    _eval_node,
    _safe_eval,
    _safe_exec,
)
from srunx.runtime.workflow.transitions import _transition_workflow_run  # noqa: F401

__all__ = ["WorkflowRunner", "run_workflow_from_file"]
