"""Workflow runner package.

Canonical home for the YAML workflow loader and DAG-scheduling runner that
used to live in :mod:`srunx.runner`. The top-level :mod:`srunx.runner`
module is preserved as a thin backward-compat shim re-exporting the public
API from here.
"""

from srunx.runtime.workflow.runner import WorkflowRunner, run_workflow_from_file

__all__ = ["WorkflowRunner", "run_workflow_from_file"]
