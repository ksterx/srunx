"""Workflow YAML validation.

Wraps the ``/api/workflows/validate`` endpoint body — parses YAML, runs
the ``python:``-prefix guard on the ``args`` block, then drives it
through :meth:`WorkflowRunner.from_yaml` + :meth:`Workflow.validate` to
surface cycle-detection and other domain errors.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

import anyio

from srunx.common.exceptions import WorkflowValidationError
from srunx.runtime.workflow.runner import WorkflowRunner

from ._submission_common import reject_python_prefix_in_yaml_args


class WorkflowValidationService:
    """Validate a YAML workflow payload end-to-end."""

    async def validate_yaml(self, yaml_content: str) -> dict[str, Any]:
        if not yaml_content:
            return {"valid": False, "errors": ["Empty YAML content"]}

        reject_python_prefix_in_yaml_args(yaml_content)

        with tempfile.NamedTemporaryFile(suffix=".yaml", mode="w", delete=False) as f:
            f.write(yaml_content)
            tmp_path = f.name

        try:
            runner = await anyio.to_thread.run_sync(
                lambda: WorkflowRunner.from_yaml(tmp_path)
            )
            await anyio.to_thread.run_sync(runner.workflow.validate)
            return {"valid": True}
        except WorkflowValidationError as e:
            return {"valid": False, "errors": [str(e)]}
        except Exception as e:
            return {"valid": False, "errors": [str(e)]}
        finally:
            Path(tmp_path).unlink(missing_ok=True)
