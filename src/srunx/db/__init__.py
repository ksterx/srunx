"""Backward-compat shim. Canonical home: :mod:`srunx.observability.storage`.

External code should migrate to ``srunx.observability.storage`` (and its
submodules). This module exists so that existing ``from srunx.db.X import Y``
call-sites keep working during the Phase 8 transition (#164).

Submodules are aliased via ``sys.modules`` so that ``srunx.db.cli_helpers``
and ``srunx.observability.storage.cli_helpers`` refer to the **same** module
object — preserving monkey-patching and ``is`` identity checks.
"""

from __future__ import annotations

import sys as _sys

from srunx.observability.storage import (  # noqa: F401
    JobRepository,
    get_job_repo,
    init_db,
    open_connection,
)

# Import every submodule eagerly so we can alias them in sys.modules before
# any ``from srunx.db.X import ...`` lookup runs.
from srunx.observability.storage import (  # noqa: F401
    cli_helpers as _cli_helpers,
)
from srunx.observability.storage import (
    connection as _connection,
)
from srunx.observability.storage import (
    migrations as _migrations,
)
from srunx.observability.storage import (
    models as _models,
)
from srunx.observability.storage import (
    repositories as _repositories,
)
from srunx.observability.storage.repositories import (  # noqa: F401
    base as _repo_base,
)
from srunx.observability.storage.repositories import (
    deliveries as _repo_deliveries,
)
from srunx.observability.storage.repositories import (
    endpoints as _repo_endpoints,
)
from srunx.observability.storage.repositories import (
    events as _repo_events,
)
from srunx.observability.storage.repositories import (
    job_state_transitions as _repo_job_state_transitions,
)
from srunx.observability.storage.repositories import (
    jobs as _repo_jobs,
)
from srunx.observability.storage.repositories import (
    resource_snapshots as _repo_resource_snapshots,
)
from srunx.observability.storage.repositories import (
    subscriptions as _repo_subscriptions,
)
from srunx.observability.storage.repositories import (
    sweep_runs as _repo_sweep_runs,
)
from srunx.observability.storage.repositories import (
    watches as _repo_watches,
)
from srunx.observability.storage.repositories import (
    workflow_run_jobs as _repo_workflow_run_jobs,
)
from srunx.observability.storage.repositories import (
    workflow_runs as _repo_workflow_runs,
)

# Register aliases: `srunx.db.X` → same module object as
# `srunx.observability.storage.X`. Must happen at import time of ``srunx.db``
# so that subsequent ``from srunx.db.X import ...`` succeeds.
_sys.modules[f"{__name__}.cli_helpers"] = _cli_helpers
_sys.modules[f"{__name__}.connection"] = _connection
_sys.modules[f"{__name__}.migrations"] = _migrations
_sys.modules[f"{__name__}.models"] = _models
_sys.modules[f"{__name__}.repositories"] = _repositories
_sys.modules[f"{__name__}.repositories.base"] = _repo_base
_sys.modules[f"{__name__}.repositories.deliveries"] = _repo_deliveries
_sys.modules[f"{__name__}.repositories.endpoints"] = _repo_endpoints
_sys.modules[f"{__name__}.repositories.events"] = _repo_events
_sys.modules[f"{__name__}.repositories.job_state_transitions"] = (
    _repo_job_state_transitions
)
_sys.modules[f"{__name__}.repositories.jobs"] = _repo_jobs
_sys.modules[f"{__name__}.repositories.resource_snapshots"] = _repo_resource_snapshots
_sys.modules[f"{__name__}.repositories.subscriptions"] = _repo_subscriptions
_sys.modules[f"{__name__}.repositories.sweep_runs"] = _repo_sweep_runs
_sys.modules[f"{__name__}.repositories.watches"] = _repo_watches
_sys.modules[f"{__name__}.repositories.workflow_run_jobs"] = _repo_workflow_run_jobs
_sys.modules[f"{__name__}.repositories.workflow_runs"] = _repo_workflow_runs

# Bind submodules as attributes of ``srunx.db`` so ``srunx.db.cli_helpers``
# attribute access works (the ``sys.modules`` entries handle the
# ``from srunx.db.X import Y`` form).
cli_helpers = _cli_helpers
connection = _connection
migrations = _migrations
models = _models
repositories = _repositories

__all__ = ["JobRepository", "get_job_repo", "init_db", "open_connection"]
