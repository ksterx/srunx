"""Backward-compat shim. Canonical home: :mod:`srunx.slurm.local`.

``Slurm`` is preserved as an alias for :class:`LocalClient` during
the migration (#156). New code should import ``LocalClient`` directly.
"""

from srunx.slurm.local import (
    LocalClient,
    cancel_job,
    retrieve_job,
    submit_job,
)

# Backward-compat alias
Slurm = LocalClient

__all__ = ["LocalClient", "Slurm", "cancel_job", "retrieve_job", "submit_job"]
