"""Shared transport-related CLI option definitions.

Every CLI subcommand that talks to SLURM (``submit`` / ``cancel`` /
``status`` / ``list`` / ``logs`` / ``monitor jobs`` / ``flow run``)
re-uses these Annotated type aliases so help text, flag names, and
short-flag reservations stay consistent.

``--cluster`` has no short flag: ``-C`` / ``-M`` already mean
``--constraint`` / ``--clusters`` in real sbatch.
"""

from __future__ import annotations

from typing import Annotated

import typer

ClusterOpt = Annotated[
    str | None,
    typer.Option(
        "--cluster",
        help=(
            "Target cluster (from 'srunx cluster list'). Defaults to "
            "$SRUNX_CLUSTER, then the project's 'cluster', then "
            "'default_cluster'. No short form: -C is sbatch's --constraint "
            "and -M is Slurm's --clusters."
        ),
    ),
]
"""``--cluster <name>``: explicit cluster selection (design §5)."""

QuietOpt = Annotated[
    bool,
    typer.Option(
        "--quiet",
        "-Q",
        help=(
            "Suppress the cluster banner on stderr. Uses -Q (not -q) to match "
            "real sbatch/squeue/scancel, where -q is the short form of "
            "--qos (a value-taking option), not --quiet."
        ),
    ),
]
"""``--quiet`` / ``-Q``: suppress the transport banner (REQ-7).

Real SLURM reserves ``-q`` for ``--qos``; its own short flag for
``--quiet`` is ``-Q``. srunx used to squat on ``-q``; this alias now
matches upstream so a future ``-q`` can be reintroduced as a passthrough
for ``--qos`` without breaking existing muscle memory.
"""
