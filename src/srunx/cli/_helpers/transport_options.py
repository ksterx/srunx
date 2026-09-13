"""Shared transport-related CLI option definitions.

Every CLI subcommand that talks to SLURM (``submit`` / ``cancel`` /
``status`` / ``list`` / ``logs`` / ``monitor jobs`` / ``flow run``)
re-uses these Annotated type aliases so help text, flag names, and
short-flag reservations stay consistent.

See REQ-6 for the enumerated CLI surface. No ``-p`` short flag is
assigned to ``--profile`` because ``resources`` / ``monitor resources``
already reserve ``-p`` for ``--partition`` (P-8).
"""

from __future__ import annotations

from typing import Annotated

import typer

ProfileOpt = Annotated[
    str | None,
    typer.Option(
        "--profile",
        help=(
            "SSH profile name (from 'srunx ssh list'). When set, "
            "commands run over SSH against that profile's cluster. "
            "Mutually exclusive with --local."
        ),
    ),
]
"""``--profile <name>``: explicit SSH profile selection (REQ-1)."""

LocalOpt = Annotated[
    bool,
    typer.Option(
        "--local",
        help=(
            "Force local SLURM transport (overrides $SRUNX_SSH_PROFILE). "
            "Mutually exclusive with --profile."
        ),
    ),
]
"""``--local``: force local transport even when env selects SSH (REQ-1)."""

QuietOpt = Annotated[
    bool,
    typer.Option(
        "--quiet",
        "-Q",
        help=(
            "Suppress the transport banner on stderr. Only meaningful "
            "when an explicit transport source is selected "
            "(--profile / --local / $SRUNX_SSH_PROFILE); the default "
            "local path never prints a banner. Uses -Q (not -q) to match "
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
