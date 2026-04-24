"""Shared SLURM state → Rich colour mapping.

Both ``srunx squeue`` and ``srunx sacct`` colourise the Status / State
column. The two commands used to keep independent copies of the map
with a "kept in sync" comment — which is exactly the drift-prone
anti-pattern this module removes. Any future state colour change
happens here and propagates to every renderer automatically.

Colour semantics — the grouping is the contract, not the individual
hues. A reviewer should be able to change ``cyan`` → ``blue`` for the
"in-progress" group without breaking anything:

* ``cyan``      — in-progress (RUNNING / COMPLETING / CONFIGURING)
* ``green``     — success
* ``yellow``    — waiting
* ``magenta``   — unusual live state (suspended / stopped)
* ``bright_black`` (gray) — user stopped / neutral terminal
* ``red``       — job-level failure
* ``bright_red``— infrastructure failure
"""

from __future__ import annotations

SLURM_STATE_COLORS: dict[str, str] = {
    "RUNNING": "cyan",
    "COMPLETING": "cyan",
    "CONFIGURING": "cyan",
    "COMPLETED": "green",
    "PENDING": "yellow",
    "SUSPENDED": "magenta",
    "STOPPED": "magenta",
    "CANCELLED": "bright_black",
    "REVOKED": "bright_black",
    "UNKNOWN": "bright_black",
    "FAILED": "red",
    "TIMEOUT": "red",
    "PREEMPTED": "red",
    "DEADLINE": "red",
    "BOOT_FAIL": "bright_red",
    "NODE_FAIL": "bright_red",
    "OUT_OF_MEMORY": "bright_red",
}


def colorize_state(state: str, *, default: str = "white") -> str:
    """Return ``state`` wrapped in Rich markup for its semantic colour.

    Unknown states fall back to ``default`` (plain white). Keeps the
    ``[color]X[/color]`` wrapping pattern in one place so the two
    table renderers can't drift on formatting either.
    """
    color = SLURM_STATE_COLORS.get(state, default)
    return f"[{color}]{state}[/{color}]"
