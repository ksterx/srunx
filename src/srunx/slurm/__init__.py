"""SLURM protocol-level constants and helpers.

This subpackage holds values that come straight from SLURM's wire
vocabulary (state strings, etc.) — distinct from ``srunx.models``,
which carries the narrower domain enum.
"""

from __future__ import annotations

from .states import SLURM_TERMINAL_JOB_STATES

__all__ = ["SLURM_TERMINAL_JOB_STATES"]
