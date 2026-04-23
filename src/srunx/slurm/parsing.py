"""Low-level SLURM output parsing — regex + datetime parsers."""

from __future__ import annotations

import re

from srunx.slurm.protocols import parse_slurm_datetime, parse_slurm_duration

# Shared regex for parsing GPU counts from SLURM TRES/Gres strings.
# Matches: "gpu:8", "gres/gpu=8", "gpu:NVIDIA-A100:8", "gpu/4", etc.
GPU_TRES_RE = re.compile(r"gpu[:/=](?:[^:]+:)?(\d+)", re.IGNORECASE)

__all__ = ["GPU_TRES_RE", "parse_slurm_datetime", "parse_slurm_duration"]
