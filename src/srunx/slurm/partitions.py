"""Partition-level ``sinfo`` info for the ``srunx sinfo`` CLI.

Native SLURM ``sinfo`` answers "what partitions exist, which nodes
are in each state, which nodelist each row covers". srunx previously
aggregated those rows into a GPU-only summary (now moved to
``srunx gpus``). This module provides the raw row model so the CLI
can render the same information a SLURM user expects.

Data flow mirrors :mod:`srunx.slurm.clients.ssh._run_slurm_cmd` vs
:mod:`subprocess.run` dispatch: the parser is transport-agnostic, and
the two fetchers wrap whichever command runner fits the transport.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from srunx.slurm.clients.ssh import SlurmSSHClient


# ``|`` delimiter: sinfo field values never contain it, and it avoids
# the ambiguity that whitespace-splitting would introduce for NODELIST
# values like ``node[01-03,05]`` or any partition name SLURM allows.
_SINFO_FORMAT = "%P|%a|%l|%D|%T|%N"
_SINFO_ARGS = ["-o", _SINFO_FORMAT, "--noheader"]


@dataclass(frozen=True)
class PartitionRow:
    """One row of ``sinfo`` output.

    ``partition`` is the bare partition name (trailing ``*`` default
    marker is lifted into :attr:`is_default`). ``state`` is the long
    form (e.g. ``idle`` / ``mixed`` / ``allocated`` / ``drained``) as
    returned by ``sinfo -o %T``.
    """

    partition: str
    is_default: bool
    avail: str
    timelimit: str
    nodes: int
    state: str
    nodelist: str

    def to_dict(self) -> dict[str, object]:
        return {
            "partition": self.partition,
            "is_default": self.is_default,
            "avail": self.avail,
            "timelimit": self.timelimit,
            "nodes": self.nodes,
            "state": self.state,
            "nodelist": self.nodelist,
        }


def parse_sinfo_partition_rows(stdout: str) -> list[PartitionRow]:
    """Parse ``sinfo -o '%P|%a|%l|%D|%T|%N' --noheader`` output.

    Malformed lines (wrong field count, non-integer node count) are
    skipped rather than raising — ``sinfo`` can emit partial output
    for partitions in odd states, and one bad row shouldn't sink the
    whole listing.
    """
    rows: list[PartitionRow] = []
    for raw in stdout.splitlines():
        line = raw.strip()
        if not line:
            continue
        parts = line.split("|")
        if len(parts) != 6:
            continue
        partition_field, avail, timelimit, nodes_str, state, nodelist = parts
        try:
            nodes = int(nodes_str)
        except ValueError:
            continue
        is_default = partition_field.endswith("*")
        partition = partition_field.rstrip("*")
        rows.append(
            PartitionRow(
                partition=partition,
                is_default=is_default,
                avail=avail,
                timelimit=timelimit,
                nodes=nodes,
                state=state,
                nodelist=nodelist,
            )
        )
    return rows


def fetch_sinfo_rows_local(
    partition: str | None = None, *, timeout: float = 30.0
) -> list[PartitionRow]:
    """Run local ``sinfo`` and return parsed partition rows.

    Raises the underlying :class:`subprocess.CalledProcessError` /
    :class:`FileNotFoundError` / :class:`subprocess.TimeoutExpired`
    so the CLI layer can render a transport-appropriate error message
    (consistent with the rest of ``srunx.cli.commands.jobs``).
    """
    cmd = ["sinfo", *_SINFO_ARGS]
    if partition:
        cmd += ["-p", partition]
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=True,
    )
    return parse_sinfo_partition_rows(result.stdout)


def fetch_sinfo_rows_ssh(
    adapter: SlurmSSHClient, partition: str | None = None
) -> list[PartitionRow]:
    """Run ``sinfo`` on the remote cluster via the SSH adapter.

    Delegates to the same ``_run_slurm_cmd`` path the rest of
    :mod:`srunx.slurm.clients.ssh` uses so login-shell env, SLURM PATH
    resolution, and the client's I/O lock all apply uniformly.
    """
    # Import locally so the CLI doesn't pay the paramiko import cost
    # on the local subprocess path.
    from srunx.slurm.clients._ssh_helpers import (
        _run_slurm_cmd,
        _validate_identifier,
    )

    cmd = f"sinfo -o '{_SINFO_FORMAT}' --noheader"
    if partition:
        _validate_identifier(partition, "partition")
        cmd += f" -p {partition}"
    stdout = _run_slurm_cmd(adapter, cmd)
    return parse_sinfo_partition_rows(stdout)
