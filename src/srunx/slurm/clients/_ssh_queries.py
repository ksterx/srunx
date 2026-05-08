"""Job listing / detail queries (squeue + sacct) for the SSH SLURM client.

Free functions that take the :class:`SlurmSSHClient` as the first
argument. The client's bound methods (``list_jobs``, ``queue_by_ids``,
``get_job``, ``_list_active_jobs``) are 1-line forwards into here.

SLURM CLI output parsing dominates these functions; isolating them
makes the parsing rules independently reviewable and testable.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from srunx.common.logging import get_logger
from srunx.slurm.clients._ssh_helpers import _run_slurm_cmd, _validate_identifier
from srunx.slurm.parsing import GPU_TRES_RE
from srunx.slurm.protocols import (
    JobSnapshot,
    parse_slurm_datetime,
    parse_slurm_duration,
)
from srunx.slurm.states import SLURM_TERMINAL_JOB_STATES

if TYPE_CHECKING:
    from srunx.slurm.clients.ssh import SlurmSSHClient

logger = get_logger(__name__)


def list_active_jobs(
    client: SlurmSSHClient, user: str | None = None
) -> tuple[list[dict[str, Any]], set[int]]:
    """Return active (PENDING / RUNNING / ...) jobs from ``squeue`` only.

    Split out from :func:`list_jobs` so :meth:`SlurmSSHClient.queue` (the
    CLI ``srunx squeue`` path) can call this without triggering the
    ``sacct -S now-6hours`` merge that would otherwise inject
    terminal-state rows from the past 6 hours — that asymmetry
    with the local ``Slurm.queue()`` broke user expectations
    (native ``squeue`` never shows finished jobs).

    ``user=None`` shows all users' jobs (matches native ``squeue``
    and local :meth:`~srunx.slurm.local.Slurm.queue`). Pass a
    username to filter.

    Format fields (all surfaced by :meth:`SlurmSSHClient.queue`):
    ``%i`` job_id | ``%P`` partition | ``%j`` name | ``%u`` user |
    ``%T`` state (long) | ``%M`` elapsed | ``%l`` time_limit |
    ``%D`` nodes | ``%C`` total CPUs | ``%R`` nodelist-or-reason |
    ``%b`` TRES_PER_NODE (for GPU extraction).

    Returns ``(entries, seen_ids)`` so the merging caller can
    dedup sacct rows against active IDs without re-scanning.
    """
    # ``|`` delimiter: nodelist reasons can contain whitespace and
    # parens (e.g. "(Resources, Priority)"), so splitting on
    # whitespace with maxsplit is fragile. Pipe is the safe
    # separator — SLURM fields never contain it.
    fmt = "%i|%P|%j|%u|%T|%M|%l|%D|%C|%R|%b"
    cmd = f'squeue --format "{fmt}" --noheader'
    if user:
        _validate_identifier(user, "user")
        cmd += f" --user {user}"

    output = _run_slurm_cmd(client, cmd)
    jobs: list[dict[str, Any]] = []
    seen_ids: set[int] = set()

    for line in output.strip().splitlines():
        parts = line.split("|")
        # Strict equality check — SLURM allows ``|`` inside user-chosen
        # fields like job name (``#SBATCH --job-name=foo|bar``). Such
        # rows split into 12+ fields; ``< 11`` would pass and the
        # subsequent indexing would silently misalign every column
        # downstream of the embedded pipe. Better to drop the row
        # than render corrupted data in an admin's queue listing.
        if len(parts) != 11:
            continue

        try:
            job_id = int(parts[0].strip())
        except ValueError:
            continue

        partition = parts[1].strip()
        name = parts[2].strip()
        owner = parts[3].strip() or None
        state = parts[4].strip()
        elapsed = parts[5].strip()
        time_limit = parts[6].strip()
        nodes_str = parts[7].strip()
        cpus_str = parts[8].strip()
        nodelist = parts[9].strip()
        tres = parts[10].strip()

        num_nodes = int(nodes_str) if nodes_str.isdigit() else 1
        try:
            cpus_total = int(cpus_str) if cpus_str else 0
        except ValueError:
            cpus_total = 0
        gpus_per_node = 0
        gpu_match = GPU_TRES_RE.search(tres)
        if gpu_match:
            gpus_per_node = int(gpu_match.group(1))

        seen_ids.add(job_id)
        jobs.append(
            {
                "name": name,
                "job_id": job_id,
                "status": state,
                "depends_on": [],
                "command": [],
                "resources": {
                    "nodes": num_nodes,
                    "gpus_per_node": gpus_per_node,
                    "partition": partition,
                    "time_limit": time_limit,
                },
                "partition": partition,
                "user": owner,
                "nodes": num_nodes,
                "cpus": cpus_total,
                "gpus": gpus_per_node * num_nodes,
                "nodelist": nodelist,
                "elapsed_time": elapsed,
                "time_limit": time_limit,
            }
        )

    return jobs, seen_ids


def list_jobs(client: SlurmSSHClient, user: str | None = None) -> list[dict[str, Any]]:
    """List SLURM jobs via squeue + recent completed/failed jobs via sacct.

    Used by the Web UI and MCP ``list_jobs`` tool. The CLI
    ``srunx squeue`` path goes through :meth:`SlurmSSHClient.queue`
    instead, which uses :func:`list_active_jobs` directly so it matches
    native ``squeue`` semantics (active jobs only).
    """
    jobs, seen_ids = list_active_jobs(client, user)

    # --- Recently finished jobs from sacct (last 6 hours) ---
    # NOTE: --state filter is omitted because some SLURM versions
    # return empty output when --state is combined with --parsable2.
    # We filter by status in Python instead.
    try:
        sacct_cmd = (
            "sacct -S now-6hours "
            "--format=JobID,JobName,State,Partition,NNodes,Elapsed,TimelimitRaw,AllocTRES,User "
            "--noheader --parsable2"
        )
        if user:
            sacct_cmd += f" --user {user}"

        sacct_output = _run_slurm_cmd(client, sacct_cmd)

        for line in sacct_output.strip().splitlines():
            parts = line.split("|")
            if len(parts) < 6:
                continue
            # Skip sub-steps (e.g., "12345.batch", "12345.extern")
            if "." in parts[0]:
                continue

            try:
                job_id = int(parts[0].strip())
            except ValueError:
                continue

            if job_id in seen_ids:
                continue

            # sacct may return e.g. "CANCELLED by 1000" — take first word only.
            # Skip non-terminal states (already covered by squeue).
            raw_state = parts[2].strip()
            status = raw_state.split()[0] if raw_state else "UNKNOWN"
            if status not in SLURM_TERMINAL_JOB_STATES:
                continue

            gpus = 0
            if len(parts) >= 8:
                gpu_match = GPU_TRES_RE.search(parts[7])
                if gpu_match:
                    gpus = int(gpu_match.group(1))

            num_nodes = int(parts[4]) if parts[4].strip().isdigit() else 1

            owner = parts[8].strip() if len(parts) > 8 else ""
            seen_ids.add(job_id)
            jobs.append(
                {
                    "name": parts[1].strip(),
                    "job_id": job_id,
                    "status": status,
                    "depends_on": [],
                    "command": [],
                    "resources": {
                        "nodes": num_nodes,
                        "gpus_per_node": gpus,
                        "partition": parts[3].strip(),
                        "time_limit": parts[6].strip() if len(parts) > 6 else None,
                    },
                    "partition": parts[3].strip(),
                    "user": owner or None,
                    "nodes": num_nodes,
                    "gpus": gpus * num_nodes,
                    "elapsed_time": parts[5].strip(),
                }
            )
    except Exception:  # noqa: BLE001
        logger.warning(
            "sacct query failed; returning squeue results only", exc_info=True
        )

    return jobs


def queue_by_ids(client: SlurmSSHClient, job_ids: list[int]) -> dict[int, JobSnapshot]:
    """Return a mapping of ``job_id`` -> :class:`JobSnapshot` for active jobs.

    Implements :class:`Client`. Active jobs are queried via
    ``squeue --jobs=...``; jobs no longer in the queue fall back to
    ``sacct``, and finally to ``scontrol show job`` for pyxis clusters
    where slurmdbd is unreachable but the in-memory job record is still
    fresh (within ``MinJobAge``). Jobs found in none of the three
    sources are omitted.
    """
    if not job_ids:
        return {}

    for jid in job_ids:
        if jid <= 0:
            raise ValueError(f"Invalid job_id: {jid}")

    id_arg = ",".join(str(i) for i in job_ids)
    results: dict[int, JobSnapshot] = {}

    # --- squeue: active jobs ---
    try:
        squeue_out = _run_slurm_cmd(
            client,
            f'squeue --jobs {id_arg} --format "%i|%T|%S|%M|%N" --noheader',
        )
    except RuntimeError:
        # squeue may fail if NO job IDs are currently in the queue;
        # this is normal when all queried jobs have already completed.
        squeue_out = ""

    for line in squeue_out.strip().splitlines():
        parts = line.split("|")
        if len(parts) < 5:
            continue
        try:
            jid = int(parts[0].strip())
        except ValueError:
            continue
        results[jid] = JobSnapshot(
            status=parts[1].strip(),
            started_at=parse_slurm_datetime(parts[2]),
            duration_secs=parse_slurm_duration(parts[3]),
            nodelist=(parts[4].strip() or None),
        )

    # --- sacct fallback: terminal jobs ---
    missing = [j for j in job_ids if j not in results]
    if missing:
        missing_arg = ",".join(str(i) for i in missing)
        try:
            sacct_out = _run_slurm_cmd(
                client,
                f"sacct --jobs {missing_arg} "
                f"--format=JobID,State,Start,End,Elapsed,NodeList "
                f"--noheader --parsable2",
            )
        except RuntimeError:
            sacct_out = ""

        for line in sacct_out.strip().splitlines():
            parts = line.split("|")
            if len(parts) < 6:
                continue
            raw_id = parts[0].strip()
            if "." in raw_id:
                continue
            try:
                jid = int(raw_id)
            except ValueError:
                continue
            if jid in results:
                continue
            raw_state = parts[1].strip()
            status = raw_state.split()[0] if raw_state else "UNKNOWN"
            results[jid] = JobSnapshot(
                status=status,
                started_at=parse_slurm_datetime(parts[2]),
                completed_at=parse_slurm_datetime(parts[3]),
                duration_secs=parse_slurm_duration(parts[4]),
                nodelist=(parts[5].strip() or None),
            )

    # --- scontrol fallback: pyxis clusters where sacct is unreachable ---
    # slurmdbd outages leave sacct returning empty for just-finished
    # jobs. scontrol keeps the record in memory for ~5 minutes
    # (MinJobAge) without needing the accounting DB, so we probe it
    # per-missing-id. Per-id cost is acceptable because the poller
    # runs every 15s and the missing set is only the final-transition
    # tail, not the full active set.
    from srunx.ssh.core.utils import parse_scontrol_job_state

    still_missing = [j for j in job_ids if j not in results]
    for jid in still_missing:
        try:
            scontrol_out = _run_slurm_cmd(
                client, f"scontrol show job {jid} 2>/dev/null"
            )
        except RuntimeError:
            continue
        parsed = parse_scontrol_job_state(scontrol_out)
        if parsed is None:
            continue
        results[jid] = JobSnapshot(status=parsed)

    return results


def get_job(client: SlurmSSHClient, job_id: int) -> dict[str, Any]:
    """Get detailed job info via sacct."""
    cmd = (
        f"sacct -j {job_id} "
        "--format=JobID,JobName,State,Partition,NNodes,NCPUS,Elapsed,TimelimitRaw,AllocTRES "
        "--noheader --parsable2"
    )
    output = _run_slurm_cmd(client, cmd)

    for line in output.strip().splitlines():
        parts = line.split("|")
        if len(parts) < 5:
            continue
        # Skip sub-steps (e.g., "12345.batch")
        if "." in parts[0]:
            continue

        gpus = 0
        if len(parts) >= 9:
            tres = parts[8]
            gpu_match = GPU_TRES_RE.search(tres)
            if gpu_match:
                gpus = int(gpu_match.group(1))

        return {
            "name": parts[1].strip(),
            "job_id": job_id,
            "status": parts[2].strip(),
            "depends_on": [],
            "command": [],
            "resources": {
                "nodes": int(parts[4]) if parts[4].strip().isdigit() else 1,
                "gpus_per_node": gpus,
                "partition": parts[3].strip(),
            },
            "partition": parts[3].strip(),
            "nodes": int(parts[4]) if parts[4].strip().isdigit() else None,
            "gpus": gpus,
            "elapsed_time": parts[6].strip() if len(parts) > 6 else None,
        }

    raise ValueError(f"No job information found for job {job_id}")
