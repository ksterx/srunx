"""Native SLURM ``sacct`` wrapper for the ``srunx sacct`` CLI.

This module is the complement to :mod:`srunx.slurm.partitions`: it
wraps the real SLURM ``sacct`` binary so ``srunx sacct`` can answer
"what happened on this cluster" queries. ``srunx history`` remains the
view into srunx's own SQLite (submissions this client made) — these
two are deliberately separate axes (cluster-wide accounting vs.
srunx's own record).

The parser and fetchers mirror the structure of
:mod:`srunx.slurm.partitions`: a transport-agnostic parser plus
subprocess / SSH fetchers that share the same row shape.
"""

from __future__ import annotations

import subprocess
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from srunx.slurm.ssh import SlurmSSHAdapter


# The column set is deliberately wider than the default ``sacct``
# output so the JSON emitter has every field a power user might want
# without a second call. ``AllocTRES`` is included so a downstream
# caller can extract GPU counts the same way ``_list_active_jobs``
# does from ``%b`` (gpu:N inside the TRES string).
_SACCT_COLUMNS: tuple[str, ...] = (
    "JobID",
    "JobName",
    "User",
    "Partition",
    "Account",
    "State",
    "ExitCode",
    "Elapsed",
    "Submit",
    "Start",
    "End",
    "AllocCPUS",
    "AllocTRES",
)
_SACCT_FORMAT = ",".join(_SACCT_COLUMNS)

# ``--parsable2`` = pipe-delimited, no trailing pipe. Combined with
# ``--noheader`` the parser sees one row per line with exactly
# ``len(_SACCT_COLUMNS)`` fields. Safer than whitespace splitting
# because JobName and AllocTRES can contain spaces or commas.
_SACCT_BASE_FLAGS: tuple[str, ...] = (
    "--parsable2",
    "--noheader",
    f"--format={_SACCT_FORMAT}",
)


@dataclass(frozen=True)
class SacctRow:
    """One line from ``sacct --parsable2``.

    ``job_id`` stays a string because SLURM uses ``.batch`` / ``.N``
    suffixes for job steps (e.g. ``"12345.batch"``) — stripping those
    would collapse distinct accounting rows. :attr:`is_step` is the
    convenience predicate the CLI uses to hide steps by default.
    """

    job_id: str
    job_name: str
    user: str | None
    partition: str | None
    account: str | None
    state: str
    exit_code: str
    elapsed: str | None
    submit: str | None
    start: str | None
    end: str | None
    alloc_cpus: int | None
    alloc_tres: str | None

    @property
    def is_step(self) -> bool:
        """True for sub-step rows (``12345.batch`` / ``12345.0`` / ...).

        Useful for the CLI's default "hide sub-steps" behaviour — real
        ``sacct`` shows them, but the median srunx user wants the
        parent job summary.
        """
        return "." in self.job_id

    def to_dict(self) -> dict[str, object]:
        return {
            "job_id": self.job_id,
            "job_name": self.job_name,
            "user": self.user,
            "partition": self.partition,
            "account": self.account,
            "state": self.state,
            "exit_code": self.exit_code,
            "elapsed": self.elapsed,
            "submit": self.submit,
            "start": self.start,
            "end": self.end,
            "alloc_cpus": self.alloc_cpus,
            "alloc_tres": self.alloc_tres,
            "is_step": self.is_step,
        }


def parse_sacct_rows(stdout: str) -> list[SacctRow]:
    """Parse the output of ``sacct --parsable2 --noheader --format=...``.

    Malformed lines (wrong field count) are skipped rather than
    raising — a single bad row shouldn't sink the whole listing, and
    ``sacct`` can emit partial data under slurmdbd hiccups.
    ``state`` is normalised to the first word so ``CANCELLED by 1000``
    becomes ``CANCELLED`` (the parent status).
    """
    rows: list[SacctRow] = []
    for raw in stdout.splitlines():
        line = raw.rstrip("\n")
        if not line.strip():
            continue
        parts = line.split("|")
        if len(parts) != len(_SACCT_COLUMNS):
            continue
        (
            job_id,
            job_name,
            user,
            partition,
            account,
            state,
            exit_code,
            elapsed,
            submit,
            start,
            end,
            alloc_cpus,
            alloc_tres,
        ) = (p.strip() for p in parts)
        cpus: int | None
        try:
            cpus = int(alloc_cpus) if alloc_cpus else None
        except ValueError:
            cpus = None
        # ``CANCELLED by 1000`` → keep just ``CANCELLED`` so the
        # downstream colour map / filter logic sees the canonical
        # state name. Full reason stays one split away if ever needed.
        state_norm = state.split()[0] if state else ""
        rows.append(
            SacctRow(
                job_id=job_id,
                job_name=job_name,
                user=user or None,
                partition=partition or None,
                account=account or None,
                state=state_norm,
                exit_code=exit_code,
                elapsed=elapsed or None,
                submit=submit or None,
                start=start or None,
                end=end or None,
                alloc_cpus=cpus,
                alloc_tres=alloc_tres or None,
            )
        )
    return rows


def build_sacct_filter_args(
    *,
    job_ids: Sequence[int] | None = None,
    user: str | None = None,
    all_users: bool = False,
    start_time: str | None = None,
    end_time: str | None = None,
    state: str | None = None,
    partition: str | None = None,
) -> list[str]:
    """Translate CLI filter options into ``sacct`` flag strings.

    Shared by the local and SSH fetchers so the two transports can't
    drift on which filters they forward. Omits validation that only
    SLURM itself can judge (e.g. state name spelling) — if the flag
    is bogus, ``sacct`` exits non-zero and the CLI surfaces the
    error as-is.
    """
    from srunx.slurm.ssh import _validate_identifier

    args: list[str] = []
    # ``-a`` and ``-u`` are independent flags in native ``sacct`` — passing
    # both is valid and means "scan all users but filter to <user>" (``-a``
    # only overrides the implicit self-filter default). Emit both when the
    # caller provides both so srunx matches real sacct's semantics instead
    # of silently dropping the narrower filter.
    if all_users:
        args.append("--allusers")
    if user:
        _validate_identifier(user, "user")
        args += ["--user", user]
    if job_ids:
        # sacct accepts a comma-separated list on ``--jobs``. Cast
        # through str(int) to keep the command shell-safe without
        # having to invoke a shell at all (subprocess runs the argv
        # directly).
        args += ["--jobs", ",".join(str(int(j)) for j in job_ids)]
    if start_time:
        args += ["--starttime", start_time]
    if end_time:
        args += ["--endtime", end_time]
    if state:
        args += ["--state", state]
    if partition:
        _validate_identifier(partition, "partition")
        args += ["--partition", partition]
    return args


def fetch_sacct_rows_local(
    *,
    job_ids: Sequence[int] | None = None,
    user: str | None = None,
    all_users: bool = False,
    start_time: str | None = None,
    end_time: str | None = None,
    state: str | None = None,
    partition: str | None = None,
    timeout: float = 60.0,
) -> list[SacctRow]:
    """Run local ``sacct`` with the requested filters and parse the output.

    Raises the underlying :class:`subprocess.CalledProcessError` /
    :class:`FileNotFoundError` / :class:`subprocess.TimeoutExpired`
    so the CLI layer can render a transport-appropriate error
    message consistent with :mod:`srunx.slurm.partitions`.
    """
    cmd = ["sacct", *_SACCT_BASE_FLAGS]
    cmd += build_sacct_filter_args(
        job_ids=job_ids,
        user=user,
        all_users=all_users,
        start_time=start_time,
        end_time=end_time,
        state=state,
        partition=partition,
    )
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=True,
    )
    return parse_sacct_rows(result.stdout)


def fetch_sacct_rows_ssh(
    adapter: SlurmSSHAdapter,
    *,
    job_ids: Sequence[int] | None = None,
    user: str | None = None,
    all_users: bool = False,
    start_time: str | None = None,
    end_time: str | None = None,
    state: str | None = None,
    partition: str | None = None,
) -> list[SacctRow]:
    """Run ``sacct`` on the remote cluster via the SSH adapter.

    Uses the same ``_run_slurm_cmd`` path as
    :mod:`srunx.slurm.partitions` so login-shell env + SLURM PATH
    resolution + the adapter's I/O lock all apply uniformly.
    """
    from srunx.slurm.ssh import _run_slurm_cmd

    parts: list[str] = ["sacct", *_SACCT_BASE_FLAGS]
    parts += build_sacct_filter_args(
        job_ids=job_ids,
        user=user,
        all_users=all_users,
        start_time=start_time,
        end_time=end_time,
        state=state,
        partition=partition,
    )
    # Shell-quote nothing because every element has already been
    # validated (_validate_identifier) or is a simple literal; the
    # identifiers can't contain whitespace or metacharacters.
    cmd = " ".join(_shell_quote(p) for p in parts)
    stdout = _run_slurm_cmd(adapter, cmd)
    return parse_sacct_rows(stdout)


def _shell_quote(arg: str) -> str:
    """Minimal shell quoting for ``_run_slurm_cmd`` argv → string.

    ``_run_slurm_cmd`` takes a single command string (runs through
    ``bash -lc`` on the remote). Fields like ``--format=JobID,...``
    need no quoting but date strings like ``2026-04-01 00:00:00``
    do. Single-quote everything that contains whitespace or a
    shell meta-character — identifiers are already ASCII-safe by
    construction (validated upstream).
    """
    if arg and all(c.isalnum() or c in "-_=:/," for c in arg):
        return arg
    escaped = arg.replace("'", "'\\''")
    return f"'{escaped}'"


def filter_out_steps(rows: Iterable[SacctRow]) -> list[SacctRow]:
    """Return only the parent job rows (drop ``.batch`` / ``.N`` steps).

    Small helper used by the CLI default view; kept here so the
    tests can exercise it directly without spinning up the CLI.
    """
    return [row for row in rows if not row.is_step]
