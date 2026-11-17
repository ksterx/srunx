"""Ask the scheduler about jobs, without ever raising and without guessing.

The command runner is injected (``SlurmExecute``) so this module does not import
``srunx.slurm``. Every command is judged only by its stdout and its exit code;
stderr is never inspected, because some clusters print permission warnings on
stderr for commands that still succeed.

Several commands are chained into one remote command line. Each runs in a
*section*: a marker line, the command's stdout, and an ``RC <n>`` line, so a
missing marker or a missing RC line is visible and treated as a failure.
"""

from __future__ import annotations

import dataclasses
import re
import secrets
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Literal

from loguru import logger

SlurmExecute = Callable[[str], tuple[str, str, int]]

TERMINAL_STATES = frozenset(
    {
        "COMPLETED",
        "FAILED",
        "CANCELLED",
        "TIMEOUT",
        "NODE_FAIL",
        "PREEMPTED",
        "OUT_OF_MEMORY",
        "BOOT_FAIL",
        "DEADLINE",
    }
)
SACCT_CHUNK = 500
# ASCII unit separator between squeue fields: it does not occur in paths or
# command lines, unlike ``|`` or a space. ``squeue -o`` does not interpret an
# escape such as ``\x1f``, so the real byte is produced by the remote shell.
SQUEUE_SEP = "\x1f"
CLUSTER_NAME_RE = re.compile(r"[A-Za-z0-9._-]+")
CLUSTER_LOOKUP_TIMEOUT = 10

_NONCE_RE = re.compile(r"[0-9a-f]{8,64}")
_SACCT_FIELDS = 4
_RC_RE = re.compile(r"RC (-?\d+)")
_MIN_JOB_AGE_RE = re.compile(r"\s*MinJobAge\s*=\s*(\d+)")
_JOB_STATE_RE = re.compile(r"\bJobState=(\S+)")
_DIGITS_RE = re.compile(r"\d+")


def _marker(nonce: str, name: str) -> str:
    return f"__SRUNX_{nonce}_{name}__"


def _section(nonce: str, name: str, command: str) -> str:
    """One marked section; ``RC`` goes on its own line even without a trailing newline."""
    return (
        f"echo {_marker(nonce, name)}; {command}; "
        "srunx_rc=$?; printf '\\nRC %s\\n' \"$srunx_rc\""
    )


def _split_sections(stdout: str, nonce: str) -> dict[str, tuple[list[str], int | None]]:
    """Split output into ``name -> (stdout lines, rc)``.

    Text before the first marker (login banners) is dropped. ``rc`` is None
    when the section has no ``RC`` line. The last ``RC`` line wins so command
    output that happens to look like one cannot mask the real exit code.
    """
    prefix = f"__SRUNX_{nonce}_"
    sections: dict[str, list[str]] = {}
    current: list[str] | None = None
    for line in stdout.splitlines():
        if (
            line.startswith(prefix)
            and line.endswith("__")
            and len(line) > len(prefix) + 2
        ):
            name = line[len(prefix) : -2]
            current = sections.setdefault(name, [])
            continue
        if current is not None:
            current.append(line)
    result: dict[str, tuple[list[str], int | None]] = {}
    for name, lines in sections.items():
        rc: int | None = None
        rc_index = None
        for i in range(len(lines) - 1, -1, -1):
            m = _RC_RE.fullmatch(lines[i])
            if m:
                rc = int(m.group(1))
                rc_index = i
                break
        body = lines if rc_index is None else lines[:rc_index]
        result[name] = (body, rc)
    return result


def _bounded(command: str) -> str:
    """Run *command* under ``timeout`` when the remote has it (one attempt)."""
    return (
        "if command -v timeout >/dev/null 2>&1; "
        f"then timeout {CLUSTER_LOOKUP_TIMEOUT} {command}; else {command}; fi"
    )


def build_cluster_command(job_id: int, nonce: str) -> str:
    """Bind-time lookup of a job's cluster: slurmctld first, then accounting.

    ``squeue`` answers from the controller with no accounting lag; ``sacct``
    is the fallback for a job the controller no longer lists. One attempt for
    each, no retry, no sleep, each bounded by ``timeout`` when available.
    """
    if not _NONCE_RE.fullmatch(nonce):
        raise ValueError(f"invalid nonce: {nonce!r}")
    jid = int(job_id)
    return "; ".join(
        (
            _section(nonce, "CLUSTERQ", _bounded(f"squeue -h -j {jid} -O Cluster")),
            _section(
                nonce, "CLUSTERA", _bounded(f"sacct -n -P -X -j {jid} -o Cluster")
            ),
        )
    )


def _single_cluster_name(
    sections: dict[str, tuple[list[str], int | None]], name: str
) -> str | None:
    body, rc = sections.get(name, ([], None))
    if rc != 0:
        return None
    names = {ln.strip() for ln in body if ln.strip()}
    if len(names) != 1:
        return None
    (only,) = names
    return only if CLUSTER_NAME_RE.fullmatch(only) else None


def query_job_cluster(job_id: int, execute: SlurmExecute | None) -> str | None:
    """Return the cluster name of *job_id*, or None when it cannot be learned.

    Asks slurmctld, then accounting; a single attempt each. Never raises.
    """
    if execute is None:
        return None
    nonce = secrets.token_hex(8)
    try:
        stdout, _stderr, code = execute(build_cluster_command(job_id, nonce))
    except Exception as exc:  # noqa: BLE001 - a lookup must never fail a bind
        logger.debug("cluster lookup for job {} failed: {}", job_id, exc)
        return None
    if code != 0:
        return None
    sections = _split_sections(stdout, nonce)
    return _single_cluster_name(sections, "CLUSTERQ") or _single_cluster_name(
        sections, "CLUSTERA"
    )


@dataclass(frozen=True)
class QueueRow:
    array_job_id: int  # %F
    job_id: int  # %A
    state: str  # %T
    workdir: str  # %Z
    command: str  # %o


@dataclass(frozen=True)
class JobState:
    job_id: int
    kind: Literal["active", "terminal", "unknown"]
    state: str | None  # raw SLURM state when known
    ended_at: int | None  # epoch; terminal only
    cluster: str | None  # Cluster of its sacct rows; None when they disagree
    source: Literal["squeue", "sacct", "scontrol"] | None
    detail: str


@dataclass(frozen=True)
class JobStateReport:
    now: int | None  # cluster ``date +%s`` taken with the query
    min_job_age: int | None  # MinJobAge seconds when readable
    queue: tuple[QueueRow, ...]  # every ``squeue -a -t all`` row of the user
    states: Mapping[int, JobState]
    error: str | None  # the query failed; every job is unknown


def _unknown(job_id: int, detail: str) -> JobState:
    return JobState(job_id, "unknown", None, None, None, None, detail)


def _failed_report(
    ids: Sequence[int], error: str, *, now: int | None = None
) -> JobStateReport:
    detail = f"scheduler state unavailable: {error}"
    return JobStateReport(
        now=now,
        min_job_age=None,
        queue=(),
        states={i: _unknown(i, detail) for i in ids},
        error=error,
    )


def _chunks(ids: Sequence[int]) -> list[list[int]]:
    return [list(ids[i : i + SACCT_CHUNK]) for i in range(0, len(ids), SACCT_CHUNK)]


def build_queue_command(job_ids: Sequence[int], nonce: str) -> str:
    """Round trip 1: clock, whole-user queue, accounting rows, MinJobAge."""
    if not _NONCE_RE.fullmatch(nonce):
        raise ValueError(f"invalid nonce: {nonce!r}")
    ids = sorted({int(i) for i in job_ids})
    parts = [
        # Quoted so the remote shell, not Python, produces the 0x1F byte.
        "US=$(printf '\\037')",
        _section(nonce, "DATE", "date +%s"),
        _section(
            nonce,
            "SQUEUE",
            'squeue -h -a -t all -u "$(id -un)" -o "%F${US}%A${US}%T${US}%Z${US}%o"',
        ),
    ]
    for k, chunk in enumerate(_chunks(ids)):
        parts.append(
            _section(
                nonce,
                f"SACCT{k}",
                "SLURM_TIME_FORMAT=%s sacct -n -P -X "
                f"-j {','.join(str(i) for i in chunk)} "
                "--format=JobID,State,End,Cluster",
            )
        )
    parts.append(_section(nonce, "CONFIG", "scontrol show config"))
    return "; ".join(parts)


def build_scontrol_command(job_ids: Sequence[int], nonce: str) -> str:
    """Round trip 2: ``scontrol show job`` per still-unresolved id."""
    if not _NONCE_RE.fullmatch(nonce):
        raise ValueError(f"invalid nonce: {nonce!r}")
    return "; ".join(
        _section(nonce, f"SCONTROL{int(i)}", f"scontrol show job {int(i)}")
        for i in sorted({int(i) for i in job_ids})
    )


def _parse_squeue(lines: Sequence[str]) -> tuple[QueueRow, ...] | None:
    """Rows, or None when any non-empty line does not parse (fail closed)."""
    rows: list[QueueRow] = []
    for line in lines:
        if not line.strip():
            continue
        fields = line.split(SQUEUE_SEP)
        if len(fields) != 5:
            return None
        array_id, job_id, state, workdir, command = fields
        if not (_DIGITS_RE.fullmatch(array_id) and _DIGITS_RE.fullmatch(job_id)):
            return None
        rows.append(QueueRow(int(array_id), int(job_id), state, workdir, command))
    return tuple(rows)


def _base_id(job_id_field: str) -> int | None:
    m = re.match(r"\d+", job_id_field)
    return int(m.group(0)) if m else None


def _sacct_state(raw: str) -> str:
    words = raw.split()
    return words[0].rstrip("+") if words else ""


def _judge_sacct(job_id: int, rows: Sequence[Sequence[str]]) -> JobState:
    """*rows* are ``[JobID, State, End, Cluster]`` for one job id."""
    states = [_sacct_state(r[1]) for r in rows]
    ends = [r[2] for r in rows]
    clusters = {r[3] for r in rows}
    cluster = None
    if len(clusters) == 1:
        (only,) = clusters
        if CLUSTER_NAME_RE.fullmatch(only):
            cluster = only
    if any(not st for st in states):
        return _unknown(job_id, "unparsable sacct state")
    live = [st for st in states if st not in TERMINAL_STATES]
    if live:
        return JobState(
            job_id,
            "active",
            live[0],
            None,
            cluster,
            "sacct",
            "non-terminal state in accounting",
        )
    ended_at = (
        max(int(x) for x in ends)
        if all(_DIGITS_RE.fullmatch(x) for x in ends)
        else None
    )
    state = next((st for st in states if st != "COMPLETED"), "COMPLETED")
    return JobState(
        job_id,
        "terminal",
        state,
        ended_at,
        cluster,
        "sacct",
        "terminal in accounting and absent from the queue",
    )


def parse_queue_output(
    stdout: str, job_ids: Sequence[int], nonce: str
) -> tuple[JobStateReport, list[int]]:
    """Judge round-trip-1 output; also return the ids still unresolved."""
    ids = sorted({int(i) for i in job_ids})
    sections = _split_sections(stdout, nonce)

    now: int | None = None
    date_body, date_rc = sections.get("DATE", ([], None))
    if date_rc == 0:
        first = next((ln.strip() for ln in date_body if ln.strip()), "")
        if _DIGITS_RE.fullmatch(first):
            now = int(first)
    if now is None:
        return _failed_report(ids, "the cluster clock could not be read"), []

    if "SQUEUE" not in sections:
        return _failed_report(ids, "squeue output section is missing", now=now), []
    sq_body, sq_rc = sections["SQUEUE"]
    if sq_rc != 0:
        return _failed_report(ids, f"squeue failed (exit {sq_rc})", now=now), []
    queue = _parse_squeue(sq_body)
    if queue is None:
        return _failed_report(ids, "squeue output could not be parsed", now=now), []

    min_job_age: int | None = None
    cfg_body, cfg_rc = sections.get("CONFIG", ([], None))
    if cfg_rc == 0:
        for line in cfg_body:
            m = _MIN_JOB_AGE_RE.match(line)
            if m:
                min_job_age = int(m.group(1))
                break

    states: dict[int, JobState] = {}
    for jid in ids:
        matching = [r for r in queue if jid in (r.array_job_id, r.job_id)]
        if matching:
            live = [r.state for r in matching if r.state not in TERMINAL_STATES]
            states[jid] = JobState(
                jid,
                "active",
                live[0] if live else matching[0].state,
                None,
                None,
                "squeue",
                "listed by squeue -t all",
            )

    chunks = _chunks(ids)
    for k in range(len(chunks)):
        if f"SACCT{k}" not in sections:
            return _failed_report(
                ids, f"sacct output section {k} is missing", now=now
            ), []
    for k, chunk in enumerate(chunks):
        body, rc = sections[f"SACCT{k}"]
        if rc != 0:
            continue  # sacct unavailable: these jobs stay unknown
        by_id: dict[int, list[list[str]]] = {}
        malformed = False
        for line in body:
            if not line.strip():
                continue
            fields = line.split("|")
            base = _base_id(fields[0]) if len(fields) == _SACCT_FIELDS else None
            if base is None:
                # A row we cannot read might have been the one that said the
                # job is still alive: nothing in this section can be trusted.
                malformed = True
                break
            by_id.setdefault(base, []).append(fields)
        if malformed:
            continue
        for jid in chunk:
            if jid not in states and jid in by_id:
                states[jid] = _judge_sacct(jid, by_id[jid])

    unresolved = [i for i in ids if i not in states]
    for jid in unresolved:
        states[jid] = _unknown(jid, "not found in squeue or sacct")
    report = JobStateReport(
        now=now, min_job_age=min_job_age, queue=queue, states=states, error=None
    )
    return report, unresolved


def parse_scontrol_output(
    stdout: str, job_ids: Sequence[int], nonce: str
) -> dict[int, JobState]:
    """``active`` for a job slurmctld still holds, ``unknown`` otherwise; never terminal."""
    sections = _split_sections(stdout, nonce)
    out: dict[int, JobState] = {}
    for jid in sorted({int(i) for i in job_ids}):
        body, rc = sections.get(f"SCONTROL{jid}", ([], None))
        state = None
        if rc == 0:
            for line in body:
                m = _JOB_STATE_RE.search(line)
                if m:
                    state = m.group(1)
                    break
        if state is None:
            out[jid] = _unknown(jid, "not known to scontrol")
        else:
            out[jid] = JobState(
                jid,
                "active",
                state,
                None,
                None,
                "scontrol",
                "still in slurmctld memory",
            )
    return out


def _run(execute: SlurmExecute, command: str) -> str | None:
    """Stdout of a chained command, or None when it could not run at all."""
    try:
        stdout, _stderr, code = execute(command)
    except Exception as exc:  # noqa: BLE001 - the query never raises
        logger.debug("scheduler query failed: {}", exc)
        return None
    return stdout if code == 0 else None


def query_job_states(
    job_ids: Sequence[int], execute: SlurmExecute | None
) -> JobStateReport:
    """Classify *job_ids* as active, terminal or unknown. Never raises.

    ``job_ids`` may be empty: the queue rows are still returned for the
    path-reference guard. Anything that cannot be established is unknown.
    """
    ids = sorted({int(i) for i in job_ids})
    if execute is None:
        return _failed_report(ids, "no SLURM command runner is available")
    nonce = secrets.token_hex(8)
    stdout = _run(execute, build_queue_command(ids, nonce))
    if stdout is None:
        return _failed_report(ids, "the SLURM query could not run")
    report, unresolved = parse_queue_output(stdout, ids, nonce)
    if report.error is not None or not unresolved:
        return report

    nonce2 = secrets.token_hex(8)
    out2 = _run(execute, build_scontrol_command(unresolved, nonce2))
    if out2 is None:
        return report  # those jobs stay unknown, which keeps their snapshots
    merged = {**report.states, **parse_scontrol_output(out2, unresolved, nonce2)}
    return dataclasses.replace(report, states=merged)
