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

import re
import secrets
from collections.abc import Callable

from loguru import logger

SlurmExecute = Callable[[str], tuple[str, str, int]]

CLUSTER_NAME_RE = re.compile(r"[A-Za-z0-9._-]+")
CLUSTER_LOOKUP_TIMEOUT = 10

_NONCE_RE = re.compile(r"[0-9a-f]{8,64}")
_RC_RE = re.compile(r"RC (-?\d+)")


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


def build_cluster_command(job_id: int, nonce: str) -> str:
    """The single bind-time ``sacct`` lookup, bounded by ``timeout`` when available."""
    if not _NONCE_RE.fullmatch(nonce):
        raise ValueError(f"invalid nonce: {nonce!r}")
    jid = int(job_id)
    sacct = f"sacct -n -P -X -j {jid} -o Cluster"
    command = (
        "if command -v timeout >/dev/null 2>&1; "
        f"then timeout {CLUSTER_LOOKUP_TIMEOUT} {sacct}; else {sacct}; fi"
    )
    return _section(nonce, "CLUSTER", command)


def query_job_cluster(job_id: int, execute: SlurmExecute | None) -> str | None:
    """Return the cluster name of *job_id*, or None when it cannot be learned.

    One attempt, no retry. Never raises.
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
    body, rc = _split_sections(stdout, nonce).get("CLUSTER", ([], None))
    if rc != 0:
        return None
    names = {ln.strip() for ln in body if ln.strip()}
    if len(names) != 1:
        return None
    name = names.pop()
    return name if CLUSTER_NAME_RE.fullmatch(name) else None
