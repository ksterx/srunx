"""Small JSON state files under the state dir, safe for concurrent writers."""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any


def read_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def update_json(path: Path, mutate: Callable[[dict[str, Any]], None]) -> None:
    """Lock, re-read, mutate and atomically replace *path*.

    The whole read-modify-write runs under an exclusive ``flock`` on a
    sibling ``.lock`` file, and each write uses its own temp file, so
    concurrent updaters neither clobber each other's entries nor race on a
    shared temp name.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path.with_name(path.name + ".lock"), "a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            data = read_json(path)
            mutate(data)
            fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)
                os.replace(tmp, path)
            except BaseException:
                with contextlib.suppress(OSError):
                    os.unlink(tmp)
                raise
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
