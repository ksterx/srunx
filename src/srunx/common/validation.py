"""Validators shared by config models in ``common`` and ``ssh``."""

from __future__ import annotations

import re

# Charset also enforced by ``srunx.snapshot.remote`` (it calls this validator): the root ends
# up unquoted-safe in remote scripts and push destinations, so anything outside
# it (spaces, quotes, ``$``, ``;``) is rejected up front. ``~`` only leads.
SNAPSHOT_ROOT_RE = re.compile(r"(~|~/[A-Za-z0-9._/+-]*|/[A-Za-z0-9._/+-]*)")


def validate_snapshot_root(value: str | None) -> str | None:
    """Validate a snapshot root; ``None`` / empty string means "unset".

    Raises:
        ValueError: When the path is relative, uses characters outside
            ``[A-Za-z0-9._/~+-]``, puts ``~`` anywhere but first, or has a
            ``..`` segment.
    """
    if value is None or value == "":
        return None
    if not SNAPSHOT_ROOT_RE.fullmatch(value):
        raise ValueError(
            f"Invalid snapshot_root {value!r}: must be absolute or start with "
            "'~', and contain only [A-Za-z0-9._/~+-] ('~' only as the first "
            "character)"
        )
    if ".." in value.split("/"):
        raise ValueError(f"Invalid snapshot_root {value!r}: '..' is not allowed")
    return value
