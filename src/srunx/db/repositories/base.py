"""Shared helpers for repository implementations."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from typing import Any, TypeVar

from pydantic import BaseModel

M = TypeVar("M", bound=BaseModel)


def now_iso() -> str:
    """Return the current UTC time as a SLURM-DB-canonical ISO string.

    Format: ``YYYY-MM-DDTHH:MM:SS.sssZ`` — lexicographically sortable,
    aligned with ``strftime('%Y-%m-%dT%H:%M:%fZ', 'now')`` on the SQL side.
    """
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _parse_dt(value: Any) -> datetime | None:
    """Parse a stored TEXT timestamp back into a ``datetime``.

    Returns ``None`` for ``None`` / empty strings. Accepts both the canonical
    ``...Z`` form written by :func:`now_iso` and plain ISO 8601.
    """
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    s = str(value)
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _maybe_json_load(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return value


def _maybe_json_dump(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value)


class BaseRepository:
    """Base class for all repositories.

    Holds the connection and exposes shared row-to-model conversion. JSON
    columns and timestamp columns are parsed up-front for the given set
    of field names (passed per-subclass).
    """

    # Fields whose stored string form should be JSON-decoded on read
    # and JSON-encoded on write. Override in subclasses.
    JSON_FIELDS: tuple[str, ...] = ()

    # Fields whose stored string form should be datetime-parsed on read.
    DATETIME_FIELDS: tuple[str, ...] = ()

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def _row_to_dict(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        data: dict[str, Any] = dict(row)
        for field in self.JSON_FIELDS:
            if field in data:
                data[field] = _maybe_json_load(data[field])
        for field in self.DATETIME_FIELDS:
            if field in data:
                data[field] = _parse_dt(data[field])
        return data

    def _row_to_model(self, row: sqlite3.Row | None, model_cls: type[M]) -> M | None:
        data = self._row_to_dict(row)
        if data is None:
            return None
        return model_cls.model_validate(data)

    @staticmethod
    def _encode_json(value: Any) -> str | None:
        return _maybe_json_dump(value)
