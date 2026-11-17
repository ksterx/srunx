"""Lease model: records that a job may be using a snapshot (schema v1)."""

from __future__ import annotations

import json
import secrets
import socket
from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class Lease(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    schema_version: Literal[1] = Field(1, alias="schema")
    lease_id: str
    repo_id: str
    sha: str
    snapshot_path: str
    state: Literal["provisional", "bound"]
    job_id: int | None = None
    profile: str
    scheduler_key: str
    client_host: str
    created_at: str
    bound_at: str | None = None
    # Cluster name from sacct at bind time; None when it could not be learned.
    # A lease written before this field existed loads as None (schema stays 1).
    cluster: str | None = None

    @classmethod
    def new_provisional(
        cls, *, repo_id: str, sha: str, snapshot_path: str, profile: str
    ) -> Lease:
        return cls(
            lease_id=secrets.token_hex(16),
            repo_id=repo_id,
            sha=sha,
            snapshot_path=snapshot_path,
            state="provisional",
            job_id=None,
            profile=profile,
            scheduler_key=f"ssh:{profile}",
            client_host=socket.gethostname(),
            created_at=_now(),
            bound_at=None,
        )

    def bound(self, job_id: int, *, cluster: str | None = None) -> Lease:
        return self.model_copy(
            update={
                "state": "bound",
                "job_id": job_id,
                "bound_at": _now(),
                "cluster": cluster,
            }
        )

    def to_json(self) -> str:
        """Single-line ASCII JSON using the ``schema`` key."""
        return json.dumps(
            self.model_dump(mode="json", by_alias=True),
            ensure_ascii=True,
            separators=(",", ":"),
        )

    @classmethod
    def from_json(cls, text: str) -> Lease:
        return cls.model_validate_json(text)
