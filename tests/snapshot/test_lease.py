"""Tests for srunx.snapshot.lease."""

from __future__ import annotations

import json

from srunx.snapshot.lease import Lease

SHA = "a" * 40


def _lease() -> Lease:
    return Lease.new_provisional(
        repo_id="repo-0123456789ab",
        sha=SHA,
        snapshot_path="/r/repo/trees/" + SHA,
        profile="p",
    )


def test_provisional_fields() -> None:
    lease = _lease()
    assert lease.state == "provisional"
    assert lease.job_id is None and lease.bound_at is None
    assert lease.scheduler_key == "ssh:p"
    assert len(lease.lease_id) == 32


def test_json_uses_schema_key_single_line_ascii() -> None:
    text = _lease().to_json()
    data = json.loads(text)
    assert data["schema"] == 1
    assert "schema_version" not in data
    assert "\n" not in text
    text.encode("ascii")


def test_non_ascii_stays_ascii() -> None:
    lease = _lease().model_copy(update={"client_host": "ホスト"})
    text = lease.to_json()
    text.encode("ascii")
    assert Lease.from_json(text).client_host == "ホスト"


def test_round_trip() -> None:
    lease = _lease().bound(42)
    assert Lease.from_json(lease.to_json()) == lease


def test_unknown_keys_ignored() -> None:
    data = json.loads(_lease().to_json())
    data["future_field"] = {"x": 1}
    assert Lease.from_json(json.dumps(data)).sha == SHA


def test_bound_sets_job_id_and_time() -> None:
    first = _lease()
    bound = first.bound(7)
    assert bound.state == "bound" and bound.job_id == 7
    assert bound.bound_at is not None
    assert bound.lease_id == first.lease_id
    assert first.state == "provisional"


def test_cluster_defaults_to_none_and_is_always_emitted() -> None:
    lease = _lease()
    assert lease.cluster is None
    assert json.loads(lease.to_json())["cluster"] is None


def test_bound_stores_cluster_and_roundtrips() -> None:
    bound = _lease().bound(7, cluster="gpucloud2")
    assert bound.cluster == "gpucloud2"
    again = Lease.from_json(bound.to_json())
    assert again.cluster == "gpucloud2" and again.job_id == 7


def test_bound_without_cluster_is_none() -> None:
    assert _lease().bound(7).cluster is None


def test_cluster_missing_key_loads_as_none_with_schema_one() -> None:
    data = json.loads(_lease().bound(7).to_json())
    del data["cluster"]
    lease = Lease.from_json(json.dumps(data))
    assert lease.cluster is None
    assert lease.schema_version == 1
