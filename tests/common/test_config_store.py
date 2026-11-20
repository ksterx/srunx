"""Tests for srunx.common.config_store (the single TOML writer)."""

from __future__ import annotations

import os
import threading

import pytest

from srunx.common.config_store import (
    ConfigConflictError,
    ConfigFileError,
    ConfigStore,
)


def test_set_preserves_comments_and_other_keys(tmp_path):
    path = tmp_path / "config.toml"
    path.write_text(
        '# my comment\ndefault_cluster = "a"\n\n[clusters.a]\ntransport = "local"\n'
    )
    store = ConfigStore(path)
    store.set(("defaults", "resources", "partition"), "gpu")
    text = path.read_text()
    assert "# my comment" in text
    assert 'default_cluster = "a"' in text
    assert store.read()["defaults"]["resources"]["partition"] == "gpu"


def test_delete_removes_only_that_key(tmp_path):
    path = tmp_path / "config.toml"
    path.write_text('[defaults.resources]\npartition = "gpu"\nnodes = 2\n')
    store = ConfigStore(path)
    store.delete(("defaults", "resources", "partition"))
    assert store.read() == {"defaults": {"resources": {"nodes": 2}}}
    store.delete(("defaults", "missing", "x"))  # no-op


def test_invalid_file_is_never_overwritten(tmp_path):
    path = tmp_path / "config.toml"
    path.write_text("not = = toml")
    store = ConfigStore(path)
    with pytest.raises(ConfigFileError):
        store.read()
    with pytest.raises(ConfigFileError):
        store.set(("a",), 1)
    assert path.read_text() == "not = = toml"


def test_validation_failure_aborts_write(tmp_path):
    path = tmp_path / "config.toml"
    path.write_text("a = 1\n")
    store = ConfigStore(path)

    def reject(_data):
        raise ValueError("nope")

    with pytest.raises(ValueError):
        store.set(("a",), 2, validate=reject)
    assert store.read() == {"a": 1}


def test_version_conflict(tmp_path):
    path = tmp_path / "config.toml"
    store = ConfigStore(path)
    version = store.version()
    store.set(("a",), 1)
    with pytest.raises(ConfigConflictError):
        store.set(("b",), 2, expected_version=version)
    store.set(("b",), 2, expected_version=store.version())
    assert store.read() == {"a": 1, "b": 2}


def test_private_file_permissions(tmp_path):
    path = tmp_path / "dir" / "config.toml"
    ConfigStore(path, private=True).set(("a",), 1)
    assert oct(path.stat().st_mode & 0o777) == "0o600"
    assert oct(path.parent.stat().st_mode & 0o777) == "0o700"


def test_concurrent_writers_do_not_lose_updates(tmp_path):
    path = tmp_path / "config.toml"
    store = ConfigStore(path)

    def writer(i: int) -> None:
        ConfigStore(path).set(("k", f"v{i}"), i)

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert store.read()["k"] == {f"v{i}": i for i in range(20)}
    assert not [p for p in os.listdir(tmp_path) if p.endswith(".tmp")]


def test_lock_file_not_next_to_config(tmp_path):
    path = tmp_path / ".srunx" / "config.toml"
    ConfigStore(path).set(("a",), 1)
    assert sorted(p.name for p in path.parent.iterdir()) == ["config.toml"]


def test_read_with_version_matches_contents(tmp_path):
    path = tmp_path / "c.toml"
    store = ConfigStore(path)
    assert store.read_with_version() == ({}, store.version())
    store.set(("a",), 1)
    data, version = store.read_with_version()
    assert data == {"a": 1}
    assert version == store.version()
