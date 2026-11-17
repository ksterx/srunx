"""End to end: real git repo -> plan -> SnapshotService over a local ``sh -s`` -> submit.

Only ``sbatch`` is stubbed. Everything else (git validation, push, the remote
scripts, the lease files) runs for real against a temp directory.
"""

from __future__ import annotations

import functools
import json
import os
import shutil
import stat
import subprocess
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from srunx.cli._helpers import snapshot_submit as ss
from srunx.domain import JobEnvironment, ShellJob
from srunx.snapshot import SnapshotService, plan_snapshot
from srunx.ssh.core.config import ServerProfile
from tests.integration._local_shell import REAL_RUN, LocalShell


def _git(repo: Path, *args: str) -> str:
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@example.com",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@example.com",
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_SYSTEM": os.devnull,
    }
    return REAL_RUN(
        ["git", *args], cwd=repo, check=True, capture_output=True, text=True, env=env
    ).stdout.strip()


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    assert shutil.which("git"), "git must be installed to run the snapshot e2e test"
    top = tmp_path / "local"
    (top / "jobs").mkdir(parents=True)
    _git(top, "init", "-q", "-b", "main")
    (top / "jobs" / "train.sbatch").write_text("#!/bin/bash\necho hi\n")
    (top / "README").write_text("x\n")
    _git(top, "add", "-A")
    _git(top, "commit", "-q", "-m", "c")
    return top


@pytest.fixture
def remote_root(tmp_path: Path) -> Iterator[Path]:
    root = tmp_path / "remote-root"
    yield root
    # Snapshots are read-only; restore write permission so pytest can clean up.
    for dirpath, dirnames, filenames in os.walk(tmp_path):
        for name in [*dirnames, *filenames]:
            p = Path(dirpath) / name
            if not p.is_symlink():
                p.chmod(p.stat().st_mode | stat.S_IWUSR)
        Path(dirpath).chmod(Path(dirpath).stat().st_mode | stat.S_IWUSR)


def test_submit_via_snapshot_creates_readonly_tree_and_binds_lease(
    monkeypatch: pytest.MonkeyPatch, repo: Path, remote_root: Path
) -> None:
    monkeypatch.setattr(subprocess, "run", REAL_RUN)  # conftest stubs it per test
    profile = ServerProfile(
        hostname="h", username="u", key_filename="/k", snapshot_root=str(remote_root)
    )
    shell = LocalShell(os.environ["PATH"])
    monkeypatch.setattr(
        ss, "SnapshotService", functools.partial(SnapshotService, shell=shell)
    )
    cm = MagicMock()
    cm.return_value.get_profile.return_value = profile
    monkeypatch.setattr("srunx.ssh.core.config.ConfigManager", cm)

    plan = plan_snapshot(
        start=repo, rev=None, script=repo / "jobs" / "train.sbatch", cwd=repo / "jobs"
    )
    seen: dict[str, Any] = {}

    def fake_sbatch(remote_path: str, *, callbacks_job: Any, **kw: Any) -> Any:
        # At submit time the lease must exist and still be provisional.
        leases = list(remote_root.glob("*/leases/*.json"))
        assert len(leases) == 1
        seen["during"] = json.loads(leases[0].read_text())
        seen["remote_path"] = remote_path
        seen["kw"] = kw
        seen["env"] = dict(callbacks_job.environment.env_vars)
        callbacks_job.job_id = 4242
        return callbacks_job

    rt = MagicMock()
    rt.job_ops.submit_remote_sbatch.side_effect = fake_sbatch
    job = ShellJob(
        name="train",
        script_path="jobs/train.sbatch",
        environment=JobEnvironment(env_vars={"SRUNX_CODE_DIR": "user"}),
    )

    submitted = ss.submit_via_snapshot(
        rt=rt,
        job=job,
        plan=plan,
        profile_name="prof",
        extra_sbatch_args=["--output=/abs/%j.log"],
        inject_job_name=True,
    )

    tree = next(remote_root.glob(f"*/trees/{plan.sha}"))
    assert (tree / "jobs" / "train.sbatch").read_text() == "#!/bin/bash\necho hi\n"
    assert not os.access(tree / "README", os.W_OK)  # read-only snapshot
    assert (tree.parent / f"{plan.sha}.complete").exists()

    assert seen["remote_path"] == f"{tree}/jobs/train.sbatch"
    assert seen["kw"]["submit_cwd"] == f"{tree}/jobs"
    assert seen["env"]["SRUNX_CODE_DIR"] == str(tree)  # snapshot beats --env
    assert seen["env"]["SRUNX_SNAPSHOT_SHA"] == plan.sha
    assert seen["during"]["state"] == "provisional"
    assert seen["during"]["job_id"] is None
    assert seen["kw"]["metadata"]["snapshot"]["sha"] == plan.sha

    (lease_file,) = list(remote_root.glob("*/leases/*.json"))
    bound = json.loads(lease_file.read_text())
    assert bound["state"] == "bound"
    assert bound["job_id"] == 4242
    assert bound["lease_id"] == seen["during"]["lease_id"]
    assert bound["sha"] == plan.sha
    assert not list(remote_root.glob("*/.lock"))
    assert submitted.job_id == 4242
