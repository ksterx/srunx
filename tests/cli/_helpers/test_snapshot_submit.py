"""Tests for ``srunx.cli._helpers.snapshot_submit``."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
import typer

from srunx.cli._helpers import snapshot_submit as ss
from srunx.common.exceptions import SbatchIndeterminateError, SbatchRejectedError
from srunx.domain import JobEnvironment, ShellJob
from srunx.snapshot import PreparedSnapshot, SnapshotPlan, SnapshotRemoteError
from srunx.snapshot.errors import SnapshotValidationError

SHA = "a" * 40


def _plan(*, cwd_rel: str | None = "sub/dir") -> SnapshotPlan:
    return SnapshotPlan(
        repo_top=Path("/repo"),
        repo_id="repo-123456789abc",
        sha=SHA,
        script_rel="scripts/job.sbatch",
        script_text="#!/bin/bash\n#SBATCH -J x\necho\n",
        cwd_rel=cwd_rel,
        warnings=(),
    )


def _prepared(plan: SnapshotPlan) -> PreparedSnapshot:
    return PreparedSnapshot(
        plan=plan,
        root="/r",
        repo_dir="/r/repo-123456789abc",
        path=f"/r/repo-123456789abc/trees/{SHA}",
        status="created",
    )


class FakeHandle:
    def __init__(self, prepared: PreparedSnapshot) -> None:
        self.prepared = prepared
        self.bound: list[int] = []

    def metadata(self) -> dict[str, Any]:
        return {"snapshot": {"sha": self.prepared.plan.sha}}

    def bind(self, job_id: int) -> None:
        self.bound.append(job_id)


class FakeService:
    last: FakeService | None = None

    def __init__(
        self, *, profile_name: str, profile: Any, slurm_execute: Any = None
    ) -> None:
        self.profile_name = profile_name
        self.slurm_execute = slurm_execute
        self.handle: FakeHandle | None = None
        self.raise_on_enter: Exception | None = None
        FakeService.last = self

    @contextmanager
    def leased_snapshot(self, plan: SnapshotPlan):
        if self.raise_on_enter is not None:
            raise self.raise_on_enter
        self.handle = FakeHandle(_prepared(plan))
        yield self.handle


@pytest.fixture
def fake_service(monkeypatch: pytest.MonkeyPatch) -> type[FakeService]:
    FakeService.last = None
    monkeypatch.setattr(ss, "SnapshotService", FakeService)
    cm = MagicMock()
    cm.return_value.get_profile.return_value = MagicMock(name="profile")
    monkeypatch.setattr("srunx.ssh.core.config.ConfigManager", cm)
    return FakeService


def _job() -> ShellJob:
    return ShellJob(
        name="x",
        script_path="job.sbatch",
        environment=JobEnvironment(env_vars={"SRUNX_CODE_DIR": "user", "K": "v"}),
    )


def _rt(side_effect: Any = None) -> MagicMock:
    rt = MagicMock()

    def _submit(remote_path: str, *, callbacks_job: Any, **_kw: Any) -> Any:
        callbacks_job.job_id = 77
        return callbacks_job

    rt.job_ops.submit_remote_sbatch.side_effect = side_effect or _submit
    return rt


def _submit(rt: MagicMock, plan: SnapshotPlan, job: ShellJob | None = None, **kw: Any):
    return ss.submit_via_snapshot(
        rt=rt,
        job=job or _job(),
        plan=plan,
        profile_name="p",
        extra_sbatch_args=kw.pop("extra_sbatch_args", ["--output=/abs/%j.log"]),
        inject_job_name=kw.pop("inject_job_name", True),
    )


# -- validate_snapshot_options ---------------------------------------------

_OK: dict[str, Any] = {
    "snapshot": True,
    "snapshot_rev": None,
    "script": Path("job.sbatch"),
    "wrap": None,
    "template": None,
    "sync": None,
    "force_sync": False,
}


def test_validate_not_snapshot_returns_false() -> None:
    assert ss.validate_snapshot_options(**{**_OK, "snapshot": False}) is False
    # even with otherwise conflicting options
    assert (
        ss.validate_snapshot_options(**{**_OK, "snapshot": False, "wrap": "x"}) is False
    )


def test_validate_snapshot_ok_and_no_sync_allowed() -> None:
    assert ss.validate_snapshot_options(**_OK) is True
    assert ss.validate_snapshot_options(**{**_OK, "sync": False}) is True


def test_validate_snapshot_rev_implies_snapshot() -> None:
    assert (
        ss.validate_snapshot_options(**{**_OK, "snapshot": False, "snapshot_rev": "v1"})
        is True
    )


@pytest.mark.parametrize(
    ("override", "flag"),
    [
        ({"wrap": "x", "script": None}, "--wrap"),
        ({"template": "t"}, "--template"),
        ({"sync": True}, "--sync"),
        ({"force_sync": True}, "--force-sync"),
    ],
)
def test_validate_conflicts_name_the_option(
    override: dict[str, Any], flag: str
) -> None:
    with pytest.raises(typer.BadParameter, match=flag):
        ss.validate_snapshot_options(**{**_OK, **override})


def test_validate_requires_script() -> None:
    with pytest.raises(typer.BadParameter, match="script"):
        ss.validate_snapshot_options(**{**_OK, "script": None})


def test_validate_rev_implied_snapshot_still_rejects_conflicts() -> None:
    with pytest.raises(typer.BadParameter, match="--wrap"):
        ss.validate_snapshot_options(
            **{**_OK, "snapshot": False, "snapshot_rev": "HEAD", "wrap": "x"}
        )


def test_validate_empty_rev_rejected() -> None:
    with pytest.raises(typer.BadParameter, match="snapshot-rev"):
        ss.validate_snapshot_options(**{**_OK, "snapshot_rev": " "})


# -- build_snapshot_plan -------------------------------------------------


def test_build_snapshot_plan_validation_error_is_bad_parameter(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def boom(**_kw: Any) -> None:
        raise SnapshotValidationError("not a git repository")

    monkeypatch.setattr(ss, "plan_snapshot", boom)
    with pytest.raises(typer.BadParameter, match="not a git repository"):
        ss.build_snapshot_plan(script=Path("j"), rev=None, cwd=tmp_path)


def test_build_snapshot_plan_passes_arguments(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    seen: dict[str, Any] = {}

    def fake(**kw: Any) -> SnapshotPlan:
        seen.update(kw)
        return _plan()

    monkeypatch.setattr(ss, "plan_snapshot", fake)
    got = ss.build_snapshot_plan(script=Path("j"), rev="v1", cwd=tmp_path)
    assert got.sha == SHA
    assert seen == {
        "start": tmp_path,
        "rev": "v1",
        "script": Path("j"),
        "cwd": tmp_path,
    }


# -- submit_via_snapshot ----------------------------------------------------


def test_submit_uses_snapshot_script_cwd_env_metadata_and_binds(
    fake_service: type[FakeService],
) -> None:
    rt = _rt()
    job = _job()
    result = _submit(rt, _plan(), job)

    path = f"/r/repo-123456789abc/trees/{SHA}"
    args, kwargs = rt.job_ops.submit_remote_sbatch.call_args
    assert args[0] == f"{path}/scripts/job.sbatch"
    assert kwargs["submit_cwd"] == f"{path}/sub/dir"
    assert kwargs["metadata"] == {"snapshot": {"sha": SHA}}
    assert kwargs["callbacks_job"] is job
    assert kwargs["job_name"] == "x"
    assert kwargs["extra_sbatch_args"] == ["--output=/abs/%j.log"]
    # snapshot env wins over --env, other vars survive
    assert job.environment.env_vars == {
        "SRUNX_CODE_DIR": path,
        "SRUNX_SNAPSHOT_SHA": SHA,
        "K": "v",
    }
    assert fake_service.last is not None and fake_service.last.handle is not None
    assert fake_service.last.handle.bound == [77]
    assert result is job


def test_submit_passes_cluster_runner_to_the_service(
    fake_service: type[FakeService],
) -> None:
    rt = _rt()
    _submit(rt, _plan())
    assert fake_service.last is not None
    assert fake_service.last.slurm_execute is rt.job_ops.run_slurm_command


def test_submit_cluster_runner_is_none_when_client_has_none(
    fake_service: type[FakeService],
) -> None:
    rt = _rt()
    rt.job_ops = MagicMock(spec=["submit_remote_sbatch"])
    rt.job_ops.submit_remote_sbatch.side_effect = (
        _rt().job_ops.submit_remote_sbatch.side_effect
    )
    _submit(rt, _plan())
    assert fake_service.last is not None
    assert fake_service.last.slurm_execute is None


def test_submit_cwd_is_snapshot_top_without_cwd_rel(
    fake_service: type[FakeService],
) -> None:
    rt = _rt()
    _submit(rt, _plan(cwd_rel=None))
    assert (
        rt.job_ops.submit_remote_sbatch.call_args.kwargs["submit_cwd"]
        == f"/r/repo-123456789abc/trees/{SHA}"
    )


def test_submit_omits_job_name_when_not_injected(
    fake_service: type[FakeService],
) -> None:
    rt = _rt()
    _submit(rt, _plan(), inject_job_name=False)
    assert rt.job_ops.submit_remote_sbatch.call_args.kwargs["job_name"] is None


def test_submit_keeps_user_chdir_in_extra_args(
    fake_service: type[FakeService],
) -> None:
    rt = _rt()
    _submit(rt, _plan(), extra_sbatch_args=["--chdir=/work", "--output=/o/%j"])
    extra = rt.job_ops.submit_remote_sbatch.call_args.kwargs["extra_sbatch_args"]
    assert "--chdir=/work" in extra


@pytest.mark.parametrize(
    "exc",
    [
        SbatchRejectedError("no"),
        SbatchIndeterminateError("maybe"),
        SnapshotRemoteError("push failed"),
    ],
)
def test_submit_remote_errors_exit_1(
    fake_service: type[FakeService], exc: Exception, capsys: pytest.CaptureFixture[str]
) -> None:
    def boom(*_a: Any, **_k: Any) -> None:
        raise exc

    with pytest.raises(typer.Exit) as ei:
        _submit(_rt(boom), _plan())
    assert ei.value.exit_code == 1
    assert str(exc) in capsys.readouterr().err


def test_submit_remote_error_from_service_exit_1(
    fake_service: type[FakeService],
) -> None:
    orig = FakeService.__init__

    def init(self: FakeService, **kw: Any) -> None:
        orig(self, **kw)
        self.raise_on_enter = SnapshotRemoteError("lock timeout")

    FakeService.__init__ = init  # type: ignore[method-assign]
    try:
        with pytest.raises(typer.Exit) as ei:
            _submit(_rt(), _plan())
    finally:
        FakeService.__init__ = orig  # type: ignore[method-assign]
    assert ei.value.exit_code == 1


def test_submit_validation_error_is_bad_parameter(
    fake_service: type[FakeService],
) -> None:
    orig = FakeService.__init__

    def init(self: FakeService, **kw: Any) -> None:
        orig(self, **kw)
        self.raise_on_enter = SnapshotValidationError("root inside mount")

    FakeService.__init__ = init  # type: ignore[method-assign]
    try:
        with pytest.raises(typer.BadParameter, match="root inside mount"):
            _submit(_rt(), _plan())
    finally:
        FakeService.__init__ = orig  # type: ignore[method-assign]


# -- output_warning ---------------------------------------------------------


def test_output_warning_no_chdir_no_output() -> None:
    assert ss.output_warning("#!/bin/bash\necho\n", []) is not None


def test_output_warning_relative_chdir() -> None:
    assert ss.output_warning("echo\n", ["--chdir=rel"]) is not None
    # a relative chdir alone is fine once the log paths are absolute
    assert ss.output_warning("echo\n", ["--chdir=rel", "--output=/abs/o"]) is None
    assert ss.output_warning("echo\n", ["-D", "rel"]) is not None


def test_output_warning_relative_output() -> None:
    assert ss.output_warning("echo\n", ["--output=logs/%j.out"]) is not None
    assert ss.output_warning("echo\n", ["-o", "logs/%j.out"]) is not None


def test_output_warning_relative_error_with_absolute_output() -> None:
    assert ss.output_warning("echo\n", ["-o", "/abs/o", "--error=err/%j"]) is not None
    assert ss.output_warning("echo\n", ["-o/abs/o", "-eerr/%j"]) is not None


def test_output_warning_absolute_chdir_no_warning() -> None:
    assert ss.output_warning("echo\n", ["--chdir=/work"]) is None


def test_output_warning_absolute_output_without_error_no_warning() -> None:
    assert ss.output_warning("echo\n", ["--output=/abs/%j.out"]) is None


def test_output_warning_directive_only_values() -> None:
    ok = "#!/bin/bash\n#SBATCH -o /abs/o.%j\n#SBATCH --error=/abs/e.%j\necho\n"
    assert ss.output_warning(ok, []) is None
    rel = "#!/bin/bash\n#SBATCH --output=o.%j\necho\n"
    assert ss.output_warning(rel, []) is not None
    chdir = "#!/bin/bash\n#SBATCH -D /w\necho\n"
    assert ss.output_warning(chdir, []) is None


def test_output_warning_cli_overrides_directive() -> None:
    text = "#SBATCH --output=rel.out\necho\n"
    assert ss.output_warning(text, ["--output=/abs/o"]) is None
    text2 = "#SBATCH --output=/abs/o\necho\n"
    assert ss.output_warning(text2, ["--output=rel.out"]) is not None
