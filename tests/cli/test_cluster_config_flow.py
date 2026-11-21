"""End-to-end CLI tests for cluster-aware defaults (design §5–§8)."""

from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from srunx.cli._helpers.sbatch_resolve import merge_script_defaults, parse_directives
from srunx.cli.main import app
from srunx.common.config import SrunxConfig, clear_settings_cache
from tests.conftest import append_user_config

runner = CliRunner()


def _project(root: Path, text: str) -> Path:
    (root / ".srunx").mkdir(parents=True, exist_ok=True)
    (root / ".git").mkdir(exist_ok=True)
    (root / ".srunx" / "config.toml").write_text(text)
    clear_settings_cache()
    return root


class TestParseDirectives:
    def test_long_short_and_last_wins(self):
        text = (
            "#!/bin/bash\n#SBATCH -p debug\n#SBATCH --time=1:00:00 -N 2\n"
            "#SBATCH --partition=gpu\n# comment\necho hi\n#SBATCH -A ignored\n"
        )
        assert parse_directives(text) == {
            "--partition": "gpu",
            "--time": "1:00:00",
            "--nodes": "2",
        }

    def test_clusters_detected(self):
        assert "--clusters" in parse_directives("#SBATCH -M other\n")


class TestMergeScriptDefaults:
    def _config(self, sources: dict[str, str], **values) -> SrunxConfig:
        cfg = SrunxConfig.model_validate(values)
        cfg.sources = sources
        return cfg

    def test_builtin_values_never_forwarded(self):
        merge = merge_script_defaults(SrunxConfig(), {}, explicit_options=set())
        assert merge.args == []

    def test_forwards_configured_values_script_does_not_set(self):
        cfg = self._config(
            {"resources.partition": "u [clusters.gpu]", "resources.time_limit": "p"},
            resources={"partition": "gpu", "time_limit": "02:00:00"},
        )
        merge = merge_script_defaults(cfg, {"--time": "1:00"}, explicit_options=set())
        assert merge.args == ["--partition=gpu"]
        assert merge.warnings == []

    def test_script_wins_with_warning_for_cluster_value(self):
        cfg = self._config(
            {"resources.partition": "u [clusters.gpu]"},
            resources={"partition": "gpu"},
        )
        merge = merge_script_defaults(
            cfg, {"--partition": "debug"}, explicit_options=set()
        )
        assert merge.args == []
        assert "script wins" in merge.warnings[0]

    def test_gres_covers_gpus_and_cli_wins(self):
        cfg = self._config(
            {"resources.gpus_per_node": "u", "resources.nodes": "u"},
            resources={"gpus_per_node": 4, "nodes": 2},
        )
        merge = merge_script_defaults(
            cfg, {"--gres": "gpu:8"}, explicit_options={"--nodes"}
        )
        assert merge.args == []

    def test_log_dir_becomes_output_and_error(self):
        cfg = self._config({"log_dir": "u"}, log_dir="out")
        merge = merge_script_defaults(cfg, {}, explicit_options=set())
        assert merge.args == ["--output=out/%x_%j.log", "--error=out/%x_%j.log"]


@pytest.fixture
def local_slurm():
    with patch("srunx.slurm.local.Slurm") as slurm_cls:
        client = MagicMock()
        client.submit.return_value = MagicMock(job_id=7, name="j", command=["x"])
        slurm_cls.return_value = client
        yield client


class TestSbatchWithClusterDefaults:
    def test_script_gets_cluster_and_project_defaults(
        self, tmp_path, monkeypatch, local_slurm
    ):
        append_user_config('[clusters.local.defaults.resources]\npartition = "cpu"')
        root = _project(
            tmp_path / "p",
            'name = "p"\n[clusters.local.defaults.resources]\ntime_limit = "03:00:00"\n',
        )
        script = root / "run.sh"
        script.write_text("#!/bin/bash\n#SBATCH -N 2\necho hi\n")
        monkeypatch.chdir(root)
        result = runner.invoke(app, ["sbatch", str(script)])
        assert result.exit_code == 0, result.output
        extra = local_slurm.submit.call_args.kwargs["extra_sbatch_args"]
        assert extra == ["--time=03:00:00", "--partition=cpu"]

    def test_explicit_flag_beats_config(self, tmp_path, local_slurm):
        append_user_config('[clusters.local.defaults.resources]\npartition = "cpu"')
        script = tmp_path / "run.sh"
        script.write_text("echo hi\n")
        result = runner.invoke(app, ["sbatch", "-p", "debug", str(script)])
        assert result.exit_code == 0, result.output
        assert local_slurm.submit.call_args.kwargs["extra_sbatch_args"] == [
            "--partition=debug"
        ]

    def test_wrap_job_uses_cluster_defaults_and_unset(self, local_slurm):
        append_user_config(
            '[clusters.local.defaults.resources]\npartition = "cpu"\nnodes = 3'
        )
        result = runner.invoke(app, ["sbatch", "--wrap", "echo", "--unset", "nodes"])
        assert result.exit_code == 0, result.output
        job = local_slurm.submit.call_args.args[0]
        assert job.resources.partition == "cpu"
        assert job.resources.nodes == 1

    def test_project_selected_with_flag(self, tmp_path, local_slurm):
        root = _project(
            tmp_path / "p",
            'name = "p"\n[defaults.resources]\npartition = "proj"\n',
        )
        result = runner.invoke(
            app, ["sbatch", "--wrap", "echo", "--project", str(root)]
        )
        assert result.exit_code == 0, result.output
        assert local_slurm.submit.call_args.args[0].resources.partition == "proj"

    def test_script_args_reach_job(self, tmp_path, local_slurm):
        script = tmp_path / "run.sh"
        script.write_text("echo\n")
        result = runner.invoke(
            app,
            ["sbatch", "-p", "gpu", str(script), "cfg.yaml", "--cluster", "x"],
        )
        assert result.exit_code == 0, result.output
        job = local_slurm.submit.call_args.args[0]
        assert job.script_args == ["cfg.yaml", "--cluster", "x"]
        assert (
            "--partition=gpu"
            in local_slurm.submit.call_args.kwargs["extra_sbatch_args"]
        )

    def test_dry_run_shows_script_args(self, tmp_path):
        script = tmp_path / "run.sh"
        script.write_text("echo\n")
        result = runner.invoke(
            app, ["sbatch", "--dry-run", str(script), "a b", "--prepare-only"]
        )
        assert result.exit_code == 0, result.output
        assert "Script arguments: 'a b' --prepare-only" in result.stdout

    def test_wrap_rejects_script_args(self):
        result = runner.invoke(app, ["sbatch", "--wrap", "echo", "--script-arg=x"])
        assert result.exit_code == 2

    def test_dry_run_shows_cluster_and_sources(self, tmp_path):
        append_user_config('[clusters.local.defaults.resources]\npartition = "cpu"')
        script = tmp_path / "run.sh"
        script.write_text("#!/bin/bash\n#SBATCH -J t\necho hi\n")
        result = runner.invoke(app, ["sbatch", "--dry-run", str(script)])
        assert result.exit_code == 0, result.output
        assert "Cluster: local" in result.stdout
        assert "--job-name=t  (#SBATCH)" in result.stdout
        assert "--partition=cpu" in result.stdout
        assert "[clusters.local]" in result.stdout

    @pytest.mark.parametrize(
        "args",
        [["--sbatch-arg=--clusters=other"], ["-M", "other"]],
    )
    def test_clusters_option_rejected(self, tmp_path, args):
        script = tmp_path / "run.sh"
        script.write_text("echo\n")
        result = runner.invoke(app, ["sbatch", *args, str(script)])
        assert result.exit_code == 2
        # Rich colours option names in error panels (e.g. on GitHub Actions).
        assert "--cluster" in re.sub(r"\x1b\[[0-9;]*m", "", result.output)

    def test_clusters_directive_rejected(self, tmp_path):
        script = tmp_path / "run.sh"
        script.write_text("#SBATCH --clusters=other\necho\n")
        result = runner.invoke(app, ["sbatch", str(script)])
        assert result.exit_code == 2

    def test_no_cluster_configured_is_an_error(self, tmp_path):
        from srunx.common import paths

        paths.user_config_path().write_text('[clusters.local]\ntransport = "local"\n')
        clear_settings_cache()
        result = runner.invoke(app, ["sbatch", "--wrap", "echo"])
        assert result.exit_code == 2
        assert "No cluster selected" in result.output

    def test_env_selects_cluster(self, monkeypatch):
        monkeypatch.setenv("SRUNX_CLUSTER", "nope")
        result = runner.invoke(app, ["sbatch", "--wrap", "echo", "--dry-run"])
        assert result.exit_code == 2
        assert "Unknown cluster 'nope'" in result.output


class TestClusterCommands:
    def test_add_list_show_update_remove(self):
        r = runner.invoke(app, ["cluster", "add", "gpu", "--host", "gpu-login"])
        assert r.exit_code == 0, r.output
        r = runner.invoke(app, ["cluster", "list"])
        assert "gpu-login (ssh config)" in r.stdout
        r = runner.invoke(app, ["cluster", "update", "gpu", "--description", "GPUs"])
        assert r.exit_code == 0, r.output
        r = runner.invoke(app, ["cluster", "show", "gpu"])
        assert "GPUs" in r.stdout
        r = runner.invoke(app, ["cluster", "remove", "gpu"])
        assert r.exit_code == 0, r.output
        assert "gpu" not in runner.invoke(app, ["cluster", "list"]).stdout.split()

    def test_add_requires_one_target(self):
        r = runner.invoke(app, ["cluster", "add", "x", "--local", "--host", "h"])
        assert r.exit_code == 1

    def test_add_duplicate_and_bad_name(self):
        assert runner.invoke(app, ["cluster", "add", "local", "--local"]).exit_code == 1
        assert (
            runner.invoke(app, ["cluster", "add", "a:b", "--host", "h"]).exit_code == 1
        )

    def test_add_default(self):
        r = runner.invoke(app, ["cluster", "add", "gpu", "--host", "g", "--default"])
        assert r.exit_code == 0
        assert "* " in runner.invoke(app, ["cluster", "list"]).stdout

    def test_test_local(self, monkeypatch):
        monkeypatch.setattr("shutil.which", lambda _n: "/usr/bin/sbatch")
        r = runner.invoke(app, ["cluster", "test", "local"])
        assert r.exit_code == 0
        assert "/usr/bin/sbatch" in r.stdout


@pytest.mark.real_approval
class TestSyncApproval:
    def test_non_interactive_first_sync_refused_then_approved(self):
        from srunx.sync.approval import (
            SyncNotApprovedError,
            ensure_approved,
            is_approved,
        )

        with pytest.raises(SyncNotApprovedError, match="--yes"):
            ensure_approved("dgx", "/l", "/r")
        ensure_approved("dgx", "/l", "/r", assume_yes=True)
        assert is_approved("dgx", "/r")
        ensure_approved("dgx", "/l", "/r")  # remembered
        assert not is_approved("foo", "/r")


def test_config_set_keeps_times_as_strings():
    from srunx.common import paths
    from srunx.common.config_store import ConfigStore

    r = runner.invoke(app, ["config", "set", "resources.time_limit", "04:00:00"])
    assert r.exit_code == 0, r.output
    data = ConfigStore(paths.user_config_path()).read()
    assert data["defaults"]["resources"]["time_limit"] == "04:00:00"


class TestReviewRegressions:
    def test_explicit_gres_covers_gpu_default(self):
        cfg = SrunxConfig.model_validate({"resources": {"gpus_per_node": 4}})
        cfg.sources = {"resources.gpus_per_node": "u"}
        merge = merge_script_defaults(cfg, {}, explicit_options={"--gres"})
        assert merge.args == []

    def test_cli_gres_drops_inherited_gpus(self, tmp_path, local_slurm):
        append_user_config("[defaults.resources]\ngpus_per_node = 4")
        script = tmp_path / "run.sh"
        script.write_text("echo\n")
        r = runner.invoke(app, ["sbatch", "--gres", "gpu:1", str(script)])
        assert r.exit_code == 0, r.output
        assert local_slurm.submit.call_args.kwargs["extra_sbatch_args"] == [
            "--gres=gpu:1"
        ]

    def test_short_passthrough_counts_as_explicit(self, tmp_path, local_slurm):
        append_user_config('[defaults.resources]\npartition = "gpu"')
        script = tmp_path / "run.sh"
        script.write_text("echo\n")
        r = runner.invoke(app, ["sbatch", "--sbatch-arg=--parti=x", str(script)])
        assert r.exit_code == 0, r.output
        assert local_slurm.submit.call_args.kwargs["extra_sbatch_args"] == ["--parti=x"]

    def test_abbreviated_directives_are_recognized(self):
        # ``--tim`` is ambiguous (--time / --time-min) and stays unexpanded,
        # as with sbatch's own getopt_long.
        assert parse_directives("#SBATCH --part=debug --chd /x --tim 5\n") == {
            "--partition": "debug",
            "--chdir": "/x",
        }

    def test_abbreviated_partition_directive_wins(self, tmp_path, local_slurm):
        append_user_config('[defaults.resources]\npartition = "gpu"')
        script = tmp_path / "run.sh"
        script.write_text("#SBATCH --part=debug\necho\n")
        r = runner.invoke(app, ["sbatch", str(script)])
        assert r.exit_code == 0, r.output
        assert not local_slurm.submit.call_args.kwargs["extra_sbatch_args"]

    def test_abbreviated_clusters_directive_rejected(self, tmp_path):
        script = tmp_path / "run.sh"
        script.write_text("#SBATCH --clust=other\necho\n")
        assert runner.invoke(app, ["sbatch", str(script)]).exit_code == 2

    def test_concurrent_remote_env_updates_keep_all_entries(self):
        import threading

        from srunx.common.remote_env import (
            RemoteEnv,
            _read_cache,
            forget_remote_env,
            store_remote_env,
        )

        errors: list[BaseException] = []

        def worker(i: int) -> None:
            try:
                store_remote_env(f"c{i}", RemoteEnv(f"/h{i}", "/d", "/c"))
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []
        assert set(_read_cache()) == {f"c{i}" for i in range(20)}
        forget_remote_env("c0")
        assert "c0" not in _read_cache()

    def test_cli_update_forgets_remote_env(self):
        from srunx.common.remote_env import RemoteEnv, _read_cache, store_remote_env

        store_remote_env("dgx", RemoteEnv("/h", "/d", "/c"))
        r = runner.invoke(app, ["cluster", "update", "dgx", "--hostname", "new"])
        assert r.exit_code == 0, r.output
        assert "dgx" not in _read_cache()


@pytest.mark.parametrize(
    "tokens",
    [["--cl=other"], ["--clus", "x"], ["-vMother"], ["-M", "x"], ["-Mx"]],
)
def test_clusters_spellings_detected(tokens):
    from srunx.cli._helpers.sbatch_resolve import has_clusters_option

    assert has_clusters_option(tokens)


@pytest.mark.parametrize("tokens", [["-pM"], ["--clean"], ["-J", "M"]])
def test_non_cluster_spellings_not_detected(tokens):
    from srunx.cli._helpers.sbatch_resolve import has_clusters_option

    assert not has_clusters_option(tokens)


def test_sbatch_arg_cluster_abbreviation_rejected(tmp_path):
    script = tmp_path / "run.sh"
    script.write_text("echo\n")
    r = runner.invoke(app, ["sbatch", "--sbatch-arg=--cl=other", str(script)])
    assert r.exit_code == 2


def test_config_delete_restores_inheritance_after_unset():
    from srunx.common import paths
    from srunx.common.config_store import ConfigStore

    runner.invoke(app, ["config", "unset", "resources.partition"])
    assert (
        "unset" in ConfigStore(paths.user_config_path()).read()["defaults"]["resources"]
    )
    r = runner.invoke(app, ["config", "delete", "resources.partition"])
    assert r.exit_code == 0, r.output
    assert (
        "unset"
        not in ConfigStore(paths.user_config_path()).read()["defaults"]["resources"]
    )


def test_second_local_cluster_rejected_before_saving():
    from srunx.common import paths

    before = paths.user_config_path().read_text()
    r = runner.invoke(app, ["cluster", "add", "two", "--local"])
    assert r.exit_code == 1
    assert "At most one cluster" in r.output
    assert paths.user_config_path().read_text() == before


def test_wrap_gres_drops_inherited_gpus(local_slurm):
    append_user_config("[defaults.resources]\ngpus_per_node = 8")
    r = runner.invoke(app, ["sbatch", "--wrap", "echo", "--gres", "gpu:a100:1"])
    assert r.exit_code == 0, r.output
    job = local_slurm.submit.call_args.args[0]
    assert job.resources.gpus_per_node == 0
    assert local_slurm.submit.call_args.kwargs["extra_sbatch_args"] == [
        "--gres=gpu:a100:1"
    ]


def test_bundled_cluster_directive_rejected(tmp_path):
    script = tmp_path / "run.sh"
    script.write_text("#SBATCH -vMother\necho\n")
    assert runner.invoke(app, ["sbatch", str(script)]).exit_code == 2


def test_locked_sync_skips_shared_storage_mount(tmp_path, monkeypatch):
    from srunx.ssh.core.config import MountConfig, ServerProfile
    from srunx.web.sync_utils import locked_sync_mount

    called = []
    monkeypatch.setattr(
        "srunx.web.sync_utils.sync_mount_by_name", lambda *a, **k: called.append(1)
    )
    profile = ServerProfile(
        hostname="h",
        username="u",
        key_filename="",
        mounts=[MountConfig(name="m", local=str(tmp_path), remote="/r", sync=False)],
    )
    assert locked_sync_mount(profile, "m", profile_name="dgx") == ""
    assert called == []


def test_cluster_remove_and_readd_forgets_remote_env():
    from srunx.common.remote_env import RemoteEnv, _read_cache, store_remote_env

    store_remote_env("dgx", RemoteEnv("/old", "/d", "/c"))
    assert runner.invoke(app, ["cluster", "remove", "dgx"]).exit_code == 0
    assert "dgx" not in _read_cache()
    store_remote_env("dgx", RemoteEnv("/old", "/d", "/c"))
    assert runner.invoke(app, ["cluster", "add", "dgx", "--host", "new"]).exit_code == 0
    assert "dgx" not in _read_cache()


def test_config_set_connection_forgets_remote_env():
    from srunx.common.remote_env import RemoteEnv, _read_cache, store_remote_env

    store_remote_env("dgx", RemoteEnv("/old", "/d", "/c"))
    store_remote_env("foo", RemoteEnv("/f", "/d", "/c"))
    r = runner.invoke(
        app, ["config", "set", "resources.partition", "x", "--cluster", "foo"]
    )
    assert r.exit_code == 0
    assert "foo" in _read_cache()  # defaults edits keep the probe
    r = runner.invoke(app, ["config", "set", "host", "other", "--cluster", "dgx"])
    assert r.exit_code == 0, r.output
    assert "dgx" not in _read_cache()


@pytest.mark.parametrize(
    ("config", "directive"),
    [
        ('memory_per_node = "64G"', "#SBATCH --mem-per-cpu=4G"),
        ("cpus_per_task = 8", "#SBATCH --cpus-per-gpu=4"),
        ("gpus_per_node = 8", "#SBATCH -G 2"),
    ],
)
def test_alternative_resource_spellings_suppress_defaults(config, directive):
    cfg = SrunxConfig.model_validate({"resources": dict([_kv(config)])})
    cfg.sources = {f"resources.{_kv(config)[0]}": "u"}
    merge = merge_script_defaults(
        cfg, parse_directives(directive + "\n"), explicit_options=set()
    )
    assert merge.args == []


def _kv(text: str) -> tuple[str, object]:
    import tomllib

    return next(iter(tomllib.loads(text).items()))


def test_alternative_resource_passthrough_suppresses_default():
    cfg = SrunxConfig.model_validate({"resources": {"memory_per_node": "64G"}})
    cfg.sources = {"resources.memory_per_node": "u"}
    merge = merge_script_defaults(cfg, {}, explicit_options={"--mem-per-cpu"})
    assert merge.args == []


def test_flow_run_yes_is_forwarded(tmp_path, monkeypatch):
    import srunx.cli.workflow.orchestrator as orch

    seen = {}
    monkeypatch.setattr(orch, "_execute_workflow", lambda **kw: seen.update(kw))
    wf = tmp_path / "wf.yaml"
    wf.write_text("name: w\njobs: []\n")
    from srunx.cli.main import app as main_app

    r = runner.invoke(main_app, ["flow", "run", str(wf), "--yes"])
    assert r.exit_code == 0, r.output
    assert seen.get("yes") is True


def test_bundled_short_directives_parsed():
    assert parse_directives("#SBATCH -vp gpu\n#SBATCH -vt5\n") == {
        "--partition": "gpu",
        "--time": "5",
    }
    assert parse_directives("#SBATCH -vpgpu -N2\n") == {
        "--partition": "gpu",
        "--nodes": "2",
    }


def test_bundled_partition_directive_wins(tmp_path, local_slurm):
    append_user_config('[defaults.resources]\npartition = "cfg"')
    script = tmp_path / "run.sh"
    script.write_text("#SBATCH -vp gpu\necho\n")
    r = runner.invoke(app, ["sbatch", str(script)])
    assert r.exit_code == 0, r.output
    assert not local_slurm.submit.call_args.kwargs["extra_sbatch_args"]


def test_translate_local_paths():
    from types import SimpleNamespace

    from srunx.cli._helpers.sbatch_resolve import translate_local_paths

    mounts = [
        SimpleNamespace(local="/Users/me/p", remote="/remote/p"),
        SimpleNamespace(local="/Users/me/p/data", remote="/data"),
    ]
    assert translate_local_paths(
        [
            "--chdir=/Users/me/p",
            "--output=/Users/me/p/logs/%x_%j.log",
            "--error=/Users/me/p/data/e.log",
            "--chdir=/elsewhere",
            "--output=logs/x.log",
            "--partition=/Users/me/p",
        ],
        mounts,
    ) == [
        "--chdir=/remote/p",
        "--output=/remote/p/logs/%x_%j.log",
        "--error=/data/e.log",
        "--chdir=/elsewhere",
        "--output=logs/x.log",
        "--partition=/Users/me/p",
    ]


def test_update_system_only_cluster_is_rejected(monkeypatch, tmp_path):
    from srunx.common import paths

    system = tmp_path / "system.toml"
    system.write_text('[clusters.sys]\ntransport = "ssh"\nhost = "s"\n')
    monkeypatch.setattr(paths, "system_config_path", lambda: system)
    clear_settings_cache()
    r = runner.invoke(app, ["cluster", "update", "sys", "--hostname", "x"])
    assert r.exit_code == 1
    assert "system config" in r.output


class TestCodeDir:
    """$SRUNX_CODE_DIR points at the project's code on the target."""

    def test_local_cluster_uses_project_root(self, tmp_path, monkeypatch, local_slurm):
        root = _project(tmp_path / "p", 'name = "p"\n')
        script = root / "run.sh"
        script.write_text("echo\n")
        monkeypatch.chdir(root)
        r = runner.invoke(app, ["sbatch", "--env", "SRUNX_CODE_DIR=/x", str(script)])
        assert r.exit_code == 0, r.output
        env = local_slurm.submit.call_args.args[0].environment.env_vars
        assert env["SRUNX_CODE_DIR"] == str(root.resolve())

    def test_no_project_no_variable(self, tmp_path, local_slurm):
        script = tmp_path / "run.sh"
        script.write_text("echo\n")
        r = runner.invoke(app, ["sbatch", str(script)])
        assert r.exit_code == 0, r.output
        env = local_slurm.submit.call_args.args[0].environment.env_vars
        assert "SRUNX_CODE_DIR" not in env

    def test_cluster_env_vars_reach_the_job(self, tmp_path, local_slurm):
        append_user_config(
            '[clusters.local.defaults.environment.env_vars]\nDATA_ROOT = "/data"'
        )
        script = tmp_path / "run.sh"
        script.write_text("echo\n")
        r = runner.invoke(app, ["sbatch", "--env", "A=1", str(script)])
        assert r.exit_code == 0, r.output
        env = local_slurm.submit.call_args.args[0].environment.env_vars
        assert env["DATA_ROOT"] == "/data"
        assert env["A"] == "1"

    def test_ssh_uses_mount_owning_the_script(self, tmp_path):
        from srunx.cli._helpers.sbatch_resolve import code_dir_for

        append_user_config('[clusters.gpu]\ntransport = "ssh"\nhost = "gpu"')
        root = _project(
            tmp_path / "p",
            'name = "p"\n[clusters.gpu]\nremote_dir = "/r/p"\n'
            '[[clusters.gpu.mounts]]\nname = "tools"\nlocal = "../tools"\n'
            'remote = "/r/tools"\n',
        )
        (tmp_path / "tools").mkdir()
        from srunx.common.config import use_project

        with use_project(root):
            assert code_dir_for("gpu", root / "a" / "run.sh") == "/r/p"
            assert code_dir_for("gpu", tmp_path / "tools" / "t.sh") == "/r/tools"
            assert code_dir_for("gpu", None) == "/r/p"
            assert code_dir_for("gpu", tmp_path / "elsewhere.sh") == "/r/p"

    def test_dry_run_shows_code_dir(self, tmp_path, monkeypatch):
        root = _project(tmp_path / "p", 'name = "p"\n')
        script = root / "run.sh"
        script.write_text("echo\n")
        monkeypatch.chdir(root)
        r = runner.invoke(app, ["sbatch", "--dry-run", str(script)])
        assert r.exit_code == 0, r.output
        assert f"SRUNX_CODE_DIR: {root.resolve()}" in r.stdout
