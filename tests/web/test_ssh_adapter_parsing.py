"""Tests for SSH adapter's SLURM output parsing logic.

These tests verify parsing without any SSH connection by testing
the parsing functions with sample SLURM command output.
"""

from __future__ import annotations

import pytest

from srunx.slurm.parsing import GPU_TRES_RE
from srunx.slurm.ssh import (
    _UNAVAILABLE_STATES,
    SlurmSSHAdapter,
    _validate_identifier,
)

# ── Input Validation ──────────────────────────────


class TestValidateIdentifier:
    def test_valid_username(self) -> None:
        _validate_identifier("researcher", "user")

    def test_valid_with_dots_dashes(self) -> None:
        _validate_identifier("user.name-01", "user")

    def test_rejects_shell_injection(self) -> None:
        with pytest.raises(ValueError, match="Invalid user"):
            _validate_identifier("foo'; rm -rf /; echo '", "user")

    def test_rejects_spaces(self) -> None:
        with pytest.raises(ValueError, match="Invalid"):
            _validate_identifier("foo bar", "partition")

    def test_rejects_semicolons(self) -> None:
        with pytest.raises(ValueError, match="Invalid"):
            _validate_identifier("gpu;cat /etc/passwd", "partition")

    def test_rejects_backticks(self) -> None:
        with pytest.raises(ValueError, match="Invalid"):
            _validate_identifier("`whoami`", "user")

    def test_rejects_empty(self) -> None:
        with pytest.raises(ValueError, match="Invalid"):
            _validate_identifier("", "user")


# ── GPU Regex ─────────────────────────────────────


class TestGPURegex:
    def test_simple_gpu_count(self) -> None:
        match = GPU_TRES_RE.search("gpu:4")
        assert match and match.group(1) == "4"

    def test_gpu_with_model(self) -> None:
        match = GPU_TRES_RE.search("gpu:NVIDIA-A100:8")
        assert match and match.group(1) == "8"

    def test_gpu_with_slash(self) -> None:
        match = GPU_TRES_RE.search("gpu/4")
        assert match and match.group(1) == "4"

    def test_gpu_with_equals(self) -> None:
        match = GPU_TRES_RE.search("gpu=4")
        assert match and match.group(1) == "4"

    def test_no_gpu(self) -> None:
        assert GPU_TRES_RE.search("cpu:16") is None

    def test_null_gres(self) -> None:
        assert GPU_TRES_RE.search("(null)") is None

    def test_case_insensitive(self) -> None:
        match = GPU_TRES_RE.search("GPU:A100:2")
        assert match and match.group(1) == "2"

    def test_tres_format(self) -> None:
        """TRES format from sacct: billing=8,cpu=8,gres/gpu=8,mem=200G"""
        match = GPU_TRES_RE.search("gres/gpu=8")
        assert match and match.group(1) == "8"


# ── Unavailable States ────────────────────────────


class TestUnavailableStates:
    def test_down_is_unavailable(self) -> None:
        assert "down" in _UNAVAILABLE_STATES

    def test_drain_is_unavailable(self) -> None:
        assert "drain" in _UNAVAILABLE_STATES

    def test_maint_is_unavailable(self) -> None:
        assert "maint" in _UNAVAILABLE_STATES

    def test_reserved_is_unavailable(self) -> None:
        assert "reserved" in _UNAVAILABLE_STATES

    def test_idle_is_available(self) -> None:
        assert "idle" not in _UNAVAILABLE_STATES


# ── squeue Output Parsing ─────────────────────────


class TestListJobsParsing:
    """Test the list_jobs parsing logic by mocking _run_slurm_cmd."""

    # Pipe-delimited format: %i|%P|%j|%u|%T|%M|%l|%D|%C|%R|%b
    # (job_id|partition|name|user|state|elapsed|time_limit|nodes|cpus|nodelist_or_reason|TRES_PER_NODE)
    SAMPLE_SQUEUE = """\
18431|defq|qwen3-tts|ksterx|RUNNING|1-00:03:20|UNLIMITED|1|16|dgx-node1|gpu:8
18477|defq|cosy|ksterx|RUNNING|12:30:45|UNLIMITED|1|32|dgx-node2|gpu:NVIDIA-A100:8
18490|defq|gemma3-cpt|alice|RUNNING|5:15:00|UNLIMITED|2|64|dgx-node[3-4]|gpu:4
18500|defq|pending|bob|PENDING|0:00|UNLIMITED|1|8|(Priority)|(null)
"""

    def test_parse_running_jobs(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "srunx.slurm.ssh._run_slurm_cmd",
            lambda _adapter, _cmd: self.SAMPLE_SQUEUE,
        )
        adapter = object.__new__(SlurmSSHAdapter)
        jobs = adapter.list_jobs()

        assert len(jobs) == 4

    def test_parse_job_id(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "srunx.slurm.ssh._run_slurm_cmd",
            lambda _a, _c: self.SAMPLE_SQUEUE,
        )
        adapter = object.__new__(SlurmSSHAdapter)
        jobs = adapter.list_jobs()

        assert jobs[0]["job_id"] == 18431
        assert jobs[1]["job_id"] == 18477

    def test_parse_gpu_count_simple(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "srunx.slurm.ssh._run_slurm_cmd",
            lambda _a, _c: self.SAMPLE_SQUEUE,
        )
        adapter = object.__new__(SlurmSSHAdapter)
        jobs = adapter.list_jobs()

        assert jobs[0]["gpus"] == 8  # gpu:8

    def test_parse_gpu_count_with_model(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "srunx.slurm.ssh._run_slurm_cmd",
            lambda _a, _c: self.SAMPLE_SQUEUE,
        )
        adapter = object.__new__(SlurmSSHAdapter)
        jobs = adapter.list_jobs()

        assert jobs[1]["gpus"] == 8  # gpu:NVIDIA-A100:8

    def test_parse_null_gres(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "srunx.slurm.ssh._run_slurm_cmd",
            lambda _a, _c: self.SAMPLE_SQUEUE,
        )
        adapter = object.__new__(SlurmSSHAdapter)
        jobs = adapter.list_jobs()

        assert jobs[3]["gpus"] == 0  # (null)

    def test_parse_status(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "srunx.slurm.ssh._run_slurm_cmd",
            lambda _a, _c: self.SAMPLE_SQUEUE,
        )
        adapter = object.__new__(SlurmSSHAdapter)
        jobs = adapter.list_jobs()

        assert jobs[0]["status"] == "RUNNING"
        assert jobs[3]["status"] == "PENDING"

    def test_parse_partition(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "srunx.slurm.ssh._run_slurm_cmd",
            lambda _a, _c: self.SAMPLE_SQUEUE,
        )
        adapter = object.__new__(SlurmSSHAdapter)
        jobs = adapter.list_jobs()

        assert jobs[0]["partition"] == "defq"

    def test_multinode_gpu_total(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Multi-node job: gpus = gpus_per_node * nodes."""
        monkeypatch.setattr(
            "srunx.slurm.ssh._run_slurm_cmd",
            lambda _a, _c: self.SAMPLE_SQUEUE,
        )
        adapter = object.__new__(SlurmSSHAdapter)
        jobs = adapter.list_jobs()

        # Job 18490: 2 nodes, gpu:4 per node → total 8 GPUs
        assert jobs[2]["resources"]["gpus_per_node"] == 4
        assert jobs[2]["nodes"] == 2
        assert jobs[2]["gpus"] == 8  # 4 * 2

    def test_single_node_gpu_unchanged(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Single-node job: gpus = gpus_per_node."""
        monkeypatch.setattr(
            "srunx.slurm.ssh._run_slurm_cmd",
            lambda _a, _c: self.SAMPLE_SQUEUE,
        )
        adapter = object.__new__(SlurmSSHAdapter)
        jobs = adapter.list_jobs()

        # Job 18431: 1 node, gpu:8 → total 8 GPUs
        assert jobs[0]["resources"]["gpus_per_node"] == 8
        assert jobs[0]["nodes"] == 1
        assert jobs[0]["gpus"] == 8  # 8 * 1

    def test_parse_user(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "srunx.slurm.ssh._run_slurm_cmd",
            lambda _a, _c: self.SAMPLE_SQUEUE,
        )
        adapter = object.__new__(SlurmSSHAdapter)
        jobs = adapter.list_jobs()

        assert jobs[0]["user"] == "ksterx"
        assert jobs[2]["user"] == "alice"
        assert jobs[3]["user"] == "bob"

    def test_parse_cpus(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """%C field surfaces as the total allocated CPU count."""
        monkeypatch.setattr(
            "srunx.slurm.ssh._run_slurm_cmd",
            lambda _a, _c: self.SAMPLE_SQUEUE,
        )
        adapter = object.__new__(SlurmSSHAdapter)
        jobs = adapter.list_jobs()

        assert jobs[0]["cpus"] == 16
        assert jobs[2]["cpus"] == 64

    def test_parse_nodelist_running_vs_pending(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """%R is the nodelist for running jobs and the reason for pending ones."""
        monkeypatch.setattr(
            "srunx.slurm.ssh._run_slurm_cmd",
            lambda _a, _c: self.SAMPLE_SQUEUE,
        )
        adapter = object.__new__(SlurmSSHAdapter)
        jobs = adapter.list_jobs()

        assert jobs[0]["nodelist"] == "dgx-node1"
        assert jobs[2]["nodelist"] == "dgx-node[3-4]"
        assert jobs[3]["nodelist"] == "(Priority)"

    def test_empty_output(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "srunx.slurm.ssh._run_slurm_cmd",
            lambda _a, _c: "",
        )
        adapter = object.__new__(SlurmSSHAdapter)
        assert adapter.list_jobs() == []

    def test_required_frontend_fields(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Every job dict must have command and resources for frontend type."""
        monkeypatch.setattr(
            "srunx.slurm.ssh._run_slurm_cmd",
            lambda _a, _c: self.SAMPLE_SQUEUE,
        )
        adapter = object.__new__(SlurmSSHAdapter)
        for job in adapter.list_jobs():
            assert "command" in job
            assert "resources" in job
            assert "status" in job
            assert "depends_on" in job


# ── sacct Output Parsing ──────────────────────────


class TestGetJobParsing:
    SAMPLE_SACCT = """\
18431|qwen3-tts|RUNNING|defq|1|8|1-00:03:20|UNLIMITED|billing=8,cpu=8,gres/gpu=8,mem=200G
18431.batch|batch|RUNNING||1|8|1-00:03:20||
18431.0|orted|RUNNING||1|1|1-00:03:20||
"""

    def test_parse_job_id(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "srunx.slurm.ssh._run_slurm_cmd",
            lambda _a, _c: self.SAMPLE_SACCT,
        )
        adapter = object.__new__(SlurmSSHAdapter)
        job = adapter.get_job(18431)

        assert job["job_id"] == 18431
        assert job["name"] == "qwen3-tts"

    def test_skips_substeps(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "srunx.slurm.ssh._run_slurm_cmd",
            lambda _a, _c: self.SAMPLE_SACCT,
        )
        adapter = object.__new__(SlurmSSHAdapter)
        job = adapter.get_job(18431)

        # Should return parent job, not .batch or .0
        assert job["name"] == "qwen3-tts"

    def test_parse_gpu_from_tres(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "srunx.slurm.ssh._run_slurm_cmd",
            lambda _a, _c: self.SAMPLE_SACCT,
        )
        adapter = object.__new__(SlurmSSHAdapter)
        job = adapter.get_job(18431)

        assert job["gpus"] == 8

    def test_not_found_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "srunx.slurm.ssh._run_slurm_cmd",
            lambda _a, _c: "",
        )
        adapter = object.__new__(SlurmSSHAdapter)

        with pytest.raises(ValueError, match="No job information found"):
            adapter.get_job(99999)


# ── sinfo/squeue Resource Parsing ─────────────────


class TestResourceParsing:
    SAMPLE_SINFO = """\
dgx-node1 gpu:NVIDIA-A100:8 mixed
dgx-node2 gpu:NVIDIA-A100:8 idle
dgx-node3 gpu:NVIDIA-A100:8 allocated
dgx-node4 gpu:NVIDIA-A100:8 down
dgx-node5 gpu:NVIDIA-A100:8 maint
"""

    SAMPLE_SQUEUE_RESOURCES = """\
18431 RUNNING gpu:8 1
18477 RUNNING gpu:NVIDIA-A100:4 2
18490 PENDING gpu:8 1
"""

    def test_node_counting(self, monkeypatch: pytest.MonkeyPatch) -> None:
        call_count = 0

        def mock_cmd(_adapter: object, cmd: str) -> str:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return self.SAMPLE_SINFO
            return self.SAMPLE_SQUEUE_RESOURCES

        monkeypatch.setattr("srunx.slurm.ssh._run_slurm_cmd", mock_cmd)
        monkeypatch.setattr("srunx.slurm.ssh._validate_identifier", lambda *a: None)
        adapter = object.__new__(SlurmSSHAdapter)
        result = adapter._get_partition_resources("gpu")

        assert result["nodes_total"] == 5

    def test_unavailable_nodes_filtered(self, monkeypatch: pytest.MonkeyPatch) -> None:
        call_count = 0

        def mock_cmd(_adapter: object, cmd: str) -> str:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return self.SAMPLE_SINFO
            return ""

        monkeypatch.setattr("srunx.slurm.ssh._run_slurm_cmd", mock_cmd)
        monkeypatch.setattr("srunx.slurm.ssh._validate_identifier", lambda *a: None)
        adapter = object.__new__(SlurmSSHAdapter)
        result = adapter._get_partition_resources("gpu")

        # down + maint = 2 unavailable
        assert result["nodes_down"] == 2
        assert result["nodes_idle"] == 1

    def test_gpu_counting_excludes_unavailable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        call_count = 0

        def mock_cmd(_adapter: object, cmd: str) -> str:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return self.SAMPLE_SINFO
            return ""

        monkeypatch.setattr("srunx.slurm.ssh._run_slurm_cmd", mock_cmd)
        monkeypatch.setattr("srunx.slurm.ssh._validate_identifier", lambda *a: None)
        adapter = object.__new__(SlurmSSHAdapter)
        result = adapter._get_partition_resources("gpu")

        # 3 available nodes * 8 GPUs = 24 (down + maint nodes excluded)
        assert result["total_gpus"] == 24

    def test_gpu_usage_from_squeue(self, monkeypatch: pytest.MonkeyPatch) -> None:
        call_count = 0

        def mock_cmd(_adapter: object, cmd: str) -> str:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return self.SAMPLE_SINFO
            return self.SAMPLE_SQUEUE_RESOURCES

        monkeypatch.setattr("srunx.slurm.ssh._run_slurm_cmd", mock_cmd)
        monkeypatch.setattr("srunx.slurm.ssh._validate_identifier", lambda *a: None)
        adapter = object.__new__(SlurmSSHAdapter)
        result = adapter._get_partition_resources("gpu")

        # RUNNING jobs: 8*1 + 4*2 = 16 GPUs (PENDING ignored, multi-node accounted)
        assert result["gpus_in_use"] == 16
        assert result["jobs_running"] == 2

    def test_utilization_calculation(self, monkeypatch: pytest.MonkeyPatch) -> None:
        call_count = 0

        def mock_cmd(_adapter: object, cmd: str) -> str:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return self.SAMPLE_SINFO
            return self.SAMPLE_SQUEUE_RESOURCES

        monkeypatch.setattr("srunx.slurm.ssh._run_slurm_cmd", mock_cmd)
        monkeypatch.setattr("srunx.slurm.ssh._validate_identifier", lambda *a: None)
        adapter = object.__new__(SlurmSSHAdapter)
        result = adapter._get_partition_resources("gpu")

        assert result["gpu_utilization"] == 16 / 24
        assert result["has_available_gpus"] is True

    def test_required_frontend_fields(self, monkeypatch: pytest.MonkeyPatch) -> None:
        call_count = 0

        def mock_cmd(_adapter: object, cmd: str) -> str:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return self.SAMPLE_SINFO
            return ""

        monkeypatch.setattr("srunx.slurm.ssh._run_slurm_cmd", mock_cmd)
        monkeypatch.setattr("srunx.slurm.ssh._validate_identifier", lambda *a: None)
        adapter = object.__new__(SlurmSSHAdapter)
        result = adapter._get_partition_resources("gpu")

        required = [
            "timestamp",
            "partition",
            "total_gpus",
            "gpus_in_use",
            "gpus_available",
            "jobs_running",
            "nodes_total",
            "nodes_idle",
            "nodes_down",
            "gpu_utilization",
            "has_available_gpus",
        ]
        for field in required:
            assert field in result, f"Missing field: {field}"


# ── get_cluster_snapshot ──────────────────────────


class TestClusterSnapshot:
    """Test the new cluster-wide snapshot method.

    Critical regression: summing per-partition ``get_resources(None)``
    dicts double-counted nodes that belong to multiple partitions.
    ``get_cluster_snapshot`` runs ONE ``sinfo`` without ``-p`` and
    dedups by node name via ``seen_nodes``.
    """

    def test_dedups_nodes_across_partitions(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Node ``dgx1`` in both ``gpu`` and ``debug`` → counted once."""
        # Two partitions report dgx1 with 8 GPUs; cluster-wide sinfo output
        # lists it twice. The dedup must count 8 total, not 16.
        sinfo_out = "dgx1 gpu:8 idle\ndgx1 gpu:8 idle\ndgx2 gpu:8 idle\n"
        squeue_out = ""

        def fake_run(_adapter, cmd: str) -> str:
            return sinfo_out if cmd.startswith("sinfo") else squeue_out

        monkeypatch.setattr("srunx.slurm.ssh._run_slurm_cmd", fake_run)
        adapter = object.__new__(SlurmSSHAdapter)

        snap = adapter.get_cluster_snapshot()

        assert snap["nodes_total"] == 2
        assert snap["total_gpus"] == 16  # 2 nodes × 8, not 3 × 8
        assert snap["gpus_available"] == 16
        assert snap["partition"] is None

    def test_excludes_down_nodes_from_available(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sinfo_out = "dgx1 gpu:8 idle\ndgx2 gpu:8 down\ndgx3 gpu:8 drain\n"
        squeue_out = ""

        def fake_run(_adapter, cmd: str) -> str:
            return sinfo_out if cmd.startswith("sinfo") else squeue_out

        monkeypatch.setattr("srunx.slurm.ssh._run_slurm_cmd", fake_run)
        adapter = object.__new__(SlurmSSHAdapter)

        snap = adapter.get_cluster_snapshot()

        assert snap["nodes_total"] == 3
        assert snap["nodes_down"] == 2  # drain + down
        assert snap["nodes_idle"] == 1
        # GPUs on DOWN/DRAIN nodes are NOT counted (matches local
        # ResourceMonitor semantics).
        assert snap["total_gpus"] == 8

    def test_counts_running_jobs_and_gpus_in_use(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sinfo_out = "dgx1 gpu:8 allocated\ndgx2 gpu:8 idle\n"
        # 1 running job using 4 GPUs on 1 node; 1 running using 2 GPUs × 2 nodes
        squeue_out = (
            "1001 RUNNING gpu:4 1\n1002 RUNNING gpu:2 2\n1003 PENDING (null) 1\n"
        )

        def fake_run(_adapter, cmd: str) -> str:
            return sinfo_out if cmd.startswith("sinfo") else squeue_out

        monkeypatch.setattr("srunx.slurm.ssh._run_slurm_cmd", fake_run)
        adapter = object.__new__(SlurmSSHAdapter)

        snap = adapter.get_cluster_snapshot()

        assert snap["jobs_running"] == 2  # PENDING excluded
        assert snap["gpus_in_use"] == 4 + 2 * 2  # 4 + 4 = 8
        assert snap["total_gpus"] == 16
        assert snap["gpus_available"] == 8
        assert snap["gpu_utilization"] == 0.5

    def test_propagates_errors(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Unlike ``get_resources(None)``, cluster snapshot fails closed.

        Silent per-partition suppression is fine for the dashboard
        listing but wrong for the snapshotter — backoff is better than
        zero rows in the time series.
        """

        def boom(_adapter, _cmd: str) -> str:
            raise RuntimeError("ssh dropped")

        monkeypatch.setattr("srunx.slurm.ssh._run_slurm_cmd", boom)
        adapter = object.__new__(SlurmSSHAdapter)

        with pytest.raises(RuntimeError, match="ssh dropped"):
            adapter.get_cluster_snapshot()


# ── queue_by_ids scontrol fallback (Phase 3 A-1) ──


class TestQueueByIdsScontrolFallback:
    """Phase 3 A-1: on pyxis clusters where sacct is unreachable,
    queue_by_ids must fall back to scontrol per-id before giving up.
    """

    def test_scontrol_fills_gap_for_ids_missing_from_sacct_and_squeue(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        scontrol_out = (
            "JobId=555 JobName=late JobState=COMPLETED Reason=None ExitCode=0:0"
        )

        def fake_run(_adapter, cmd: str) -> str:
            if cmd.startswith("squeue"):
                # squeue has no record of 555 (already finished)
                return ""
            if cmd.startswith("sacct"):
                # sacct returns empty on pyxis (slurmdbd unreachable)
                return ""
            if cmd.startswith("scontrol show job 555"):
                return scontrol_out
            return ""

        monkeypatch.setattr("srunx.slurm.ssh._run_slurm_cmd", fake_run)
        adapter = object.__new__(SlurmSSHAdapter)

        result = adapter.queue_by_ids([555])

        assert 555 in result
        assert result[555].status == "COMPLETED"

    def test_scontrol_completed_with_nonzero_exit_is_failed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        scontrol_out = "JobId=556 JobState=COMPLETED Reason=NonZeroExit ExitCode=2:0"

        def fake_run(_adapter, cmd: str) -> str:
            if cmd.startswith("scontrol show job 556"):
                return scontrol_out
            return ""

        monkeypatch.setattr("srunx.slurm.ssh._run_slurm_cmd", fake_run)
        adapter = object.__new__(SlurmSSHAdapter)

        result = adapter.queue_by_ids([556])

        assert 556 in result
        # ExitCode=2:0 → disambiguated to FAILED (not COMPLETED)
        assert result[556].status == "FAILED"

    def test_all_three_empty_omits_id_from_results(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When no source knows about a job, it stays out of the result
        map (unchanged vs. pre-fix behaviour — the caller treats absence
        as 'no update')."""

        def fake_run(_adapter, _cmd: str) -> str:
            return ""

        monkeypatch.setattr("srunx.slurm.ssh._run_slurm_cmd", fake_run)
        adapter = object.__new__(SlurmSSHAdapter)

        result = adapter.queue_by_ids([999])

        assert 999 not in result
