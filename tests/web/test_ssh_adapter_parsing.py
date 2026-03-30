"""Tests for SSH adapter's SLURM output parsing logic.

These tests verify parsing without any SSH connection by testing
the parsing functions with sample SLURM command output.
"""

from __future__ import annotations

import pytest

from srunx.web.ssh_adapter import (
    _GPU_RE,
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
        match = _GPU_RE.search("gpu:4")
        assert match and match.group(1) == "4"

    def test_gpu_with_model(self) -> None:
        match = _GPU_RE.search("gpu:NVIDIA-A100:8")
        assert match and match.group(1) == "8"

    def test_gpu_with_slash(self) -> None:
        match = _GPU_RE.search("gpu/4")
        assert match and match.group(1) == "4"

    def test_gpu_with_equals(self) -> None:
        match = _GPU_RE.search("gpu=4")
        assert match and match.group(1) == "4"

    def test_no_gpu(self) -> None:
        assert _GPU_RE.search("cpu:16") is None

    def test_null_gres(self) -> None:
        assert _GPU_RE.search("(null)") is None

    def test_case_insensitive(self) -> None:
        match = _GPU_RE.search("GPU:A100:2")
        assert match and match.group(1) == "2"

    def test_tres_format(self) -> None:
        """TRES format from sacct: billing=8,cpu=8,gres/gpu=8,mem=200G"""
        match = _GPU_RE.search("gres/gpu=8")
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

    SAMPLE_SQUEUE = """\
             18431     defq     qwen3-tts   ksterx  RUNNING 1-00:03:20  UNLIMITED        1 dgx-node1 gpu:8
             18477     defq          cosy   ksterx  RUNNING   12:30:45  UNLIMITED        1 dgx-node2 gpu:NVIDIA-A100:8
             18490     defq    gemma3-cpt   ksterx  RUNNING    5:15:00  UNLIMITED        2 dgx-node[3-4] gpu:4
             18500     defq       pending   ksterx  PENDING       0:00  UNLIMITED        1 (Priority) (null)
"""

    def test_parse_running_jobs(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "srunx.web.ssh_adapter._run_slurm_cmd",
            lambda _adapter, _cmd: self.SAMPLE_SQUEUE,
        )
        adapter = object.__new__(SlurmSSHAdapter)
        jobs = adapter.list_jobs()

        assert len(jobs) == 4

    def test_parse_job_id(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "srunx.web.ssh_adapter._run_slurm_cmd",
            lambda _a, _c: self.SAMPLE_SQUEUE,
        )
        adapter = object.__new__(SlurmSSHAdapter)
        jobs = adapter.list_jobs()

        assert jobs[0]["job_id"] == 18431
        assert jobs[1]["job_id"] == 18477

    def test_parse_gpu_count_simple(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "srunx.web.ssh_adapter._run_slurm_cmd",
            lambda _a, _c: self.SAMPLE_SQUEUE,
        )
        adapter = object.__new__(SlurmSSHAdapter)
        jobs = adapter.list_jobs()

        assert jobs[0]["gpus"] == 8  # gpu:8

    def test_parse_gpu_count_with_model(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "srunx.web.ssh_adapter._run_slurm_cmd",
            lambda _a, _c: self.SAMPLE_SQUEUE,
        )
        adapter = object.__new__(SlurmSSHAdapter)
        jobs = adapter.list_jobs()

        assert jobs[1]["gpus"] == 8  # gpu:NVIDIA-A100:8

    def test_parse_null_gres(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "srunx.web.ssh_adapter._run_slurm_cmd",
            lambda _a, _c: self.SAMPLE_SQUEUE,
        )
        adapter = object.__new__(SlurmSSHAdapter)
        jobs = adapter.list_jobs()

        assert jobs[3]["gpus"] == 0  # (null)

    def test_parse_status(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "srunx.web.ssh_adapter._run_slurm_cmd",
            lambda _a, _c: self.SAMPLE_SQUEUE,
        )
        adapter = object.__new__(SlurmSSHAdapter)
        jobs = adapter.list_jobs()

        assert jobs[0]["status"] == "RUNNING"
        assert jobs[3]["status"] == "PENDING"

    def test_parse_partition(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "srunx.web.ssh_adapter._run_slurm_cmd",
            lambda _a, _c: self.SAMPLE_SQUEUE,
        )
        adapter = object.__new__(SlurmSSHAdapter)
        jobs = adapter.list_jobs()

        assert jobs[0]["partition"] == "defq"

    def test_multinode_gpu_total(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Multi-node job: gpus = gpus_per_node * nodes."""
        monkeypatch.setattr(
            "srunx.web.ssh_adapter._run_slurm_cmd",
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
            "srunx.web.ssh_adapter._run_slurm_cmd",
            lambda _a, _c: self.SAMPLE_SQUEUE,
        )
        adapter = object.__new__(SlurmSSHAdapter)
        jobs = adapter.list_jobs()

        # Job 18431: 1 node, gpu:8 → total 8 GPUs
        assert jobs[0]["resources"]["gpus_per_node"] == 8
        assert jobs[0]["nodes"] == 1
        assert jobs[0]["gpus"] == 8  # 8 * 1

    def test_empty_output(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "srunx.web.ssh_adapter._run_slurm_cmd",
            lambda _a, _c: "",
        )
        adapter = object.__new__(SlurmSSHAdapter)
        assert adapter.list_jobs() == []

    def test_required_frontend_fields(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Every job dict must have command and resources for frontend type."""
        monkeypatch.setattr(
            "srunx.web.ssh_adapter._run_slurm_cmd",
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
            "srunx.web.ssh_adapter._run_slurm_cmd",
            lambda _a, _c: self.SAMPLE_SACCT,
        )
        adapter = object.__new__(SlurmSSHAdapter)
        job = adapter.get_job(18431)

        assert job["job_id"] == 18431
        assert job["name"] == "qwen3-tts"

    def test_skips_substeps(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "srunx.web.ssh_adapter._run_slurm_cmd",
            lambda _a, _c: self.SAMPLE_SACCT,
        )
        adapter = object.__new__(SlurmSSHAdapter)
        job = adapter.get_job(18431)

        # Should return parent job, not .batch or .0
        assert job["name"] == "qwen3-tts"

    def test_parse_gpu_from_tres(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "srunx.web.ssh_adapter._run_slurm_cmd",
            lambda _a, _c: self.SAMPLE_SACCT,
        )
        adapter = object.__new__(SlurmSSHAdapter)
        job = adapter.get_job(18431)

        assert job["gpus"] == 8

    def test_not_found_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "srunx.web.ssh_adapter._run_slurm_cmd",
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

        monkeypatch.setattr("srunx.web.ssh_adapter._run_slurm_cmd", mock_cmd)
        monkeypatch.setattr(
            "srunx.web.ssh_adapter._validate_identifier", lambda *a: None
        )
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

        monkeypatch.setattr("srunx.web.ssh_adapter._run_slurm_cmd", mock_cmd)
        monkeypatch.setattr(
            "srunx.web.ssh_adapter._validate_identifier", lambda *a: None
        )
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

        monkeypatch.setattr("srunx.web.ssh_adapter._run_slurm_cmd", mock_cmd)
        monkeypatch.setattr(
            "srunx.web.ssh_adapter._validate_identifier", lambda *a: None
        )
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

        monkeypatch.setattr("srunx.web.ssh_adapter._run_slurm_cmd", mock_cmd)
        monkeypatch.setattr(
            "srunx.web.ssh_adapter._validate_identifier", lambda *a: None
        )
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

        monkeypatch.setattr("srunx.web.ssh_adapter._run_slurm_cmd", mock_cmd)
        monkeypatch.setattr(
            "srunx.web.ssh_adapter._validate_identifier", lambda *a: None
        )
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

        monkeypatch.setattr("srunx.web.ssh_adapter._run_slurm_cmd", mock_cmd)
        monkeypatch.setattr(
            "srunx.web.ssh_adapter._validate_identifier", lambda *a: None
        )
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
