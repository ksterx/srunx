"""Tests for Job render metadata fields (template, srun_args, launch_prefix).

Step 2 of Phase 2 SSH sweep integration: Job now carries optional render
metadata that ``render_job_script`` falls back to when no explicit
``extra_srun_args`` / ``extra_launch_prefix`` argument is given. Explicit
arguments must always take precedence to preserve the Web non-sweep path.
"""

from pathlib import Path

from srunx.models import Job, render_job_script


class TestJobRenderMetadataFields:
    """Job should expose optional render metadata fields."""

    def test_all_fields_settable(self):
        """All three render metadata fields can be set on Job construction."""
        job = Job(
            name="x",
            command=["echo"],
            template="custom.jinja",
            srun_args="--qos=high",
            launch_prefix="mpirun",
        )
        assert job.template == "custom.jinja"
        assert job.srun_args == "--qos=high"
        assert job.launch_prefix == "mpirun"

    def test_fields_optional_default_none(self):
        """Omitting the new fields keeps backward compatibility (all None)."""
        job = Job(name="x", command=["echo"])
        assert job.template is None
        assert job.srun_args is None
        assert job.launch_prefix is None

    def test_backward_compat_existing_yaml_shape(self):
        """An existing Job dict without the new keys still validates."""
        job = Job.model_validate(
            {
                "name": "legacy",
                "command": ["python", "train.py"],
                "log_dir": "",
                "work_dir": "",
            }
        )
        assert job.template is None
        assert job.srun_args is None
        assert job.launch_prefix is None


class TestRenderJobScriptFallback:
    """``render_job_script`` should fall back to Job fields when extras absent."""

    def _write_template(self, temp_dir: Path) -> Path:
        """Write a minimal Jinja template that surfaces both vars."""
        template_path = temp_dir / "test.jinja"
        template_path.write_text(
            "#!/bin/bash\n"
            "srun_args={{ srun_args }}\n"
            "launch_prefix={{ launch_prefix }}\n"
            "cmd={{ command }}\n"
        )
        return template_path

    def test_job_srun_args_used_when_extra_absent(self, temp_dir):
        """Job.srun_args is injected when extra_srun_args is not provided."""
        template_path = self._write_template(temp_dir)
        job = Job(
            name="j_fallback_srun",
            command=["python", "train.py"],
            srun_args="--qos=high",
            log_dir="",
            work_dir="",
        )
        script_path = render_job_script(template_path, job, temp_dir)
        content = Path(script_path).read_text()
        assert "--qos=high" in content

    def test_explicit_extra_srun_args_overrides_job_field(self, temp_dir):
        """Explicit extra_srun_args wins over Job.srun_args."""
        template_path = self._write_template(temp_dir)
        job = Job(
            name="j_override_srun",
            command=["python", "train.py"],
            srun_args="--qos=fallback",
            log_dir="",
            work_dir="",
        )
        script_path = render_job_script(
            template_path,
            job,
            temp_dir,
            extra_srun_args="--qos=override",
        )
        content = Path(script_path).read_text()
        assert "--qos=override" in content
        assert "--qos=fallback" not in content

    def test_job_launch_prefix_used_when_extra_absent(self, temp_dir):
        """Job.launch_prefix is injected when extra_launch_prefix is not provided."""
        template_path = self._write_template(temp_dir)
        job = Job(
            name="j_fallback_prefix",
            command=["python", "train.py"],
            launch_prefix="mpirun -np 4",
            log_dir="",
            work_dir="",
        )
        script_path = render_job_script(template_path, job, temp_dir)
        content = Path(script_path).read_text()
        assert "mpirun -np 4" in content

    def test_explicit_extra_launch_prefix_overrides_job_field(self, temp_dir):
        """Explicit extra_launch_prefix wins over Job.launch_prefix."""
        template_path = self._write_template(temp_dir)
        job = Job(
            name="j_override_prefix",
            command=["python", "train.py"],
            launch_prefix="mpirun",
            log_dir="",
            work_dir="",
        )
        script_path = render_job_script(
            template_path,
            job,
            temp_dir,
            extra_launch_prefix="torchrun --nproc_per_node=4",
        )
        content = Path(script_path).read_text()
        assert "torchrun --nproc_per_node=4" in content
        assert "launch_prefix=mpirun\n" not in content

    def test_no_extras_and_no_job_fields_unchanged(self, temp_dir):
        """No extras + no Job fields reproduces the pre-Step-2 behavior."""
        template_path = self._write_template(temp_dir)
        job = Job(
            name="j_none",
            command=["echo", "hello"],
            log_dir="",
            work_dir="",
        )
        script_path = render_job_script(template_path, job, temp_dir)
        content = Path(script_path).read_text()
        # With no extras and no container, both render-time values are empty.
        assert "srun_args=\n" in content
        assert "launch_prefix=\n" in content
        assert "echo hello" in content

    def test_empty_string_extra_does_not_override_job_field(self, temp_dir):
        """An empty string (not None) for extra_srun_args should still be
        treated as 'explicit', overriding the Job field.

        This matches the ``is not None`` sentinel: callers that pass ``""``
        explicitly are opting out of the fallback. Prevents accidental
        double-injection when callers zero-out the extras intentionally.
        """
        template_path = self._write_template(temp_dir)
        job = Job(
            name="j_empty",
            command=["python", "train.py"],
            srun_args="--should-not-appear",
            log_dir="",
            work_dir="",
        )
        script_path = render_job_script(
            template_path, job, temp_dir, extra_srun_args=""
        )
        content = Path(script_path).read_text()
        assert "--should-not-appear" not in content
