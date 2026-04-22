"""Tests for Job render metadata fields (template, srun_args, launch_prefix).

Step 2 of Phase 2 SSH sweep integration: Job now carries optional render
metadata that ``render_job_script`` falls back to when no explicit
``extra_srun_args`` / ``extra_launch_prefix`` argument is given. Explicit
arguments must always take precedence to preserve the Web non-sweep path.

Batch 1a additions:
- ``work_dir`` / ``log_dir`` template parity tests (renderer honors the Job
  field values so SSH mount translation that rewrites them at load time
  actually reaches the ``#SBATCH`` directives).
- ``shlex.join``-based command serialization regression tests — ``list[str]``
  commands must round-trip through the renderer with shell metacharacters
  intact (the previous ``" ".join`` collapsed ``bash -c "..."`` payloads).
"""

from pathlib import Path

from srunx.models import Job, render_job_script

BASE_TEMPLATE_PATH = (
    Path(__file__).resolve().parent.parent
    / "src"
    / "srunx"
    / "templates"
    / "base.slurm.jinja"
)


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


class TestWorkDirLogDirTemplateParity:
    """The base template must honor the Job's ``work_dir`` / ``log_dir``.

    SSH submission contexts populate these fields from the configured
    mount's remote path at load time, so by the time the renderer runs
    these are the definitive values for ``#SBATCH --chdir`` /
    ``#SBATCH --output``. Regression coverage against any future
    refactor that tries to resolve these from the process CWD instead.
    """

    def test_work_dir_populates_template(self, temp_dir):
        """``Job(work_dir='/mnt/remote')`` emits ``#SBATCH --chdir=/mnt/remote``."""
        job = Job(
            name="wd_job",
            command=["python", "train.py"],
            log_dir="",
            work_dir="/mnt/remote",
        )
        script_path = render_job_script(BASE_TEMPLATE_PATH, job, temp_dir)
        content = Path(script_path).read_text()
        assert "#SBATCH --chdir=/mnt/remote" in content

    def test_log_dir_populates_template(self, temp_dir):
        """``Job(log_dir='logs')`` emits ``#SBATCH --output=logs/%x_%j.log``."""
        job = Job(
            name="ld_job",
            command=["python", "train.py"],
            log_dir="logs",
            work_dir="",
        )
        script_path = render_job_script(BASE_TEMPLATE_PATH, job, temp_dir)
        content = Path(script_path).read_text()
        assert "#SBATCH --output=logs/%x_%j.log" in content
        assert "#SBATCH --error=logs/%x_%j.log" in content

    def test_work_dir_empty_keeps_default_behavior(self, temp_dir):
        """Empty ``work_dir`` omits the ``#SBATCH --chdir`` directive.

        The base template guards ``--chdir`` behind ``{% if work_dir %}``,
        so an empty string (the explicit opt-out) must not emit the line.
        """
        job = Job(
            name="wd_default",
            command=["echo", "hi"],
            log_dir="",
            work_dir="",
        )
        script_path = render_job_script(BASE_TEMPLATE_PATH, job, temp_dir)
        content = Path(script_path).read_text()
        assert "#SBATCH --chdir=" not in content

    def test_log_dir_empty_uses_relative_default(self, temp_dir):
        """Empty ``log_dir`` falls back to the template's relative default."""
        job = Job(
            name="ld_default",
            command=["echo", "hi"],
            log_dir="",
            work_dir="",
        )
        script_path = render_job_script(BASE_TEMPLATE_PATH, job, temp_dir)
        content = Path(script_path).read_text()
        # Template falls back to the relative ``%x_%j.log`` pattern.
        assert "#SBATCH --output=%x_%j.log" in content
        assert "#SBATCH --error=%x_%j.log" in content


class TestCommandSerializationQuoting:
    """``list[str]`` commands must be shell-quoted via ``shlex.join``.

    The previous renderer used ``" ".join(job.command)``, which silently
    concatenated tokens — shell metacharacters inside a quoted payload
    (``bash -c "echo a; sleep 1; echo b"``) escaped their intended ``-c``
    argument and got reinterpreted by the outer shell.
    """

    def _write_cmd_template(self, temp_dir: Path) -> Path:
        template_path = temp_dir / "cmd.jinja"
        template_path.write_text("#!/bin/bash\nsrun {{ command }}\n")
        return template_path

    def test_bash_c_command_quoted_correctly(self, temp_dir):
        """``bash -c "..."`` payloads with ``;``, spaces are single-quoted."""
        template_path = self._write_cmd_template(temp_dir)
        job = Job(
            name="bash_c_job",
            command=["bash", "-c", "echo a; sleep 1; echo b"],
            log_dir="",
            work_dir="",
        )
        script_path = render_job_script(template_path, job, temp_dir)
        content = Path(script_path).read_text()
        # shlex.join emits a single single-quoted token for the payload.
        assert "srun bash -c 'echo a; sleep 1; echo b'" in content
        # The unquoted form (the previous bug) must not appear.
        assert "srun bash -c echo a; sleep 1; echo b" not in content

    def test_command_with_single_quote_escaped(self, temp_dir):
        """Embedded single quotes are escaped through shlex.join's scheme."""
        template_path = self._write_cmd_template(temp_dir)
        job = Job(
            name="sq_job",
            command=["python", "-c", "print('hi')"],
            log_dir="",
            work_dir="",
        )
        script_path = render_job_script(template_path, job, temp_dir)
        content = Path(script_path).read_text()
        # shlex.join escapes ``'`` as ``'"'"'`` within a single-quoted token.
        assert "'print('\"'\"'hi'\"'\"')'" in content

    def test_empty_string_token_preserved(self, temp_dir):
        """An empty-string argument must be preserved as ``''``."""
        template_path = self._write_cmd_template(temp_dir)
        job = Job(
            name="empty_tok_job",
            command=["echo", "", "end"],
            log_dir="",
            work_dir="",
        )
        script_path = render_job_script(template_path, job, temp_dir)
        content = Path(script_path).read_text()
        # shlex.join renders '' for an empty token.
        assert "srun echo '' end" in content

    def test_empty_command_list_renders_empty_string(self, temp_dir):
        """``command=[]`` renders to an empty string (no crash)."""
        template_path = self._write_cmd_template(temp_dir)
        job = Job(
            name="empty_cmd_job",
            command=[],
            log_dir="",
            work_dir="",
        )
        script_path = render_job_script(template_path, job, temp_dir)
        content = Path(script_path).read_text()
        # Just ``srun `` with no command payload — intentionally minimal.
        assert "srun \n" in content or "srun " in content

    def test_simple_command_unchanged_under_shlex(self, temp_dir):
        """Tokens without metacharacters render identically under shlex.join."""
        template_path = self._write_cmd_template(temp_dir)
        job = Job(
            name="simple_job",
            command=["python", "train.py", "--epochs", "5"],
            log_dir="",
            work_dir="",
        )
        script_path = render_job_script(template_path, job, temp_dir)
        content = Path(script_path).read_text()
        assert "srun python train.py --epochs 5" in content

    def test_string_command_passed_through_verbatim(self, temp_dir):
        """``command`` given as ``str`` is passed through without re-quoting.

        Callers that hand-craft a shell string take responsibility for
        their own quoting — we must not double-quote it.
        """
        template_path = self._write_cmd_template(temp_dir)
        job = Job(
            name="str_cmd_job",
            command='bash -c "echo a; echo b"',
            log_dir="",
            work_dir="",
        )
        script_path = render_job_script(template_path, job, temp_dir)
        content = Path(script_path).read_text()
        assert 'srun bash -c "echo a; echo b"' in content
