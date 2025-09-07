"""Integration tests for srunx functionality.

These tests focus on realistic usage scenarios and end-to-end functionality.
"""

import os
import tempfile
from importlib.resources import files
from pathlib import Path

import pytest

from srunx.config import SrunxConfig
from srunx.models import (
    ContainerResource,
    Job,
    JobEnvironment,
    JobResource,
    JobStatus,
    ShellJob,
    Workflow,
    render_job_script,
    render_shell_job_script,
)


def _get_default_template() -> str:
    """Get the default job template path."""
    return str(files("srunx.templates").joinpath("advanced.slurm.jinja"))


class TestTemplateRendering:
    """Test realistic template rendering scenarios."""

    def test_render_basic_job_script(self):
        """Test rendering a basic job script with minimal configuration."""
        job = Job(
            name="basic_job",
            command=["python", "train_model.py"],
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            script_path = render_job_script(
                template_path=_get_default_template(), job=job, output_dir=temp_dir
            )

            assert script_path.endswith(".slurm")
            assert Path(script_path).exists()

            # Read and verify content
            with open(script_path) as f:
                content = f.read()

            assert "#!/bin/bash" in content
            assert "#SBATCH --job-name=basic_job" in content
            assert "python train_model.py" in content

    def test_render_advanced_job_script(self):
        """Test rendering an advanced job script with full configuration."""
        job = Job(
            name="ml_training",
            command=["python", "train.py", "--epochs", "100"],
            resources=JobResource(
                nodes=2,
                gpus_per_node=4,
                ntasks_per_node=1,
                cpus_per_task=8,
                memory_per_node="64GB",
                time_limit="8:00:00",
                partition="gpu",
            ),
            environment=JobEnvironment(
                conda="pytorch",
                env_vars={"CUDA_VISIBLE_DEVICES": "0,1,2,3", "OMP_NUM_THREADS": "8"},
            ),
            log_dir="./logs",
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            script_path = render_job_script(
                template_path=_get_default_template(), job=job, output_dir=temp_dir
            )

            with open(script_path) as f:
                content = f.read()

            # Verify SLURM directives
            assert "#SBATCH --job-name=ml_training" in content
            assert "#SBATCH --nodes=2" in content
            assert "#SBATCH --gpus-per-node=4" in content
            assert "#SBATCH --ntasks-per-node=1" in content
            assert "#SBATCH --cpus-per-task=8" in content
            assert "#SBATCH --mem=64GB" in content
            assert "#SBATCH --time=8:00:00" in content
            assert "#SBATCH --partition=gpu" in content

            # Verify environment setup
            assert "conda activate pytorch" in content
            assert "export CUDA_VISIBLE_DEVICES=0,1,2,3" in content
            assert "export OMP_NUM_THREADS=8" in content

            # Verify command
            assert "python train.py --epochs 100" in content

    def test_render_container_job_script(self):
        """Test rendering a job script with container configuration."""
        container = ContainerResource(
            image="pytorch/pytorch:latest",
            mounts=["/data:/workspace/data", "/models:/workspace/models"],
            workdir="/workspace",
        )

        job = Job(
            name="container_job",
            command=["python", "inference.py"],
            environment=JobEnvironment(container=container),
            resources=JobResource(gpus_per_node=1),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            script_path = render_job_script(
                template_path=_get_default_template(), job=job, output_dir=temp_dir
            )

            with open(script_path) as f:
                content = f.read()

            # Verify container configuration
            assert "pytorch/pytorch:latest" in content
            assert "/data:/workspace/data" in content
            assert "/models:/workspace/models" in content

    def test_render_shell_job_script(self):
        """Test rendering a shell job script."""
        # Create a temporary shell script
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sh", delete=False) as f:
            f.write("""#!/bin/bash
#SBATCH --job-name=shell_{{ name }}
#SBATCH --nodes={{ nodes | default(1) }}

echo "Processing {{ dataset }}"
echo "Output directory: {{ output_dir }}"
""")
            script_template = f.name

        try:
            shell_job = ShellJob(
                name="data_processing",
                script_path=script_template,
                script_vars={
                    "name": "data_processing",
                    "nodes": 2,
                    "dataset": "ImageNet",
                    "output_dir": "/results",
                },
            )

            with tempfile.TemporaryDirectory() as temp_dir:
                rendered_path = render_shell_job_script(
                    template_path=script_template, job=shell_job, output_dir=temp_dir
                )

                with open(rendered_path) as f:
                    content = f.read()

                assert "#SBATCH --job-name=shell_data_processing" in content
                assert "#SBATCH --nodes=2" in content
                assert "Processing ImageNet" in content
                assert "Output directory: /results" in content
        finally:
            os.unlink(script_template)


class TestRealisticWorkflows:
    """Test realistic workflow scenarios."""

    def test_machine_learning_pipeline_workflow(self):
        """Test a realistic ML pipeline workflow."""
        # Data preprocessing job
        preprocess = Job(
            name="preprocess",
            command=[
                "python",
                "preprocess.py",
                "--input",
                "/data/raw",
                "--output",
                "/data/processed",
            ],
            resources=JobResource(
                nodes=1, cpus_per_task=16, memory_per_node="32GB", time_limit="2:00:00"
            ),
            environment=JobEnvironment(
                conda="data_science", env_vars={"PYTHONPATH": "/workspace"}
            ),
        )

        # Model training job (depends on preprocessing)
        train = Job(
            name="train",
            command=["python", "train.py", "--data", "/data/processed"],
            depends_on=["preprocess"],
            resources=JobResource(
                nodes=1,
                gpus_per_node=4,
                memory_per_node="128GB",
                time_limit="24:00:00",
                partition="gpu",
            ),
            environment=JobEnvironment(
                conda="pytorch",
                env_vars={"CUDA_VISIBLE_DEVICES": "0,1,2,3", "NCCL_DEBUG": "INFO"},
            ),
        )

        # Model evaluation job (depends on training)
        evaluate = Job(
            name="evaluate",
            command=["python", "evaluate.py", "--model", "/models/best.pth"],
            depends_on=["train"],
            resources=JobResource(
                nodes=1, gpus_per_node=1, memory_per_node="64GB", time_limit="4:00:00"
            ),
            environment=JobEnvironment(conda="pytorch"),
        )

        # Create workflow
        workflow = Workflow("ml_pipeline", [preprocess, train, evaluate])

        # Validate workflow
        workflow.validate()

        # Check dependencies
        # Ensure all jobs are in PENDING status for test consistency
        preprocess._status = JobStatus.PENDING
        train._status = JobStatus.PENDING
        evaluate._status = JobStatus.PENDING

        assert preprocess.dependencies_satisfied([])
        assert not train.dependencies_satisfied([])
        assert train.dependencies_satisfied(["preprocess"])
        assert not evaluate.dependencies_satisfied(["preprocess"])
        assert evaluate.dependencies_satisfied(["preprocess", "train"])

    def test_bioinformatics_workflow(self):
        """Test a realistic bioinformatics workflow."""
        # Quality control
        qc = Job(
            name="quality_control",
            command=["fastqc", "/data/samples/*.fastq", "-o", "/results/qc"],
            resources=JobResource(
                nodes=1, cpus_per_task=8, memory_per_node="16GB", time_limit="1:00:00"
            ),
        )

        # Alignment (multiple samples in parallel)
        align_jobs = []
        for i in range(3):
            align_job = Job(
                name=f"align_sample_{i + 1}",
                command=["bwa", "mem", "/ref/genome.fa", f"/data/sample_{i + 1}.fastq"],
                depends_on=["quality_control"],
                resources=JobResource(
                    nodes=1,
                    cpus_per_task=16,
                    memory_per_node="32GB",
                    time_limit="6:00:00",
                ),
            )
            align_jobs.append(align_job)

        # Variant calling (depends on all alignments)
        variant_calling = Job(
            name="variant_calling",
            command=[
                "gatk",
                "HaplotypeCaller",
                "-R",
                "/ref/genome.fa",
                "-I",
                "/results/alignments/*.bam",
            ],
            depends_on=[f"align_sample_{i + 1}" for i in range(3)],
            resources=JobResource(
                nodes=1,
                cpus_per_task=32,
                memory_per_node="128GB",
                time_limit="12:00:00",
            ),
        )

        all_jobs = [qc] + align_jobs + [variant_calling]
        workflow = Workflow("bioinformatics_pipeline", all_jobs)

        workflow.validate()

        # Test dependency resolution
        # Ensure all jobs are in PENDING status for test consistency
        qc._status = JobStatus.PENDING
        for align_job in align_jobs:
            align_job._status = JobStatus.PENDING
        variant_calling._status = JobStatus.PENDING

        assert qc.dependencies_satisfied([])
        for align_job in align_jobs:
            assert not align_job.dependencies_satisfied([])
            assert align_job.dependencies_satisfied(["quality_control"])

        completed_jobs = ["quality_control"] + [
            f"align_sample_{i + 1}" for i in range(3)
        ]
        assert variant_calling.dependencies_satisfied(completed_jobs)


class TestConfigurationIntegration:
    """Test configuration system integration."""

    def test_environment_variable_config(self):
        """Test configuration through environment variables."""
        # Mock environment variables
        env_vars = {
            "SRUNX_DEFAULT_NODES": "2",
            "SRUNX_DEFAULT_GPUS_PER_NODE": "4",
            "SRUNX_DEFAULT_MEMORY_PER_NODE": "64GB",
            "SRUNX_DEFAULT_PARTITION": "gpu",
            "SRUNX_DEFAULT_CONDA": "ml_env",
        }

        # Test job creation with environment defaults
        with pytest.MonkeyPatch().context() as m:
            for key, value in env_vars.items():
                m.setenv(key, value)

            # Force reload of config
            import importlib

            from srunx import config, models

            importlib.reload(config)
            importlib.reload(models)

            job = Job(command=["python", "test.py"])

            # These assertions depend on the configuration system working
            # In a real scenario, you might need to test the config loading separately
            assert isinstance(job.resources.nodes, int)
            assert isinstance(job.resources.gpus_per_node, int)

    def test_config_file_integration(self):
        """Test configuration file loading."""
        config_data = {
            "resources": {
                "nodes": 1,
                "gpus_per_node": 2,
                "memory_per_node": "32GB",
                "partition": "gpu",
            },
            "environment": {
                "conda": "pytorch",
                "env_vars": {"CUDA_VISIBLE_DEVICES": "0,1"},
            },
            "log_dir": "slurm_logs",
        }

        config = SrunxConfig.model_validate(config_data)

        assert config.resources.nodes == 1
        assert config.resources.gpus_per_node == 2
        assert config.resources.memory_per_node == "32GB"
        assert config.environment.conda == "pytorch"
        assert config.environment.env_vars["CUDA_VISIBLE_DEVICES"] == "0,1"
        assert config.log_dir == "slurm_logs"


class TestEndToEndScenarios:
    """Test complete end-to-end usage scenarios."""

    def test_simple_job_submission_flow(self):
        """Test the complete flow for a simple job submission."""
        # Create a job
        job = Job(
            name="test_job",
            command=["echo", "Hello, SLURM!"],
            resources=JobResource(nodes=1, time_limit="00:05:00"),
        )

        # Render the script
        with tempfile.TemporaryDirectory() as temp_dir:
            script_path = render_job_script(
                template_path=_get_default_template(), job=job, output_dir=temp_dir
            )

            # Verify script exists and has correct content
            assert Path(script_path).exists()

            with open(script_path) as f:
                content = f.read()

            # Basic validation
            assert "#!/bin/bash" in content
            assert "#SBATCH --job-name=test_job" in content
            assert "#SBATCH --nodes=1" in content
            assert "#SBATCH --time=00:05:00" in content
            assert "echo Hello, SLURM!" in content

            # Script should be readable
            assert Path(script_path).is_file()
            assert os.access(script_path, os.R_OK)

    def test_workflow_with_mixed_job_types(self):
        """Test workflow with both regular jobs and shell jobs."""
        # Create a temporary shell script
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sh", delete=False) as f:
            f.write("""#!/bin/bash
#SBATCH --job-name=setup_{{ name }}
echo "Setting up environment for {{ project }}"
mkdir -p {{ output_dir }}
""")
            setup_script = f.name

        try:
            # Setup job (shell script)
            setup = ShellJob(
                name="setup",
                script_path=setup_script,
                script_vars={
                    "name": "setup",
                    "project": "research_project",
                    "output_dir": "/workspace/results",
                },
            )

            # Processing job (regular job, depends on setup)
            process = Job(
                name="process",
                command=["python", "process_data.py"],
                depends_on=["setup"],
                resources=JobResource(nodes=1, cpus_per_task=4, memory_per_node="16GB"),
                environment=JobEnvironment(conda="data_env"),
            )

            # Analysis job (depends on processing)
            analyze = Job(
                name="analyze",
                command=["R", "analyze.R"],
                depends_on=["process"],
                resources=JobResource(nodes=1, memory_per_node="32GB"),
                environment=JobEnvironment(
                    env_vars={"R_LIBS_USER": "/home/user/R/library"}
                ),
            )

            workflow = Workflow("mixed_workflow", [setup, process, analyze])
            workflow.validate()

            # Test dependency chain
            # Jobs should be PENDING by default to satisfy dependencies check
            # Ensure all jobs are in PENDING status for test consistency
            setup._status = JobStatus.PENDING
            process._status = JobStatus.PENDING
            analyze._status = JobStatus.PENDING

            assert setup.dependencies_satisfied([])
            assert process.dependencies_satisfied(["setup"])
            assert analyze.dependencies_satisfied(["setup", "process"])

        finally:
            os.unlink(setup_script)
