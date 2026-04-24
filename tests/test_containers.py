"""Tests for srunx.containers module (T6.1).

Unit tests for PyxisRuntime and ApptainerRuntime,
verifying correct LaunchSpec output for various configurations.
"""

from srunx.containers import ApptainerRuntime, PyxisRuntime, get_runtime
from srunx.containers.base import LaunchSpec
from srunx.domain import ContainerResource


class TestPyxisRuntime:
    """Test PyxisRuntime produces correct LaunchSpec."""

    def test_pyxis_image_only(self):
        """Test PyxisRuntime with image only."""
        config = ContainerResource.model_validate(
            {"runtime": "pyxis", "image": "pytorch/pytorch:latest"}
        )
        runtime = PyxisRuntime()
        spec = runtime.build_launch_spec(config)

        assert isinstance(spec, LaunchSpec)
        assert "--container-image pytorch/pytorch:latest" in spec.prelude
        assert "CONTAINER_ARGS" in spec.prelude
        assert spec.srun_args == '"${CONTAINER_ARGS[@]}"'
        assert spec.launch_prefix == ""

    def test_pyxis_with_mounts(self):
        """Test PyxisRuntime with mounts."""
        config = ContainerResource.model_validate(
            {
                "runtime": "pyxis",
                "image": "pytorch/pytorch:latest",
                "mounts": ["/data:/workspace/data", "/models:/workspace/models"],
            }
        )
        runtime = PyxisRuntime()
        spec = runtime.build_launch_spec(config)

        assert "--container-image pytorch/pytorch:latest" in spec.prelude
        assert (
            "--container-mounts /data:/workspace/data,/models:/workspace/models"
            in spec.prelude
        )

    def test_pyxis_with_workdir(self):
        """Test PyxisRuntime with workdir."""
        config = ContainerResource.model_validate(
            {
                "runtime": "pyxis",
                "image": "pytorch/pytorch:latest",
                "workdir": "/workspace",
            }
        )
        runtime = PyxisRuntime()
        spec = runtime.build_launch_spec(config)

        assert "--container-workdir /workspace" in spec.prelude

    def test_pyxis_with_all_options(self):
        """Test PyxisRuntime with image, mounts, and workdir."""
        config = ContainerResource.model_validate(
            {
                "runtime": "pyxis",
                "image": "nvcr.io/nvidia/pytorch:24.01-py3",
                "mounts": ["/data:/data"],
                "workdir": "/workspace",
            }
        )
        runtime = PyxisRuntime()
        spec = runtime.build_launch_spec(config)

        assert "--container-image nvcr.io/nvidia/pytorch:24.01-py3" in spec.prelude
        assert "--container-mounts /data:/data" in spec.prelude
        assert "--container-workdir /workspace" in spec.prelude
        assert spec.srun_args == '"${CONTAINER_ARGS[@]}"'
        assert spec.launch_prefix == ""

    def test_pyxis_no_image(self):
        """Test PyxisRuntime with no image (edge case)."""
        config = ContainerResource.model_validate({"runtime": "pyxis"})
        runtime = PyxisRuntime()
        spec = runtime.build_launch_spec(config)

        assert "CONTAINER_ARGS" in spec.prelude
        assert "--container-image" not in spec.prelude
        assert spec.launch_prefix == ""

    def test_pyxis_prelude_is_declare_array(self):
        """Test that PyxisRuntime prelude uses declare -a CONTAINER_ARGS."""
        config = ContainerResource.model_validate(
            {"runtime": "pyxis", "image": "test:latest"}
        )
        runtime = PyxisRuntime()
        spec = runtime.build_launch_spec(config)

        lines = spec.prelude.split("\n")
        assert lines[0] == "declare -a CONTAINER_ARGS=("
        assert lines[-1] == ")"


class TestApptainerRuntime:
    """Test ApptainerRuntime produces correct LaunchSpec."""

    def test_apptainer_image_only(self):
        """Test ApptainerRuntime with image only."""
        config = ContainerResource.model_validate(
            {"runtime": "apptainer", "image": "my_container.sif"}
        )
        runtime = ApptainerRuntime(binary="apptainer")
        spec = runtime.build_launch_spec(config)

        assert isinstance(spec, LaunchSpec)
        assert spec.prelude == ""
        assert spec.srun_args == ""
        assert spec.launch_prefix == "apptainer exec my_container.sif"

    def test_apptainer_nv_flag(self):
        """Test ApptainerRuntime with --nv flag (AC-2)."""
        config = ContainerResource.model_validate(
            {"runtime": "apptainer", "image": "test.sif", "nv": True}
        )
        runtime = ApptainerRuntime(binary="apptainer")
        spec = runtime.build_launch_spec(config)

        assert "--nv" in spec.launch_prefix
        assert spec.launch_prefix.startswith("apptainer exec --nv")

    def test_apptainer_rocm_flag(self):
        """Test ApptainerRuntime with --rocm flag."""
        config = ContainerResource.model_validate(
            {"runtime": "apptainer", "image": "test.sif", "rocm": True}
        )
        runtime = ApptainerRuntime(binary="apptainer")
        spec = runtime.build_launch_spec(config)

        assert "--rocm" in spec.launch_prefix

    def test_apptainer_bind_mounts(self):
        """Test ApptainerRuntime with --bind mounts (AC-3)."""
        config = ContainerResource.model_validate(
            {
                "runtime": "apptainer",
                "image": "test.sif",
                "mounts": ["/data:/data", "/scratch:/scratch"],
            }
        )
        runtime = ApptainerRuntime(binary="apptainer")
        spec = runtime.build_launch_spec(config)

        assert "--bind /data:/data" in spec.launch_prefix
        assert "--bind /scratch:/scratch" in spec.launch_prefix

    def test_apptainer_overlay(self):
        """Test ApptainerRuntime with --overlay."""
        config = ContainerResource.model_validate(
            {
                "runtime": "apptainer",
                "image": "test.sif",
                "overlay": "/path/to/overlay.img",
            }
        )
        runtime = ApptainerRuntime(binary="apptainer")
        spec = runtime.build_launch_spec(config)

        assert "--overlay /path/to/overlay.img" in spec.launch_prefix

    def test_apptainer_cleanenv(self):
        """Test ApptainerRuntime with --cleanenv."""
        config = ContainerResource.model_validate(
            {"runtime": "apptainer", "image": "test.sif", "cleanenv": True}
        )
        runtime = ApptainerRuntime(binary="apptainer")
        spec = runtime.build_launch_spec(config)

        assert "--cleanenv" in spec.launch_prefix

    def test_apptainer_fakeroot(self):
        """Test ApptainerRuntime with --fakeroot."""
        config = ContainerResource.model_validate(
            {"runtime": "apptainer", "image": "test.sif", "fakeroot": True}
        )
        runtime = ApptainerRuntime(binary="apptainer")
        spec = runtime.build_launch_spec(config)

        assert "--fakeroot" in spec.launch_prefix

    def test_apptainer_writable_tmpfs(self):
        """Test ApptainerRuntime with --writable-tmpfs."""
        config = ContainerResource.model_validate(
            {"runtime": "apptainer", "image": "test.sif", "writable_tmpfs": True}
        )
        runtime = ApptainerRuntime(binary="apptainer")
        spec = runtime.build_launch_spec(config)

        assert "--writable-tmpfs" in spec.launch_prefix

    def test_apptainer_env_vars(self):
        """Test ApptainerRuntime with --env flags."""
        config = ContainerResource.model_validate(
            {
                "runtime": "apptainer",
                "image": "test.sif",
                "env": {"CUDA_VISIBLE_DEVICES": "0", "OMP_NUM_THREADS": "4"},
            }
        )
        runtime = ApptainerRuntime(binary="apptainer")
        spec = runtime.build_launch_spec(config)

        assert "--env CUDA_VISIBLE_DEVICES=0" in spec.launch_prefix
        assert "--env OMP_NUM_THREADS=4" in spec.launch_prefix

    def test_apptainer_workdir_uses_pwd(self):
        """Test ApptainerRuntime uses --pwd for workdir."""
        config = ContainerResource.model_validate(
            {
                "runtime": "apptainer",
                "image": "test.sif",
                "workdir": "/workspace",
            }
        )
        runtime = ApptainerRuntime(binary="apptainer")
        spec = runtime.build_launch_spec(config)

        assert "--pwd /workspace" in spec.launch_prefix

    def test_apptainer_all_options(self):
        """Test ApptainerRuntime with all options combined."""
        config = ContainerResource.model_validate(
            {
                "runtime": "apptainer",
                "image": "test.sif",
                "nv": True,
                "rocm": True,
                "cleanenv": True,
                "fakeroot": True,
                "writable_tmpfs": True,
                "overlay": "/overlay.img",
                "mounts": ["/data:/data"],
                "env": {"KEY": "VAL"},
                "workdir": "/workspace",
            }
        )
        runtime = ApptainerRuntime(binary="apptainer")
        spec = runtime.build_launch_spec(config)

        assert spec.prelude == ""
        assert spec.srun_args == ""
        prefix = spec.launch_prefix
        assert prefix.startswith("apptainer exec")
        assert "--nv" in prefix
        assert "--rocm" in prefix
        assert "--cleanenv" in prefix
        assert "--fakeroot" in prefix
        assert "--writable-tmpfs" in prefix
        assert "--overlay /overlay.img" in prefix
        assert "--bind /data:/data" in prefix
        assert "--env KEY=VAL" in prefix
        assert "--pwd /workspace" in prefix
        assert prefix.endswith("test.sif")

    def test_apptainer_no_image(self):
        """Test ApptainerRuntime with no image (edge case)."""
        config = ContainerResource.model_validate({"runtime": "apptainer"})
        runtime = ApptainerRuntime(binary="apptainer")
        spec = runtime.build_launch_spec(config)

        assert spec.launch_prefix == "apptainer exec"
        assert spec.prelude == ""
        assert spec.srun_args == ""

    def test_singularity_binary_name(self):
        """Test ApptainerRuntime with singularity binary (AC-8)."""
        config = ContainerResource.model_validate(
            {"runtime": "singularity", "image": "test.sif", "nv": True}
        )
        runtime = ApptainerRuntime(binary="singularity")
        spec = runtime.build_launch_spec(config)

        assert spec.launch_prefix.startswith("singularity exec")
        assert "--nv" in spec.launch_prefix
        assert "test.sif" in spec.launch_prefix


class TestGetRuntime:
    """Test get_runtime() factory function."""

    def test_get_pyxis_runtime(self):
        """Test getting PyxisRuntime."""
        runtime = get_runtime("pyxis")
        assert isinstance(runtime, PyxisRuntime)

    def test_get_apptainer_runtime(self):
        """Test getting ApptainerRuntime with apptainer binary."""
        runtime = get_runtime("apptainer")
        assert isinstance(runtime, ApptainerRuntime)
        assert runtime.binary == "apptainer"

    def test_get_singularity_runtime(self):
        """Test getting ApptainerRuntime with singularity binary."""
        runtime = get_runtime("singularity")
        assert isinstance(runtime, ApptainerRuntime)
        assert runtime.binary == "singularity"

    def test_get_unknown_runtime_raises(self):
        """Test unknown runtime raises ValueError."""
        import pytest

        with pytest.raises(ValueError, match="Unknown container runtime"):
            get_runtime("docker")
