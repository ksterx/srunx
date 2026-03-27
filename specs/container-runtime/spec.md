# Spec: Container Runtime Abstraction + Apptainer Support

## Overview
Introduce a pluggable container runtime abstraction that decouples srunx from NVIDIA Pyxis/Enroot, and add first-class Apptainer/Singularity support as a new runtime backend.

## Background
srunx currently supports only NVIDIA Pyxis containers. The entire container pipeline — from `ContainerResource` model fields, through `_build_environment_setup()`, to `advanced.slurm.jinja` — is hardcoded to Pyxis `--container-*` srun flags. Apptainer (formerly Singularity) is the most widely used container runtime in academic and government HPC environments, but uses a fundamentally different execution model (`apptainer exec [opts] image.sif command` vs Pyxis srun flags). Additionally, only `advanced.slurm.jinja` handles containers; the base, pytorch_ddp, horovod, and tensorflow_multiworker templates ignore container configuration entirely (a pre-existing bug).

Key pre-existing issues:
- `SRUNX_DEFAULT_CONTAINER` env var handling is broken (`config.py:168` raises `KeyError`)
- Job ID extraction uses fragile `split()[-1]` instead of `sbatch --parsable`
- Container is treated as mutually exclusive with conda/venv in `JobEnvironment`, but in practice users often run conda inside a container (especially with Apptainer `--bind` mounts)

## Requirements

### Must Have
- REQ-1: Container runtime abstraction — a thin backend interface that produces `prelude` (shell setup lines), `srun_args` (srun-specific flags), and `launch_prefix` (command wrapper) from a container config. These three outputs are distinct: `srun_args` are passed to `srun`, `launch_prefix` wraps the command, and `prelude` sets up shell state.
- REQ-2: Apptainer runtime backend supporting: `apptainer exec`, `--nv` (NVIDIA GPU), `--rocm` (AMD GPU), `--bind` mounts, `--overlay`, `--cleanenv`, `--fakeroot`, `--writable-tmpfs`, `--env` passthrough, SIF file paths
- REQ-3: Pyxis runtime backend — refactor existing Pyxis logic into the new abstraction. The generated script output for `advanced.slurm.jinja` + Pyxis config must be identical to the current implementation.
- REQ-4: Singularity as Apptainer alias — same backend, different binary name (`singularity` vs `apptainer`)
- REQ-5: Runtime selection via `ContainerResource.runtime` field with values `"pyxis"`, `"apptainer"`, `"singularity"`, defaulting to `"pyxis"` for backward compatibility
- REQ-6: Container support in `base` and `advanced` templates using the runtime-agnostic launch output. The `pytorch_ddp` template should also support containers as a Must Have.
- REQ-7: CLI `--container-runtime` option on `srunx submit` and `srunx template apply`
- REQ-8: YAML workflow support for runtime selection via `environment.container.runtime` field
- REQ-9: Config support — `SRUNX_DEFAULT_CONTAINER_RUNTIME` env var plus config file `container_runtime` field in `EnvironmentDefaults`. Runtime resolution order: explicit CLI flag > YAML field > env var > config file > `"pyxis"` fallback.
- REQ-10: Fix `SRUNX_DEFAULT_CONTAINER` env var bug in `config.py:168`
- REQ-11: Fix job ID extraction to use `sbatch --parsable` in `client.py:131`
- REQ-12: Container + conda/venv coexistence — remove mutual exclusivity constraint so users can use conda/venv alongside a container. Add `--no-container` CLI flag to explicitly suppress config-default container injection.
- REQ-13: Runtime-specific field validation — Apptainer-only fields (nv, rocm, cleanenv, fakeroot, writable_tmpfs, overlay) must raise `ValidationError` when used with `runtime="pyxis"`.
- REQ-14: CLI `--container` key=value parser accepts `bind=` as alias for `mounts=`, and `runtime=` key.

### Nice to Have
- REQ-N1: Container support in `horovod` and `tensorflow_multiworker` templates (requires launcher-specific integration: `horovodrun`, TF `%runscript`)
- REQ-N2: Auto-detection of container runtime from image extension (`.sif` → apptainer, `.sqsh` → pyxis)
- REQ-N3: `template apply` command full container support (currently missing even for Pyxis)

## Acceptance Criteria
- AC-1: Given a job with `runtime="apptainer"` and `image="/path/to/img.sif"`, when `render_job_script()` is called, then the generated script contains `apptainer exec` with correct flags
- AC-2: Given a job with `runtime="apptainer"` and `nv=true`, when rendered, then the script contains `--nv` flag
- AC-3: Given a job with `runtime="apptainer"` and `mounts=["/data:/workspace"]` (or `bind=["/data:/workspace"]` in CLI), when rendered, then the script contains `--bind /data:/workspace`
- AC-4: Given a job with `runtime="pyxis"` (or no runtime specified), when `render_job_script()` is called with `advanced.slurm.jinja`, then script output is identical to current implementation
- AC-5: Given `SRUNX_DEFAULT_CONTAINER_RUNTIME=apptainer` in environment, when a job is submitted without explicit runtime, then apptainer runtime is used
- AC-6: Given a workflow YAML with `environment.container.runtime: apptainer`, when parsed, then the job uses the apptainer runtime backend
- AC-7: Given `base`, `advanced`, or `pytorch_ddp` template, when a container is configured, then the command execution uses the runtime's launch output
- AC-8: Given `runtime="singularity"`, when rendered, then the script uses `singularity exec` instead of `apptainer exec` with otherwise identical behavior
- AC-9: Given `SRUNX_DEFAULT_CONTAINER=myimage.sif` in environment, when config is loaded, then no KeyError is raised and the container default is set correctly
- AC-10: When `sbatch` is called, then job ID is extracted using `--parsable` flag output
- AC-11: All existing tests maintain equivalent coverage (test code updates for `sbatch --parsable` mock changes are expected and permitted)
- AC-12: `uv run pytest && uv run mypy . && uv run ruff check .` passes
- AC-13: Given `runtime="pyxis"` and `nv=true`, when model is validated, then `ValidationError` is raised
- AC-14: Given `--conda ml_env --container img.sif`, when submitted, then both conda activation and container launch are present in the generated script (conda activates on host, container wraps the command)
- AC-15: Given config default container set and `--no-container` flag used, then no container is applied to the job

## Container + Environment Semantics
When container coexists with conda/venv:
- **Execution order**: Environment setup (conda activate / venv source) runs in the host shell **before** the command is launched. For Pyxis, these run before `srun` with container flags. For Apptainer, the activation happens in the host shell; the activated environment is available inside the container only if the relevant paths are bind-mounted.
- **`--cleanenv` responsibility**: When Apptainer `--cleanenv` is used, host environment variables (including those set by conda/venv activation) are stripped. Users must explicitly pass needed variables via `ContainerResource.env` or bind-mount the environment. This is documented as user responsibility, not enforced by srunx.

## Out of Scope
- Docker or Podman runtime support (future work)
- Shifter or Charliecloud runtime support
- Container image building or pulling
- SSH CLI container handling (raw script path upload, no template rendering)
- SLURM-native `--dependency` (separate feature)
- `--account`, `--qos`, `--reservation`, array jobs (separate features)
- Environment modules support (separate feature)
- Container-internal environment activation (running `conda activate` inside the container process)

## Constraints
- Backward compatible: existing Pyxis configs and workflows using `advanced.slurm.jinja` must produce identical script output
- No new Python dependencies (Apptainer is invoked via shell commands in generated scripts)
- `ContainerResource.runtime` defaults to `"pyxis"` for backward compatibility
- Follow existing code patterns: Pydantic models, Jinja2 templates, Typer CLI
- Python 3.12+ (project requirement)

## Resolved Questions
- Q1: Container + conda/venv coexistence → **Yes, remove mutual exclusivity.** Container is a launch strategy, not an environment. Add `--no-container` to suppress config defaults. (Promoted to REQ-12)
- Q2: Default runtime → **`"pyxis"`** for backward compatibility. Auto-detection is Nice to Have (REQ-N2).
- Q3: Runtime resolution order → **Explicit CLI > YAML > env var > config file > `"pyxis"` fallback.** (Added to REQ-9)
- Q4: horovod/tensorflow container support → **Nice to Have** (REQ-N1). These templates use non-srun launchers that require launcher-specific integration.
- Q5: Container + conda/venv semantics → **Host-side activation by default.** `--cleanenv` interaction is user responsibility. (Documented in new section above)
