# Plan: Container Runtime Abstraction + Apptainer Support

## Spec Reference
[specs/container-runtime/spec.md](spec.md) — REQ-1 through REQ-14, REQ-N1 through REQ-N3.

## Approach
Introduce a `ContainerRuntime` protocol with thin backend implementations (`PyxisRuntime`, `ApptainerRuntime`). Each backend produces a `LaunchSpec` dataclass with three distinct outputs: `prelude` (shell setup), `srun_args` (srun-specific flags), and `launch_prefix` (command wrapper). This three-way split correctly models the fundamental difference between Pyxis (srun flags) and Apptainer (command wrapping), and also supports templates that use non-srun launchers (horovod, pytorch_ddp).

### Trade-offs Considered

| Option | Pros | Cons |
|--------|------|------|
| **Chosen: Runtime protocol + 3-field LaunchSpec** | Thin abstraction, correctly models srun-flags vs command-wrap, templates stay simple, easy to add new runtimes | One new module + protocol to maintain |
| Alt A: Template-per-runtime (apptainer.slurm.jinja) | No abstraction needed | 5 templates x N runtimes = combinatorial explosion |
| Alt B: 2-field LaunchSpec (prelude + launch_prefix only) | Simpler | Cannot distinguish srun flags from command wrapping; breaks horovod/non-srun templates |
| Alt C: Generic `extra_sbatch_args` passthrough | Maximum flexibility | No validation, error-prone, poor UX |

## Architecture

### Components

| Component | File Path | Responsibility |
|-----------|-----------|----------------|
| `ContainerRuntime` protocol | `src/srunx/containers/base.py` | Defines `build_launch_spec(config) -> LaunchSpec` interface |
| `LaunchSpec` dataclass | `src/srunx/containers/base.py` | Holds `prelude`, `srun_args`, `launch_prefix` (3 fields) |
| `PyxisRuntime` | `src/srunx/containers/pyxis.py` | Generates Pyxis `--container-*` as `srun_args` |
| `ApptainerRuntime` | `src/srunx/containers/apptainer.py` | Generates `apptainer exec ...` as `launch_prefix` |
| `get_runtime()` factory | `src/srunx/containers/__init__.py` | Returns runtime backend by name |
| `ContainerResource` (updated) | `src/srunx/models.py` | Add `runtime` field + Apptainer options + runtime-specific validator |
| `_build_environment_setup()` (updated) | `src/srunx/models.py` | Delegates container logic to runtime backend; handles container independently from conda/venv |
| `render_job_script()` (updated) | `src/srunx/models.py` | Pass `srun_args` and `launch_prefix` as new template variables |
| Templates (base, advanced, pytorch_ddp) | `src/srunx/templates/*.jinja` | Use `srun_args` + `launch_prefix` uniformly |
| CLI submit (updated) | `src/srunx/cli/main.py` | Add `--container-runtime`, `--no-container`; extend parser for `bind=`/`runtime=` |
| CLI template_apply (updated) | `src/srunx/cli/main.py` | Add `--container`, `--container-runtime` options |
| Config (updated) | `src/srunx/config.py` | Fix `SRUNX_DEFAULT_CONTAINER` bug + add `container_runtime` default |
| Client (updated) | `src/srunx/client.py` | Use `sbatch --parsable` for job ID |

### Interfaces

```python
# src/srunx/containers/base.py

from dataclasses import dataclass
from typing import Protocol

@dataclass(frozen=True)
class LaunchSpec:
    """Runtime-agnostic container launch specification.

    Three distinct outputs model different injection points:
    - prelude: Shell setup lines executed before the command (e.g., declare arrays)
    - srun_args: Flags passed to srun itself (Pyxis uses this)
    - launch_prefix: Command wrapper prepended to the user command (Apptainer uses this)
    """
    prelude: str = ""        # Shell setup lines (env vars, array declarations)
    srun_args: str = ""      # srun-specific flags (e.g., "${CONTAINER_ARGS[@]}")
    launch_prefix: str = ""  # Command wrapper (e.g., "apptainer exec --nv img.sif")

class ContainerRuntime(Protocol):
    def build_launch_spec(self, config: "ContainerResource") -> LaunchSpec: ...
```

```python
# Updated ContainerResource in models.py

class ContainerResource(BaseModel):
    runtime: Literal["pyxis", "apptainer", "singularity"] = "pyxis"
    image: str | None = None
    mounts: list[str] = []
    workdir: str | None = None
    # Apptainer-specific (validated: error if used with runtime="pyxis")
    nv: bool = False              # --nv (NVIDIA GPU passthrough)
    rocm: bool = False            # --rocm (AMD GPU passthrough)
    cleanenv: bool = False        # --cleanenv
    fakeroot: bool = False        # --fakeroot
    writable_tmpfs: bool = False  # --writable-tmpfs
    overlay: str | None = None    # --overlay path
    env: dict[str, str] = {}      # --env KEY=VAL

    @model_validator(mode="after")
    def validate_runtime_fields(self) -> Self:
        """Ensure Apptainer-only fields are not set for Pyxis runtime."""
        if self.runtime == "pyxis":
            apptainer_fields = {
                "nv": self.nv, "rocm": self.rocm, "cleanenv": self.cleanenv,
                "fakeroot": self.fakeroot, "writable_tmpfs": self.writable_tmpfs,
                "overlay": self.overlay, "env": self.env,
            }
            set_fields = [k for k, v in apptainer_fields.items()
                          if v and v != {} and v is not False]
            if set_fields:
                raise ValueError(
                    f"Fields {set_fields} are only valid for apptainer/singularity runtime, "
                    f"not '{self.runtime}'"
                )
        return self
```

### Data Flow

```
CLI --container / YAML environment.container
    |
    v
ContainerResource (Pydantic model with runtime field)
    |
    v
_build_environment_setup()
    |  1. Generate conda/venv activation lines (if any)
    |  2. Call get_runtime(config.runtime)
    |  3. runtime.build_launch_spec(config) -> LaunchSpec
    v
render_job_script() receives:
    - environment_setup: conda/venv lines + LaunchSpec.prelude
    - srun_args: LaunchSpec.srun_args
    - launch_prefix: LaunchSpec.launch_prefix
    |
    v
Template renders (example for advanced/base):
    {{ environment_setup }}
    srun {{ srun_args }} {{ launch_prefix }} {{ command }}

Template renders (example for pytorch_ddp, no srun):
    {{ environment_setup }}
    {{ launch_prefix }} {{ command }}
```

**PyxisRuntime output:**
```python
LaunchSpec(
    prelude='declare -a CONTAINER_ARGS=(\n--container-image nvcr.io/nvidia/pytorch:24.01-py3\n--container-mounts /data:/data\n)',
    srun_args='"${CONTAINER_ARGS[@]}"',
    launch_prefix='',
)
```

**ApptainerRuntime output:**
```python
LaunchSpec(
    prelude='',
    srun_args='',
    launch_prefix='apptainer exec --nv --bind /data:/data /path/to/image.sif',
)
```

### Template Changes

**`advanced.slurm.jinja` and `base.slurm.jinja`** — use srun:
```jinja
# Execute command
{% if srun_args or launch_prefix -%}
srun {{ srun_args }} {{ launch_prefix }} {{ command }}
{% else -%}
srun {{ command }}
{% endif -%}
```

**`pytorch_ddp.slurm.jinja`** — no srun, direct command execution:
```jinja
# Run the command
{% if launch_prefix -%}
{{ launch_prefix }} {{ command }}
{% else -%}
{{ command }}
{% endif -%}
```

Note: `srun_args` is not used in `pytorch_ddp` because this template doesn't use `srun`. For Pyxis + pytorch_ddp, the `CONTAINER_ARGS` array is declared in `prelude` (via `environment_setup`) but the srun-style flags are not applicable. This is a known limitation documented in REQ-N1 scope. For Apptainer + pytorch_ddp, `launch_prefix` wraps the command correctly.

### JobEnvironment Changes

Remove container from the mutual exclusivity validator (REQ-12). Container is now orthogonal to conda/venv:

```python
@model_validator(mode="after")
def validate_environment(self) -> Self:
    envs = [self.conda, self.venv]  # container removed — it's a launch strategy
    non_none_count = sum(x is not None for x in envs)
    if non_none_count > 1:
        raise ValueError("Only one of conda or venv can be specified")
    return self
```

**Config default injection guard**: When the mutual exclusivity constraint is removed, a config-default container could be silently injected into conda/venv-only jobs. To prevent this:
- Add `--no-container` CLI flag that explicitly sets `container=None`, overriding config defaults
- The `_default_container()` factory function already returns config-based defaults; no change needed there. The `--no-container` flag is the user escape hatch.

**`_build_environment_setup()` restructure**: Change from `if/elif/elif` (mutually exclusive) to sequential:
```python
def _build_environment_setup(environment: JobEnvironment) -> tuple[str, str, str]:
    """Returns (env_setup_lines, srun_args, launch_prefix)."""
    setup_lines = []

    # 1. Environment variables
    for key, value in environment.env_vars.items():
        setup_lines.append(f"export {key}={value}")

    # 2. Conda/venv activation (independent of container)
    if environment.conda:
        setup_lines.extend([...])  # conda activate
    elif environment.venv:
        setup_lines.append(f"source {environment.venv}/bin/activate")

    # 3. Container setup (independent of conda/venv)
    srun_args = ""
    launch_prefix = ""
    if environment.container:
        runtime = get_runtime(environment.container.runtime)
        spec = runtime.build_launch_spec(environment.container)
        if spec.prelude:
            setup_lines.append(spec.prelude)
        srun_args = spec.srun_args
        launch_prefix = spec.launch_prefix

    return "\n".join(setup_lines), srun_args, launch_prefix
```

### CLI Parser Changes

**`_parse_container_args()` updates:**
- Accept `runtime=` key in key=value format
- Accept `bind=` as alias for `mounts=`
- Accept Apptainer-specific keys: `nv=`, `rocm=`, `cleanenv=`, `fakeroot=`, `writable_tmpfs=`, `overlay=`, `env=`

**New CLI options on `submit`:**
- `--container-runtime`: Explicit runtime selection (overrides key=value `runtime=`)
- `--no-container`: Suppress config-default container

**New CLI options on `template_apply`:**
- `--container`: Container image or config string
- `--container-runtime`: Runtime selection

### Config Changes

**`load_config_from_env()` fix (REQ-10):**
```python
# Before (broken):
environment["container"]["image"] = container

# After (fixed):
environment["container"] = {"image": container}
```

**New env var `SRUNX_DEFAULT_CONTAINER_RUNTIME` (REQ-9):**
```python
if container_runtime := os.getenv("SRUNX_DEFAULT_CONTAINER_RUNTIME"):
    environment.setdefault("container", {})
    environment["container"]["runtime"] = container_runtime
```

**`EnvironmentDefaults` model** — no separate `container_runtime` field needed. The runtime is stored within `ContainerResource.runtime`. Config resolution uses the existing nested merge.

### Client Changes

**`sbatch --parsable` (REQ-11):**
```python
# Before:
sbatch_cmd = ["sbatch", script_path]
job_id = int(result.stdout.split()[-1])

# After:
sbatch_cmd = ["sbatch", "--parsable", script_path]
job_id = int(result.stdout.strip().split(";")[0])  # --parsable outputs "job_id" or "job_id;cluster_name"
```

Existing tests mock `subprocess.run` with `stdout="Submitted batch job 12345"`. These mocks must be updated to return `"12345\n"` (parsable format). This is an expected test change per AC-11.

## Integration Points

- **`render_job_script()`**: Returns `srun_args` and `launch_prefix` as new template variables alongside `environment_setup`
- **`WorkflowRunner.parse_job()`**: No changes needed — Pydantic model validation handles the new `runtime` field via `ContainerResource`
- **Existing Pyxis tests**: Test code updates needed for `sbatch --parsable` mock format. Behavioral coverage unchanged.
- **`template_apply` command**: Add `--container` and `--container-runtime` options, reuse `_parse_container_args()`.

## Dependencies

### Internal
- `srunx.models` — ContainerResource, JobEnvironment, _build_environment_setup, render_job_script
- `srunx.cli.main` — _parse_container_args, submit, template_apply
- `srunx.config` — EnvironmentDefaults, load_config_from_env
- `srunx.client` — Slurm.submit

### External
- None (Apptainer invoked via shell commands in generated scripts)

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Breaking existing Pyxis workflows (advanced template) | High | Default `runtime="pyxis"`, AC-4 verifies identical output |
| Config-default container injected into non-container jobs | High | `--no-container` CLI flag, clear config docs |
| `sbatch --parsable` test mock updates | Med | Expected change, documented in AC-11 |
| Pyxis + non-srun templates (pytorch_ddp) edge case | Med | Document as known limitation; Pyxis srun_args are ignored in non-srun templates |
| Apptainer CLI flag differences across versions | Low | Target Apptainer 1.x+ (current stable), document minimum version |
| `--cleanenv` + conda/venv confusion | Low | Documented in spec "Container + Environment Semantics" section |

## Testing Strategy

- **Unit**: Test each runtime backend (`PyxisRuntime`, `ApptainerRuntime`) produces correct 3-field `LaunchSpec` for various configs
- **Unit**: Test `ContainerResource` runtime-specific field validation (Apptainer fields + pyxis runtime → error)
- **Unit**: Test `_parse_container_args()` with `runtime=`, `bind=` alias, Apptainer options
- **Unit**: Test `--no-container` flag suppresses config defaults
- **Integration**: Test `render_job_script()` with `advanced` + Pyxis → identical to current output
- **Integration**: Test `render_job_script()` with `advanced` + Apptainer → contains `apptainer exec`
- **Integration**: Test `render_job_script()` with `base` + Apptainer → contains `apptainer exec`
- **Integration**: Test `render_job_script()` with `pytorch_ddp` + Apptainer → `launch_prefix` wraps command
- **Integration**: Test config loading with `SRUNX_DEFAULT_CONTAINER` (bug fix) and `SRUNX_DEFAULT_CONTAINER_RUNTIME`
- **Integration**: Test `sbatch --parsable` job ID extraction
- **Regression**: All existing tests updated for `--parsable` mock format, behavioral coverage unchanged
