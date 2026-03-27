# Tasks: Container Runtime Abstraction + Apptainer Support

## Prerequisites
- [x] Spec approved: `specs/container-runtime/spec.md`
- [x] Plan approved: `specs/container-runtime/plan.md`

## Scope
- **In scope (Must Have):** REQ-1 through REQ-14
- **Out of scope (Nice to Have, future work):** REQ-N1 (horovod/tensorflow container support), REQ-N2 (auto-detection from extension), REQ-N3 (template_apply full container support beyond basic --container/--container-runtime)

## Phase 0: Baseline Capture
- [ ] T0.1: Capture current `advanced.slurm.jinja` + Pyxis output as baseline string for AC-4 regression test
      Files: `tests/test_integration.py`
      Note: Run existing `test_render_container_job_script` and record exact output before any changes

## Phase 1: Foundation — Container Runtime Abstraction
- [ ] T1.1: Create `src/srunx/containers/base.py` — `LaunchSpec` dataclass (prelude, srun_args, launch_prefix) and `ContainerRuntime` protocol (REQ-1)
      Files: `src/srunx/containers/base.py`
- [ ] T1.2: Create `src/srunx/containers/pyxis.py` — `PyxisRuntime` that generates existing Pyxis `--container-*` flags as `srun_args` (REQ-3)
      Files: `src/srunx/containers/pyxis.py`
- [ ] T1.3: Create `src/srunx/containers/apptainer.py` — `ApptainerRuntime` that generates `apptainer exec` / `singularity exec` as `launch_prefix` (REQ-2, REQ-4)
      Files: `src/srunx/containers/apptainer.py`
- [ ] T1.4: Create `src/srunx/containers/__init__.py` — `get_runtime()` factory function (REQ-1)
      Files: `src/srunx/containers/__init__.py`

## Phase 2: Model Updates
- [ ] T2.1: Update `ContainerResource` model — add `runtime` field, Apptainer-specific fields (nv, rocm, cleanenv, fakeroot, writable_tmpfs, overlay, env), and runtime-specific field validator (REQ-5, REQ-13)
      Files: `src/srunx/models.py`
      Depends: T1.1
- [ ] T2.2: Update `JobEnvironment` validator — remove container from mutual exclusivity constraint, allow container + conda/venv coexistence (REQ-12)
      Files: `src/srunx/models.py`
- [ ] T2.3: Refactor `_build_environment_setup()` — change from if/elif/elif to sequential (conda/venv then container), delegate container to runtime backend, return tuple of (env_setup, srun_args, launch_prefix) (REQ-1)
      Files: `src/srunx/models.py`
      Depends: T1.4, T2.1, T2.2
      Note: Do not mutate shared config default objects; always copy before modifying.
- [ ] T2.4: Update `render_job_script()` — pass `srun_args` and `launch_prefix` as new template variables (REQ-1)
      Files: `src/srunx/models.py`
      Depends: T2.3

## Phase 3: Template Updates
- [ ] T3.1: Update `advanced.slurm.jinja` — use `srun {{ srun_args }} {{ launch_prefix }} {{ command }}` pattern (REQ-6)
      Files: `src/srunx/templates/advanced.slurm.jinja`
      Depends: T2.4
- [ ] T3.2: Update `base.slurm.jinja` — add container-aware command execution (REQ-6)
      Files: `src/srunx/templates/base.slurm.jinja`
      Depends: T2.4
- [ ] T3.3: Update `pytorch_ddp.slurm.jinja` — add container support using `launch_prefix` only (no srun_args) (REQ-6)
      Files: `src/srunx/templates/pytorch_ddp.slurm.jinja`
      Depends: T2.4
      Note: Pyxis + pytorch_ddp is a known limitation (Pyxis requires srun flags, but this template doesn't use srun). Container support here is Apptainer-effective only. This is not a regression — current implementation also ignores containers in this template.

## Phase 4: CLI + Config Updates
- [ ] T4.1: Update `_parse_container_args()` — accept `runtime=`, `bind=` (alias for mounts), Apptainer options (nv, rocm, cleanenv, fakeroot, writable_tmpfs, overlay, env) (REQ-14)
      Files: `src/srunx/cli/main.py`
      Depends: T2.1
- [ ] T4.2: Add `--container-runtime` and `--no-container` CLI options to `submit` command. Implement runtime merge logic: when `--container-runtime` is specified without `--container`, override the runtime field of the config-default container (if any). Priority: explicit CLI > YAML > env var > config > "pyxis" fallback (REQ-7, REQ-9, REQ-12)
      Files: `src/srunx/cli/main.py`
      Depends: T4.1
- [ ] T4.3: Add `--container`, `--container-runtime`, and `--no-container` options to `template_apply` command. Reuse `_parse_container_args()` for parsing. (REQ-7, REQ-12)
      Files: `src/srunx/cli/main.py`
      Depends: T4.1
- [ ] T4.4: Fix `SRUNX_DEFAULT_CONTAINER` env var bug (`environment["container"] = {"image": container}`) + add `SRUNX_DEFAULT_CONTAINER_RUNTIME` support in `load_config_from_env()`. Also update `create_example_config()` to include container runtime example. (REQ-10, REQ-9)
      Files: `src/srunx/config.py`

## Phase 5: Client Fix
- [ ] T5.1: Update `Slurm.submit()` to use `sbatch --parsable` and parse job ID with `stdout.strip().split(";")[0]` (REQ-11)
      Files: `src/srunx/client.py`

## Phase 6: Tests
- [ ] T6.1: Unit tests for `PyxisRuntime` and `ApptainerRuntime` — verify 3-field LaunchSpec output for various configs, including singularity binary name (AC-8)
      Files: `tests/test_containers.py`
      Depends: T1.2, T1.3
- [ ] T6.2: Unit tests for updated `ContainerResource` — runtime-specific validation (pyxis + nv=true → error), default runtime, Apptainer fields
      Files: `tests/test_models.py`
      Depends: T2.1
- [ ] T6.3: Unit tests for CLI — `_parse_container_args()` with runtime=, bind= alias, Apptainer options. Also test `template_apply` with `--container` and `--container-runtime` flags.
      Files: `tests/test_cli.py` or `tests/test_models.py`
      Depends: T4.1, T4.3
- [ ] T6.4: Integration tests for `render_job_script()` — advanced+Pyxis identical to T0.1 baseline (AC-4), advanced+Apptainer, base+Apptainer, pytorch_ddp+Apptainer, conda+container coexistence (AC-14)
      Files: `tests/test_integration.py`
      Depends: T0.1, T3.1, T3.2, T3.3
- [ ] T6.5: Update existing test mocks for `sbatch --parsable` format (stdout="12345\n" instead of "Submitted batch job 12345")
      Files: `tests/test_client.py`, `tests/conftest.py`
      Depends: T5.1
- [ ] T6.6: Integration tests for config loading — `SRUNX_DEFAULT_CONTAINER` fix (AC-9), `SRUNX_DEFAULT_CONTAINER_RUNTIME` (AC-5)
      Files: `tests/test_config.py` or `tests/test_integration.py`
      Depends: T4.4
- [ ] T6.7: Test `--no-container` flag suppresses config defaults on both `submit` and `template_apply` (AC-15)
      Files: `tests/test_cli.py` or `tests/test_integration.py`
      Depends: T4.2, T4.3
- [ ] T6.8: Test YAML workflow parsing with `environment.container.runtime: apptainer` via `WorkflowRunner.parse_job()` (AC-6)
      Files: `tests/test_runner.py`
      Depends: T2.1
- [ ] T6.9: Test runtime merge logic — `--container-runtime apptainer` without `--container` overrides config-default container runtime (AC-5, REQ-9)
      Files: `tests/test_cli.py`
      Depends: T4.2

## Phase 7: Verification
- [ ] T7.1: Run `uv run pytest && uv run mypy . && uv run ruff check .` — all pass (AC-12)
- [ ] T7.2: Verify AC-4 — advanced+Pyxis output from T6.4 matches T0.1 baseline exactly

## Verification Checklist
### Acceptance Criteria
- [ ] AC-1: Apptainer exec in rendered script (T6.4)
- [ ] AC-2: --nv flag in rendered script (T6.1, T6.4)
- [ ] AC-3: --bind mount in rendered script (T6.1, T6.4)
- [ ] AC-4: Pyxis output identical to current (T0.1, T6.4, T7.2)
- [ ] AC-5: SRUNX_DEFAULT_CONTAINER_RUNTIME honored (T6.6, T6.9)
- [ ] AC-6: YAML runtime field parsed via WorkflowRunner.parse_job() (T6.8)
- [ ] AC-7: base/advanced/pytorch_ddp container support (T6.4)
- [ ] AC-8: singularity exec binary name (T6.1)
- [ ] AC-9: SRUNX_DEFAULT_CONTAINER no KeyError (T6.6)
- [ ] AC-10: sbatch --parsable job ID (T6.5)
- [ ] AC-11: All tests pass with equivalent coverage (T7.1)
- [ ] AC-12: pytest + mypy + ruff pass (T7.1)
- [ ] AC-13: Pyxis + nv=true raises ValidationError (T6.2)
- [ ] AC-14: conda + container coexistence (T6.4)
- [ ] AC-15: --no-container suppresses defaults (T6.7)

### Quality Gates
- [ ] All tests pass
- [ ] Lint/Type-check pass
- [ ] No security vulnerabilities
- [ ] Backward compatibility verified (AC-4)
