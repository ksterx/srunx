# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Package Management
- `uv sync` - Install dependencies
- `uv add <package>` - Add new dependency
- `uv run <command>` - Run commands in virtual environment

### CLI Usage
- `uv run srunx submit <command>` - Submit SLURM job
- `uv run srunx status <job_id>` - Check job status
- `uv run srunx list` - List jobs
- `uv run srunx cancel <job_id>` - Cancel job
- `uv run srunx flow run <yaml_file>` - Execute workflow from YAML
- `uv run srunx flow validate <yaml_file>` - Validate workflow YAML

### Testing
- `uv run pytest` - Run all tests
- `uv run pytest --cov=srunx` - Run tests with coverage
- `uv run pytest tests/test_models.py` - Run specific test file
- `uv run pytest -v` - Run tests with verbose output

### Direct Usage Examples
- `uv run srunx submit python train.py --name ml_job --gpus-per-node 1`
- `uv run srunx submit python process.py --conda ml_env --nodes 2`
- `uv run srunx flow run workflow.yaml`

## Architecture Overview

### Current Modular Structure
```
src/srunx/
├── models.py          # Data models and validation
├── client.py          # SLURM client for job operations
├── runner.py          # Workflow execution engine
├── callbacks.py       # Callback system for job notifications
├── exceptions.py      # Custom exceptions
├── logging.py         # Centralized logging configuration
├── utils.py           # Utility functions
├── cli/               # Command-line interfaces
│   ├── main.py        # Main CLI commands
│   └── workflow.py    # Workflow CLI
└── templates/         # SLURM script templates
    ├── base.slurm.jinja
    └── advanced.slurm.jinja
```

### Core Components

#### Models (`models.py`)
- **Job**: Complete job configuration with resources and environment
- **JobResource**: Resource allocation (nodes, GPUs, memory, time)
- **JobEnvironment**: Environment setup (conda, venv, sqsh)
- **JobStatus**: Job status enumeration
- **Workflow/WorkflowTask**: Workflow definitions with dependencies
- **render_job_script()**: Template rendering function

#### Client (`client.py`)
- **Slurm**: Main interface for SLURM operations
  - `submit()`: Submit jobs with full configuration
  - `retrieve()`: Query job status
  - `cancel()`: Cancel running jobs
  - `queue()`: List user jobs
  - `monitor()`: Wait for job completion
  - `run()`: Submit and monitor job

#### Workflow Runner (`runner.py`)
- **WorkflowRunner**: YAML workflow execution engine
  - `from_yaml()`: Load workflow from YAML file
  - `run()`: Execute workflow with dynamic job scheduling
  - `get_independent_jobs()`: Find jobs without dependencies
  - `parse_job()`: Parse job configuration from YAML

#### Callbacks (`callbacks.py`)
- **Callback**: Base class for job state notifications
- **SlackCallback**: Send notifications to Slack via webhook

#### Logging (`logging.py`)
- **configure_logging()**: General logging configuration
- **configure_cli_logging()**: CLI-specific logging
- **configure_workflow_logging()**: Workflow-specific logging
- **get_logger()**: Get logger instance for module

#### Utilities (`utils.py`)
- **get_job_status()**: Query job status from SLURM
- **job_status_msg()**: Format status messages with icons

#### Exceptions (`exceptions.py`)
- **WorkflowError**: Base workflow exception
- **WorkflowValidationError**: Workflow validation errors
- **WorkflowExecutionError**: Workflow execution errors

#### CLI (`cli/`)
- **Main CLI**: Job management commands (submit, status, list, cancel)
- **Workflow CLI**: YAML workflow execution with validation

### Template System
- Enhanced Jinja2 templates with conditional resource allocation
- `base.slurm.jinja`: Simple job template
- `advanced.slurm.jinja`: Full-featured template with all options
- Automatic environment setup integration

### Workflow Definition
Enhanced YAML workflow format:
```yaml
name: ml_pipeline
tasks:
  - name: preprocess
    command: ["python", "preprocess.py"]
    nodes: 1

  - name: train
    command: ["python", "train.py"]
    depends_on: [preprocess]
    gpus_per_node: 1
    conda: ml_env
    memory_per_node: "32GB"
    time_limit: "4:00:00"

  - name: evaluate
    command: ["python", "evaluate.py"]
    depends_on: [train]
    async: true
```

### Key Improvements
- **Unified Job Model**: Single `Job` class with comprehensive configuration
- **Modular Architecture**: Clear separation of concerns
- **Enhanced CLI**: Subcommands with rich options
- **Better Error Handling**: Comprehensive validation and error messages
- **Resource Management**: Full SLURM resource specification
- **Workflow Validation**: Dependency checking and cycle detection

## Dependencies
- **Jinja2**: Template rendering
- **Pydantic**: Data validation and serialization
- **Loguru**: Structured logging
- **PyYAML**: YAML parsing
- **Rich**: Terminal UI and tables
- **slack-sdk**: Slack notifications

## Code Quality and Linting

### Quality Checks
- `uv run mypy .` - Type checking with mypy
- `uv run ruff check .` - Code linting
- `uv run ruff format .` - Code formatting

### Pre-commit Quality Checks
Always run these before committing:
```bash
uv run pytest && uv run mypy . && uv run ruff check .
```

# important-instruction-reminders
Do what has been asked; nothing more, nothing less.
NEVER create files unless they're absolutely necessary for achieving your goal.
ALWAYS prefer editing an existing file to creating a new one.
NEVER proactively create documentation files (*.md) or README files. Only create documentation files if explicitly requested by the User.
