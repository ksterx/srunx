# Spec: DAG Builder

## Overview
Add an interactive visual DAG builder to the web UI that allows users to construct SLURM workflows by creating jobs, connecting dependencies, and exporting to YAML — complementing the existing YAML upload/view functionality.

## Background
The web UI currently supports loading workflows from YAML files and visualizing them as DAGs via `DAGView.tsx` (ReactFlow). Users can upload YAML and view the DAG, but cannot construct workflows visually. A DAG builder enables users who are less familiar with YAML syntax to create workflows interactively, and provides a faster iteration loop for all users.

## Requirements
### Must Have
- REQ-1: DAG builder page at `/workflows/new` with a ReactFlow-based interactive canvas
- REQ-2: Add job nodes to the canvas via a toolbar/button, with editable properties (name, command, resources, environment)
- REQ-3: Create dependency edges by dragging from one node's source handle to another's target handle. Each edge carries a dependency type (`afterok` default, `after`, `afterany`, `afternotok`) editable via edge context (click/select)
- REQ-4: Edit job properties via a side panel when a node is selected: name, command (as string, split to `list[str]` on save by whitespace), `nodes`, `gpus_per_node`, `memory_per_node`, `time_limit`, `partition`, `conda`/`venv` (mutually exclusive)
- REQ-5: Delete nodes and edges (keyboard Delete/Backspace or UI button)
- REQ-6: Workflow name input field. Name must match `^[\w\-]+$` (alphanumeric, hyphens, underscores). Name `"new"` is reserved and rejected. Name doubles as the YAML filename stem
- REQ-7: Validate the DAG before save: cycle detection, duplicate job names, unknown dependency targets (via `Workflow.validate()`), plus field-level validation (Pydantic model construction for `Job`/`JobResource`/`JobEnvironment`). Return structured errors with field paths to the frontend
- REQ-8: Export/save workflow — serialize the DAG to YAML and save via backend. If a workflow with the same name already exists, return `409 Conflict`
- REQ-9: Backend `POST /api/workflows/create` endpoint that accepts a domain-neutral workflow JSON payload (not coupled to ReactFlow state), validates via model construction, serializes to YAML, and saves to `workflow_dir`. Shares validation logic with the existing `/upload` and `/validate` endpoints

### Nice to Have
- REQ-N1: YAML preview panel showing real-time generated YAML
- REQ-N2: Import existing workflow into the builder for editing (ShellJob nodes shown as read-only/non-editable placeholders)

## Acceptance Criteria
- AC-1: Given the Workflows page, when user clicks "New Workflow", then they navigate to `/workflows/new` with an empty canvas
- AC-2: Given the builder canvas, when user clicks "Add Job", then a new editable job node appears on the canvas
- AC-3: Given two job nodes, when user drags from source handle to target handle, then a dependency edge is created with `afterok` as default type
- AC-4: Given a selected node, when user edits properties in the side panel, then the node reflects the changes
- AC-5: Given a node or edge is selected, when user presses Delete, then the element is removed
- AC-6: Given a valid DAG with named workflow, when user clicks "Save", then the workflow is saved as YAML and user is redirected to `/workflows/{name}`
- AC-7: Given an invalid DAG (cycle, missing name/command), when user clicks "Save", then validation errors are displayed with field-level detail
- AC-8: Given the builder, when user constructs a DAG and saves, then the resulting YAML can be loaded by `WorkflowRunner.from_yaml()` and produces an equivalent `Workflow`
- AC-9: Given a dependency edge, when user clicks the edge, then they can change its dependency type (afterok/after/afterany/afternotok)
- AC-10: Given a workflow name that already exists, when user clicks "Save", then a `409 Conflict` error is shown
- AC-11: Given a workflow name of `"new"` or containing invalid characters, when user attempts to save, then a validation error is shown

## Out of Scope
- Real-time collaborative editing
- Drag-and-drop reordering/layout (auto-layout is sufficient)
- ShellJob creation (only command-based Job for now; ShellJob shown read-only if imported via REQ-N2)
- `python:` args in workflow — blocked by security restrictions
- Workflow execution from the builder (existing run functionality handles this)
- Workflow overwrite/update (v1 is create-only; edit requires delete + recreate)

## Constraints
- Must use existing `@xyflow/react` library (already installed)
- Must follow the existing "HPC Mission Control" dark theme (CSS variables in `globals.css`)
- Backend validation must construct actual `Job`/`JobResource`/`JobEnvironment`/`Workflow` Pydantic models to leverage existing validators, not just `Workflow.validate()`
- YAML output must be compatible with `WorkflowRunner.from_yaml()` parsing
- No new npm dependencies unless strictly necessary
- The `POST /api/workflows/create` request schema must be a domain-neutral workflow representation (job list with properties), not tied to ReactFlow node/edge shapes
