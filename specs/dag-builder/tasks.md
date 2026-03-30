# Tasks: DAG Builder

## Prerequisites
- [x] Spec approved: `specs/dag-builder/spec.md`
- [x] Plan approved: `specs/dag-builder/plan.md`

## Phase 1: Backend API
- [x] T1.1: Add `POST /api/workflows/create` endpoint with `WorkflowCreateRequest` Pydantic model, shared `_validate_and_build_workflow()` function, YAML serialization, conflict detection (REQ-9, REQ-7, REQ-8)
      Files: `src/srunx/web/routers/workflows.py`

## Phase 2: Frontend Types & API
- [x] T2.1: Add `WorkflowCreateRequest`, `BuilderJob`, `BuilderEdge` types to `lib/types.ts` and `workflows.create()` to `lib/api.ts` (REQ-9)
      Files: `frontend/src/lib/types.ts`, `frontend/src/lib/api.ts`

## Phase 3: Builder Hook
- [x] T3.1: Create `useWorkflowBuilder` hook — manages ReactFlow nodes/edges state, job property map, add/remove/update jobs, connect/disconnect edges with dep_type, client-side validation (cycles, required fields), `serializeToRequest()` (REQ-2, REQ-3, REQ-5, REQ-7)
      Files: `frontend/src/hooks/use-workflow-builder.ts`
      Depends: T2.1

## Phase 4: UI Components
- [x] T4.1: Create `JobPropertyPanel` component — side panel for editing selected job (name, command, resources, environment), matches WorkflowDetail sidebar style (REQ-4)
      Files: `frontend/src/components/JobPropertyPanel.tsx`
      Depends: T2.1
- [x] T4.2: Create `WorkflowBuilder` page — ReactFlow canvas with EditableJobNode, toolbar (workflow name, Add Job, Save), JobPropertyPanel integration, edge dep_type editing, validation error display, save + redirect (REQ-1, REQ-3, REQ-6, REQ-8)
      Files: `frontend/src/pages/WorkflowBuilder.tsx`
      Depends: T3.1, T4.1

## Phase 5: Integration
- [x] T5.1: Add route `/workflows/new` to App.tsx (before `/:name`), add "New Workflow" button to Workflows.tsx (AC-1)
      Files: `frontend/src/App.tsx`, `frontend/src/pages/Workflows.tsx`
      Depends: T4.2

## Verification Checklist
### Acceptance Criteria
- [x] AC-1: "New Workflow" navigates to `/workflows/new`
- [x] AC-2: "Add Job" creates editable node
- [x] AC-3: Drag handle creates dependency edge with afterok default
- [x] AC-4: Side panel edits reflect in node
- [x] AC-5: Delete removes node/edge
- [x] AC-6: Save creates YAML, redirects to `/workflows/{name}`
- [x] AC-7: Invalid DAG shows validation errors
- [x] AC-8: Saved YAML loads via `WorkflowRunner.from_yaml()`
- [x] AC-9: Edge dep_type editable
- [x] AC-10: Duplicate name returns 409
- [x] AC-11: Invalid/reserved name shows error

### Quality Gates
- [x] All tests pass
- [x] Lint/Type-check pass
- [x] No security vulnerabilities
