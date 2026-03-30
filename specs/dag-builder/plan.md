# Plan: DAG Builder

## Spec Reference
`specs/dag-builder/spec.md` — Interactive DAG builder for constructing SLURM workflows visually.

## Approach
Extend the existing ReactFlow-based DAG viewer into an interactive editor by building a new `WorkflowBuilder` page that reuses the `JobNode` component and design system. Backend adds a single `POST /api/workflows/create` endpoint with a domain-neutral `WorkflowCreateRequest` schema, sharing validation logic with existing endpoints.

### Trade-offs Considered
| Option | Pros | Cons |
|--------|------|------|
| **Extend existing DAGView** (chosen) | Reuses JobNode, layout algo, CSS; consistent UX | Need to refactor DAGView to accept editable mode |
| Separate builder component from scratch | Clean separation, no risk of breaking viewer | Code duplication, divergent visual styles |
| Modal-based job editor | Simpler state management | Poor UX for frequent edits, blocks canvas interaction |
| **Side panel editor** (chosen) | Matches WorkflowDetail pattern, non-blocking | Slightly more complex layout |

## Architecture

### Components

| Component | File Path | Responsibility |
|-----------|-----------|----------------|
| `WorkflowBuilder` (page) | `frontend/src/pages/WorkflowBuilder.tsx` | Builder page with ReactFlow canvas, toolbar, side panel |
| `JobPropertyPanel` (component) | `frontend/src/components/JobPropertyPanel.tsx` | Side panel for editing selected job's properties |
| `BuilderToolbar` (component) | Inline in `WorkflowBuilder.tsx` | Top bar with workflow name, Add Job, Save, validation errors |
| `EditableJobNode` (component) | Inline in `WorkflowBuilder.tsx` | Extended JobNode for builder context (draft status styling) |
| `useWorkflowBuilder` (hook) | `frontend/src/hooks/use-workflow-builder.ts` | State management: nodes, edges, validation, serialization |
| `workflows.create()` (API) | `frontend/src/lib/api.ts` | API call for `POST /api/workflows/create` |
| `create` endpoint | `src/srunx/web/routers/workflows.py` | Validate + serialize + persist workflow |
| `_validate_and_build_workflow` | `src/srunx/web/routers/workflows.py` | Shared function: build Job/Workflow models from dict, validate |

### Interfaces

```typescript
// Frontend: Builder job state (not ReactFlow-coupled)
type BuilderJob = {
  name: string;
  command: string;              // User types as string, split on save
  nodes: number | null;
  gpus_per_node: number | null;
  memory_per_node: string | null;
  time_limit: string | null;
  partition: string | null;
  conda: string | null;
  venv: string | null;
};

// Frontend: Edge with dependency type
type BuilderEdge = {
  source: string;    // source job name (node id)
  target: string;    // target job name (node id)
  dep_type: "afterok" | "after" | "afterany" | "afternotok";
};

// API request body for POST /api/workflows/create
type WorkflowCreateRequest = {
  name: string;
  jobs: Array<{
    name: string;
    command: string[];
    depends_on: string[];            // e.g. ["afterok:preprocess", "afterany:validate"]
    resources?: {
      nodes?: number;
      gpus_per_node?: number;
      memory_per_node?: string;
      time_limit?: string;
      partition?: string;
    };
    environment?: {
      conda?: string;
      venv?: string;
    };
  }>;
};
```

```python
# Backend: Pydantic request model
class WorkflowCreateRequest(BaseModel):
    name: str
    jobs: list[WorkflowJobInput]

class WorkflowJobInput(BaseModel):
    name: str
    command: list[str]
    depends_on: list[str] = []
    resources: dict[str, Any] | None = None
    environment: dict[str, Any] | None = None
```

### Data Flow

```
User interaction (add/connect/edit nodes)
    ↓
useWorkflowBuilder hook (manages ReactFlow state + BuilderJob map)
    ↓
"Save" clicked → serializeToRequest() converts to WorkflowCreateRequest
    ↓
POST /api/workflows/create
    ↓
_validate_and_build_workflow() → constructs Job/JobResource/JobEnvironment/Workflow models
    ↓
Workflow.validate() (cycles, duplicates, unknown deps)
    ↓
_workflow_to_yaml() → serializes to YAML dict → yaml.dump()
    ↓
Write to workflow_dir/{name}.yaml
    ↓
Return serialized workflow → frontend redirects to /workflows/{name}
```

## Integration Points
- **App.tsx**: Add route `/workflows/new` BEFORE `/workflows/:name` (route ordering matters)
- **Workflows.tsx**: Add "New Workflow" button linking to `/workflows/new`
- **workflows.py router**: Add `POST /api/workflows/create` endpoint + shared `_validate_and_build_workflow()`
- **lib/api.ts**: Add `workflows.create(request)` method
- **lib/types.ts**: Add `BuilderJob`, `BuilderEdge`, `WorkflowCreateRequest` types

## Dependencies
### Internal
- `DAGView.tsx` → Reuse `JobNode` component styling, `STATUS_COLORS`, `layoutDAG` algorithm
- `WorkflowDetail.tsx` → Reference side panel pattern for `JobPropertyPanel`
- `models.py` → `Job`, `JobResource`, `JobEnvironment`, `Workflow`, `DependencyType`
- `workflows.py` → Existing `_serialize_workflow`, `_SAFE_NAME` regex

### External
- `@xyflow/react` — Already installed, provides interactive node/edge editing
- `PyYAML` — Already installed, for YAML serialization
- No new dependencies needed

## Risks & Mitigations
| Risk | Impact | Mitigation |
|------|--------|------------|
| ReactFlow state management complexity | Med | Encapsulate in `useWorkflowBuilder` hook; test with 10+ node DAGs |
| YAML output incompatible with `from_yaml()` | High | AC-8 roundtrip test; use same field names as `parse_job()` expects |
| Route `/workflows/new` vs `/workflows/:name` conflict | Low | Place `/workflows/new` route BEFORE `/:name` in App.tsx |
| Pydantic validation errors not user-friendly | Med | Catch `ValidationError`, transform to field-path error map |

## Testing Strategy
- Unit: `useWorkflowBuilder` hook — add/remove/connect nodes, cycle detection, serialization
- Integration: `POST /api/workflows/create` — roundtrip test (create → load with `from_yaml`)
- E2E: Playwright — create workflow via builder, verify appears in workflow list
