# Plan: Settings Web UI — Full Configuration Management

## Spec Reference
See `specs/settings-webui/spec.md` — REQ-1 through REQ-9.

## Approach
Extend existing config router and Settings page. Add tab-based navigation to organize config areas. Backend uses existing ConfigManager for SSH profiles and existing config.py for SLURM/notification settings.

### Trade-offs Considered
| Option | Pros | Cons |
|--------|------|------|
| **Extend existing config router** (chosen) | Single router, consistent patterns, fewer files | Router file grows larger |
| Separate routers per section | Better separation | More files, more app.py wiring, fragmented API |
| **Tab navigation in single page** (chosen) | Familiar UX, shared state (save/reset), URL reflects tab | Page component grows |
| Separate pages per section | Smaller components | Inconsistent save UX, more routing |

## Architecture

### Components

| Component | File Path | Responsibility |
|-----------|-----------|----------------|
| Config Router | `src/srunx/web/routers/config.py` | All /api/config/* endpoints |
| NotificationConfig | `src/srunx/config.py` | Notification settings model |
| Settings Page | `src/srunx/web/frontend/src/pages/Settings.tsx` | Tab container + General tab |
| SSHProfilesTab | `src/srunx/web/frontend/src/pages/settings/SSHProfilesTab.tsx` | SSH profile CRUD + mounts |
| NotificationsTab | `src/srunx/web/frontend/src/pages/settings/NotificationsTab.tsx` | Slack webhook config |
| EnvironmentTab | `src/srunx/web/frontend/src/pages/settings/EnvironmentTab.tsx` | SRUNX_* env var display |
| ProjectTab | `src/srunx/web/frontend/src/pages/settings/ProjectTab.tsx` | Project config management |
| API Client | `src/srunx/web/frontend/src/lib/api.ts` | config.* API methods |
| Types | `src/srunx/web/frontend/src/lib/types.ts` | SSH profile, notification types |

### API Endpoints

```
GET    /api/config                          → SrunxConfig (existing)
PUT    /api/config                          → SrunxConfig (existing)
POST   /api/config/reset                    → SrunxConfig (existing)
GET    /api/config/paths                    → ConfigPathInfo[] (existing)

GET    /api/config/ssh/profiles             → { current: str|null, profiles: {name: ServerProfile}[] }
POST   /api/config/ssh/profiles             → ServerProfile (body: { name, ...fields })
PUT    /api/config/ssh/profiles/{name}      → ServerProfile
DELETE /api/config/ssh/profiles/{name}      → { ok: true }
POST   /api/config/ssh/profiles/{name}/activate → { ok: true }
POST   /api/config/ssh/profiles/{name}/mounts   → MountConfig
DELETE /api/config/ssh/profiles/{name}/mounts/{mount_name} → { ok: true }

GET    /api/config/env                      → { name: str, value: str, description: str }[]

GET    /api/config/project                  → { exists: bool, path: str, config: SrunxConfig|null }
PUT    /api/config/project                  → SrunxConfig
POST   /api/config/project/init             → { path: str, config: SrunxConfig }
```

### Data Flow
```
Frontend Tab Component
    ↓ fetch on mount
API Client (lib/api.ts)
    ↓ HTTP request
FastAPI Router (config.py)
    ↓ calls
ConfigManager / load_config / save_user_config
    ↓ reads/writes
~/.config/srunx/config.json or .srunx.json
```

### Frontend Tab Structure
```
Settings Page
├── Tab Bar: [General] [SSH Profiles] [Notifications] [Environment] [Project]
├── General Tab (existing content: resources, environment, general, config paths)
├── SSH Profiles Tab
│   ├── Profile List (cards with activate/edit/delete)
│   ├── Add Profile Form (expandable)
│   └── Per-profile: Mounts list + Env vars list
├── Notifications Tab
│   └── Slack Webhook URL field + save
├── Environment Tab
│   └── Read-only SRUNX_* variable list with descriptions
└── Project Tab
    ├── Status (exists/not exists, path)
    ├── Initialize button
    └── Project config form (resource/env overrides)
```

## Integration Points
- `ConfigManager` (ssh/core/config.py): SSH profile CRUD, mount management
- `SrunxConfig` (config.py): SLURM defaults, notifications — save_user_config/load_config
- `load_config_from_file` / `get_config_paths`: Project config detection

## Dependencies
### Internal
- `srunx.config` — SrunxConfig, save_user_config, load_config, get_config_paths, load_config_from_file
- `srunx.ssh.core.config` — ConfigManager, ServerProfile, MountConfig

### External
- No new dependencies

## Risks & Mitigations
| Risk | Impact | Mitigation |
|------|--------|------------|
| ConfigManager and SrunxConfig share the same config.json file | Med | Both operate on different top-level keys (profiles vs resources/environment) |
| Project config cwd-relative may differ from server cwd | Med | Show absolute path in UI, let user know which directory |
| Large Settings.tsx file | Low | Split into tab sub-components |

## Testing Strategy
- Manual: Playwright MCP browser verification for each tab
- Lint: ruff check for backend, tsc --noEmit for frontend
- API: curl verification of each endpoint
