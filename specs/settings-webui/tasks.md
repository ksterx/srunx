# Tasks: Settings Web UI

## Phase 1: Backend — Data Model & API Endpoints

- [ ] 1.1 Add `NotificationConfig` to `SrunxConfig` in `src/srunx/config.py`
- [ ] 1.2 Add SSH profile endpoints to `src/srunx/web/routers/config.py` (list, add, update, delete, activate, mounts)
- [ ] 1.3 Add notification endpoints to config router (GET/PUT using SrunxConfig)
- [ ] 1.4 Add environment variable endpoint to config router (GET /api/config/env)
- [ ] 1.5 Add project config endpoints to config router (GET, PUT, POST init)
- [ ] 1.6 Verify backend: ruff check + import test

## Phase 2: Frontend — Types, API Client, Tab Infrastructure

- [ ] 2.1 Add SSH profile, notification, env var, project config types to `types.ts`
- [ ] 2.2 Add all new API methods to `api.ts` (config.sshProfiles.*, config.notifications.*, config.env, config.project.*)
- [ ] 2.3 Refactor Settings.tsx into tab-based layout, extract existing content to GeneralTab

## Phase 3: Frontend — Tab Components

- [ ] 3.1 Create `SSHProfilesTab.tsx` — profile list, add/edit/delete forms, mount management, env vars, activate
- [ ] 3.2 Create `NotificationsTab.tsx` — Slack webhook URL input with validation
- [ ] 3.3 Create `EnvironmentTab.tsx` — read-only SRUNX_* env var display
- [ ] 3.4 Create `ProjectTab.tsx` — project config status, init, edit form

## Phase 4: Verification

- [ ] 4.1 TypeScript type check (tsc --noEmit)
- [ ] 4.2 Python lint (ruff check)
- [ ] 4.3 Playwright MCP browser verification of each tab
