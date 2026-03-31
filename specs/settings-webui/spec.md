# Spec: Settings Web UI — Full Configuration Management

## Overview
Expand the Settings page to manage all srunx configuration: SSH profiles (with mounts), SLURM defaults, notifications, environment variable overrides, and project-scoped config.

## Background
The web UI currently only manages SLURM resource/environment defaults. All other configuration (SSH profiles, mounts, notifications) requires CLI or manual file editing. Users need a single place to manage all configuration through the browser.

## Requirements

### Must Have
- REQ-1: SSH Profile management — list, add, edit, delete profiles with all ServerProfile fields (hostname, username, key_filename, port, description, ssh_host, proxy_jump)
- REQ-2: SSH Profile activation — set current/active profile, visually indicate which profile is active
- REQ-3: Per-profile mount management — add/remove MountConfig (name, local, remote) per profile
- REQ-4: Per-profile environment variables — add/remove env vars per profile
- REQ-5: Notification settings — configure Slack webhook URL with format validation, persist in user config
- REQ-6: Environment variable overview — read-only display of all active SRUNX_* env vars and their values
- REQ-7: Project config management — detect if .srunx.json exists, initialize, edit project-scoped config
- REQ-8: SLURM resource/environment defaults — already implemented, must be preserved
- REQ-9: Tab navigation — organize settings into logical tabs (General, SSH Profiles, Notifications, Environment, Project)

### Nice to Have
- REQ-N1: SSH connection test button per profile
- REQ-N2: Webhook test (send test notification)

## Acceptance Criteria
- AC-1: Given the Settings page, when user navigates between tabs, then each section loads and displays correct data
- AC-2: Given SSH Profiles tab, when user adds a profile with valid fields, then it persists and appears in the list
- AC-3: Given SSH Profiles tab, when user activates a profile, then it becomes the current profile (highlighted)
- AC-4: Given SSH Profiles tab, when user adds/removes a mount, then changes persist and the mount list updates
- AC-5: Given Notifications tab, when user enters a valid Slack webhook URL and saves, then it persists in user config
- AC-6: Given Notifications tab, when user enters an invalid URL, then validation error is shown
- AC-7: Given Environment tab, when page loads, then all SRUNX_* env vars are displayed read-only
- AC-8: Given Project tab, when no .srunx.json exists, then "Initialize" button is shown
- AC-9: Given Project tab, when user initializes project config, then .srunx.json is created with example values
- AC-10: All existing Settings functionality (resource defaults, save, reset, config paths) continues to work

## Out of Scope
- Web server config (host/port/CORS) — requires restart, not useful via UI
- Monitor config (poll_interval/timeout) — runtime-only, per-operation
- SSH key generation or ~/.ssh/config editing
- Container resource defaults (complex nested config, low priority for v1)

## Constraints
- TypeScript strict mode, no `any` types
- Use existing CSS design system (panel, btn, input classes, CSS variables)
- ConfigManager from ssh/core/config.py must be instantiated per-request (reads from file)
- Slack webhook URL validation must match existing pattern in callbacks.py
- Project config writes to cwd-relative .srunx.json

## Open Questions
- None — requirements confirmed by user
