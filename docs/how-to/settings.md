---
description: Configure srunx from the Web UI Settings page — manage clusters, layered job defaults, project settings, and notification endpoints without editing TOML by hand.
---

# Settings

The Settings page lets you manage srunx configuration directly from the Web UI instead of editing config files or running CLI commands. Every edit is written to one TOML file — the user config (`$XDG_CONFIG_HOME/srunx/config.toml`) or the selected project's `.srunx/config.toml` — and the effective values are always recomputed from all layers.

## Open Settings

Click **Settings** (gear icon) in the sidebar. The page has five tabs: General, Clusters, Notifications, Environment, and Project.

The cluster and project used by the page are the ones selected in the sidebar. They are sent with every API request (`?cluster=` / `?project=`), so two browser tabs on different clusters never affect each other.

## Configure Job Defaults

The **General** tab edits job defaults at one **scope** at a time and shows the effective value of every field together with the file it comes from.

1.  Pick a scope in the scope selector:
    - **User (all clusters)** — `[defaults]` in the user config
    - **User · this cluster** — `[clusters.<c>.defaults]` in the user config
    - **Project** — `[defaults]` in the project config
    - **Project · this cluster** — `[clusters.<c>.defaults]` in the project config

    The project scopes are only available when a project is selected.
2.  Adjust resource defaults (Nodes, GPUs per Node, Tasks per Node, CPUs per Task, Memory per Node, Time Limit, Partition, Nodelist), environment defaults (Conda Environment, Virtual Environment), and general options (Log Directory, Working Directory).
3.  Save. Only the changed keys of that one file are written.

Per field you can also:

- **Inherit** — remove the key from this scope so the value comes from a lower layer again (same as `srunx config delete`)
- **Explicitly clear** — add the key to the table's `unset = [...]` list so an inherited value is ignored (same as `srunx config unset`)

The **Config File Paths** panel at the bottom lists the files srunx reads, lowest precedence first (system, user, project), plus the state directory.

!!! note
    Edits carry a version token. If the file changed on disk since the page loaded (for example via the CLI), the save is rejected with a conflict and the page reloads the file instead of overwriting it.

Precedence, lowest to highest: built-in → system `defaults` → user `defaults` → user `clusters.<c>.defaults` → project `defaults` → project `clusters.<c>.defaults` → script `#SBATCH` / workflow job fields → explicit submission values.

## Manage Clusters

The **Clusters** tab manages the clusters (job targets) defined in the user config as `[clusters.<name>]`.

### Add a cluster

1.  Open the **Clusters** tab and start a new cluster
2.  Fill in the form:
    - **Name** — Cluster identifier (e.g. `gpu`)
    - **Transport** — `local` (submit with the local `sbatch`) or `ssh`
    - For SSH: **Host alias** (from `~/.ssh/config`) or **Hostname**, plus optional **Username**, **Key file**, **Port**, and **Proxy jump**
    - **Description** and **Snapshot root** (optional)
    - **Make default** — also set `default_cluster`
3.  Save

### Test, edit, make default, delete

Each cluster row has buttons to **Test connection** (for SSH this also caches the remote `$HOME` / XDG directories), **Make default**, **Edit**, and **Delete**. Deleting a cluster that is the `default_cluster` also clears `default_cluster`.

!!! note
    Connection settings live only in the user (or system) config. A project config can never define how to connect to a host.

## Configure Notifications

The **Notifications** tab manages notification endpoints (Slack webhooks) and the notification defaults (default endpoint and preset) stored under `[notifications]` in the user config.

## View Environment Variables

The **Environment** tab shows the srunx-related environment variables that are currently set (for example `SRUNX_CLUSTER`, `SRUNX_WEB_PROJECTS`, `SRUNX_WEB_TOKEN` (masked), `SLACK_WEBHOOK_URL`, `XDG_CONFIG_HOME`, `XDG_STATE_HOME`).

This is a **read-only** view. To change environment variables, set them in your shell and restart the web server.

## Manage Project Configuration

The **Project** tab edits the selected project's `.srunx/config.toml`. If no project is selected, start `srunx ui` inside a project (a directory with `.srunx/config.toml`) or pass `--project PATH`.

- **Cluster** — the project's default cluster (`cluster = "..."`); choose "(user default)" to remove it
- Per cluster: **Remote directory** (`[clusters.<c>] remote_dir`), **Sync** (rsync the project before runs), and **Exclude** patterns
- **Resolved mounts** — the mounts the project has on each SSH cluster: the project root mapped to `remote_dir` (or, without one, to the remote default workspace `$XDG_DATA_HOME/srunx/workspaces/<name>-<hash>`), plus any `[[clusters.<c>.mounts]]` entries from the file

To create a project config, run `srunx config init-project` in the project root.

## Equivalent CLI Commands

| Settings Action | CLI Equivalent |
|----|----|
| Show effective config and sources | `srunx config show [--cluster C] [--project PATH]` |
| Set a default (user scope) | `srunx config set resources.partition gpu` |
| Set a default (user · this cluster) | `srunx config set resources.partition gpu --cluster C` |
| Set a default (project scopes) | `srunx config set ... --project [--cluster C]` |
| Inherit / explicitly clear | `srunx config delete KEY` / `srunx config unset KEY` |
| Add a cluster | `srunx cluster add <name> --host <alias>` (or `--hostname H`, `--local`) `[--default]` |
| Edit / remove / test a cluster | `srunx cluster update <name> ...` / `srunx cluster remove <name>` / `srunx cluster test <name>` |
| Show a cluster and the project's mounts on it | `srunx cluster show <name>` |
| Set the project's cluster | `srunx config set cluster <name> --project` |
| Set a project remote directory | `srunx config set remote_dir /abs/path --cluster <name> --project` |
| Create a project config | `srunx config init-project` |
| View config paths | `srunx config paths` |
