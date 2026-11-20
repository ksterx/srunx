---
description: Browse local project files and submit scripts to SLURM from the srunx Web UI File Explorer — a VS Code-style tree panel for sbatch scripts.
---

# File Explorer

The file explorer is a VS Code-style tree panel that lets you browse local project files and submit scripts to SLURM directly from the Web UI.

## Open the Explorer

Click the **Explorer** button (folder icon) at the top of the sidebar. The panel slides in between the sidebar and the main content area.

## Browse Project Files

The explorer shows one tree root per mount of the project selected in the sidebar, on the selected cluster. Each mount corresponds to a local directory of that project.

1.  Click a mount name to expand it and load the top-level entries
2.  Click a folder to expand it (directories are loaded lazily on demand)
3.  Shell scripts (`.sh`, `.slurm`, `.sbatch`, `.bash`) are shown with a code file icon in the running-status color (green)
4.  Symlinks are shown with a link icon; links outside the mount boundary are marked inaccessible and cannot be followed. Accessible symlinks pointing to directories can be expanded like regular folders

!!! note
    Hidden files (names starting with `.`) are automatically filtered out.

## Submit a Script as sbatch

![Submitting a shell script as sbatch](../assets/images/ui-explorer-sbatch.gif)

1.  Right-click any shell script (`.sh`, `.slurm`, `.sbatch`, `.bash`) in the tree
2.  Select **Submit as sbatch** from the context menu
3.  In the submit dialog:
    - The script path is shown as a read-only field
    - The job name defaults to the filename (editable)
4.  Click **Submit** — the script content is read from the mount and sent to the SLURM cluster
5.  The result shows the assigned job ID or an error message

!!! note
    Only shell script files show the submit option. Right-clicking other file types shows a grayed-out "Not a submittable script" message.

## Sync Files to Remote

Each mount has a **Sync** button. Click it to push local files to the remote SLURM server via rsync.

- During sync, the button shows a loading spinner
- After a successful sync, a green checkmark appears briefly
- Sync state is tracked per mount (syncing one mount does not affect others)

!!! warning
    If you modify local files after syncing, sync again before running jobs that depend on the updated files.

## Add Mounts

Mounts come from the selected project's `.srunx/config.toml`. The project root
is always a mount (mapped to `[clusters.<name>] remote_dir`, or the remote
default workspace); extra ones are `[[clusters.<name>.mounts]]` entries. To set
them up:

- **From Settings**: Navigate to **Settings \> Project** and set the remote directory, sync, and excludes per cluster (see [Settings](settings.md))
- **From CLI**: `srunx config set remote_dir /abs/remote/path --cluster <name> --project`
- **Extra mounts**: add a `[[clusters.<name>.mounts]]` table (`name`, `local`, `remote`, optional `sync` / `exclude`) to `.srunx/config.toml` — see [Project Synchronization](sync.md#mount-points)

## Keyboard Navigation

- Press **Escape** to close the context menu
- Click outside the context menu to dismiss it
- Click the **Explorer** button again to close the panel
