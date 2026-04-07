Settings
========

The Settings page lets you manage srunx configuration directly from the Web UI instead of editing config files or running CLI commands.

Open Settings
-------------

Click **Settings** (gear icon) in the sidebar. The page has five tabs: General, SSH Profiles, Notifications, Environment, and Project.

Configure SLURM Defaults
-------------------------

The **General** tab controls default resource allocation and environment settings.

1. Open the **General** tab
2. Adjust resource defaults:

   * **Nodes** — Default number of compute nodes
   * **GPUs per Node** — Default GPU count per node
   * **Tasks per Node** — Default number of tasks per node
   * **CPUs per Task** — Default CPU cores per task
   * **Memory per Node** — Default memory allocation (e.g. ``32GB``)
   * **Time Limit** — Default wall time (e.g. ``4:00:00``)
   * **Partition** — Default SLURM partition
   * **Nodelist** — Specific nodes to target (e.g. ``node[01-04]``)

3. Set environment defaults:

   * **Conda Environment** — Default conda environment name
   * **Virtual Environment** — Default virtualenv path
   * **Environment Variables** — Key-value pairs injected into job scripts (add/remove inline)

4. Set general options:

   * **Log Directory** — Default directory for SLURM log output
   * **Working Directory** — Default working directory for jobs

5. Click **Save** to persist changes to ``~/.config/srunx/config.json``

.. note::

   Click **Reset** to restore all settings to their defaults. The **Config File Paths** panel at the bottom of the General tab shows which config files are active and their precedence.

Manage SSH Profiles
--------------------

The **SSH Profiles** tab provides full CRUD for SSH connection profiles.

Add a profile
~~~~~~~~~~~~~

1. Open the **SSH Profiles** tab
2. Fill in the form:

   * **Name** — Profile identifier (e.g. ``dgx-server``)
   * **Hostname** — Remote server address
   * **Username** — SSH login user
   * **Key File** — Path to SSH private key (e.g. ``~/.ssh/id_ed25519``)
   * **Port** — SSH port (default: 22)
   * **SSH Host** — Optional ``~/.ssh/config`` host alias
   * **Proxy Jump** — Optional jump host for multi-hop connections

3. Click **Add Profile**

Switch the active profile
~~~~~~~~~~~~~~~~~~~~~~~~~~

Click **Activate** on any profile card to set it as the current profile. The active profile is indicated with a visual badge.

Edit a profile
~~~~~~~~~~~~~~~

Click the edit button on a profile card, modify fields, and save. Only changed fields are updated.

Delete a profile
~~~~~~~~~~~~~~~~~

Click the delete button and confirm. The profile and all its mounts are removed.

Manage per-profile mounts
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each profile can have mount points that map local directories to remote paths.

1. Expand a profile card to see its mounts section
2. To add a mount, fill in **Name**, **Local Path**, and **Remote Path**, then click **Add**
3. To remove a mount, click the delete button next to it

.. note::

   Mounts added here are identical to those created via ``srunx ssh profile mount add``. Both modify the same config file.

Manage per-profile environment variables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each profile can have environment variables that are passed to jobs run on that server.

1. Expand a profile card and scroll to the **Environment Variables** section
2. Enter a key and value, then click **Add**
3. To remove a variable, click the delete button next to it

Environment variables are stored in the profile configuration and sent via the ``env_vars`` field when updating the profile.

Configure Notifications
------------------------

The **Notifications** tab manages Slack webhook integration.

1. Open the **Notifications** tab
2. Enter your Slack webhook URL (format: ``https://hooks.slack.com/services/...``)
3. Click **Save**

The webhook is used by the callback system to send job state notifications.

View Environment Variables
---------------------------

The **Environment** tab shows all active ``SRUNX_*`` environment variables and ``SLACK_WEBHOOK_URL``.

This is a **read-only** view. Each variable displays its current value and a description. To change environment variables, set them in your shell profile or ``.env`` file and restart the web server.

Manage Project Configuration
------------------------------

The **Project** tab lists projects derived from the active SSH profile's mounts. Each mount corresponds to a project.

Initialize a project
~~~~~~~~~~~~~~~~~~~~~

1. Open the **Project** tab
2. Find a mount that shows "No config"
3. Click **Initialize** to create a ``srunx.json`` with example values in the mount's local directory

Edit project config
~~~~~~~~~~~~~~~~~~~~

1. Click **Edit** on a project that has an existing ``srunx.json``
2. Modify per-project resource defaults, environment settings, or notification overrides
3. Click **Save**

Project-level settings in ``srunx.json`` override the global user config for workflows that execute within that mount's directory.

Equivalent CLI Commands
------------------------

Every action in the Settings page has a CLI equivalent:

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Settings Action
     - CLI Equivalent
   * - Save general config
     - ``srunx config show`` / edit ``~/.config/srunx/config.json``
   * - Add SSH profile
     - ``srunx ssh profile add <name> --hostname ... --username ...``
   * - Activate profile
     - ``srunx ssh profile add <name>`` (sets as current)
   * - Add mount to profile
     - ``srunx ssh profile mount add <profile> <name> --local ... --remote ...``
   * - List mounts
     - ``srunx ssh profile mount list <profile>``
   * - Remove mount
     - ``srunx ssh profile mount remove <profile> <name>``
   * - View config paths
     - ``srunx config paths``
