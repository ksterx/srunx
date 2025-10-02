#!/usr/bin/env python3

import argparse


def show_ssh_help() -> None:
    """Show comprehensive SSH help with examples."""
    help_text = """
Usage: srunx ssh <script> [options]
       srunx ssh profile <command> [options]
       srunx ssh test [options]

Submit and monitor SLURM jobs on remote servers via SSH.

SCRIPT SUBMISSION:
  srunx ssh <script_file> [OPTIONS]

  Submit a local script file to a remote SLURM server.

CONNECTION TEST:
  srunx ssh test [OPTIONS]

  Test SSH connection and SLURM availability.

CONNECTION OPTIONS:
  --host HOST                SSH config host name (from ~/.ssh/config)
  --profile PROFILE          Use saved connection profile
  --hostname HOSTNAME        Direct connection hostname
  --username USERNAME        SSH username
  --key-file PATH           SSH private key file path
  --port PORT               SSH port (default: 22)

JOB OPTIONS:
  --job-name NAME           SLURM job name
  --env KEY=VALUE           Set environment variable (can be used multiple times)
  --env-local KEY           Transfer local environment variable
  --poll-interval SECONDS   Job status polling interval (default: 10)
  --timeout SECONDS         Maximum monitoring time (default: unlimited)
  --no-monitor             Submit job without monitoring
  --no-cleanup             Keep uploaded files for debugging

PROFILE MANAGEMENT:
  srunx ssh profile list                    List all profiles
  srunx ssh profile add NAME OPTIONS       Add new profile
  srunx ssh profile remove NAME            Remove profile
  srunx ssh profile set NAME               Set default profile
  srunx ssh profile show NAME              Show profile details
  srunx ssh profile update NAME OPTIONS    Update profile

EXAMPLES:

  # Test connection
  srunx ssh test --host dgx-server
  srunx ssh test --profile ml-cluster

  # Submit using SSH config host
  srunx ssh train.py --host dgx-server

  # Submit using saved profile
  srunx ssh experiment.sh --profile ml-cluster

  # Direct connection
  srunx ssh script.py --hostname dgx.example.com --username researcher --key-file ~/.ssh/dgx_key

  # With environment variables
  srunx ssh train.py --host server --env CUDA_VISIBLE_DEVICES=0,1,2,3 --env-local WANDB_API_KEY

  # Background job submission
  srunx ssh long_job.sh --host server --no-monitor

  # Profile management
  srunx ssh profile add dgx --ssh-host dgx1 --description "Main DGX server"
  srunx ssh profile add direct --hostname 10.0.1.100 --username ml --key-file ~/.ssh/ml_key
  srunx ssh profile set dgx
  srunx ssh profile list

For more details on specific commands, use:
  srunx ssh profile --help
"""
    print(help_text.strip())


def run_from_argv(argv: list[str]) -> None:
    """Main CLI entry point with clean subcommand routing for ssh integration.

    Delegates to profile, test, or submit handlers depending on first arg.

    If no arguments provided, shows help with examples.
    """

    # If no arguments, show comprehensive help
    if not argv:
        show_ssh_help()
        return

    # Check if first argument is 'test' - if so, handle connection test
    if len(argv) > 0 and argv[0] == "test":
        from .test import handle_test_command

        # Remove 'test' and parse the rest
        handle_test_command(argv[1:])
        return

    # Check if first argument is 'profile' - if so, handle profile commands
    if len(argv) > 0 and argv[0] == "profile":
        from .profile import handle_profile_command

        # Create profile parser
        parser = argparse.ArgumentParser(
            prog="srunx ssh profile", description="Manage SSH SLURM server profiles"
        )
        parser.add_argument(
            "--config", help="Config file path (default: ~/.config/srunx/config.json)"
        )

        subparsers = parser.add_subparsers(
            dest="profile_command", help="Profile commands"
        )

        # Add profile subcommands
        add_parser = subparsers.add_parser("add", help="Add a new profile")
        add_parser.add_argument("name", help="Profile name")
        add_parser.add_argument(
            "--ssh-host", help="SSH config host name (from ~/.ssh/config)"
        )

        # Direct connection parameters group
        direct_group = add_parser.add_argument_group(
            "Direct connection (use when not using --ssh-host)"
        )
        direct_group.add_argument("--hostname", help="Server hostname")
        direct_group.add_argument("--username", help="SSH username")
        direct_group.add_argument("--key-file", help="SSH private key file path")
        direct_group.add_argument(
            "--port", type=int, default=22, help="SSH port (default: 22)"
        )
        add_parser.add_argument("--description", help="Profile description")

        # Other profile subcommands
        remove_parser = subparsers.add_parser("remove", help="Remove a profile")
        remove_parser.add_argument("name", help="Profile name")

        subparsers.add_parser("list", help="List all profiles")

        set_parser = subparsers.add_parser("set", help="Set current profile")
        set_parser.add_argument("name", help="Profile name")

        show_parser = subparsers.add_parser("show", help="Show profile details")
        show_parser.add_argument(
            "name", nargs="?", help="Profile name (default: current)"
        )

        update_parser = subparsers.add_parser("update", help="Update a profile")
        update_parser.add_argument("name", help="Profile name")
        update_parser.add_argument("--ssh-host", help="SSH config host name")
        update_parser.add_argument("--hostname", help="Server hostname")
        update_parser.add_argument("--username", help="SSH username")
        update_parser.add_argument("--key-file", help="SSH private key file path")
        update_parser.add_argument("--port", type=int, help="SSH port")
        update_parser.add_argument("--description", help="Profile description")

        # Environment variable management
        env_parser = subparsers.add_parser(
            "env", help="Manage environment variables for a profile"
        )
        env_parser.add_argument("name", help="Profile name")
        env_subparsers = env_parser.add_subparsers(
            dest="env_command", help="Environment variable commands"
        )

        env_set_parser = env_subparsers.add_parser(
            "set", help="Set environment variable"
        )
        env_set_parser.add_argument("key", help="Environment variable name")
        env_set_parser.add_argument("value", help="Environment variable value")

        env_unset_parser = env_subparsers.add_parser(
            "unset", help="Unset environment variable"
        )
        env_unset_parser.add_argument("key", help="Environment variable name")

        env_subparsers.add_parser("list", help="List environment variables")

        # Remove 'profile' and parse the rest
        args = parser.parse_args(argv[1:])
        handle_profile_command(args)
        return

    # Default behavior - job submission
    from .submit import run_from_argv as submit_run

    submit_run(argv)
