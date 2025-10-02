#!/usr/bin/env python3

import sys
from pathlib import Path

from ..core.config import ConfigManager, ServerProfile
from ..core.ssh_config import get_ssh_config_host


def handle_profile_command(args):
    """Handle profile subcommands with SSH config support"""

    if not args.profile_command:
        print("Error: No profile command specified", file=sys.stderr)
        print("Use 'srunx ssh profile --help' for available commands", file=sys.stderr)
        sys.exit(1)

    try:
        config_manager = ConfigManager(args.config)

        if args.profile_command == "add":
            cmd_add(args, config_manager)
        elif args.profile_command == "remove":
            cmd_remove(args, config_manager)
        elif args.profile_command == "list":
            cmd_list(args, config_manager)
        elif args.profile_command == "set":
            cmd_set(args, config_manager)
        elif args.profile_command == "show":
            cmd_show(args, config_manager)
        elif args.profile_command == "update":
            cmd_update(args, config_manager)
        elif args.profile_command == "env":
            cmd_env(args, config_manager)

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_add(args, config_manager: ConfigManager):
    if args.ssh_host:
        # Use SSH config host
        ssh_host = get_ssh_config_host(args.ssh_host)
        if not ssh_host:
            print(
                f"Error: SSH host '{args.ssh_host}' not found in SSH config",
                file=sys.stderr,
            )
            sys.exit(1)

        # Verify required SSH config values
        if not ssh_host.user:
            print(
                f"Error: No username found for SSH host '{args.ssh_host}' in SSH config",
                file=sys.stderr,
            )
            sys.exit(1)

        if not ssh_host.identity_file:
            print(
                f"Error: No identity file found for SSH host '{args.ssh_host}' in SSH config",
                file=sys.stderr,
            )
            sys.exit(1)

        # Verify key file exists
        if not Path(ssh_host.identity_file).exists():
            print(
                f"Error: SSH key file '{ssh_host.identity_file}' not found",
                file=sys.stderr,
            )
            sys.exit(1)

        profile = ServerProfile(
            hostname=ssh_host.hostname,
            username=ssh_host.user,
            key_filename=ssh_host.identity_file,
            port=ssh_host.port,
            description=getattr(args, "description", None),
            ssh_host=args.ssh_host,  # Store the SSH config host name
        )

        print(f"Added profile '{args.name}' using SSH config host '{args.ssh_host}'")
        print(f"  Hostname: {ssh_host.hostname}")
        print(f"  Username: {ssh_host.user}")
        print(f"  Key file: {ssh_host.identity_file}")
        print(f"  Port: {ssh_host.port}")
        if ssh_host.proxy_jump:
            print(f"  ProxyJump: {ssh_host.proxy_jump}")

    else:
        # Use direct connection parameters
        if not all([args.hostname, args.username, args.key_file]):
            print(
                "Error: When not using --ssh-host, you must specify --hostname, --username, and --key-file",
                file=sys.stderr,
            )
            sys.exit(1)

        key_path = config_manager.expand_path(args.key_file)
        if not Path(key_path).exists():
            print(f"Error: SSH key file '{key_path}' not found", file=sys.stderr)
            sys.exit(1)

        profile = ServerProfile(
            hostname=args.hostname,
            username=args.username,
            key_filename=key_path,
            port=args.port,
            description=args.description,
            ssh_host=None,
        )

        print(f"Added profile '{args.name}' with direct connection")

    config_manager.add_profile(args.name, profile)
    print(f"Profile '{args.name}' added successfully")


def cmd_remove(args, config_manager: ConfigManager):
    if config_manager.remove_profile(args.name):
        print(f"Profile '{args.name}' removed successfully")
    else:
        print(f"Error: Profile '{args.name}' not found", file=sys.stderr)
        sys.exit(1)


def cmd_list(args, config_manager: ConfigManager):
    profiles = config_manager.list_profiles()
    if not profiles:
        print("No profiles found")
        return

    print("Available profiles:")
    for name, profile in profiles.items():
        current_mark = "*" if config_manager.get_current_profile_name() == name else " "
        source = f" (ssh: {profile.ssh_host})" if profile.ssh_host else ""
        desc = f" - {profile.description}" if profile.description else ""
        print(f"{current_mark} {name}{source}{desc}")


def cmd_set(args, config_manager: ConfigManager):
    if config_manager.set_current_profile(args.name):
        print(f"Current profile set to '{args.name}'")
    else:
        print(f"Error: Profile '{args.name}' not found", file=sys.stderr)
        sys.exit(1)


def cmd_show(args, config_manager: ConfigManager):
    name = args.name
    if name:
        profile = config_manager.get_profile(name)
        if not profile:
            print(f"Error: Profile '{name}' not found", file=sys.stderr)
            sys.exit(1)
        print(f"Profile: {name}")
        if profile.ssh_host:
            print(f"  SSH Config Host: {profile.ssh_host}")
        print(f"  Hostname: {profile.hostname}")
        print(f"  Username: {profile.username}")
        print(f"  Key file: {profile.key_filename}")
        print(f"  Port: {profile.port}")
        if profile.description:
            print(f"  Description: {profile.description}")
    else:
        current = config_manager.get_current_profile_name()
        if current:
            print(f"Current profile: {current}")
            profile = config_manager.get_current_profile()
            if profile:
                if profile.ssh_host:
                    print(f"  SSH Config Host: {profile.ssh_host}")
                print(f"  Hostname: {profile.hostname}")
                print(f"  Username: {profile.username}")
                print(f"  Key file: {profile.key_filename}")
                print(f"  Port: {profile.port}")
                if profile.description:
                    print(f"  Description: {profile.description}")
        else:
            print("No current profile set")


def cmd_update(args, config_manager: ConfigManager):
    existing_profile = config_manager.get_profile(args.name)
    if not existing_profile:
        print(f"Error: Profile '{args.name}' not found", file=sys.stderr)
        sys.exit(1)

    updates = {}

    # Handle SSH host updates
    if args.ssh_host:
        ssh_host = get_ssh_config_host(args.ssh_host)
        if not ssh_host:
            print(
                f"Error: SSH host '{args.ssh_host}' not found in SSH config",
                file=sys.stderr,
            )
            sys.exit(1)

        # Update with SSH config values
        updates["ssh_host"] = args.ssh_host
        updates["hostname"] = ssh_host.hostname
        updates["username"] = ssh_host.user
        updates["key_filename"] = ssh_host.identity_file
        updates["port"] = ssh_host.port
        print(f"Updated profile '{args.name}' to use SSH config host '{args.ssh_host}'")

    # Handle direct parameter updates
    if args.hostname:
        updates["hostname"] = args.hostname
        updates["ssh_host"] = None  # Clear SSH host if setting direct hostname
    if args.username:
        updates["username"] = args.username
    if args.key_file:
        key_path = config_manager.expand_path(args.key_file)
        if not Path(key_path).exists():
            print(f"Error: SSH key file '{key_path}' not found", file=sys.stderr)
            sys.exit(1)
        updates["key_filename"] = key_path
        updates["ssh_host"] = None  # Clear SSH host if setting direct key file
    if args.port:
        updates["port"] = args.port
    if args.description is not None:
        updates["description"] = args.description

    if not updates:
        print("No updates specified", file=sys.stderr)
        sys.exit(1)

    if config_manager.update_profile(args.name, **updates):
        print(f"Profile '{args.name}' updated successfully")
    else:
        print(f"Error: Failed to update profile '{args.name}'", file=sys.stderr)
        sys.exit(1)


def cmd_env(args, config_manager: ConfigManager):
    """Manage environment variables for a profile"""
    if not args.env_command:
        print("Error: No environment command specified", file=sys.stderr)
        print("Available commands: set, unset, list", file=sys.stderr)
        sys.exit(1)

    profile = config_manager.get_profile(args.name)
    if not profile:
        print(f"Error: Profile '{args.name}' not found", file=sys.stderr)
        sys.exit(1)

    if args.env_command == "set":
        cmd_env_set(args, config_manager, profile)
    elif args.env_command == "unset":
        cmd_env_unset(args, config_manager, profile)
    elif args.env_command == "list":
        cmd_env_list(args, config_manager, profile)
    else:
        print(
            f"Error: Unknown environment command '{args.env_command}'", file=sys.stderr
        )
        sys.exit(1)


def cmd_env_set(args, config_manager: ConfigManager, profile):
    """Set an environment variable for a profile"""
    if profile.env_vars is None:
        profile.env_vars = {}

    profile.env_vars[args.key] = args.value
    config_manager.update_profile(args.name, env_vars=profile.env_vars)
    print(f"Environment variable '{args.key}' set for profile '{args.name}'")


def cmd_env_unset(args, config_manager: ConfigManager, profile):
    """Unset an environment variable for a profile"""
    if profile.env_vars is None or args.key not in profile.env_vars:
        print(
            f"Error: Environment variable '{args.key}' not found in profile '{args.name}'",
            file=sys.stderr,
        )
        sys.exit(1)

    del profile.env_vars[args.key]
    config_manager.update_profile(args.name, env_vars=profile.env_vars)
    print(f"Environment variable '{args.key}' removed from profile '{args.name}'")


def cmd_env_list(args, config_manager: ConfigManager, profile):
    """List environment variables for a profile"""
    if not profile.env_vars:
        print(f"No environment variables set for profile '{args.name}'")
        return

    print(f"Environment variables for profile '{args.name}':")
    for key, value in profile.env_vars.items():
        print(f"  {key}={value}")
