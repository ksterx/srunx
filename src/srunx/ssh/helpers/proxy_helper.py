#!/usr/bin/env python3

import argparse
import sys

from ..core.ssh_config import get_ssh_config_host


def suggest_port_forwarding(host: str, ssh_config_path: str | None = None):
    """Suggest SSH port forwarding setup for ProxyJump hosts"""
    ssh_host = get_ssh_config_host(host, ssh_config_path)
    if not ssh_host or not ssh_host.proxy_jump:
        print(f"Host '{host}' does not use ProxyJump")
        return

    print(f"Host '{host}' uses ProxyJump: {ssh_host.proxy_jump}")
    print("\nTo use this host with srunx ssh, consider these alternatives:")
    print("\n1. SSH Port Forwarding:")
    print(
        f"   ssh -L <local_port>:{ssh_host.hostname}:{ssh_host.port} {ssh_host.proxy_jump}"
    )
    print("   Then create a profile pointing to localhost:<local_port>")

    print("\n2. Create a direct connection profile:")
    print("   If you have direct access to the target host, create a profile with:")
    print(
        f"   srunx ssh profile add {host}-direct <real_hostname> {ssh_host.user} {ssh_host.identity_file}"
    )

    print("\n3. Use SSH config with a bastion setup:")
    print(
        "   Configure your SSH client to handle ProxyJump automatically outside of srunx ssh"
    )


def check_connectivity(hostname: str, port: int = 22):
    """Check if a host is directly reachable"""
    import socket

    try:
        sock = socket.create_connection((hostname, port), timeout=5)
        sock.close()
        return True
    except (TimeoutError, OSError):
        return False


def main():
    parser = argparse.ArgumentParser(description="SSH SLURM Proxy Helper")
    parser.add_argument("host", help="SSH host to analyze")
    parser.add_argument(
        "--ssh-config", help="SSH config file path (default: ~/.ssh/config)"
    )
    parser.add_argument(
        "--test-connection",
        action="store_true",
        help="Test direct connectivity to the target host",
    )

    args = parser.parse_args()

    ssh_host = get_ssh_config_host(args.host, args.ssh_config)
    if not ssh_host:
        print(f"Error: Host '{args.host}' not found in SSH config", file=sys.stderr)
        sys.exit(1)

    print(f"SSH Config Analysis for '{args.host}':")
    print(f"  Hostname: {ssh_host.hostname}")
    print(f"  Username: {ssh_host.user}")
    print(f"  Port: {ssh_host.port}")
    print(f"  Identity File: {ssh_host.identity_file}")
    if ssh_host.proxy_jump:
        print(f"  ProxyJump: {ssh_host.proxy_jump}")

    if ssh_host.proxy_jump:
        print("\n" + "=" * 50)
        suggest_port_forwarding(args.host, args.ssh_config)
    else:
        print(f"\nHost '{args.host}' should work directly with srunx ssh.")

    if args.test_connection and not ssh_host.proxy_jump:
        print(f"\nTesting connectivity to {ssh_host.hostname}:{ssh_host.port}...")
        if check_connectivity(ssh_host.hostname, ssh_host.port):
            print("✓ Direct connection successful")
        else:
            print("✗ Direct connection failed")


if __name__ == "__main__":
    main()
