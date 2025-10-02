import os
import re
from pathlib import Path

from pydantic import BaseModel, Field, field_validator


class SSHHost(BaseModel):
    hostname: str = Field(..., description="The hostname of the SSH host")
    user: str = Field(..., description="The username of the SSH host")
    port: int = Field(22, description="The port of the SSH host")
    identity_file: str | None = Field(
        None, description="The identity file of the SSH host"
    )
    proxy_command: str | None = Field(
        None, description="The proxy command of the SSH host"
    )
    proxy_jump: str | None = Field(None, description="The proxy jump of the SSH host")
    forward_agent: bool | None = Field(
        None, description="The forward agent of the SSH host"
    )

    @field_validator("identity_file")
    @classmethod
    def effective_identity_file(cls, v: str | None) -> str | None:
        if v:
            return os.path.expanduser(v)
        return None


class SSHConfigParser:
    def __init__(self, config_path: str | None = None):
        self.config_path = (
            Path(config_path) if config_path else Path.home() / ".ssh" / "config"
        )
        self.hosts: dict[str, SSHHost] = {}
        self._parse_config()

    def _parse_config(self) -> None:
        if not self.config_path.exists():
            return

        with open(self.config_path, encoding="utf-8") as f:
            lines = f.readlines()

        current_host: str | None = None
        current_config: dict[str, str] = {}

        for line in lines:
            line = line.strip()

            # Skip comments and empty lines
            if not line or line.startswith("#"):
                continue

            # Parse key-value pairs
            match = re.match(r"(\w+)\s+(.+)", line, re.IGNORECASE)
            if not match:
                continue

            key = match.group(1).lower()
            value = match.group(2).strip()

            if key == "host":
                # Save previous host config
                if current_host:
                    host = self._create_host(current_host, current_config)
                    if host:
                        self.hosts[current_host] = host

                # Start new host
                current_host = value
                current_config = {}
            elif current_host:
                current_config[key] = value

        # Save last host
        if current_host:
            host = self._create_host(current_host, current_config)
            if host:
                self.hosts[current_host] = host

    def _create_host(self, host_pattern: str, config: dict[str, str]) -> SSHHost | None:
        # For specific hosts, use HostName if specified, otherwise use the host pattern
        # For wildcard patterns (*), always use the provided hostname when querying
        hostname = config.get("hostname", host_pattern)

        if "user" not in config:
            return None

        # Parse port
        if config.get("port"):
            port = int(config["port"])
        else:
            port = 22

        # Parse boolean values
        forward_agent = None
        if "forwardagent" in config:
            forward_agent = config["forwardagent"].lower() in ("yes", "true", "1")

        return SSHHost(
            hostname=hostname,
            user=config["user"],
            port=port,
            identity_file=config.get("identityfile"),
            proxy_command=config.get("proxycommand"),
            proxy_jump=config.get("proxyjump"),
            forward_agent=forward_agent,
        )

    def get_host(self, host_pattern: str) -> SSHHost | None:
        """Get host configuration by exact match or pattern matching"""
        # First try exact match
        if host_pattern in self.hosts:
            return self.hosts[host_pattern]

        # Then try pattern matching
        for pattern, host in self.hosts.items():
            if self._match_pattern(pattern, host_pattern):
                # Create a copy with the actual hostname
                return SSHHost(
                    hostname=host.hostname
                    if host.hostname != pattern
                    else host_pattern,
                    user=host.user,
                    port=host.port,
                    identity_file=host.identity_file,
                    proxy_command=host.proxy_command,
                    proxy_jump=host.proxy_jump,
                    forward_agent=host.forward_agent,
                )

        return None

    def _match_pattern(self, pattern: str, hostname: str) -> bool:
        """Simple pattern matching for SSH host patterns"""
        if "*" not in pattern and "?" not in pattern:
            return pattern == hostname

        # Convert SSH pattern to regex
        regex_pattern = pattern.replace(".", r"\.")
        regex_pattern = regex_pattern.replace("*", ".*")
        regex_pattern = regex_pattern.replace("?", ".")
        regex_pattern = f"^{regex_pattern}$"

        return bool(re.match(regex_pattern, hostname, re.IGNORECASE))

    def list_hosts(self) -> dict[str, SSHHost]:
        """List all configured hosts"""
        return self.hosts.copy()

    def find_identity_files(self, hostname: str) -> list[str]:
        """Find all possible identity files for a host"""
        host = self.get_host(hostname)
        files = []

        if host and host.identity_file:
            files.append(host.identity_file)

        # Add default identity files if none specified
        if not files:
            default_keys = [
                "~/.ssh/id_rsa",
                "~/.ssh/id_ed25519",
                "~/.ssh/id_ecdsa",
                "~/.ssh/id_dsa",
            ]
            for key in default_keys:
                key_path = os.path.expanduser(key)
                if os.path.exists(key_path):
                    files.append(str(key_path))

        return files


def get_ssh_config_host(
    hostname: str, config_path: str | None = None
) -> SSHHost | None:
    """Convenience function to get host configuration"""
    parser = SSHConfigParser(config_path)
    return parser.get_host(hostname)
