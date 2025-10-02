import logging

import paramiko

from .ssh_config import SSHHost, get_ssh_config_host


class ProxySSHClient:
    """SSH Client with ProxyJump support using Paramiko"""

    def __init__(self, logger: logging.Logger | None = None):
        self.logger = logger or logging.getLogger(__name__)
        self.proxy_client: paramiko.SSHClient | None = None
        self.proxy_transport: paramiko.Transport | None = None

    def create_proxy_connection(
        self, proxy_host_config: SSHHost, target_host: str, target_port: int
    ) -> paramiko.Channel:
        """Create a proxy connection through a jump host"""
        try:
            # Connect to proxy host
            self.proxy_client = paramiko.SSHClient()
            self.proxy_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            proxy_connect_kwargs = {
                "hostname": proxy_host_config.hostname,
                "username": proxy_host_config.user,
                "port": proxy_host_config.port,
            }

            if proxy_host_config.identity_file:
                proxy_connect_kwargs["key_filename"] = proxy_host_config.identity_file

            self.logger.info(f"Connecting to proxy host: {proxy_host_config.hostname}")
            self.proxy_client.connect(**proxy_connect_kwargs)

            # Create a channel through the proxy to the target host
            self.proxy_transport = self.proxy_client.get_transport()
            proxy_channel = self.proxy_transport.open_channel(
                "direct-tcpip", (target_host, target_port), ("", 0)
            )

            self.logger.info(f"Created proxy channel to {target_host}:{target_port}")
            return proxy_channel

        except Exception as e:
            self.logger.error(f"Failed to create proxy connection: {e}")
            self.close_proxy()
            raise

    def close_proxy(self):
        """Close proxy connections"""
        if self.proxy_transport:
            self.proxy_transport.close()
            self.proxy_transport = None
        if self.proxy_client:
            self.proxy_client.close()
            self.proxy_client = None

    def connect_through_proxy(
        self,
        target_host_config: SSHHost,
        proxy_host_name: str,
        ssh_config_path: str | None = None,
    ) -> tuple[paramiko.SSHClient, paramiko.Channel]:
        """Connect to target host through proxy"""

        # Get proxy host configuration
        proxy_host_config = get_ssh_config_host(proxy_host_name, ssh_config_path)
        if not proxy_host_config:
            raise ValueError(f"Proxy host '{proxy_host_name}' not found in SSH config")

        # Check for nested ProxyJump (not supported yet)
        if proxy_host_config.proxy_jump:
            raise NotImplementedError(
                f"Nested ProxyJump not supported. Proxy host '{proxy_host_name}' also uses ProxyJump."
            )

        # Create proxy connection
        proxy_channel = self.create_proxy_connection(
            proxy_host_config,
            target_host_config.hostname,
            target_host_config.port,
        )

        # Connect to target through proxy
        target_client = paramiko.SSHClient()
        target_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Create transport over proxy channel
        target_transport = paramiko.Transport(proxy_channel)
        target_transport.start_client()

        # Authenticate with target host
        if target_host_config.identity_file:
            try:
                key = paramiko.RSAKey.from_private_key_file(
                    target_host_config.identity_file
                )
                target_transport.auth_publickey(target_host_config.user, key)
            except paramiko.SSHException:
                try:
                    key = paramiko.Ed25519Key.from_private_key_file(
                        target_host_config.identity_file
                    )
                    target_transport.auth_publickey(target_host_config.user, key)
                except paramiko.SSHException:
                    try:
                        key = paramiko.ECDSAKey.from_private_key_file(
                            target_host_config.identity_file
                        )
                        target_transport.auth_publickey(target_host_config.user, key)
                    except paramiko.SSHException as e:
                        self.close_proxy()
                        raise Exception(
                            f"Failed to authenticate with any key type: {e}"
                        ) from e
        else:
            self.close_proxy()
            raise ValueError("No identity file specified for target host")

        if not target_transport.is_authenticated():
            self.close_proxy()
            raise Exception("Authentication failed")

        # Create SSH client over the transport
        target_client._transport = target_transport

        self.logger.info(
            f"Successfully connected to {target_host_config.hostname} through {proxy_host_name}"
        )
        return target_client, proxy_channel


def create_proxy_aware_connection(
    hostname: str,
    username: str,
    key_filename: str,
    port: int = 22,
    proxy_jump: str | None = None,
    ssh_config_path: str | None = None,
    logger: logging.Logger | None = None,
) -> tuple[paramiko.SSHClient, ProxySSHClient | None]:
    """Create SSH connection with optional ProxyJump support"""

    if not proxy_jump:
        # Direct connection
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=hostname, username=username, key_filename=key_filename, port=port
        )
        return client, None

    # ProxyJump connection
    from .ssh_config import SSHHost

    target_host_config = SSHHost(
        hostname=hostname, user=username, port=port, identity_file=key_filename
    )

    proxy_client = ProxySSHClient(logger)
    ssh_client, _ = proxy_client.connect_through_proxy(
        target_host_config, proxy_jump, ssh_config_path
    )

    return ssh_client, proxy_client
