import logging

import paramiko  # type: ignore

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
                "hostname": proxy_host_config.effective_hostname,
                "username": proxy_host_config.effective_user,
                "port": proxy_host_config.effective_port,
            }

            if proxy_host_config.effective_identity_file:
                proxy_connect_kwargs["key_filename"] = (
                    proxy_host_config.effective_identity_file
                )

            self.logger.info(
                f"Connecting to proxy host: {proxy_host_config.effective_hostname}"
            )
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
            target_host_config.effective_hostname,
            target_host_config.effective_port,
        )

        # Connect to target through proxy
        target_client = paramiko.SSHClient()
        target_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Create transport over proxy channel
        target_transport = paramiko.Transport(proxy_channel)
        target_transport.start_client()

        # Authenticate with target host
        if target_host_config.effective_identity_file:
            try:
                key = paramiko.RSAKey.from_private_key_file(
                    target_host_config.effective_identity_file
                )
                target_transport.auth_publickey(target_host_config.effective_user, key)
            except paramiko.SSHException:
                try:
                    key = paramiko.Ed25519Key.from_private_key_file(
                        target_host_config.effective_identity_file
                    )
                    target_transport.auth_publickey(
                        target_host_config.effective_user, key
                    )
                except paramiko.SSHException:
                    try:
                        key = paramiko.ECDSAKey.from_private_key_file(
                            target_host_config.effective_identity_file
                        )
                        target_transport.auth_publickey(
                            target_host_config.effective_user, key
                        )
                    except paramiko.SSHException as e:
                        self.close_proxy()
                        raise Exception(
                            f"Failed to authenticate with any key type: {e}"
                        ) from e
        else:
            self.close_proxy()
            raise ValueError("No identity file specified for target host")

        # Open a session channel
        session = target_transport.open_session()
        return target_client, session


def create_proxy_aware_connection(
    hostname: str,
    username: str,
    key_filename: str,
    port: int = 22,
    proxy_jump: str | None = None,
    ssh_config_path: str | None = None,
    logger: logging.Logger | None = None,
) -> tuple[paramiko.SSHClient, ProxySSHClient | None]:
    """Create an SSH connection that uses ProxyJump if specified"""
    logger = logger or logging.getLogger(__name__)

    # If ProxyJump is not specified, create a direct connection
    if not proxy_jump:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=hostname,
            username=username,
            key_filename=key_filename,
            port=port,
        )
        return client, None

    # Parse SSH config for target host to inherit settings
    target_config = get_ssh_config_host(hostname, ssh_config_path)
    if not target_config:
        target_config = SSHHost(hostname=hostname, user=username, port=port)

    # Create ProxySSHClient and connect through proxy
    proxy_client = ProxySSHClient(logger=logger)
    target_client, proxy_channel = proxy_client.connect_through_proxy(
        target_config, proxy_jump, ssh_config_path
    )

    # Wrap the channel in a Transport and open an SFTP client
    transport = paramiko.Transport(proxy_channel)
    transport.start_client()
    if target_config.effective_identity_file:
        try:
            key = paramiko.RSAKey.from_private_key_file(
                target_config.effective_identity_file
            )
        except paramiko.SSHException:
            try:
                key = paramiko.Ed25519Key.from_private_key_file(
                    target_config.effective_identity_file
                )
            except paramiko.SSHException:
                key = paramiko.ECDSAKey.from_private_key_file(
                    target_config.effective_identity_file
                )
        transport.auth_publickey(target_config.effective_user, key)

    # Create a client object using the transport
    client = paramiko.SSHClient()
    client._transport = transport  # type: ignore[attr-defined]

    logger.info(
        f"Connected to {hostname} via ProxyJump {proxy_jump} with user {username}"
    )

    return client, proxy_client
