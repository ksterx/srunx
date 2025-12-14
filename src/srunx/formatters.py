"""Unified Slack message formatters with table-based layouts."""

from datetime import datetime, timedelta


class SlackTableFormatter:
    """Format data as ASCII tables for Slack code blocks."""

    @staticmethod
    def _sanitize_text(text: str) -> str:
        """Sanitize text for safe use in Slack messages.

        Prevents injection attacks by escaping special characters and
        removing control characters that could break message formatting.

        Args:
            text: Text to sanitize.

        Returns:
            Sanitized text with special characters escaped and control
            characters removed.
        """
        # Remove or replace control characters
        text = text.replace("\n", " ").replace("\r", " ").replace("\t", " ")

        # Escape special characters that could enable injection attacks
        # Note: & must be first to avoid double-escaping
        replacements = {
            "&": "&amp;",  # HTML entity escape (must be first)
            "<": "&lt;",  # Prevent HTML/script tag injection
            ">": "&gt;",  # Prevent HTML/script tag injection
            "`": "'",  # Prevent code block injection
            "*": "\\*",  # Escape markdown bold
            "_": "\\_",  # Escape markdown italic
            "~": "\\~",  # Escape markdown strikethrough
            "[": "\\[",  # Escape markdown link syntax
            "]": "\\]",  # Escape markdown link syntax
        }
        for char, replacement in replacements.items():
            text = text.replace(char, replacement)

        # Limit length to prevent message overflow
        max_length = 1000
        if len(text) > max_length:
            text = text[:max_length] + "..."

        return text

    @staticmethod
    def header(title: str, timestamp: datetime | None = None) -> str:
        """Create formatted header.

        Args:
            title: Header title with emoji
            timestamp: Optional timestamp to display

        Returns:
            Formatted header string
        """
        lines = [title, "━" * 40]
        if timestamp:
            lines.append(f"🕐 {timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        return "\n".join(lines)

    @staticmethod
    def box_title(text: str, width: int = 40) -> str:
        """Create box with title.

        Args:
            text: Title text
            width: Box width

        Returns:
            Box string
        """
        padding = width - len(text) - 2
        left_pad = padding // 2
        right_pad = padding - left_pad
        return f"┌{'─' * (width - 2)}┐\n│{' ' * left_pad}{text}{' ' * right_pad}│\n└{'─' * (width - 2)}┘"

    @staticmethod
    def key_value_table(data: dict[str, str], width: int = 40) -> str:
        """Create key-value table.

        Args:
            data: Dictionary of key-value pairs
            width: Table width

        Returns:
            Formatted table string
        """
        if not data:
            return ""

        # Calculate column widths
        max_key_len = max(len(k) for k in data.keys())
        key_width = min(max_key_len + 2, width // 2)
        value_width = width - key_width - 3

        # Calculate actual table width (including borders)
        actual_width = key_width + value_width + 3

        lines = [f"┌{'─' * (actual_width - 2)}┐"]

        for i, (key, value) in enumerate(data.items()):
            # Truncate if needed
            key_display = key[:key_width].ljust(key_width)
            value_display = value[:value_width].ljust(value_width)

            if i == 0:
                lines.append(f"│ {key_display}│ {value_display}│")
                lines.append(f"├{'─' * key_width}┼{'─' * value_width}┤")
            else:
                lines.append(f"│ {key_display}│ {value_display}│")

        lines.append(f"└{'─' * key_width}┴{'─' * value_width}┘")
        return "\n".join(lines)

    @staticmethod
    def data_table(
        headers: list[str],
        rows: list[list[str]],
        title: str | None = None,
        width: int = 60,
    ) -> str:
        """Create multi-column data table.

        Args:
            headers: Column headers
            rows: Data rows
            title: Optional table title
            width: Table width

        Returns:
            Formatted table string
        """
        if not rows:
            return f"┌{'─' * (width - 2)}┐\n│ No data{' ' * (width - 10)}│\n└{'─' * (width - 2)}┘"

        # Calculate column widths
        num_cols = len(headers)
        col_width = (width - num_cols - 1) // num_cols

        # Calculate actual table width (including borders)
        actual_width = num_cols * col_width + num_cols + 1

        def format_row(items: list[str]) -> str:
            cells = [item[:col_width].ljust(col_width) for item in items]
            return "│" + "│".join(cells) + "│"

        lines = []

        # Title if provided
        if title:
            padding = actual_width - len(title) - 4
            left_pad = padding // 2
            right_pad = padding - left_pad
            lines.append(f"┌{'─' * (actual_width - 2)}┐")
            lines.append(f"│ {' ' * left_pad}{title}{' ' * right_pad} │")
            lines.append(f"├{'─' * (actual_width - 2)}┤")
        else:
            lines.append(f"┌{'─' * (actual_width - 2)}┐")

        # Headers
        lines.append(format_row(headers))
        lines.append("├" + "┼".join(["─" * col_width] * num_cols) + "┤")

        # Data rows
        for row in rows:
            lines.append(format_row(row))

        lines.append("└" + "┴".join(["─" * col_width] * num_cols) + "┘")

        return "\n".join(lines)

    @staticmethod
    def progress_bar(value: float, total: float, width: int = 10) -> str:
        """Create progress bar.

        Args:
            value: Current value
            total: Total value
            width: Bar width in characters

        Returns:
            Progress bar string (e.g., "██████░░░░")
        """
        if total == 0:
            return "░" * width

        ratio = min(value / total, 1.0)
        filled = int(ratio * width)
        empty = width - filled

        return "█" * filled + "░" * empty


class SlackNotificationFormatter:
    """Format different notification types with unified style."""

    def __init__(self):
        self.table = SlackTableFormatter()

    def job_status_change(
        self,
        job_id: int,
        name: str,
        old_status: str,
        new_status: str,
        partition: str | None = None,
        runtime: str | None = None,
        gpus: int | None = None,
        success: bool = True,
    ) -> str:
        """Format job status change notification.

        Args:
            job_id: Job ID
            name: Job name
            old_status: Previous status
            new_status: Current status
            partition: SLURM partition
            runtime: Runtime string
            gpus: Number of GPUs
            success: Whether the status change is successful

        Returns:
            Formatted Slack message
        """
        timestamp = datetime.now()
        emoji = "🎉" if success else "❌"
        status_text = "Completed" if success else "Failed"

        header = self.table.header("📊 Job Status Update", timestamp)

        # Build data dict
        data = {
            "Job ID": str(job_id),
            "Name": name,
            "Status": f"{old_status}→{new_status}",
        }

        if partition:
            data["Partition"] = partition
        if runtime:
            data["Runtime"] = runtime
        if gpus:
            data["GPUs"] = str(gpus)

        title_box = self.table.box_title(f"{emoji} Job {status_text}")
        kv_table = self.table.key_value_table(data)

        return f"```\n{header}\n\n{title_box}\n{kv_table}\n```"

    def job_status_report(
        self, jobs: list[dict], timestamp: datetime | None = None
    ) -> str:
        """Format job status report.

        Args:
            jobs: List of job dictionaries with keys: id, name, status, runtime, gpus
            timestamp: Report timestamp

        Returns:
            Formatted Slack message
        """
        if timestamp is None:
            timestamp = datetime.now()

        header = self.table.header("📊 Job Status Report", timestamp)

        if not jobs:
            return f"```\n{header}\n\nNo jobs to report\n```"

        # Prepare table data
        headers = ["ID", "Name", "Status", "Runtime", "GPU"]
        rows = [
            [
                str(job.get("id", "-")),
                job.get("name", "-")[:15],
                job.get("status", "-")[:10],
                job.get("runtime", "-")[:8],
                str(job.get("gpus", "-")),
            ]
            for job in jobs
        ]

        table = self.table.data_table(
            headers, rows, title=f"Monitored Jobs ({len(jobs)})", width=60
        )

        return f"```\n{header}\n\n{table}\n```"

    def resource_available(
        self,
        partition: str | None,
        available_gpus: int,
        total_gpus: int,
        idle_nodes: int,
        total_nodes: int,
        utilization: float,
    ) -> str:
        """Format resource availability notification.

        Args:
            partition: SLURM partition
            available_gpus: Number of available GPUs
            total_gpus: Total GPUs
            idle_nodes: Number of idle nodes
            total_nodes: Total nodes
            utilization: GPU utilization (0-100)

        Returns:
            Formatted Slack message
        """
        timestamp = datetime.now()
        header = self.table.header("📊 Resource Status Update", timestamp)

        progress = self.table.progress_bar(total_gpus - available_gpus, total_gpus)

        # Sanitize partition name
        partition_safe = self.table._sanitize_text(partition) if partition else "all"

        data = {
            "Partition": partition_safe,
            "Available GPUs": f"{available_gpus} / {total_gpus}",
            "Utilization": f"{utilization:.0f}% {progress}",
            "Idle Nodes": f"{idle_nodes} / {total_nodes}",
        }

        title_box = self.table.box_title("🎮 GPU Resources Available")
        kv_table = self.table.key_value_table(data)

        return f"```\n{header}\n\n{title_box}\n{kv_table}\n\n✅ You can now submit your jobs!\n```"

    def cluster_status(
        self,
        job_stats: dict | None = None,
        resource_stats: dict | None = None,
        running_jobs: list[dict] | None = None,
        timestamp: datetime | None = None,
    ) -> str:
        """Format cluster status report.

        Args:
            job_stats: Job statistics dict
            resource_stats: Resource statistics dict
            running_jobs: List of running job dicts
            timestamp: Report timestamp

        Returns:
            Formatted Slack message
        """
        if timestamp is None:
            timestamp = datetime.now()

        header = self.table.header("📊 SLURM Cluster Status", timestamp)
        sections = []

        # Queue Status
        if job_stats:
            # Calculate total_active from pending + running (not included in asdict)
            pending = job_stats.get("pending", 0)
            running = job_stats.get("running", 0)
            total_active = pending + running

            queue_data = {
                "Total Active": str(total_active),
                "Pending": str(pending),
                "Running": str(running),
            }
            sections.append(self.table.key_value_table(queue_data))

        # GPU Resources
        if resource_stats:
            total = resource_stats.get("total_gpus", 0)
            in_use = resource_stats.get("gpus_in_use", 0)
            available = resource_stats.get("gpus_available", 0)
            # Calculate utilization from in_use/total (not included in asdict)
            utilization = (in_use / total * 100) if total > 0 else 0.0
            progress = self.table.progress_bar(in_use, total)

            partition_info = resource_stats.get("partition")
            partition_text = (
                f" ({self.table._sanitize_text(partition_info)})"
                if partition_info
                else ""
            )

            resource_data = {
                f"GPU Resources{partition_text}": "",
                "Total": str(total),
                "In Use": f"{in_use} ({utilization:.0f}%)",
                "Available": str(available),
                "Utilization": progress,
                "Nodes (idle)": f"{resource_stats.get('nodes_idle', 0)} / {resource_stats.get('nodes_total', 0)}",
            }
            sections.append(self.table.key_value_table(resource_data))

        # Running Jobs
        if running_jobs:
            headers = ["ID", "Name", "User", "Runtime", "GPU"]
            rows = []
            for job in running_jobs:
                # Format runtime from timedelta
                runtime_str = "-"
                if job.get("runtime"):
                    rt = job["runtime"]
                    # asdict() keeps timedelta objects as-is
                    if isinstance(rt, timedelta):
                        days = rt.days
                        seconds = rt.seconds
                    elif isinstance(rt, dict):
                        # Fallback for dict format (shouldn't happen with asdict)
                        days = rt.get("days", 0)
                        seconds = rt.get("seconds", 0)
                    else:
                        days = 0
                        seconds = 0

                    hours, remainder = divmod(seconds, 3600)
                    minutes, _ = divmod(remainder, 60)

                    if days > 0:
                        runtime_str = f"{days}d{hours:02d}:{minutes:02d}"
                    else:
                        runtime_str = f"{hours:02d}:{minutes:02d}"

                # Sanitize job name and user
                job_name = self.table._sanitize_text(job.get("name", "-"))
                job_user = self.table._sanitize_text(job.get("user", "-"))

                rows.append(
                    [
                        str(job.get("job_id", "-")),
                        job_name[:12],
                        job_user[:8],
                        runtime_str[:8],
                        str(job.get("gpus", "-")),
                    ]
                )

            sections.append(
                self.table.data_table(
                    headers, rows, title=f"Active Jobs ({len(running_jobs)})", width=70
                )
            )

        content = "\n\n".join(sections)
        return f"```\n{header}\n\n{content}\n```"
