"""Tests for SlackTableFormatter and SlackNotificationFormatter."""

from datetime import datetime, timedelta

from srunx.observability.notifications.formatting import (
    SlackNotificationFormatter,
    SlackTableFormatter,
)

# ---------------------------------------------------------------------------
# SlackTableFormatter
# ---------------------------------------------------------------------------


class TestSanitizeText:
    """Tests for SlackTableFormatter._sanitize_text()."""

    def test_escapes_ampersand(self):
        assert "&amp;" in SlackTableFormatter._sanitize_text("a & b")

    def test_escapes_less_than(self):
        assert "&lt;" in SlackTableFormatter._sanitize_text("<script>")

    def test_escapes_greater_than(self):
        assert "&gt;" in SlackTableFormatter._sanitize_text("a > b")

    def test_escapes_backtick(self):
        result = SlackTableFormatter._sanitize_text("`code`")
        assert "`" not in result
        assert "'" in result

    def test_escapes_asterisk(self):
        assert "\\*" in SlackTableFormatter._sanitize_text("*bold*")

    def test_escapes_underscore(self):
        assert "\\_" in SlackTableFormatter._sanitize_text("_italic_")

    def test_escapes_tilde(self):
        assert "\\~" in SlackTableFormatter._sanitize_text("~strike~")

    def test_escapes_square_brackets(self):
        result = SlackTableFormatter._sanitize_text("[link](url)")
        assert "\\[" in result
        assert "\\]" in result

    def test_ampersand_escaped_before_others(self):
        # & must be escaped first so &lt; does not become &amp;lt;
        result = SlackTableFormatter._sanitize_text("&<")
        assert "&amp;" in result
        assert "&lt;" in result
        assert "&amp;lt;" not in result

    def test_removes_newline(self):
        assert "\n" not in SlackTableFormatter._sanitize_text("line1\nline2")

    def test_removes_carriage_return(self):
        assert "\r" not in SlackTableFormatter._sanitize_text("line1\rline2")

    def test_removes_tab(self):
        assert "\t" not in SlackTableFormatter._sanitize_text("col1\tcol2")

    def test_control_chars_replaced_with_space(self):
        result = SlackTableFormatter._sanitize_text("a\nb")
        assert result == "a b"

    def test_truncates_long_text(self):
        long_text = "x" * 1500
        result = SlackTableFormatter._sanitize_text(long_text)
        # 1000 chars + "..."
        assert len(result) == 1003

    def test_truncation_adds_ellipsis(self):
        long_text = "a" * 1100
        result = SlackTableFormatter._sanitize_text(long_text)
        assert result.endswith("...")

    def test_text_at_limit_not_truncated(self):
        text = "a" * 1000
        result = SlackTableFormatter._sanitize_text(text)
        assert len(result) == 1000
        assert "..." not in result

    def test_empty_string(self):
        assert SlackTableFormatter._sanitize_text("") == ""

    def test_plain_text_unchanged(self):
        assert SlackTableFormatter._sanitize_text("hello world") == "hello world"


class TestHeader:
    """Tests for SlackTableFormatter.header()."""

    def test_contains_title(self):
        result = SlackTableFormatter.header("My Title")
        assert "My Title" in result

    def test_contains_divider(self):
        result = SlackTableFormatter.header("Title")
        assert "━" * 40 in result

    def test_divider_is_on_second_line(self):
        result = SlackTableFormatter.header("Title")
        lines = result.split("\n")
        assert lines[0] == "Title"
        assert lines[1] == "━" * 40

    def test_with_timestamp(self):
        ts = datetime(2025, 3, 15, 10, 30, 45)
        result = SlackTableFormatter.header("Title", timestamp=ts)
        assert "2025-03-15 10:30:45" in result
        assert "🕐" in result

    def test_without_timestamp(self):
        result = SlackTableFormatter.header("Title")
        assert "🕐" not in result
        # Should only be 2 lines: title + divider
        assert len(result.split("\n")) == 2


class TestBoxTitle:
    """Tests for SlackTableFormatter.box_title()."""

    def test_contains_text(self):
        result = SlackTableFormatter.box_title("Hello")
        assert "Hello" in result

    def test_has_top_border(self):
        result = SlackTableFormatter.box_title("Hello")
        lines = result.split("\n")
        assert lines[0].startswith("┌")
        assert lines[0].endswith("┐")

    def test_has_bottom_border(self):
        result = SlackTableFormatter.box_title("Hello")
        lines = result.split("\n")
        assert lines[2].startswith("└")
        assert lines[2].endswith("┘")

    def test_has_three_lines(self):
        result = SlackTableFormatter.box_title("Hello")
        assert len(result.split("\n")) == 3

    def test_text_centered_in_middle_line(self):
        result = SlackTableFormatter.box_title("Hi", width=40)
        lines = result.split("\n")
        middle = lines[1]
        assert middle.startswith("│")
        assert middle.endswith("│")
        assert "Hi" in middle

    def test_custom_width(self):
        result = SlackTableFormatter.box_title("Test", width=50)
        lines = result.split("\n")
        # Top border: ┌ + 48 dashes + ┐ = 50 chars
        assert len(lines[0]) == 50


class TestKeyValueTable:
    """Tests for SlackTableFormatter.key_value_table()."""

    def test_empty_dict_returns_empty(self):
        assert SlackTableFormatter.key_value_table({}) == ""

    def test_contains_keys_and_values(self):
        result = SlackTableFormatter.key_value_table({"Name": "Alice", "Age": "30"})
        assert "Name" in result
        assert "Alice" in result
        assert "Age" in result
        assert "30" in result

    def test_has_borders(self):
        result = SlackTableFormatter.key_value_table({"Key": "Value"})
        assert "┌" in result
        assert "┘" in result
        assert "└" in result
        assert "┐" in result

    def test_first_row_separator(self):
        result = SlackTableFormatter.key_value_table({"A": "1", "B": "2"})
        # After first row, there should be a separator with ├ and ┼
        assert "├" in result
        assert "┼" in result

    def test_single_entry(self):
        result = SlackTableFormatter.key_value_table({"Key": "Value"})
        assert "Key" in result
        assert "Value" in result
        # Should have top border, one data row, separator row (from first item),
        # and bottom border
        lines = result.split("\n")
        assert len(lines) >= 3

    def test_multiple_entries(self):
        data = {"A": "1", "B": "2", "C": "3"}
        result = SlackTableFormatter.key_value_table(data)
        for key, val in data.items():
            assert key in result
            assert val in result


class TestDataTable:
    """Tests for SlackTableFormatter.data_table()."""

    def test_empty_rows_shows_no_data(self):
        result = SlackTableFormatter.data_table(["Col1", "Col2"], [])
        assert "No data" in result

    def test_contains_headers(self):
        result = SlackTableFormatter.data_table(["ID", "Name"], [["1", "Alice"]])
        assert "ID" in result
        assert "Name" in result

    def test_contains_row_data(self):
        result = SlackTableFormatter.data_table(
            ["ID", "Name"], [["1", "Alice"], ["2", "Bob"]]
        )
        assert "Alice" in result
        assert "Bob" in result

    def test_with_title(self):
        result = SlackTableFormatter.data_table(["Col"], [["val"]], title="My Table")
        assert "My Table" in result

    def test_without_title(self):
        result = SlackTableFormatter.data_table(["Col"], [["val"]], title=None)
        # Should still have top border
        assert result.startswith("┌")

    def test_header_separator(self):
        result = SlackTableFormatter.data_table(["A", "B"], [["1", "2"]])
        # Header separator line should contain ┼
        assert "┼" in result

    def test_bottom_border(self):
        result = SlackTableFormatter.data_table(["A"], [["1"]])
        lines = result.split("\n")
        assert lines[-1].startswith("└")
        assert lines[-1].endswith("┘")


class TestProgressBar:
    """Tests for SlackTableFormatter.progress_bar()."""

    def test_full_bar(self):
        result = SlackTableFormatter.progress_bar(10, 10, width=10)
        assert result == "█" * 10

    def test_empty_bar(self):
        result = SlackTableFormatter.progress_bar(0, 10, width=10)
        assert result == "░" * 10

    def test_half_bar(self):
        result = SlackTableFormatter.progress_bar(5, 10, width=10)
        assert "█" in result
        assert "░" in result
        assert len(result) == 10

    def test_zero_total(self):
        result = SlackTableFormatter.progress_bar(5, 0, width=10)
        assert result == "░" * 10

    def test_value_exceeds_total(self):
        # Should cap at 100%
        result = SlackTableFormatter.progress_bar(15, 10, width=10)
        assert result == "█" * 10

    def test_bar_length_matches_width(self):
        for width in [5, 10, 20]:
            result = SlackTableFormatter.progress_bar(3, 10, width=width)
            assert len(result) == width

    def test_default_width(self):
        result = SlackTableFormatter.progress_bar(5, 10)
        assert len(result) == 10


# ---------------------------------------------------------------------------
# SlackNotificationFormatter
# ---------------------------------------------------------------------------


class TestJobStatusChange:
    """Tests for SlackNotificationFormatter.job_status_change()."""

    def setup_method(self):
        self.fmt = SlackNotificationFormatter()

    def test_contains_job_id(self):
        result = self.fmt.job_status_change(
            job_id=12345, name="train", old_status="RUNNING", new_status="COMPLETED"
        )
        assert "12345" in result

    def test_contains_job_name(self):
        result = self.fmt.job_status_change(
            job_id=1, name="my_job", old_status="PENDING", new_status="RUNNING"
        )
        assert "my_job" in result

    def test_contains_status_transition(self):
        result = self.fmt.job_status_change(
            job_id=1, name="job", old_status="PENDING", new_status="RUNNING"
        )
        assert "PENDING" in result
        assert "RUNNING" in result

    def test_success_emoji(self):
        result = self.fmt.job_status_change(
            job_id=1,
            name="job",
            old_status="RUNNING",
            new_status="COMPLETED",
            success=True,
        )
        assert "🎉" in result
        assert "Completed" in result

    def test_failure_emoji(self):
        result = self.fmt.job_status_change(
            job_id=1,
            name="job",
            old_status="RUNNING",
            new_status="FAILED",
            success=False,
        )
        assert "❌" in result
        assert "Failed" in result

    def test_optional_partition(self):
        result = self.fmt.job_status_change(
            job_id=1,
            name="job",
            old_status="RUNNING",
            new_status="COMPLETED",
            partition="gpu",
        )
        assert "gpu" in result

    def test_optional_runtime(self):
        result = self.fmt.job_status_change(
            job_id=1,
            name="job",
            old_status="RUNNING",
            new_status="COMPLETED",
            runtime="01:23:45",
        )
        assert "01:23:45" in result

    def test_optional_gpus(self):
        result = self.fmt.job_status_change(
            job_id=1,
            name="job",
            old_status="RUNNING",
            new_status="COMPLETED",
            gpus=4,
        )
        assert "4" in result

    def test_wrapped_in_code_block(self):
        result = self.fmt.job_status_change(
            job_id=1,
            name="job",
            old_status="RUNNING",
            new_status="COMPLETED",
        )
        assert result.startswith("```")
        assert result.endswith("```")

    def test_contains_header(self):
        result = self.fmt.job_status_change(
            job_id=1,
            name="job",
            old_status="RUNNING",
            new_status="COMPLETED",
        )
        assert "Job Status Update" in result


class TestResourceAvailable:
    """Tests for SlackNotificationFormatter.resource_available()."""

    def setup_method(self):
        self.fmt = SlackNotificationFormatter()

    def test_contains_partition(self):
        result = self.fmt.resource_available(
            partition="gpu",
            available_gpus=4,
            total_gpus=8,
            idle_nodes=2,
            total_nodes=4,
            utilization=50.0,
        )
        assert "gpu" in result

    def test_none_partition_shows_all(self):
        result = self.fmt.resource_available(
            partition=None,
            available_gpus=4,
            total_gpus=8,
            idle_nodes=2,
            total_nodes=4,
            utilization=50.0,
        )
        assert "all" in result

    def test_contains_gpu_counts(self):
        result = self.fmt.resource_available(
            partition="gpu",
            available_gpus=4,
            total_gpus=8,
            idle_nodes=2,
            total_nodes=4,
            utilization=50.0,
        )
        assert "4 / 8" in result

    def test_contains_node_counts(self):
        result = self.fmt.resource_available(
            partition="gpu",
            available_gpus=4,
            total_gpus=8,
            idle_nodes=2,
            total_nodes=4,
            utilization=50.0,
        )
        assert "2 / 4" in result

    def test_contains_utilization(self):
        result = self.fmt.resource_available(
            partition="gpu",
            available_gpus=4,
            total_gpus=8,
            idle_nodes=2,
            total_nodes=4,
            utilization=75.0,
        )
        assert "75%" in result

    def test_contains_progress_bar(self):
        result = self.fmt.resource_available(
            partition="gpu",
            available_gpus=4,
            total_gpus=8,
            idle_nodes=2,
            total_nodes=4,
            utilization=50.0,
        )
        assert "█" in result or "░" in result

    def test_contains_submit_prompt(self):
        result = self.fmt.resource_available(
            partition="gpu",
            available_gpus=4,
            total_gpus=8,
            idle_nodes=2,
            total_nodes=4,
            utilization=50.0,
        )
        assert "submit your jobs" in result

    def test_wrapped_in_code_block(self):
        result = self.fmt.resource_available(
            partition="gpu",
            available_gpus=4,
            total_gpus=8,
            idle_nodes=2,
            total_nodes=4,
            utilization=50.0,
        )
        assert result.startswith("```")
        assert result.endswith("```")

    def test_sanitizes_partition_name(self):
        result = self.fmt.resource_available(
            partition="<script>alert(1)</script>",
            available_gpus=4,
            total_gpus=8,
            idle_nodes=2,
            total_nodes=4,
            utilization=50.0,
        )
        assert "<script>" not in result
        assert "&lt;script&gt;" in result


class TestClusterStatus:
    """Tests for SlackNotificationFormatter.cluster_status()."""

    def setup_method(self):
        self.fmt = SlackNotificationFormatter()

    def test_contains_header(self):
        result = self.fmt.cluster_status()
        assert "SLURM Cluster Status" in result

    def test_job_stats(self):
        result = self.fmt.cluster_status(
            job_stats={"pending": 5, "running": 10},
        )
        assert "15" in result  # total_active = 5 + 10
        assert "5" in result  # pending
        assert "10" in result  # running

    def test_resource_stats(self):
        result = self.fmt.cluster_status(
            resource_stats={
                "total_gpus": 16,
                "gpus_in_use": 12,
                "gpus_available": 4,
                "nodes_idle": 1,
                "nodes_total": 4,
            },
        )
        assert "16" in result
        assert "12" in result
        assert "4" in result

    def test_resource_stats_with_partition(self):
        result = self.fmt.cluster_status(
            resource_stats={
                "total_gpus": 8,
                "gpus_in_use": 4,
                "gpus_available": 4,
                "partition": "gpu-a100",
                "nodes_idle": 2,
                "nodes_total": 4,
            },
        )
        assert "gpu-" in result  # partition name may be truncated by column width

    def test_resource_stats_utilization(self):
        result = self.fmt.cluster_status(
            resource_stats={
                "total_gpus": 10,
                "gpus_in_use": 5,
                "gpus_available": 5,
                "nodes_idle": 2,
                "nodes_total": 4,
            },
        )
        assert "50%" in result

    def test_running_jobs_with_timedelta(self):
        result = self.fmt.cluster_status(
            running_jobs=[
                {
                    "job_id": 100,
                    "name": "train_model",
                    "user": "alice",
                    "runtime": timedelta(hours=2, minutes=30),
                    "gpus": 4,
                },
            ],
        )
        assert "100" in result
        assert "train" in result  # may be truncated to 12 chars
        assert "alice" in result
        assert "02:30" in result

    def test_running_jobs_with_days(self):
        result = self.fmt.cluster_status(
            running_jobs=[
                {
                    "job_id": 200,
                    "name": "long_job",
                    "user": "bob",
                    "runtime": timedelta(days=3, hours=5, minutes=10),
                    "gpus": 2,
                },
            ],
        )
        assert "3d05:10" in result

    def test_running_jobs_no_runtime(self):
        result = self.fmt.cluster_status(
            running_jobs=[
                {
                    "job_id": 300,
                    "name": "new_job",
                    "user": "charlie",
                    "gpus": 1,
                },
            ],
        )
        assert "300" in result
        assert "-" in result  # runtime should show "-"

    def test_running_jobs_title(self):
        result = self.fmt.cluster_status(
            running_jobs=[
                {"job_id": 1, "name": "j", "user": "u", "gpus": 1},
                {"job_id": 2, "name": "j", "user": "u", "gpus": 1},
            ],
        )
        assert "Active Jobs (2)" in result

    def test_all_sections_combined(self):
        result = self.fmt.cluster_status(
            job_stats={"pending": 3, "running": 7},
            resource_stats={
                "total_gpus": 16,
                "gpus_in_use": 10,
                "gpus_available": 6,
                "nodes_idle": 2,
                "nodes_total": 4,
            },
            running_jobs=[
                {"job_id": 42, "name": "test", "user": "dev", "gpus": 2},
            ],
        )
        assert "10" in result  # total_active
        assert "16" in result  # total_gpus
        assert "42" in result  # job_id

    def test_explicit_timestamp(self):
        ts = datetime(2025, 6, 1, 12, 0, 0)
        result = self.fmt.cluster_status(timestamp=ts)
        assert "2025-06-01 12:00:00" in result

    def test_wrapped_in_code_block(self):
        result = self.fmt.cluster_status()
        assert result.startswith("```")
        assert result.endswith("```")

    def test_no_sections_still_valid(self):
        result = self.fmt.cluster_status()
        assert "SLURM Cluster Status" in result
        # Should not raise

    def test_sanitizes_job_name(self):
        result = self.fmt.cluster_status(
            running_jobs=[
                {"job_id": 1, "name": "<malicious>", "user": "u", "gpus": 1},
            ],
        )
        assert "<malicious>" not in result
        assert "&lt;" in result

    def test_sanitizes_user_name(self):
        result = self.fmt.cluster_status(
            running_jobs=[
                {"job_id": 1, "name": "j", "user": "a&b", "gpus": 1},
            ],
        )
        assert "a&b" not in result
        assert "&amp;" in result


class TestJobStatusReport:
    """Tests for SlackNotificationFormatter.job_status_report()."""

    def setup_method(self):
        self.fmt = SlackNotificationFormatter()

    def test_empty_jobs(self):
        result = self.fmt.job_status_report([])
        assert "No jobs to report" in result

    def test_contains_job_data(self):
        jobs = [
            {
                "id": 123,
                "name": "train",
                "status": "RUNNING",
                "runtime": "01:00",
                "gpus": 4,
            }
        ]
        result = self.fmt.job_status_report(jobs)
        assert "123" in result
        assert "train" in result
        assert "RUNNING" in result

    def test_table_title(self):
        jobs = [{"id": 1}, {"id": 2}]
        result = self.fmt.job_status_report(jobs)
        assert "Monitored Jobs (2)" in result

    def test_wrapped_in_code_block(self):
        result = self.fmt.job_status_report([])
        assert result.startswith("```")
        assert result.endswith("```")

    def test_explicit_timestamp(self):
        ts = datetime(2025, 1, 15, 8, 30, 0)
        result = self.fmt.job_status_report([], timestamp=ts)
        assert "2025-01-15 08:30:00" in result
