"""Tests for ``srunx.slurm.local`` (and SSH adapter) log-streaming surfaces."""

from unittest.mock import patch

import pytest

from srunx.slurm.local import Slurm


class TestLogStreaming:
    """Test log streaming functionality for local SLURM."""

    def test_tail_log_static_mode(self, tmp_path, monkeypatch):
        """Test static log display mode."""
        # Create mock log file
        log_file = tmp_path / "job_123.log"
        log_file.write_text("Line 1\nLine 2\nLine 3\n")

        # Mock get_job_output_detailed to return our test file
        def mock_get_job_output_detailed(
            self, job_id, job_name=None, skip_content=False
        ):
            return {
                "found_files": [str(log_file)],
                "primary_log": str(log_file),
                "output": "" if skip_content else log_file.read_text(),
                "error": "",
                "slurm_log_dir": str(tmp_path),
                "searched_dirs": [str(tmp_path)],
            }

        monkeypatch.setattr(
            Slurm, "get_job_output_detailed", mock_get_job_output_detailed
        )

        client = Slurm()

        # Mock console for testing - patch where it's imported
        with patch("rich.console.Console") as MockConsole:
            mock_console = MockConsole.return_value

            # Test without last_n
            client.tail_log(job_id=123, job_name="test_job", follow=False)

            # Verify console.print was called
            assert mock_console.print.called

    def test_tail_log_with_last_n(self, tmp_path, monkeypatch):
        """Test static log display with last N lines."""
        # Create mock log file with multiple lines
        log_content = "\n".join([f"Line {i}" for i in range(1, 101)])
        log_file = tmp_path / "job_456.log"
        log_file.write_text(log_content)

        # Mock get_job_output_detailed
        def mock_get_job_output_detailed(
            self, job_id, job_name=None, skip_content=False
        ):
            return {
                "found_files": [str(log_file)],
                "primary_log": str(log_file),
                "output": "" if skip_content else log_file.read_text(),
                "error": "",
                "slurm_log_dir": str(tmp_path),
                "searched_dirs": [str(tmp_path)],
            }

        monkeypatch.setattr(
            Slurm, "get_job_output_detailed", mock_get_job_output_detailed
        )

        client = Slurm()

        with patch("rich.console.Console") as MockConsole:
            mock_console = MockConsole.return_value

            # Test with last_n=10
            client.tail_log(job_id=456, job_name="test_job", follow=False, last_n=10)

            assert mock_console.print.called

    def test_tail_log_no_file_found(self, monkeypatch):
        """Test log display when no log file is found."""

        # Mock get_job_output_detailed to return empty results
        def mock_get_job_output_detailed(
            self, job_id, job_name=None, skip_content=False
        ):
            return {
                "found_files": [],
                "primary_log": None,
                "output": "",
                "error": "",
                "slurm_log_dir": None,
                "searched_dirs": ["/tmp", "./"],
            }

        monkeypatch.setattr(
            Slurm, "get_job_output_detailed", mock_get_job_output_detailed
        )

        client = Slurm()

        with patch("rich.console.Console") as MockConsole:
            mock_console = MockConsole.return_value

            # Test when no log file found
            client.tail_log(job_id=789, job_name="missing_job", follow=False)

            # Verify error message was printed
            assert mock_console.print.called


class TestLastNOptimization:
    """Test that --last N reads files efficiently."""

    def test_tail_log_last_n_skips_content_read(self, tmp_path, monkeypatch):
        """Test that last_n mode skips full content read via skip_content."""
        log_file = tmp_path / "job_789.log"
        log_file.write_text("\n".join([f"Line {i}" for i in range(1, 51)]))

        skip_content_called_with = []

        def mock_get_job_output_detailed(
            self, job_id, job_name=None, skip_content=False
        ):
            skip_content_called_with.append(skip_content)
            return {
                "found_files": [str(log_file)],
                "primary_log": str(log_file),
                "output": "" if skip_content else log_file.read_text(),
                "error": "",
                "slurm_log_dir": str(tmp_path),
                "searched_dirs": [str(tmp_path)],
            }

        monkeypatch.setattr(
            Slurm, "get_job_output_detailed", mock_get_job_output_detailed
        )
        client = Slurm()

        with patch("rich.console.Console") as MockConsole:
            mock_console = MockConsole.return_value
            client.tail_log(job_id=789, follow=False, last_n=5)

        # Should have called with skip_content=True
        assert skip_content_called_with == [True]

    def test_tail_log_last_n_reads_correct_lines(self, tmp_path, monkeypatch):
        """Test that last_n reads exactly the last N lines."""
        lines = [f"Line {i}\n" for i in range(1, 101)]
        log_file = tmp_path / "job_100.log"
        log_file.write_text("".join(lines))

        def mock_get_job_output_detailed(
            self, job_id, job_name=None, skip_content=False
        ):
            return {
                "found_files": [str(log_file)],
                "primary_log": str(log_file),
                "output": "",
                "error": "",
                "slurm_log_dir": str(tmp_path),
                "searched_dirs": [str(tmp_path)],
            }

        monkeypatch.setattr(
            Slurm, "get_job_output_detailed", mock_get_job_output_detailed
        )
        client = Slurm()

        printed_texts = []
        with patch("rich.console.Console") as MockConsole:
            mock_console = MockConsole.return_value
            mock_console.print.side_effect = lambda *args, **kwargs: (
                printed_texts.append(args[0] if args else "")
            )
            client.tail_log(job_id=100, follow=False, last_n=3)

        # The second print call (after the file path) should contain last 3 lines
        output_text = printed_texts[1]  # index 0 is the file path line
        assert "Line 98" in output_text
        assert "Line 99" in output_text
        assert "Line 100" in output_text
        assert "Line 97" not in output_text

    def test_tail_log_static_no_last_n_reads_full_content(self, tmp_path, monkeypatch):
        """Test that static mode without last_n reads full content."""
        log_file = tmp_path / "job_200.log"
        log_file.write_text("Full content here\n")

        skip_content_called_with = []

        def mock_get_job_output_detailed(
            self, job_id, job_name=None, skip_content=False
        ):
            skip_content_called_with.append(skip_content)
            return {
                "found_files": [str(log_file)],
                "primary_log": str(log_file),
                "output": "" if skip_content else log_file.read_text(),
                "error": "",
                "slurm_log_dir": str(tmp_path),
                "searched_dirs": [str(tmp_path)],
            }

        monkeypatch.setattr(
            Slurm, "get_job_output_detailed", mock_get_job_output_detailed
        )
        client = Slurm()

        with patch("rich.console.Console") as MockConsole:
            mock_console = MockConsole.return_value
            client.tail_log(job_id=200, follow=False)

        # Should have called with skip_content=False
        assert skip_content_called_with == [False]


@pytest.mark.skip(reason="SSH tests require actual SSH connection")
class TestSSHLogStreaming:
    """Test SSH log streaming functionality (integration tests)."""

    def test_ssh_tail_log_static(self):
        """Test SSH static log display."""

        # This would require actual SSH connection
        # Skipped for unit testing
        pass

    def test_ssh_tail_log_follow(self):
        """Test SSH real-time log streaming."""

        # This would require actual SSH connection
        # Skipped for unit testing
        pass
