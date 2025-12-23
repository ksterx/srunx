"""Tests for srunx.callbacks module."""

from unittest.mock import Mock, patch

import pytest

from srunx.callbacks import Callback, SlackCallback
from srunx.models import BaseJob, Job, JobEnvironment, JobStatus


class TestCallback:
    """Test base Callback class."""

    def test_callback_methods_exist(self):
        """Test that all callback methods exist and are callable."""
        callback = Callback()
        job = BaseJob(name="test_job", job_id=12345)

        # All methods should exist and not raise exceptions
        callback.on_job_submitted(job)
        callback.on_job_completed(job)
        callback.on_job_failed(job)
        callback.on_job_running(job)
        callback.on_job_cancelled(job)

    def test_callback_methods_do_nothing(self):
        """Test that base callback methods do nothing by default."""
        callback = Callback()
        job = BaseJob(name="test_job", job_id=12345)

        # Methods should return None and not raise exceptions
        assert callback.on_job_submitted(job) is None
        assert callback.on_job_completed(job) is None
        assert callback.on_job_failed(job) is None
        assert callback.on_job_running(job) is None
        assert callback.on_job_cancelled(job) is None


class TestSlackCallback:
    """Test SlackCallback class."""

    def test_slack_callback_init(self):
        """Test SlackCallback initialization."""
        webhook_url = "https://hooks.slack.com/services/T00/B00/XXX"
        callback = SlackCallback(webhook_url)

        assert callback.client is not None
        # Check that the client was initialized with the webhook URL
        assert hasattr(callback, "client")

    @patch("srunx.callbacks.WebhookClient")
    def test_slack_callback_init_with_mock(self, mock_webhook_client):
        """Test SlackCallback initialization with mock."""
        webhook_url = "https://hooks.slack.com/services/T00/B00/XXX"
        mock_client = Mock()
        mock_webhook_client.return_value = mock_client

        callback = SlackCallback(webhook_url)

        mock_webhook_client.assert_called_once_with(webhook_url)
        assert callback.client is mock_client

    @patch("srunx.callbacks.WebhookClient")
    def test_on_job_completed(self, mock_webhook_client):
        """Test on_job_completed method."""
        mock_client = Mock()
        mock_webhook_client.return_value = mock_client

        webhook_url = "https://hooks.slack.com/services/T00/B00/XXX"
        callback = SlackCallback(webhook_url)

        job = BaseJob(name="test_job", job_id=12345)
        job.status = JobStatus.COMPLETED

        callback.on_job_completed(job)

        # Check that the client.send was called
        mock_client.send.assert_called_once()

        # Check the call arguments
        call_args = mock_client.send.call_args
        assert call_args[1]["text"] == "Job completed"
        assert len(call_args[1]["blocks"]) == 1
        assert call_args[1]["blocks"][0]["type"] == "section"
        # Note: underscores are escaped in sanitization
        assert "Job test\\_job" in call_args[1]["blocks"][0]["text"]["text"]

    @patch("srunx.callbacks.WebhookClient")
    def test_on_job_failed(self, mock_webhook_client):
        """Test on_job_failed method."""
        mock_client = Mock()
        mock_webhook_client.return_value = mock_client

        webhook_url = "https://hooks.slack.com/services/T00/B00/XXX"
        callback = SlackCallback(webhook_url)

        job = BaseJob(name="failed_job", job_id=67890)
        job.status = JobStatus.FAILED

        callback.on_job_failed(job)

        # Check that the client.send was called
        mock_client.send.assert_called_once()

        # Check the call arguments
        call_args = mock_client.send.call_args
        assert call_args[1]["text"] == "Job failed"
        assert len(call_args[1]["blocks"]) == 1
        assert call_args[1]["blocks"][0]["type"] == "section"
        # Note: underscores are escaped in sanitization
        assert "Job failed\\_job" in call_args[1]["blocks"][0]["text"]["text"]

    @patch("srunx.callbacks.WebhookClient")
    def test_on_job_completed_with_full_job(self, mock_webhook_client):
        """Test on_job_completed with a full Job object."""
        mock_client = Mock()
        mock_webhook_client.return_value = mock_client

        webhook_url = "https://hooks.slack.com/services/T00/B00/XXX"
        callback = SlackCallback(webhook_url)

        job = Job(
            name="ml_training",
            command=["python", "train.py"],
            environment=JobEnvironment(conda="ml_env"),
            job_id=12345,
        )
        job.status = JobStatus.COMPLETED

        callback.on_job_completed(job)

        mock_client.send.assert_called_once()
        call_args = mock_client.send.call_args
        # Note: underscores are escaped in sanitization
        assert "Job ml\\_training" in call_args[1]["blocks"][0]["text"]["text"]

    @patch("srunx.callbacks.WebhookClient")
    def test_on_job_failed_with_full_job(self, mock_webhook_client):
        """Test on_job_failed with a full Job object."""
        mock_client = Mock()
        mock_webhook_client.return_value = mock_client

        webhook_url = "https://hooks.slack.com/services/T00/B00/XXX"
        callback = SlackCallback(webhook_url)

        job = Job(
            name="preprocessing",
            command=["python", "preprocess.py"],
            environment=JobEnvironment(venv="/path/to/venv"),
            job_id=67890,
        )
        job.status = JobStatus.FAILED

        callback.on_job_failed(job)

        mock_client.send.assert_called_once()
        call_args = mock_client.send.call_args
        # No underscores in "preprocessing", so no escaping needed
        assert "Job preprocessing" in call_args[1]["blocks"][0]["text"]["text"]

    @patch("srunx.callbacks.WebhookClient")
    def test_slack_callback_other_methods_implemented(self, mock_webhook_client):
        """Test that on_job_running and on_job_cancelled send notifications."""
        mock_client = Mock()
        mock_webhook_client.return_value = mock_client

        webhook_url = "https://hooks.slack.com/services/T00/B00/XXX"
        callback = SlackCallback(webhook_url)

        job = BaseJob(name="test_job", job_id=12345)
        job.status = JobStatus.RUNNING

        # Test on_job_running
        callback.on_job_running(job)
        assert mock_client.send.call_count == 1
        call_args = mock_client.send.call_args
        assert call_args[1]["text"] == "Job running"

        # Reset mock
        mock_client.reset_mock()

        # Test on_job_cancelled
        job.status = JobStatus.CANCELLED
        callback.on_job_cancelled(job)
        assert mock_client.send.call_count == 1
        call_args = mock_client.send.call_args
        assert call_args[1]["text"] == "Job cancelled"

    @patch("srunx.callbacks.WebhookClient")
    def test_slack_callback_handles_send_error(self, mock_webhook_client):
        """Test SlackCallback handles send errors gracefully."""
        mock_client = Mock()
        mock_client.send.side_effect = Exception("Network error")
        mock_webhook_client.return_value = mock_client

        webhook_url = "https://hooks.slack.com/services/T00/B00/XXX"
        callback = SlackCallback(webhook_url)

        job = BaseJob(name="test_job", job_id=12345)

        # Currently, SlackCallback doesn't handle errors gracefully
        # This would raise exceptions, which is the current behavior
        with pytest.raises(Exception):
            callback.on_job_completed(job)

        with pytest.raises(Exception):
            callback.on_job_failed(job)

    @patch("srunx.callbacks.WebhookClient")
    def test_slack_callback_message_format(self, mock_webhook_client):
        """Test SlackCallback message format details."""
        mock_client = Mock()
        mock_webhook_client.return_value = mock_client

        webhook_url = "https://hooks.slack.com/services/T00/B00/XXX"
        callback = SlackCallback(webhook_url)

        job = BaseJob(name="format_test_job", job_id=99999)
        job.status = JobStatus.COMPLETED  # Set status to avoid refresh call

        # Test completion message format
        callback.on_job_completed(job)

        call_args = mock_client.send.call_args
        blocks = call_args[1]["blocks"]

        assert len(blocks) == 1
        block = blocks[0]
        assert block["type"] == "section"
        assert block["text"]["type"] == "mrkdwn"
        # Note: underscores are escaped in sanitization
        assert "Job format\\_test\\_job" in block["text"]["text"]

        # Reset mock
        mock_client.reset_mock()

        # Test failure message format
        callback.on_job_failed(job)

        call_args = mock_client.send.call_args
        blocks = call_args[1]["blocks"]

        assert len(blocks) == 1
        block = blocks[0]
        assert block["type"] == "section"
        assert block["text"]["type"] == "mrkdwn"
        # Note: underscores are escaped in sanitization
        assert "Job format\\_test\\_job" in block["text"]["text"]

    @patch("srunx.callbacks.WebhookClient")
    def test_slack_callback_with_long_job_name(self, mock_webhook_client):
        """Test SlackCallback with very long job name."""
        mock_client = Mock()
        mock_webhook_client.return_value = mock_client

        webhook_url = "https://hooks.slack.com/services/T00/B00/XXX"
        callback = SlackCallback(webhook_url)

        long_name = (
            "very_long_job_name_that_might_cause_formatting_issues_in_slack_messages"
        )
        # Note: underscores will be escaped in sanitization
        escaped_name = long_name.replace("_", "\\_")
        job = BaseJob(name=long_name, job_id=12345)
        job.status = JobStatus.COMPLETED  # Set status to avoid refresh call

        callback.on_job_completed(job)

        call_args = mock_client.send.call_args
        assert f"Job {escaped_name}" in call_args[1]["blocks"][0]["text"]["text"]


class TestSlackCallbackSecurity:
    """Security-focused tests for SlackCallback."""

    def test_invalid_webhook_url_http_rejected(self):
        """Test that HTTP URLs are rejected (must be HTTPS)."""
        with pytest.raises(ValueError, match="Invalid Slack webhook URL"):
            SlackCallback("http://hooks.slack.com/services/T00/B00/XXX")

    def test_invalid_webhook_url_wrong_domain_rejected(self):
        """Test that wrong domain URLs are rejected."""
        with pytest.raises(ValueError, match="Invalid Slack webhook URL"):
            SlackCallback("https://example.com/webhook")

    def test_invalid_webhook_url_missing_services_rejected(self):
        """Test that URLs without /services/ are rejected."""
        with pytest.raises(ValueError, match="Invalid Slack webhook URL"):
            SlackCallback("https://hooks.slack.com/test/path")

    def test_invalid_webhook_url_too_few_segments_rejected(self):
        """Test that URLs with fewer than 3 segments are rejected."""
        invalid_urls = [
            "https://hooks.slack.com/services/A",  # Only 1 segment
            "https://hooks.slack.com/services/A/B",  # Only 2 segments
        ]
        for url in invalid_urls:
            with pytest.raises(ValueError, match="Invalid Slack webhook URL"):
                SlackCallback(url)

    def test_invalid_webhook_url_javascript_injection_rejected(self):
        """Test that JavaScript injection attempts are rejected."""
        with pytest.raises(ValueError, match="Invalid Slack webhook URL"):
            SlackCallback("javascript:alert(1)")

    def test_invalid_webhook_url_empty_string_rejected(self):
        """Test that empty strings are rejected."""
        with pytest.raises(ValueError, match="Invalid Slack webhook URL"):
            SlackCallback("")

    def test_invalid_webhook_url_not_url_rejected(self):
        """Test that non-URL strings are rejected."""
        with pytest.raises(ValueError, match="Invalid Slack webhook URL"):
            SlackCallback("not-a-url")

    @patch("srunx.callbacks.WebhookClient")
    def test_valid_webhook_url_accepted(self, mock_webhook_client):
        """Test that valid webhook URLs are accepted."""
        valid_urls = [
            "https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXX",
            "https://hooks.slack.com/services/T123ABC/B456DEF/abcdef1234567890",
            "https://hooks.slack.com/services/WORKSPACE_ID/CHANNEL_ID/TOKEN_HERE",
        ]
        for url in valid_urls:
            callback = SlackCallback(url)
            assert callback.client is not None

    def test_sanitize_html_script_injection(self):
        """Test that HTML script injection is prevented."""
        malicious_input = "<script>alert('xss')</script>"
        sanitized = SlackCallback._sanitize_text(malicious_input)
        # All angle brackets should be escaped
        assert "<" not in sanitized
        assert ">" not in sanitized
        assert "&lt;script&gt;" in sanitized

    def test_sanitize_html_img_injection(self):
        """Test that HTML img tag injection is prevented."""
        malicious_input = "<img src=x onerror=alert(1)>"
        sanitized = SlackCallback._sanitize_text(malicious_input)
        assert "<" not in sanitized
        assert ">" not in sanitized
        assert "&lt;img" in sanitized

    def test_sanitize_html_iframe_injection(self):
        """Test that iframe injection is prevented."""
        malicious_input = "test<iframe>evil</iframe>"
        sanitized = SlackCallback._sanitize_text(malicious_input)
        assert "<" not in sanitized
        assert ">" not in sanitized
        assert "iframe" in sanitized
        assert "&lt;" in sanitized

    def test_sanitize_markdown_backtick_injection(self):
        """Test that backtick code block injection is prevented."""
        malicious_input = "`malicious code`"
        sanitized = SlackCallback._sanitize_text(malicious_input)
        # Backticks should be replaced with single quotes
        assert "`" not in sanitized
        assert "'malicious code'" in sanitized

    def test_sanitize_markdown_bold_asterisk(self):
        """Test that markdown bold asterisks are escaped."""
        input_text = "*bold attack*"
        sanitized = SlackCallback._sanitize_text(input_text)
        # Asterisks should be escaped
        assert sanitized.count("\\*") == 2
        assert "\\*bold attack\\*" in sanitized

    def test_sanitize_markdown_italic_underscore(self):
        """Test that markdown italic underscores are escaped."""
        input_text = "_italic attack_"
        sanitized = SlackCallback._sanitize_text(input_text)
        # Underscores should be escaped
        assert sanitized.count("\\_") == 2
        assert "\\_italic attack\\_" in sanitized

    def test_sanitize_markdown_strikethrough_tilde(self):
        """Test that markdown strikethrough tildes are escaped."""
        input_text = "~strike attack~"
        sanitized = SlackCallback._sanitize_text(input_text)
        # Tildes should be escaped
        assert sanitized.count("\\~") == 2
        assert "\\~strike attack\\~" in sanitized

    def test_sanitize_markdown_link_brackets(self):
        """Test that markdown link brackets are escaped."""
        input_text = "[link](http://evil.com)"
        sanitized = SlackCallback._sanitize_text(input_text)
        # Square brackets should be escaped with backslashes
        assert "\\[" in sanitized
        assert "\\]" in sanitized
        assert "\\[link\\]" in sanitized

    def test_sanitize_control_characters_newline(self):
        """Test that newline characters are removed."""
        text_with_newline = "line1\nline2"
        sanitized = SlackCallback._sanitize_text(text_with_newline)
        assert "\n" not in sanitized
        assert "line1 line2" in sanitized

    def test_sanitize_control_characters_carriage_return(self):
        """Test that carriage return characters are removed."""
        text_with_cr = "line1\rline2"
        sanitized = SlackCallback._sanitize_text(text_with_cr)
        assert "\r" not in sanitized
        assert "line1 line2" in sanitized

    def test_sanitize_control_characters_tab(self):
        """Test that tab characters are removed."""
        text_with_tab = "col1\tcol2"
        sanitized = SlackCallback._sanitize_text(text_with_tab)
        assert "\t" not in sanitized
        assert "col1 col2" in sanitized

    def test_sanitize_control_characters_all_combined(self):
        """Test that all control characters are handled together."""
        text_with_controls = "line1\nline2\rline3\ttab"
        sanitized = SlackCallback._sanitize_text(text_with_controls)
        assert "\n" not in sanitized
        assert "\r" not in sanitized
        assert "\t" not in sanitized
        assert "line1 line2 line3 tab" in sanitized

    def test_sanitize_length_limit_short_text(self):
        """Test that short text is not truncated."""
        short_text = "A" * 100
        sanitized = SlackCallback._sanitize_text(short_text)
        assert len(sanitized) == 100
        assert "..." not in sanitized

    def test_sanitize_length_limit_exact_limit(self):
        """Test that text at exactly 1000 chars is not truncated."""
        text_1000 = "A" * 1000
        sanitized = SlackCallback._sanitize_text(text_1000)
        assert len(sanitized) == 1000
        assert "..." not in sanitized

    def test_sanitize_length_limit_exceeds_limit(self):
        """Test that text exceeding 1000 chars is truncated."""
        long_text = "A" * 2000
        sanitized = SlackCallback._sanitize_text(long_text)
        assert len(sanitized) == 1003  # 1000 + '...'
        assert sanitized.endswith("...")
        assert sanitized.startswith("A")

    def test_sanitize_ampersand_escaping_order(self):
        """Test that ampersand is escaped first to avoid double-escaping."""
        text_with_ampersand = "A&B<C>D"
        sanitized = SlackCallback._sanitize_text(text_with_ampersand)
        # Ampersand should be escaped to &amp; first
        # Then < and > are escaped to &lt; and &gt;
        # We should NOT see &amp;lt; (double-escaped)
        assert "&amp;" in sanitized
        assert "&lt;" in sanitized
        assert "&gt;" in sanitized
        assert "&amp;lt;" not in sanitized  # No double-escaping
        assert sanitized == "A&amp;B&lt;C&gt;D"

    def test_sanitize_combined_attack_vectors(self):
        """Test multiple attack vectors combined."""
        malicious_input = (
            "<script>alert('xss')</script>\n`code`\n*bold*_italic_~strike~[link](url)"
        )
        sanitized = SlackCallback._sanitize_text(malicious_input)
        # Dangerous raw characters should not be present
        assert "<" not in sanitized
        assert ">" not in sanitized
        assert "`" not in sanitized
        assert "\n" not in sanitized
        # Should contain escaped/replaced versions
        assert "&lt;" in sanitized  # HTML tags escaped
        assert "&gt;" in sanitized  # HTML tags escaped
        assert "\\*" in sanitized  # Markdown bold escaped
        assert "\\_" in sanitized  # Markdown italic escaped
        assert "\\[" in sanitized  # Markdown link brackets escaped
        assert "\\]" in sanitized  # Markdown link brackets escaped
