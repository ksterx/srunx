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
        webhook_url = "https://hooks.slack.com/services/test/webhook"
        callback = SlackCallback(webhook_url)

        assert callback.client is not None
        # Check that the client was initialized with the webhook URL
        assert hasattr(callback, "client")

    @patch("srunx.callbacks.WebhookClient")
    def test_slack_callback_init_with_mock(self, mock_webhook_client):
        """Test SlackCallback initialization with mock."""
        webhook_url = "https://hooks.slack.com/services/test/webhook"
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

        webhook_url = "https://hooks.slack.com/services/test/webhook"
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
        assert "Job test_job" in call_args[1]["blocks"][0]["text"]["text"]

    @patch("srunx.callbacks.WebhookClient")
    def test_on_job_failed(self, mock_webhook_client):
        """Test on_job_failed method."""
        mock_client = Mock()
        mock_webhook_client.return_value = mock_client

        webhook_url = "https://hooks.slack.com/services/test/webhook"
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
        assert "Job failed_job" in call_args[1]["blocks"][0]["text"]["text"]

    @patch("srunx.callbacks.WebhookClient")
    def test_on_job_completed_with_full_job(self, mock_webhook_client):
        """Test on_job_completed with a full Job object."""
        mock_client = Mock()
        mock_webhook_client.return_value = mock_client

        webhook_url = "https://hooks.slack.com/services/test/webhook"
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
        assert "Job ml_training" in call_args[1]["blocks"][0]["text"]["text"]

    @patch("srunx.callbacks.WebhookClient")
    def test_on_job_failed_with_full_job(self, mock_webhook_client):
        """Test on_job_failed with a full Job object."""
        mock_client = Mock()
        mock_webhook_client.return_value = mock_client

        webhook_url = "https://hooks.slack.com/services/test/webhook"
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
        assert "Job preprocessing" in call_args[1]["blocks"][0]["text"]["text"]

    @patch("srunx.callbacks.WebhookClient")
    def test_slack_callback_other_methods_not_implemented(self, mock_webhook_client):
        """Test that other callback methods are not implemented in SlackCallback."""
        mock_client = Mock()
        mock_webhook_client.return_value = mock_client

        webhook_url = "https://hooks.slack.com/services/test/webhook"
        callback = SlackCallback(webhook_url)

        job = BaseJob(name="test_job", job_id=12345)
        job.status = JobStatus.PENDING  # Set status to avoid refresh call

        # These methods should exist but do nothing (inherited from base Callback)
        callback.on_job_running(job)
        callback.on_job_cancelled(job)

        # Client should not have been called for these methods
        mock_client.send.assert_not_called()

    @patch("srunx.callbacks.WebhookClient")
    def test_slack_callback_handles_send_error(self, mock_webhook_client):
        """Test SlackCallback handles send errors gracefully."""
        mock_client = Mock()
        mock_client.send.side_effect = Exception("Network error")
        mock_webhook_client.return_value = mock_client

        webhook_url = "https://hooks.slack.com/services/test/webhook"
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

        webhook_url = "https://hooks.slack.com/services/test/webhook"
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
        assert "Job format_test_job" in block["text"]["text"]

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
        assert "Job format_test_job" in block["text"]["text"]

    @patch("srunx.callbacks.WebhookClient")
    def test_slack_callback_with_long_job_name(self, mock_webhook_client):
        """Test SlackCallback with very long job name."""
        mock_client = Mock()
        mock_webhook_client.return_value = mock_client

        webhook_url = "https://hooks.slack.com/services/test/webhook"
        callback = SlackCallback(webhook_url)

        long_name = (
            "very_long_job_name_that_might_cause_formatting_issues_in_slack_messages"
        )
        job = BaseJob(name=long_name, job_id=12345)
        job.status = JobStatus.COMPLETED  # Set status to avoid refresh call

        callback.on_job_completed(job)

        call_args = mock_client.send.call_args
        assert f"Job {long_name}" in call_args[1]["blocks"][0]["text"]["text"]
