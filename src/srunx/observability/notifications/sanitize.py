"""Shared Slack text sanitization utility.

Extracted verbatim from :meth:`srunx.observability.notifications.legacy_slack.SlackCallback._sanitize_text`
so both the CLI-side ``SlackCallback`` and the new
``SlackWebhookAdapter`` can share a single implementation.
"""

from __future__ import annotations

import re

# Canonical Slack Incoming Webhook URL pattern. Shared by the Web endpoint
# router (create/update), the delivery adapter (send time), and the config
# bootstrap migration so every path that stores or POSTs a webhook applies the
# same anti-SSRF check.
SLACK_WEBHOOK_URL_RE = re.compile(
    r"^https://hooks\.slack\.com/services/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+$"
)


def is_valid_slack_webhook_url(url: object) -> bool:
    """Return True iff ``url`` is a well-formed Slack Incoming Webhook URL."""
    return isinstance(url, str) and SLACK_WEBHOOK_URL_RE.match(url) is not None


def sanitize_slack_text(text: str) -> str:
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
