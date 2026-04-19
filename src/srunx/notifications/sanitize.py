"""Shared Slack text sanitization utility.

Extracted verbatim from :meth:`srunx.callbacks.SlackCallback._sanitize_text`
so both the CLI-side ``SlackCallback`` and the new
``SlackWebhookDeliveryAdapter`` can share a single implementation.
"""

from __future__ import annotations


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
