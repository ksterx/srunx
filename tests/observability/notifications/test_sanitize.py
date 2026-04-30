"""Tests for :func:`srunx.observability.notifications.sanitize.sanitize_slack_text`.

Covers every entry of the replacement table, control-character handling
and the 1000-character truncation. These tests live separately from the
older ``tests/test_callbacks.py`` sanitize coverage so refactoring /
removing the ``SlackCallback._sanitize_text`` wrapper later does not
break the shared utility's test surface.
"""

from __future__ import annotations

from srunx.observability.notifications.sanitize import sanitize_slack_text


class TestSanitizeSlackText:
    """Replacement-table coverage."""

    def test_ampersand_escaped_first(self) -> None:
        # `&` must be escaped before `<`/`>` to avoid producing `&amp;lt;`.
        assert sanitize_slack_text("A&B<C>D") == "A&amp;B&lt;C&gt;D"
        assert "&amp;lt;" not in sanitize_slack_text("<&")

    def test_lt_escaped(self) -> None:
        assert "&lt;" in sanitize_slack_text("<script>")
        assert "<" not in sanitize_slack_text("<")

    def test_gt_escaped(self) -> None:
        assert "&gt;" in sanitize_slack_text("a > b")
        assert ">" not in sanitize_slack_text(">")

    def test_backtick_replaced_with_single_quote(self) -> None:
        assert sanitize_slack_text("`code`") == "'code'"
        assert "`" not in sanitize_slack_text("``")

    def test_asterisk_escaped(self) -> None:
        assert sanitize_slack_text("*bold*") == "\\*bold\\*"

    def test_underscore_escaped(self) -> None:
        assert sanitize_slack_text("_italic_") == "\\_italic\\_"

    def test_tilde_escaped(self) -> None:
        assert sanitize_slack_text("~strike~") == "\\~strike\\~"

    def test_square_brackets_escaped(self) -> None:
        result = sanitize_slack_text("[link](url)")
        assert "\\[" in result
        assert "\\]" in result
        assert "[" not in result.replace("\\[", "")
        assert "]" not in result.replace("\\]", "")


class TestControlCharacters:
    """Newline / carriage return / tab handling."""

    def test_newline_replaced_with_space(self) -> None:
        result = sanitize_slack_text("a\nb")
        assert "\n" not in result
        assert result == "a b"

    def test_carriage_return_replaced_with_space(self) -> None:
        result = sanitize_slack_text("a\rb")
        assert "\r" not in result
        assert result == "a b"

    def test_tab_replaced_with_space(self) -> None:
        result = sanitize_slack_text("a\tb")
        assert "\t" not in result
        assert result == "a b"

    def test_mixed_control_characters(self) -> None:
        result = sanitize_slack_text("x\ny\rz\tw")
        assert result == "x y z w"


class TestTruncation:
    """Length clamp at 1000 characters."""

    def test_short_text_not_truncated(self) -> None:
        result = sanitize_slack_text("A" * 100)
        assert len(result) == 100
        assert not result.endswith("...")

    def test_exactly_1000_not_truncated(self) -> None:
        text = "A" * 1000
        result = sanitize_slack_text(text)
        assert len(result) == 1000
        assert not result.endswith("...")

    def test_over_1000_truncated(self) -> None:
        text = "A" * 2000
        result = sanitize_slack_text(text)
        # Truncated to 1000 + "..." suffix.
        assert len(result) == 1003
        assert result.endswith("...")
        # Prefix is intact.
        assert result.startswith("A")


class TestEmptyAndIdentity:
    """Edge cases."""

    def test_empty_string(self) -> None:
        assert sanitize_slack_text("") == ""

    def test_plain_text_unchanged(self) -> None:
        assert sanitize_slack_text("hello world") == "hello world"
