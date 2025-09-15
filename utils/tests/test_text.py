"""Tests for text processing utilities."""

import pytest
import pandas as pd

from utils.control.text import (
    normalize_txt_column,
    remove_accents,
    clean_text,
    check_emails_format,
)


class TestNormalizeTxtColumn:
    """Test suite for normalize_txt_column function."""

    def test_normalize_txt_column_basic(self):
        """Test basic text normalization."""
        series = pd.Series(["  hello   world  ", " test ", "normal"])
        result = normalize_txt_column(series)

        expected = pd.Series(["hello world", "test", "normal"])
        pd.testing.assert_series_equal(result, expected)

    def test_normalize_txt_column_invalid_input(self):
        """Test with invalid input type."""
        with pytest.raises(TypeError, match="Expected pandas Series"):
            normalize_txt_column("not a series")

    def test_normalize_txt_column_empty_series(self):
        """Test with empty series."""
        series = pd.Series([])
        result = normalize_txt_column(series)
        assert len(result) == 0

    def test_normalize_txt_column_with_nan(self):
        """Test with NaN values."""
        series = pd.Series(["  hello  ", None, "  world  "])
        result = normalize_txt_column(series)

        # NaN should remain NaN
        assert result.iloc[0] == "hello"
        assert pd.isna(result.iloc[1])
        assert result.iloc[2] == "world"


class TestRemoveAccents:
    """Test suite for remove_accents function."""

    def test_remove_accents_basic(self):
        """Test basic accent removal."""
        assert remove_accents("été") == "ete"
        assert remove_accents("café") == "cafe"
        assert remove_accents("naïve") == "naive"

    def test_remove_accents_no_accents(self):
        """Test with text without accents."""
        assert remove_accents("hello world") == "hello world"

    def test_remove_accents_empty_string(self):
        """Test with empty string."""
        assert remove_accents("") == ""

    def test_remove_accents_invalid_input(self):
        """Test with invalid input type."""
        with pytest.raises(TypeError, match="Expected string"):
            remove_accents(123)

    def test_remove_accents_mixed_characters(self):
        """Test with mixed characters."""
        assert remove_accents("Café123!@#") == "Cafe123!@#"


class TestCleanText:
    """Test suite for clean_text function."""

    def test_clean_text_all_options(self):
        """Test with all cleaning options enabled."""
        text = "  Hello CAFÉ! 123  "
        result = clean_text(text)
        assert result == "hello cafe 123"

    def test_clean_text_lowercase_only(self):
        """Test with only lowercase option."""
        text = "Hello WORLD"
        result = clean_text(
            text,
            lower=True,
            remove_accents_=False,
            remove_numbers=False,
            remove_punctuation=False,
            remove_extra_spaces=False,
        )
        assert result == "hello world"

    def test_clean_text_remove_numbers(self):
        """Test number removal."""
        text = "Hello123World456"
        result = clean_text(text, remove_numbers=True, remove_punctuation=False)
        assert result == "helloworld"

    def test_clean_text_keep_punctuation(self):
        """Test keeping punctuation."""
        text = "Hello, World!"
        result = clean_text(text, remove_punctuation=False)
        assert result == "hello, world!"

    def test_clean_text_non_string_input(self):
        """Test with non-string input."""
        result = clean_text(123)
        assert result == ""

    def test_clean_text_empty_string(self):
        """Test with empty string."""
        result = clean_text("")
        assert result == ""


class TestCheckEmailsFormat:
    """Test suite for check_emails_format function."""

    def test_check_emails_format_single_email(self):
        """Test with single email."""
        text = "Contact us at info@example.com"
        result = check_emails_format(text)
        assert result == ["info@example.com"]

    def test_check_emails_format_multiple_emails(self):
        """Test with multiple emails."""
        text = "Contact info@example.com or support@test.org for help"
        result = check_emails_format(text)
        assert result == ["info@example.com", "support@test.org"]

    def test_check_emails_format_no_emails(self):
        """Test with no emails."""
        text = "This text has no emails"
        result = check_emails_format(text)
        assert result == []

    def test_check_emails_format_invalid_emails(self):
        """Test with invalid email formats."""
        text = "Invalid emails: @example.com, test@, incomplete@"
        result = check_emails_format(text)
        assert result == []

    def test_check_emails_format_complex_emails(self):
        """Test with complex but valid emails."""
        text = "user.name+tag@example-domain.co.uk"
        result = check_emails_format(text)
        assert result == ["user.name+tag@example-domain.co.uk"]

    def test_check_emails_format_invalid_input(self):
        """Test with invalid input type."""
        with pytest.raises(TypeError, match="Expected string"):
            check_emails_format(123)

    def test_check_emails_format_empty_string(self):
        """Test with empty string."""
        result = check_emails_format("")
        assert result == []
