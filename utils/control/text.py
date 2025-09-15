"""Text cleaning and normalization utilities.

This module provides utilities for text processing including normalization,
accent removal, and email validation.
"""

import re
import unicodedata
from typing import List, Optional

import pandas as pd


def normalize_txt_column(series: pd.Series) -> pd.Series:
    """Normalize text in a pandas Series by removing extra whitespace.

    Removes leading/trailing whitespace and normalizes internal whitespace
    to single spaces between words.

    Args:
        series: Input text Series to normalize

    Returns:
        Normalized text Series with single spaces between words

    Raises:
        TypeError: If input is not a pandas Series
    """
    if not isinstance(series, pd.Series):
        raise TypeError(f"Expected pandas Series, got {type(series)}")

    return series.str.strip().str.split().str.join(" ")


def remove_accents(text: str) -> str:
    """Remove accents from text while preserving base characters.

    Uses Unicode normalization to decompose characters with accents
    and removes combining characters.

    Args:
        text: Input text to process

    Returns:
        Text with accents removed

    Raises:
        TypeError: If text is not a string

    Example:
        >>> remove_accents("été")
        "ete"
        >>> remove_accents("café")
        "cafe"
    """
    if not isinstance(text, str):
        raise TypeError(f"Expected string, got {type(text)}")

    return "".join(
        c for c in unicodedata.normalize("NFKD", text) if not unicodedata.combining(c)
    )


def clean_text(
    text: str,
    lower: bool = True,
    remove_accents_: bool = True,
    remove_numbers: bool = False,
    remove_punctuation: bool = True,
    remove_extra_spaces: bool = True,
) -> str:
    """Clean text by applying various normalization steps.

    Applies multiple text cleaning operations in sequence to normalize
    text for analysis or comparison.

    Args:
        text: Input text to clean
        lower: Convert to lowercase
        remove_accents_: Remove accents from characters
        remove_numbers: Remove numeric digits
        remove_punctuation: Remove punctuation marks
        remove_extra_spaces: Normalize whitespace to single spaces

    Returns:
        Cleaned and normalized text

    Raises:
        TypeError: If text is not a string
    """
    if not isinstance(text, str):
        return ""

    if lower:
        text = text.lower()

    if remove_accents_:
        text = remove_accents(text)

    if remove_numbers:
        text = re.sub(r"\d+", "", text)

    if remove_punctuation:
        text = re.sub(r"[^\w\s]", "", text)

    if remove_extra_spaces:
        text = " ".join(text.split())

    return text.strip()


def check_emails_format(text: str) -> List[str]:
    """Extract valid email addresses from text.

    Uses regular expression pattern matching to find email addresses
    in the input text.

    Args:
        text: Input text to search for emails

    Returns:
        List of email addresses found in the text

    Raises:
        TypeError: If text is not a string

    Example:
        >>> check_emails_format("Contact us at info@example.com or support@test.org")
        ["info@example.com", "support@test.org"]
    """
    if not isinstance(text, str):
        raise TypeError(f"Expected string, got {type(text)}")

    email_pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
    return re.findall(email_pattern, text)
