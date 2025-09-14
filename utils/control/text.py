"""Text cleaning and normalization utilities."""

import re
import unicodedata
from typing import List, Optional, Set, Union

import pandas as pd


def normalize_txt_column(series: pd.Series) -> pd.Series:
    """Normalize text in a pandas Series by removing extra whitespace.

    Args:
        series: Input text Series

    Returns:
        Normalized text Series with single spaces between words
    """
    return series.str.strip().str.split().str.join(" ")


def remove_accents(text: str) -> str:
    """Remove accents from text while preserving base characters.

    Args:
        text: Input text

    Returns:
        Text with accents removed

    Example:
        >>> remove_accents("été")
        "ete"
    """
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

    Args:
        text: Input text to clean
        lower: Convert to lowercase
        remove_accents_: Remove accents from characters
        remove_numbers: Remove numeric digits
        remove_punctuation: Remove punctuation marks
        remove_extra_spaces: Normalize whitespace

    Returns:
        Cleaned text
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
    """Extract email addresses from text.

    Args:
        text: Input text

    Returns:
        List of email addresses found
    """
    email_pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
    return re.findall(email_pattern, text)
