"""Data control and validation utilities.

This module provides functionality for:
- Numerical data validation
- Text processing and normalization
- Data structure manipulation
"""

from .number import is_lower, is_upper, is_in_range
from .text import normalize_txt_column, remove_accents, clean_text, check_emails_format
from .structures import convert_str_of_list_to_list, are_lists_egal

__all__ = [
    "is_lower",
    "is_upper",
    "is_in_range",
    "normalize_txt_column",
    "remove_accents",
    "clean_text",
    "check_emails_format",
    "convert_str_of_list_to_list",
    "are_lists_egal",
]
