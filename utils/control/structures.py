"""Data structure manipulation utilities.

This module provides utilities for working with data structures including
list conversions and comparison functions.
"""

import ast
from typing import List, Set, Union

import pandas as pd


def convert_str_of_list_to_list(df: pd.DataFrame, col_to_convert: str) -> pd.DataFrame:
    """Convert string representations of lists to actual lists in a DataFrame column.

    Takes a DataFrame with a column containing string representations of lists
    (e.g., "[1, 2, 3]") and converts them to actual Python lists.

    Args:
        df: DataFrame containing the column to convert
        col_to_convert: Name of the column to convert

    Returns:
        DataFrame with the specified column converted to lists

    Raises:
        KeyError: If col_to_convert is not found in DataFrame
        TypeError: If df is not a pandas DataFrame
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected pandas DataFrame, got {type(df)}")

    if col_to_convert not in df.columns:
        raise KeyError(
            f"Column '{col_to_convert}' not found in DataFrame. "
            f"Available columns: {list(df.columns)}"
        )

    # Create a copy to avoid modifying the original DataFrame
    result_df = df.copy()

    def safe_literal_eval(x):
        """Safely evaluate string representations of lists."""
        if isinstance(x, str):
            try:
                return ast.literal_eval(x)
            except (ValueError, SyntaxError):
                # Return original value if it can't be parsed
                return x
        return x

    result_df[col_to_convert] = result_df[col_to_convert].apply(safe_literal_eval)
    return result_df


def are_lists_equal(list_a: List[str], list_b: List[str]) -> bool:
    """Compare two lists and check if they contain the same elements.

    Compares two lists by converting them to sets and checking for differences.
    Prints detailed information about differences found.

    Args:
        list_a: First list to compare
        list_b: Second list to compare

    Returns:
        True if lists contain the same elements (order doesn't matter), False otherwise

    Raises:
        TypeError: If inputs are not lists
    """
    if not isinstance(list_a, list):
        raise TypeError(f"list_a must be a list, got {type(list_a)}")
    if not isinstance(list_b, list):
        raise TypeError(f"list_b must be a list, got {type(list_b)}")

    # Convert to sets for comparison
    set_a = set(list_a)
    set_b = set(list_b)

    # Elements in A but not in B
    only_in_a = list(set_a - set_b)
    # Elements in B but not in A
    only_in_b = list(set_b - set_a)

    if len(only_in_b) == 0 and len(only_in_a) == 0:
        print("The lists contain identical elements")
        return True

    if len(only_in_a) > 0:
        print(f"Elements present in first list but not in second: \n{only_in_a}")
    if len(only_in_b) > 0:
        print(f"Elements present in second list but not in first: \n{only_in_b}")

    return False


# Keep the old function name for backward compatibility
are_lists_egal = are_lists_equal
