"""Numerical data validation utilities for DataFrames.

This module provides functions for validating numerical data in pandas DataFrames,
including range checks and threshold validations.
"""

import pandas as pd
from typing import List, Union


def is_lower(
    df: pd.DataFrame,
    cols_to_check: List[str],
    seuil: Union[float, int],
    inclusive: bool = True,
) -> bool:
    """Check if all values in specified columns are below a threshold.

    Args:
        df: DataFrame to check
        cols_to_check: List of column names to validate
        seuil: Threshold value for comparison
        inclusive: If True, uses <= (less than or equal), if False uses < (less than)

    Returns:
        True if all values are below threshold, False otherwise

    Raises:
        KeyError: If any column in cols_to_check is not found in DataFrame
        TypeError: If df is not a pandas DataFrame
        ValueError: If cols_to_check is empty
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected pandas DataFrame, got {type(df)}")

    if not cols_to_check:
        raise ValueError("cols_to_check cannot be empty")

    # Check if all columns exist
    missing_cols = [col for col in cols_to_check if col not in df.columns]
    if missing_cols:
        raise KeyError(f"Columns not found in DataFrame: {missing_cols}")

    if inclusive:
        df_comparison = df[cols_to_check].le(seuil)
    else:
        df_comparison = df[cols_to_check].lt(seuil)

    return df_comparison.all(axis=None)


def is_upper(
    df: pd.DataFrame,
    cols_to_check: List[str],
    seuil: Union[float, int],
    inclusive: bool = True,
) -> bool:
    """Check if all values in specified columns are above a threshold.

    Args:
        df: DataFrame to check
        cols_to_check: List of column names to validate
        seuil: Threshold value for comparison
        inclusive: If True, uses >= (greater than or equal), if False uses > (greater than)

    Returns:
        True if all values are above threshold, False otherwise

    Raises:
        KeyError: If any column in cols_to_check is not found in DataFrame
        TypeError: If df is not a pandas DataFrame
        ValueError: If cols_to_check is empty
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected pandas DataFrame, got {type(df)}")

    if not cols_to_check:
        raise ValueError("cols_to_check cannot be empty")

    # Check if all columns exist
    missing_cols = [col for col in cols_to_check if col not in df.columns]
    if missing_cols:
        raise KeyError(f"Columns not found in DataFrame: {missing_cols}")

    if inclusive:
        df_comparison = df[cols_to_check].ge(seuil)
    else:
        df_comparison = df[cols_to_check].gt(seuil)

    return df_comparison.all(axis=None)


def is_in_range(
    df: pd.DataFrame,
    cols_to_check: List[str],
    seuil_inf: Union[float, int],
    seuil_sup: Union[float, int],
    inclusive: bool = True,
) -> bool:
    """Check if all values in specified columns are within a given range.

    Args:
        df: DataFrame to check
        cols_to_check: List of column names to validate
        seuil_inf: Lower bound of the range
        seuil_sup: Upper bound of the range
        inclusive: If True, uses <= and >= (inclusive bounds), if False uses < and > (exclusive bounds)

    Returns:
        True if all values are within range, False otherwise

    Raises:
        KeyError: If any column in cols_to_check is not found in DataFrame
        TypeError: If df is not a pandas DataFrame
        ValueError: If cols_to_check is empty or if seuil_inf > seuil_sup
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected pandas DataFrame, got {type(df)}")

    if not cols_to_check:
        raise ValueError("cols_to_check cannot be empty")

    if seuil_inf > seuil_sup:
        raise ValueError(
            f"Lower bound ({seuil_inf}) cannot be greater than upper bound ({seuil_sup})"
        )

    # Check if all columns exist
    missing_cols = [col for col in cols_to_check if col not in df.columns]
    if missing_cols:
        raise KeyError(f"Columns not found in DataFrame: {missing_cols}")

    if inclusive:
        df_lower_bound = df[cols_to_check].ge(seuil_inf)
        df_upper_bound = df[cols_to_check].le(seuil_sup)
    else:
        df_lower_bound = df[cols_to_check].gt(seuil_inf)
        df_upper_bound = df[cols_to_check].lt(seuil_sup)

    df_in_range = df_lower_bound & df_upper_bound

    return df_in_range.all(axis=None)
