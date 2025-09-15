"""DataFrame utility functions for data analysis and processing.

This module provides utility functions for analyzing and processing
pandas DataFrames, including information display and data tagging.
"""

import pandas as pd
import numpy as np
from typing import Optional

# Configure pandas display options for better output
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 1000)


def df_info(
    df: pd.DataFrame, df_name: str, sample_size: int = 5, full_logs: bool = True
) -> None:
    """Print comprehensive information about a pandas DataFrame.

    Args:
        df: The DataFrame to analyze
        df_name: A descriptive name for the DataFrame (used in output)
        sample_size: Number of rows to display as sample (default: 5)
        full_logs: If True, includes descriptive statistics and data types

    Raises:
        TypeError: If df is not a pandas DataFrame
        ValueError: If sample_size is negative
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected pandas DataFrame, got {type(df)}")

    if sample_size < 0:
        raise ValueError("sample_size must be non-negative")

    # Check for None values in column names
    col_names = list(df.columns)
    nb_none = sum(1 for x in col_names if x is None)
    if nb_none > 0:
        print(f"Warning: {nb_none} None values found in DataFrame column names!")

    # Format column names for display (5 columns per line)
    col_str_format = "\n".join(
        [
            ", ".join(str(col_name) for col_name in col_names[i : i + 5])
            for i in range(0, len(col_names), 5)
        ]
    )

    # Calculate memory usage in MB
    memory_usage_mb = df.memory_usage(deep=True).sum() / 1048576

    if full_logs:
        print(
            f"""
            ### {df_name} ### 

            Shape of DataFrame: {df.shape} 

            Columns: {col_str_format} 

            Missing Values: 
{df.isnull().sum()} 

            Columns data types: 
{df.dtypes} 

            Descriptive Statistics (Numerical Columns): 
{df.describe()} 

            Sample Data (First {sample_size} rows): 
{df.head(sample_size)} 

            Memory Usage: {memory_usage_mb:.2f} MB
            """
        )
    else:
        print(
            f"""
            ### {df_name} ### 

            Shape of DataFrame: {df.shape} 

            Columns: {col_str_format} 

            Missing Values: 
{df.isnull().sum()} 

            Sample Data (First {sample_size} rows): 
{df.head(sample_size)} 

            Memory Usage: {memory_usage_mb:.2f} MB
            """
        )


def tag_last_value_rows(df: pd.DataFrame, colname_max_value: str) -> pd.DataFrame:
    """Tag rows containing the maximum value in a specified column.

    This function adds a boolean column 'is_last_value' that indicates
    whether each row contains the maximum value for the specified column.
    Useful for filtering datasets to the most recent values.

    Args:
        df: Input DataFrame to process
        colname_max_value: Name of the column to find the maximum value in

    Returns:
        DataFrame with added 'is_last_value' boolean column

    Raises:
        KeyError: If colname_max_value is not found in DataFrame columns
        TypeError: If df is not a pandas DataFrame
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected pandas DataFrame, got {type(df)}")

    if colname_max_value not in df.columns:
        raise KeyError(
            f"Column '{colname_max_value}' not found in DataFrame. "
            f"Available columns: {list(df.columns)}"
        )

    # Create a copy to avoid modifying the original DataFrame
    result_df = df.copy()

    try:
        max_value = result_df[colname_max_value].max()
        result_df["is_last_value"] = np.where(
            result_df[colname_max_value] == max_value, True, False
        )
    except Exception as e:
        raise ValueError(
            f"Error calculating maximum for column '{colname_max_value}': {e}"
        )

    return result_df
