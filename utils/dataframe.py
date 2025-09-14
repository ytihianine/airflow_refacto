import pandas as pd
import numpy as np

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 1000)


def df_info(
    df: pd.DataFrame, df_name: str, sample_size=5, full_logs: bool = True
) -> None:
    """
    Print important information about a Pandas DataFrame.

    Parameters:
        df (pd.DataFrame): The DataFrame to summarize.
        df_name (str): The title you want to show at top of the log.
        sample_size (int): Number of rows to show as a sample (default is 5).
    """
    col_names = list(df.columns)
    nb_none = len([x for x in col_names if x is None])
    if nb_none > 0:
        print(f"Warning: {nb_none} None values are in df column names !!")
    col_str_format = "\n".join(
        [
            ", ".join(str(col_name) for col_name in col_names[i : i + 5])
            for i in range(0, len(col_names), 5)
        ]
    )

    if full_logs:
        print(
            f"""
            ### {df_name} ### \n
            Shape of DataFrame: {df.shape} \n
            Columns: {col_str_format} \n
            Missing Values: \n{df.isnull().sum()} \n
            Columns data types: \n{df.dtypes} \n
            Descriptive Statistics (Numerical Columns): \n{df.describe()} \n
            Sample Data (First {sample_size} rows): \n{df.head(sample_size)} \n
            Memory Usage: {df.memory_usage(deep=True).sum() / 1048576} Mo
            """
        )
    else:
        print(
            f"""
            ### {df_name} ### \n
            Shape of DataFrame: {df.shape} \n
            Columns: {col_str_format} \n
            Missing Values: \n{df.isnull().sum()} \n
            Sample Data (First {sample_size} rows): \n{df.head(sample_size)} \n
            Memory Usage: {df.memory_usage(deep=True).sum() / 1048576} Mo
            """
        )


def tag_last_value_rows(df: pd.DataFrame, colname_max_value: str) -> pd.DataFrame:
    """
    The objectiv of this function is to tag every rows which contains the last values.
    The primary goal is to be able to filter the dataset on the new created column

    Args:
        df (pd.DataFrame): _description_
        colname_max_value (str): the name of the column to find the max value.

    Returns:
        pd.DataFrame: _description_
    """
    max_value = df[colname_max_value].max()
    df["is_last_value"] = np.where(df[colname_max_value] == max_value, True, False)

    return df
