import pandas as pd


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.set_axis([colname.lower() for colname in df.columns], axis="columns")
    return df
