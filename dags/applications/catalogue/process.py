import pandas as pd


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.set_axis([colname.lower() for colname in df.columns], axis="columns")
    df = df.drop(df.filter(regex="^(grist|manual)").columns, axis=1)
    return df
