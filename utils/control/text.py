import pandas as pd


def normalize_txt_column(series: pd.Series) -> pd.Series:
    return series.str.strip().str.split().str.join(" ")
