import numpy as np
import pandas as pd
from pandas._typing import DateTimeErrorChoices


def convert_to_numeric(
    df: pd.DataFrame,
    columns: list[str],
    errors: DateTimeErrorChoices,
) -> pd.DataFrame:
    if isinstance(columns, str):
        columns = [columns]

    for date_col in columns:
        df[date_col] = pd.to_numeric(
            arg=df.loc[:, date_col],
            errors=errors,
        )

    return df


def is_lower(
    df: pd.DataFrame,
    cols_to_check: list[str],
    seuil: float | int,
    inclusive=True,
) -> np.bool_:
    """
    Checks if all values are below seuil.
    """
    df_inf = df[cols_to_check].le(seuil) if inclusive else df[cols_to_check].lt(seuil)

    return df_inf.all(axis=None)


def is_upper(df: pd.DataFrame, cols_to_check: list[str], seuil: float | int, inclusive=True) -> np.bool_:
    """
    Checks if all values are above seuil.
    """
    df_sup = df[cols_to_check].ge(seuil) if inclusive else df[cols_to_check].gt(seuil)

    return df_sup.all(axis=None)


def is_in_range(
    df: pd.DataFrame,
    cols_to_check: list[str],
    seuil_inf: float | int,
    seuil_sup: float | int,
    inclusive=True,
) -> np.bool_:
    """
    Checks if all values are within a given range.
    """
    if inclusive:
        df_inf = df[cols_to_check].le(seuil_sup)
        df_sup = df[cols_to_check].ge(seuil_inf)
    else:
        df_inf = df[cols_to_check].lt(seuil_sup)
        df_sup = df[cols_to_check].gt(seuil_inf)

    df_union = df_inf & df_sup

    return df_union.all(axis=None)
