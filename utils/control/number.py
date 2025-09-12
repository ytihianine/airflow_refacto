import pandas as pd


def is_lower(
    df: pd.DataFrame,
    cols_to_check: list[str],
    seuil: float | int = None,
    inclusive=True,
) -> bool:
    """
    Checks if all values are below seuil.
    """
    if inclusive:
        df_inf = df[cols_to_check].le(seuil)
    else:
        df_inf = df[cols_to_check].lt(seuil)

    return df_inf.all(axis=None)


def is_upper(
    df: pd.DataFrame, cols_to_check: list[str], seuil: float | int, inclusive=True
) -> bool:
    """
    Checks if all values are above seuil.
    """
    if inclusive:
        df_sup = df[cols_to_check].ge(seuil)
    else:
        df_sup = df[cols_to_check].gt(seuil)

    return df_sup.all(axis=None)


def is_in_range(
    df: pd.DataFrame,
    cols_to_check: list[str],
    seuil_inf: float | int,
    seuil_sup: float | int,
    inclusive=True,
) -> bool:
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
