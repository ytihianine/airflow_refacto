import ast
from enum import Enum

import numpy as np
import pandas as pd


def remove_grist_internal_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(labels=list(df.filter(regex="^(grist|manual)").columns), axis=1)
    return df


def convert_str_of_list_to_list(df: pd.DataFrame, col_to_convert: str) -> pd.DataFrame:
    df[col_to_convert] = df[col_to_convert].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)
    return df


def lower_dataframe_labels(df: pd.DataFrame) -> pd.DataFrame:
    df = df.set_axis(labels=map(str.lower, df.columns), axis="columns")  # type: ignore
    return df


def normalize_grist_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    print("Normalizing dataframe")
    df = lower_dataframe_labels(df=df)
    df = remove_grist_internal_cols(df=df)
    return df


def handle_grist_null_references(df: pd.DataFrame, columns: list[str], keep_zero: bool = False) -> pd.DataFrame:
    for col in columns:
        df[col] = pd.to_numeric(arg=df[col], errors="raise")
        if not keep_zero:
            df[col] = df[col].replace({0: pd.NA})  # type: ignore
    return df


def handle_grist_boolean_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for col in columns:
        df[col] = df[col].replace({0: False, 1: True})
    return df


def are_lists_egal(list_A: list[str], list_B: list[str]) -> bool:
    # Convert to sets
    set_A = set(list_A)
    set_B = set(list_B)

    # Elements in A but not in B
    only_in_set_A = list(set_A - set_B)

    # Elements in B but not in A
    only_in_set_B = list(set_B - set_A)

    if len(only_in_set_B) == 0 and len(only_in_set_A) == 0:
        print("Les colonnes sont identiques entre le DataFrame et la table")
        return True

    if len(only_in_set_A) > 0:
        print(f"""Les éléments suivants sont présents dans la 1ère liste mais pas la 2nd:
                {only_in_set_A}
            """)
    if len(only_in_set_B) > 0:
        print(f"""Les éléments suivants sont présents dans la 2nd liste mais pas la 1ère:
                {only_in_set_B}
            """)

    return False


def validate_enum_column(df: pd.DataFrame, column: str, enum_class: type[Enum], allow_null: bool = True) -> None:
    """
    Valide que toutes les valeurs d'une colonne correspondent aux valeurs d'un Enum.

    Args:
        df: DataFrame à valider
        column: Nom de la colonne à vérifier
        enum_class: Classe Enum contenant les valeurs valides
        allow_null: Si True, ignore les valeurs NaN/None

    Raises:
        ValueError: Si des valeurs invalides sont trouvées
    """
    valid_values = {e.value for e in enum_class}

    if allow_null:
        column_values = set(df[column].dropna().unique())
    else:
        column_values = set(df[column].unique())
        if pd.isna(list(column_values)).any():
            raise ValueError(f"Valeurs null trouvées dans '{column}' alors que allow_null=False")

    invalid_values = column_values - valid_values

    if invalid_values:
        raise ValueError(f"Valeurs invalides dans '{column}': {invalid_values}. " f"Valeurs acceptées: {valid_values}")


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
