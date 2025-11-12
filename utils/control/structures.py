import pandas as pd
import ast


def remove_grist_internal_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.drop(list[df.filter(regex="^(grist|manual)").columns], axis=1)
    return df


def convert_str_of_list_to_list(df: pd.DataFrame, col_to_convert: str) -> pd.DataFrame:
    df[col_to_convert] = df[col_to_convert].apply(
        lambda x: ast.literal_eval(x) if isinstance(x, str) else x
    )
    return df


def are_lists_egal(list_A: list[str], list_B: list[str]) -> bool:
    # Convert to sets
    list_A = set(list_A)
    list_B = set(list_B)

    # Elements in A but not in B
    only_in_list_A = list(list_A - list_B)

    # Elements in B but not in A
    only_in_list_B = list(list_B - list_A)

    if len(only_in_list_B) == 0 and len(only_in_list_A) == 0:
        print("Les colonnes sont identiques entre le DataFrame et la table")
        return True

    if len(only_in_list_A) > 0:
        print(
            f"Les éléments suivants sont présents dans la 1ère liste mais pas la 2nd: \n{only_in_list_A}"
        )
    if len(only_in_list_B) > 0:
        print(
            f"Les éléments suivants sont présents dans la 2nd liste mais pas la 1ère: \n{only_in_list_B}"
        )

    return False
