import pandas as pd
import ast


def remove_grist_internal_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(labels=list(df.filter(regex="^(grist|manual)").columns), axis=1)
    return df


def convert_str_of_list_to_list(df: pd.DataFrame, col_to_convert: str) -> pd.DataFrame:
    df[col_to_convert] = df[col_to_convert].apply(
        lambda x: ast.literal_eval(x) if isinstance(x, str) else x
    )
    return df


def lower_dataframe_labels(df: pd.DataFrame) -> pd.DataFrame:
    df = df.set_axis(labels=map(str.lower, df.columns), axis="columns")
    return df


def normalize_grist_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    print("Normalizing dataframe")
    df = lower_dataframe_labels(df=df)
    df = remove_grist_internal_cols(df=df)
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
        print(
            f"""Les éléments suivants sont présents dans la 1ère liste mais pas la 2nd:
                {only_in_set_A}
            """
        )
    if len(only_in_set_B) > 0:
        print(
            f"""Les éléments suivants sont présents dans la 2nd liste mais pas la 1ère:
                {only_in_set_B}
            """
        )

    return False
