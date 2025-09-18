import pandas as pd


def process_typologie_bien(df: pd.DataFrame) -> pd.DataFrame:
    df = (
        df.dropna(subset=["usage_detaille_du_bien"])
        .drop_duplicates(subset=["usage_detaille_du_bien"], ignore_index=True)
        .assign(
            usage_detaille_du_bien=df["usage_detaille_du_bien"]
            .str.split()
            .str.join(" "),
            type_de_bien=df["type_de_bien"].str.split().str.join(" "),
            famille_de_bien=df["famille_de_bien"].str.split().str.join(" "),
            famille_de_bien_simplifiee=df["famille_de_bien_simplifiee"]
            .str.split()
            .str.join(" "),
            bati_non_bati=df["bati_non_bati"].str.split().str.join(" "),
        )
        .drop(columns=["id"])
        .convert_dtypes()
    )

    return df
