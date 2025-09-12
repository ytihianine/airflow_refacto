import pandas as pd


def replace_values(
    df: pd.DataFrame, to_replace: dict[str, str], cols: list[str] = None
) -> pd.DataFrame:
    if cols:
        df[cols] = df[cols].replace(to_replace=to_replace)
    else:
        df = df.replace(to_replace=to_replace)
    return df


def process_direction(df: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(direction=df["direction"].str.strip()).convert_dtypes()
    df = df.drop_duplicates(subset=["direction"])
    df = df.dropna(subset=["direction"])

    # Sort columns to match db cols order
    cols = df.columns
    sorted_cols = sorted(cols)
    df = df[sorted_cols]

    return df


def process_service(df: pd.DataFrame) -> pd.DataFrame:
    # Rename
    cols_to_rename = {"direction": "id_direction"}

    df = (
        df.rename(columns=cols_to_rename)
        .assign(service=df["service"].str.strip())
        .convert_dtypes()
    )
    df = df.drop_duplicates(subset=["id_direction", "service"])
    df = df.dropna(subset=["id_direction", "service"])

    # Sort columns to match db cols order
    cols = df.columns
    sorted_cols = sorted(cols)
    df = df[sorted_cols]

    return df


def process_projets(df: pd.DataFrame) -> pd.DataFrame:
    # Rename
    cols_to_rename = {
        "Projet": "nom_projet",
        "direction": "id_direction",
        "service": "id_service",
    }

    df = (
        df.rename(columns=cols_to_rename)
        .assign(nom_projet=lambda df: df["nom_projet"].str.strip())
        .convert_dtypes()
    )
    df = df.drop_duplicates(subset=["nom_projet", "id_direction", "id_service"])
    df = df.dropna(subset=["nom_projet", "id_direction", "id_service"])

    # Sort columns to match db cols order
    cols = df.columns
    sorted_cols = sorted(cols)
    df = df[sorted_cols]

    return df


def process_selecteur(df: pd.DataFrame) -> pd.DataFrame:
    # Rename
    cols_to_rename = {
        "Projet": "id_projet",
        "Type_de_selecteur": "type_selecteur",
        "Selecteur": "selecteur",
    }
    df = (
        df.rename(columns=cols_to_rename)
        .assign(selecteur=lambda df: df["selecteur"].str.strip().str.lower())
        .convert_dtypes()
    )
    df = df.drop_duplicates(subset=["id_projet", "selecteur"])
    df = df.dropna(subset=["id_projet", "selecteur"])

    # Sort columns to match db cols order
    cols = df.columns
    sorted_cols = sorted(cols)
    df = df[sorted_cols]

    return df


def process_source(df: pd.DataFrame) -> pd.DataFrame:
    # Rename
    cols_to_rename = {
        "Projet": "id_projet",
        "Type": "type_source",
        "Selecteur": "id_selecteur",
    }
    df = df.drop(columns=["Sous_type"])
    df = (
        df.rename(columns=cols_to_rename)
        .pipe(replace_values, to_replace={0: None}, cols=["id_projet", "id_selecteur"])
        .assign(nom_source=df["nom_source"].str.strip())
        .convert_dtypes()
    )
    df = df.drop_duplicates(subset=["id_projet", "id_selecteur", "nom_source"])
    df = df.dropna(subset=["id_projet", "id_selecteur", "nom_source"])

    # Sort columns to match db cols order
    cols = df.columns
    sorted_cols = sorted(cols)
    df = df[sorted_cols]

    return df


def process_storage_path(df: pd.DataFrame) -> pd.DataFrame:
    # Rename
    cols_to_rename = {
        "Projet": "id_projet",
        "Selecteur": "id_selecteur",
        "db_tbl_name": "tbl_name",
    }
    df = (
        df.rename(columns=cols_to_rename)
        .pipe(replace_values, to_replace={0: None}, cols=["id_projet", "id_selecteur"])
        .assign(
            local_tmp_dir=df["local_tmp_dir"].str.strip(),
            s3_bucket=df["s3_bucket"].str.strip(),
            s3_key=df["s3_key"].str.strip(),
            s3_tmp_key=df["s3_tmp_key"].str.strip(),
        )
        .convert_dtypes()
    )
    df = df.dropna(subset=["id_projet", "id_selecteur"])

    # Sort columns to match db cols order
    cols = df.columns
    sorted_cols = sorted(cols)
    df = df[sorted_cols]

    return df


def process_col_mapping(df: pd.DataFrame) -> pd.DataFrame:
    # Rename
    cols_to_rename = {
        "Projet": "id_projet",
        "Selecteur": "id_selecteur",
    }
    df = df.drop(columns=["Nombre_d_utilisation"])
    df = (
        df.rename(columns=cols_to_rename)
        .pipe(replace_values, to_replace={0: None}, cols=["id_projet", "id_selecteur"])
        .assign(
            colname_source=(df["colname_source"].str.split().str.join(" ")),
            colname_dest=(df["colname_dest"].str.split().str.join("_")),
        )
        .convert_dtypes()
    )
    df = df.dropna(subset=["id_projet", "id_selecteur"])
    df = df.set_axis([colname.lower() for colname in df.columns], axis="columns")

    # Sort columns to match db cols order
    cols = df.columns
    sorted_cols = sorted(cols)
    df = df[sorted_cols]

    return df


def process_col_requises(df: pd.DataFrame) -> pd.DataFrame:
    # Rename
    cols_to_rename = {
        "Projet": "id_projet",
        "Selecteur": "id_selecteur",
        "Colonne_requise": "id_correspondance_colonne",
    }
    col_to_keep = ["id", "id_projet", "id_selecteur", "id_correspondance_colonne"]
    df = (
        df.rename(columns=cols_to_rename)
        .pipe(
            replace_values,
            to_replace={0: None},
            cols=["id_projet", "id_selecteur", "id_correspondance_colonne"],
        )
        .convert_dtypes()
    )
    df = df.dropna(subset=["id_projet", "id_selecteur", "id_correspondance_colonne"])
    df = df.drop(columns=list(set(df.columns) - set(col_to_keep)))

    # Sort columns to match db cols order
    cols = df.columns
    sorted_cols = sorted(cols)
    df = df[sorted_cols]

    return df
