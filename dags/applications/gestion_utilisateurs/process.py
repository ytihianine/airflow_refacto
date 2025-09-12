import pandas as pd


def process_user_info(df: pd.DataFrame) -> pd.DataFrame:
    df = (
        df.dropna(subset=["username", "nom", "prenom"])
        # Get user not created
        .loc[~df["user_created"]]
    )

    return df
