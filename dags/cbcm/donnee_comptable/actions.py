from airflow.models import Variable
from infra.http_client.adapters import RequestsClient
from infra.http_client.config import ClientConfig
import pandas as pd

from infra.grist.client import GristAPI
from infra.database.factory import create_db_handler
from utils.config.vars import AGENT, DEFAULT_PG_DATA_CONN_ID, PROXY


def load_new_sp(dfs: list[pd.DataFrame]) -> None:
    # Concaténer tous les CF et CC
    cols_to_keep = ["centre_financier", "centre_cout"]
    df_source = pd.concat(objs=[df[cols_to_keep] for df in dfs])

    # Supprimer les doublons
    df_source = df_source.drop_duplicates(subset=cols_to_keep)  # type: ignore

    # Récupérer les SP déjà connus
    db = create_db_handler(connection_id=DEFAULT_PG_DATA_CONN_ID)
    df_sp = db.fetch_df(
        query="SELECT DISTINCT centre_financier, centre_cout FROM donnee_comptable.service_prescripteur;"
    )
    print(f"Nombre de SP connus: {len(df_sp)}")

    # Réaliser une jointure
    df = pd.merge(
        left=df_sp, right=df_source, on=cols_to_keep, how="outer", indicator=True
    )

    # Conserver uniquement les lignes sans SP
    df = df.loc[df["_merge"] == "right_only", cols_to_keep]

    # Intégrer ces lignes dans Grist
    data = {"records": [{"fieds": record} for record in df.to_dict(orient="records")]}
    print(f"Nouveau couple CF-CC sans SP: {len(data["records"])}")
    print(f"Exemple: {data['records'][0]}")

    http_config = ClientConfig(proxy=PROXY, user_agent=AGENT)
    request_client = RequestsClient(config=http_config)
    grist_client = GristAPI(
        http_client=request_client,
        base_url="https://grist.numerique.gouv.fr",
        workspace_id="dsci",
        doc_id=Variable.get(key="grist_doc_id_cbcm"),
        api_token=Variable.get(key="grist_secret_key"),
    )
    # grist_client.post_records(tbl_name="Service_prescripteur", json=data)
