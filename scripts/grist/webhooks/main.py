from collections.abc import Iterable, Sequence

from modules.constants import DEFAULT_GRIST_HOST, custom_logger
from modules.enums.http import HttpHandlerType
from modules.infra.grist.client import GristClient
from modules.infra.http_client.config import ClientConfig
from modules.infra.http_client.factory import create_http_client
from scripts.settings import get_settings

EVENT_TYPES = ["add", "update"]
TBL_TO_EXCLUDE = ("test", "onglet", "doc")

settings = get_settings()


def filter_tables(tables: Iterable[str], tbl_to_exclude: Iterable[str] = TBL_TO_EXCLUDE) -> Sequence[str]:
    return [table for table in tables if not table.lower().startswith(tuple(tbl_to_exclude))]


if __name__ == "__main__":
    # External clients
    http_config = ClientConfig(user_agent=settings.http.agent, proxy=settings.http.proxy)
    http_client = create_http_client(client_type=HttpHandlerType.REQUEST, config=http_config)
    grist_client = GristClient(http_client=http_client, grist_host=DEFAULT_GRIST_HOST, api_token=settings.grist.token)

    # Récupérer toutes les tables du document
    response = grist_client.list_tables(doc_id=settings.grist.doc_id)
    tables = [table["id"] for table in response.json()["tables"]]
    custom_logger.info(msg=f"Nombre de tables dans le document: {len(tables)}")
    custom_logger.info(msg=f"Liste des tables: {tables}")
    tables = filter_tables(tables=tables)
    custom_logger.info(msg=f"Nombre de tables dans le document après filtrage: {len(tables)}")
    custom_logger.info(msg=f"Liste des tables: {tables}")

    # Créer les webhooks
    webhooks = []
    for table in tables:
        webhook_info = {
            "fields": {
                "name": table,
                "memo": "Auto generated",
                "url": settings.grist.n8n_pipeline_url,
                "enabled": True,
                "eventTypes": EVENT_TYPES,
                "isReadyColumn": None,
                "tableId": table,
            }
        }
        webhooks.append(webhook_info)

    custom_logger.info(msg=f"Exemple: \n{webhooks[0]}")

    grist_client.create_webhook(doc_id=settings.grist.doc_id, body={"webhooks": webhooks})
