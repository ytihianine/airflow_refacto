import re
from datetime import timedelta
from airflow.decorators import task
from airflow.models import Variable

from infra.file_handling.factory import create_file_handler
from infra.http_client.adapters import RequestsClient
from infra.http_client.config import ClientConfig
from infra.grist.client import GristAPI
from utils.config.dag_params import get_project_name
from utils.config.tasks import get_selecteur_config

from utils.config.vars import DEFAULT_S3_BUCKET, DEFAULT_S3_CONN_ID, PROXY, AGENT


def clean_sql_tables(input_file: str, output_file: str) -> None:
    """
    Nettoie un fichier SQL de CREATE TABLE :
    - Supprime les tables dont le nom contient 'grist', 'onglet' ou 'doc'
    - Supprime les colonnes dont le nom contient 'grist' ou 'manualsort'
    - Met en minuscule noms de tables et colonnes
    - Supprime les guillemets
    """
    with open(input_file, "r", encoding="utf-8") as f:
        sql_content = f.read()

    # Découper les blocs CREATE TABLE
    tables = re.split(r";\s*\n", sql_content)

    cleaned_blocks = []
    for block in tables:
        if not block.strip():
            continue

        m = re.search(r"CREATE TABLE\s+([^\s(]+)", block, re.IGNORECASE)
        if not m:
            continue

        table_name = m.group(1).replace('"', "").lower()

        # Filtrer les tables à supprimer
        if any(x in table_name for x in ["grist", "onglet", "doc", "summary"]):
            continue

        # Supprimer guillemets
        block = block.replace('"', "")

        # Split lignes de définition
        lines = block.splitlines()
        new_lines = []

        for line in lines:
            # garder CREATE TABLE intact, juste nom minuscule
            if line.strip().upper().startswith("CREATE TABLE"):
                new_lines.append(f"CREATE TABLE {table_name} (")
                continue

            # ignorer les lignes de colonnes "grist" ou "manualsort"
            col_match = re.match(r"\s*([A-Za-z0-9_]+)\s+", line.strip())
            if col_match:
                col_name = col_match.group(1)
                if "grist" in col_name.lower() or col_name.lower() == "manualsort":
                    continue
                # mettre colname en minuscule
                line = re.sub(rf"\b{col_name}\b", col_name.lower(), line, count=1)

            new_lines.append("\t" + line.lower())

        cleaned_block = "\n".join(new_lines).rstrip(",")
        cleaned_blocks.append(cleaned_block + ";")

    # Écriture fichier
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("\n\n".join(cleaned_blocks))

    print(f"✅ Fichier nettoyé écrit dans {output_file}")


def generate_sql_tbl_script(
    sqlite_path: str, output_file: str = "./tables.sql"
) -> None:
    """
    Helper fonction pour le développement. Elle permet de lire un fichier SQLite
    et de générer les scripts SQL des tables qui y sont présentes.
    """
    import sqlite3

    # Ouvre ton fichier SQLite
    conn = sqlite3.connect(sqlite_path)
    cursor = conn.cursor()

    # Liste des tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]

    with open(output_file, "w", encoding="utf-8") as f:
        for table in tables:
            cursor.execute(
                f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}'"
            )
            create_sql = cursor.fetchone()[0]
            f.write(f"-- Table: {table}\n")
            f.write(create_sql.replace("(", "(\n").replace(", ", ",\n"))
            f.write(";\n\n")


@task(task_id="download_grist_doc_to_s3", retries=1, retry_delay=timedelta(seconds=20))
def download_grist_doc_to_s3(
    selecteur: str,
    workspace_id: str,
    doc_id_key: str,
    grist_host: str = "https://grist.numerique.gouv.fr",
    api_token_key: str = "grist_secret_key",
    http_client_over_internet: bool = True,
    **context,
) -> None:
    """Download SQLite from a specific Grist doc to S3"""
    nom_projet = get_project_name(context=context)

    selecteur_config = get_selecteur_config(nom_projet=nom_projet, selecteur=selecteur)

    # Instanciate Grist client
    if http_client_over_internet:
        http_config = ClientConfig(proxy=PROXY, user_agent=AGENT)
        request_client = RequestsClient(config=http_config)
    else:
        http_config = ClientConfig()
        request_client = RequestsClient(config=http_config)

    grist_client = GristAPI(
        http_client=request_client,
        base_url=grist_host,
        workspace_id=workspace_id,
        doc_id=Variable.get(doc_id_key),
        api_token=Variable.get(api_token_key),
    )

    # Hooks
    s3_handler = create_file_handler(
        handler_type="s3", connection_id=DEFAULT_S3_CONN_ID, bucket=DEFAULT_S3_BUCKET
    )

    # Get document data from Grist
    grist_response = grist_client.get_doc_sqlite_file()

    # Export sqlite file to S3
    print(f"Exporting file to < {selecteur_config.filepath_tmp_s3} >")
    s3_handler.write(
        file_path=selecteur_config.filepath_tmp_s3,
        content=grist_response,
    )
    print("✅ Export done!")
