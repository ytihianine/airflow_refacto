import json
import os
from datetime import datetime
from pathlib import Path

import psycopg2
from modules.constants import custom_logger
from scripts.settings import get_settings

if __name__ == "__main__":
    dir = os.path.dirname(os.path.realpath(__file__))
    config_path = Path(dir, "config.json")
    settings = get_settings()

    # Load config
    with open(file=config_path) as f:
        config = json.load(fp=f)

    # Init db connections
    pg_conn = psycopg2.connect(
        host=settings.db.host,
        port=settings.db.port,
        dbname=settings.db.name,
        user=settings.db.user,
        password=settings.db.password,
    )
    pg_cur = pg_conn.cursor()

    for item in config:
        # Récupérer l'id du projet
        query = f"""
            SELECT id_projet
            FROM conf_projets.projet cpp
            WHERE cpp.projet = '{item["nom_projet"]}'
            ORDER BY cpp.import_timestamp DESC
            LIMIT 1;
        """
        pg_cur.execute(query=query)
        row = pg_cur.fetchone()
        if row is None:
            custom_logger.error(msg=f"L'ID du projet <{item['nom_projet']}> est introuvable")
            continue

        id_projet = row[0]
        custom_logger.info(msg=f"ID du projet <{item['nom_projet']}> : {id_projet}")

        # Créer les snapshots
        import_timestamp = datetime.fromisoformat(item["timestamp"])
        snapshot_id = import_timestamp.strftime(format="%Y%m%d_%H:%M:%S")
        import_date = import_timestamp.date()
        custom_logger.info(
            msg=f"Création du snapshot pour le projet <{item['nom_projet']}> "
            f"avec snapshot_id <{snapshot_id}> et import_date <{import_date}>"
        )

        query = """
            INSERT INTO versioning.snapshot (id_projet, import_timestamp, snapshot_id, import_date)
            VALUES (%s, %s, %s, %s);
        """
        pg_cur.execute(query=query, vars=(id_projet, import_timestamp, snapshot_id, import_date))

    pg_conn.commit()
    pg_conn.close()
