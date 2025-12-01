import os
import pandas as pd
from pathlib import Path

# Chemin du dossier contenant les fichiers
DOSSIER_PARQUET = "/home/onyxia/work/traitement-des-donnees/scripts/generate_sql/tmp"

# Fichier de sortie pour les scripts SQL
FICHIER_SQL = "create_tables.sql"


def mapper_type_postgres(type_pandas):
    """Convertit un type pandas/pyarrow en type PostgreSQL"""
    type_str = str(type_pandas).lower()

    # Mapping des types
    if "int64" in type_str or "int32" in type_str:
        return "INTEGER"
    elif "int16" in type_str or "int8" in type_str:
        return "SMALLINT"
    elif "float64" in type_str or "double" in type_str:
        return "DOUBLE PRECISION"
    elif "float32" in type_str or "float" in type_str:
        return "REAL"
    elif "bool" in type_str:
        return "BOOLEAN"
    elif "datetime64" in type_str or "timestamp" in type_str:
        return "TIMESTAMP"
    elif "date" in type_str:
        return "DATE"
    elif "object" in type_str or "string" in type_str:
        return "TEXT"
    elif "category" in type_str:
        return "VARCHAR(255)"
    else:
        return "TEXT"  # Type par défaut


def generer_create_table(fichier_parquet, nom_table=None):
    """Génère le script CREATE TABLE à partir d'un fichier Parquet"""
    try:
        # Lire le fichier Parquet
        df = pd.read_parquet(fichier_parquet)

        # Générer le nom de la table
        if nom_table is None:
            nom_table = Path(fichier_parquet).stem.lower()

        # Début du script CREATE TABLE
        sql = f"-- Table générée depuis: {os.path.basename(fichier_parquet)}\n"
        sql += f"-- Nombre de lignes: {len(df)}\n"
        sql += f"DROP TABLE IF EXISTS {nom_table};\n"
        sql += f"CREATE TABLE {nom_table} (\n"

        # Générer les colonnes
        colonnes = []
        for col_name, col_type in df.dtypes.items():
            # Nettoyer le nom de colonne (remplacer espaces, caractères spéciaux)
            col_name_clean = col_name.strip().replace(" ", "_").replace("-", "_")
            type_pg = mapper_type_postgres(col_type)
            colonnes.append(f"    {col_name_clean} {type_pg}")

        sql += ",\n".join(colonnes)
        sql += "\n);\n\n"

        # Ajouter des commentaires sur les colonnes
        sql += "-- Aperçu des données:\n"
        sql += f"-- {df.head(3).to_string()}\n\n"

        return sql, nom_table, len(df)

    except Exception as e:
        print(f"✗ Erreur avec {os.path.basename(fichier_parquet)}: {str(e)}")
        return None, None, 0


def lire_fichiers_parquet(dossier):
    """Retourne la liste des fichiers .parquet dans le dossier"""
    fichiers = []
    for fichier in os.listdir(dossier):
        if fichier.endswith(".parquet"):
            fichiers.append(os.path.join(dossier, fichier))
    return fichiers


def main():
    """Fonction principale"""
    print("Génération des scripts SQL CREATE TABLE depuis fichiers Parquet\n")

    # Trouver les fichiers Parquet
    fichiers = lire_fichiers_parquet(DOSSIER_PARQUET)

    if not fichiers:
        print(f"Aucun fichier .parquet trouvé dans {DOSSIER_PARQUET}")
        return

    print(f"Trouvé {len(fichiers)} fichier(s) Parquet\n")

    # Générer les scripts SQL
    scripts_sql = []
    tables_info = []

    for fichier in fichiers:
        sql, nom_table, nb_lignes = generer_create_table(fichier)
        if sql:
            scripts_sql.append(sql)
            tables_info.append((nom_table, nb_lignes, os.path.basename(fichier)))
            print(f"✓ Script généré pour table '{nom_table}' ({nb_lignes} lignes)")

    # Écrire dans le fichier SQL
    if scripts_sql:
        with open(FICHIER_SQL, "w", encoding="utf-8") as f:
            f.write("-- Scripts SQL générés automatiquement depuis fichiers Parquet\n")
            f.write(f"-- Date de génération: {pd.Timestamp.now()}\n")
            f.write("-- " + "=" * 70 + "\n\n")
            f.write("\n".join(scripts_sql))

        print(f"\n✓ Fichier SQL créé: {FICHIER_SQL}")
        print("\nRésumé des tables générées:")
        print("-" * 60)
        for nom, lignes, fichier in tables_info:
            print(f"  {nom:30} {lignes:>8} lignes  ({fichier})")
    else:
        print("Aucun script SQL généré")


if __name__ == "__main__":
    main()
