# Guide de Création de DAGs Airflow

Ce guide explique comment créer des DAGs Airflow en utilisant les tâches pré-définies disponibles dans le dossier `utils/` et en créant ses propres fonctions de processing personnalisées.

## Table des matières

1. [Architecture et Principes](#architecture-et-principes)
2. [Structure des Paramètres](#structure-des-paramètres)
3. [Tâches Pré-définies Disponibles](#tâches-pré-définies-disponibles)
4. [Créer ses Fonctions de Processing](#créer-ses-fonctions-de-processing)
5. [Exemple Complet de DAG](#exemple-complet-de-dag)
6. [Bonnes Pratiques](#bonnes-pratiques)
7. [Gestion des Erreurs](#gestion-des-erreurs)

## Architecture et Principes

### Principe de Séparation des Responsabilités

Le framework propose une architecture en couches :

- **Infrastructure (`infra/`)** : Gestion des connexions (base de données, S3, HTTP, mails)
- **Utilitaires (`utils/`)** : Tâches réutilisables et configuration
- **DAGs** : Orchestration métier utilisant uniquement les tâches pré-définies

### Workflow Standard

1. **Validation des paramètres** : Vérification des paramètres requis du DAG
2. **Extraction** : Lecture des données depuis diverses sources (S3, Grist, base de données)
3. **Transformation** : Application de fonctions de processing personnalisées
4. **Chargement** : Sauvegarde des résultats (S3, base de données)
5. **Notification** : Envoi de mails de succès/échec

## Structure des Paramètres

Chaque DAG doit définir ses paramètres selon la structure TypedDict suivante :

```python
from utils.config.types import DagParams

# Paramètres obligatoires
params = {
    "nom_projet": "Mon Projet",  # Nom du projet
    "db": {
        "prod_schema": "production",    # Schéma de production
        "tmp_schema": "temporaire"      # Schéma temporaire
    },
    "mail": {
        "enable": True,                 # Activation des mails
        "to": ["email@example.com"],    # Destinataires
        "cc": ["cc@example.com"]        # Copie (optionnel)
    },
    "docs": {
        "lien_pipeline": "https://...", # Documentation pipeline
        "lien_donnees": "https://..."   # Documentation données
    }
}
```

## Tâches Pré-définies Disponibles

### 1. Validation des Paramètres

```python
from utils.tasks.validation import create_validate_params_task

# Création de la tâche de validation
validate_params = create_validate_params_task(
    required_paths=[
        "nom_projet",
        "db.prod_schema",
        "db.tmp_schema",
        "mail.to"
    ],
    require_truthy=["mail.enable"],  # Paramètres devant être True
    task_id="validate_dag_params"
)
```

### 2. Tâches ETL (Extract, Transform, Load)

#### ETL depuis Grist
```python
from utils.tasks.etl import create_grist_etl_task

# ETL Grist avec fonction de processing personnalisée
grist_etl = create_grist_etl_task(
    selecteur="mon_selecteur",
    doc_selecteur="grist_doc",
    normalisation_process_func=ma_fonction_normalisation,
    process_func=ma_fonction_processing
)
```

#### ETL Générique
```python
from utils.tasks.etl import create_etl_task

# ETL générique avec traitement personnalisé
etl_task = create_etl_task(
    selecteur="mon_selecteur",
    process_func=ma_fonction_processing,
    task_params={"task_id": "process_data"}
)
```

### 3. Gestion des Fichiers

#### Conversion vers Parquet
```python
from utils.tasks.file import create_parquet_converter_task

# Conversion de fichiers vers Parquet
convert_to_parquet = create_parquet_converter_task(
    selecteur="mon_selecteur",
    process_func=ma_fonction_processing,
    read_options={"encoding": "utf-8", "sep": ";"},
    apply_cols_mapping=True
)
```

### 4. Opérations SQL

#### Création de Tables Temporaires
```python
from utils.tasks.sql import create_tmp_tables

# Création des tables temporaires
create_tables = create_tmp_tables()
```

#### Copie vers Tables de Production
```python
from utils.tasks.sql import copy_tmp_table_to_real_table

# Copie des données vers production
copy_to_prod = copy_tmp_table_to_real_table()
```

#### Gestion des Partitions
```python
from utils.tasks.sql import ensure_monthly_partition

# Création de partition mensuelle
create_partition = ensure_monthly_partition(
    schema="production",
    table="ma_table",
    date_column="date_creation"
)
```

#### Import de Données
```python
from utils.tasks.sql import create_import_file_to_db_task, LoadStrategy

# Import avec stratégie full load
import_task = create_import_file_to_db_task(
    selecteur="mon_selecteur",
    load_strategy=LoadStrategy.FULL_LOAD,
    task_params={"task_id": "import_data"}
)

# Import incrémental
import_incremental = create_import_file_to_db_task(
    selecteur="mon_selecteur",
    load_strategy=LoadStrategy.INCREMENTAL,
    date_column="date_maj",
    task_params={"task_id": "import_incremental"}
)
```

### 5. Opérations S3

```python
from utils.tasks.s3 import copy_s3_files, del_s3_files

# Copie de fichiers S3
copy_files = copy_s3_files(bucket="mon-bucket")

# Suppression de fichiers S3
delete_files = del_s3_files(bucket="mon-bucket")
```

### 6. Opérations Grist

```python
from utils.tasks.grist import create_grist_to_s3_task

# Export Grist vers S3
grist_export = create_grist_to_s3_task(
    selecteur="mon_selecteur",
    process_func=ma_fonction_processing
)
```

## Créer ses Fonctions de Processing

### Structure des Fonctions de Processing

Les fonctions de processing doivent respecter la signature suivante :

```python
import pandas as pd
from typing import Optional, Dict, Any

def ma_fonction_processing(
    df: pd.DataFrame,
    context: Optional[Dict[str, Any]] = None
) -> pd.DataFrame:
    """
    Fonction de processing personnalisée.

    Args:
        df: DataFrame à traiter
        context: Contexte Airflow (optionnel)

    Returns:
        DataFrame traité
    """
    # Votre logique de transformation
    df_processed = df.copy()

    # Exemple : nettoyage des données
    df_processed = df_processed.dropna()

    # Exemple : ajout de colonnes calculées
    df_processed['date_processing'] = pd.Timestamp.now()

    # Exemple : filtrage
    df_processed = df_processed[df_processed['status'] == 'active']

    return df_processed
```

### Exemples de Fonctions Métier

#### Normalisation des Données
```python
def normaliser_donnees_financieres(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise les données financières."""
    df = df.copy()

    # Conversion des montants
    df['montant'] = pd.to_numeric(df['montant'], errors='coerce')

    # Normalisation des dates
    df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')

    # Nettoyage des codes
    df['code'] = df['code'].str.upper().str.strip()

    return df
```

#### Agrégation de Données
```python
def agreger_par_periode(df: pd.DataFrame) -> pd.DataFrame:
    """Agrège les données par période mensuelle."""
    df = df.copy()

    # Création de la période
    df['periode'] = df['date'].dt.to_period('M')

    # Agrégation
    df_agg = df.groupby(['periode', 'categorie']).agg({
        'montant': 'sum',
        'quantite': 'count',
        'date': 'max'
    }).reset_index()

    return df_agg
```

#### Enrichissement de Données
```python
def enrichir_donnees_geographiques(df: pd.DataFrame) -> pd.DataFrame:
    """Enrichit les données avec des informations géographiques."""
    from utils.config.tasks import get_cols_mapping

    df = df.copy()

    # Mapping des codes postaux vers régions
    # (utilisation de la configuration projet)
    mapping_geo = get_cols_mapping(
        nom_projet="referentiels",
        selecteur="geo_mapping"
    )

    # Application du mapping
    df = df.merge(mapping_geo, on='code_postal', how='left')

    return df
```

## Exemple Complet de DAG

```python
from datetime import timedelta
from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from infra.mails.sender import create_airflow_callback, MailStatus
from utils.tasks.validation import create_validate_params_task
from utils.tasks.etl import create_grist_etl_task
from utils.tasks.file import create_parquet_converter_task
from utils.tasks.sql import (
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    create_import_file_to_db_task,
    LoadStrategy
)
from utils.tasks.s3 import copy_s3_files, del_s3_files
from utils.config.tasks import get_s3_keys_source

import pandas as pd

# === FONCTIONS DE PROCESSING PERSONNALISÉES ===

def nettoyer_donnees_ventes(df: pd.DataFrame) -> pd.DataFrame:
    """Nettoie et normalise les données de ventes."""
    df = df.copy()

    # Nettoyage des montants
    df['montant_vente'] = pd.to_numeric(
        df['montant_vente'].str.replace(',', '.'),
        errors='coerce'
    )

    # Filtrage des ventes valides
    df = df[df['montant_vente'] > 0]

    # Normalisation des dates
    df['date_vente'] = pd.to_datetime(df['date_vente'])

    # Ajout de métadonnées
    df['date_processing'] = pd.Timestamp.now()
    df['source'] = 'grist_export'

    return df

def calculer_indicateurs_ventes(df: pd.DataFrame) -> pd.DataFrame:
    """Calcule les indicateurs de performance des ventes."""
    df = df.copy()

    # Calcul du CA mensuel
    df['mois'] = df['date_vente'].dt.to_period('M')
    df_indicateurs = df.groupby(['mois', 'region']).agg({
        'montant_vente': ['sum', 'mean', 'count'],
        'quantite': 'sum'
    }).reset_index()

    # Aplatissement des colonnes
    df_indicateurs.columns = [
        'mois', 'region', 'ca_total', 'ca_moyen', 'nb_ventes', 'quantite_totale'
    ]

    return df_indicateurs

# === CONFIGURATION DU DAG ===

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    "pipeline_ventes_mensuelles",
    schedule_interval="0 6 1 * *",  # 1er du mois à 6h
    max_active_runs=1,
    catchup=False,
    tags=["VENTES", "MENSUEL", "REPORTING"],
    description="Pipeline mensuel de traitement des données de ventes",
    default_args=default_args,
    params={
        "nom_projet": "Analyse Ventes",
        "db": {
            "prod_schema": "ventes",
            "tmp_schema": "temporaire"
        },
        "mail": {
            "enable": True,
            "to": ["equipe-data@example.com"],
            "cc": ["direction@example.com"]
        },
        "docs": {
            "lien_pipeline": "https://wiki.example.com/pipeline-ventes",
            "lien_donnees": "https://catalogue.example.com/ventes"
        }
    },
    on_failure_callback=create_airflow_callback(mail_status=MailStatus.ERROR),
    on_success_callback=create_airflow_callback(mail_status=MailStatus.SUCCESS),
)
def pipeline_ventes_mensuelles():

    # === VALIDATION DES PARAMÈTRES ===
    validate_params = create_validate_params_task(
        required_paths=[
            "nom_projet",
            "db.prod_schema",
            "db.tmp_schema",
            "mail.to"
        ],
        require_truthy=["mail.enable"],
        task_id="validate_dag_params"
    )

    # === DÉTECTION DES FICHIERS ===
    detect_files = S3KeySensor(
        task_id="detect_source_files",
        aws_conn_id="minio_bucket_data",
        bucket_name="data-lake",
        bucket_key=get_s3_keys_source(nom_projet="Analyse Ventes"),
        mode="reschedule",
        poke_interval=timedelta(minutes=5),
        timeout=timedelta(hours=2),
        on_success_callback=create_airflow_callback(mail_status=MailStatus.START)
    )

    # === EXTRACTION DEPUIS GRIST ===
    extract_grist = create_grist_etl_task(
        selecteur="donnees_ventes_brutes",
        doc_selecteur="grist_ventes",
        normalisation_process_func=nettoyer_donnees_ventes,
        process_func=None  # Pas de processing supplémentaire
    )

    # === CONVERSION PARQUET ===
    convert_files = create_parquet_converter_task(
        selecteur="fichiers_externes",
        process_func=nettoyer_donnees_ventes,
        read_options={"encoding": "utf-8", "sep": ";"},
        apply_cols_mapping=True
    )

    # === CALCUL D'INDICATEURS ===
    compute_indicators = create_grist_etl_task(
        selecteur="donnees_ventes_clean",
        process_func=calculer_indicateurs_ventes
    )

    # === PRÉPARATION BASE DE DONNÉES ===
    create_tables = create_tmp_tables()

    # === IMPORT EN BASE ===
    import_ventes = create_import_file_to_db_task(
        selecteur="donnees_ventes_brutes",
        load_strategy=LoadStrategy.FULL_LOAD,
        task_params={"task_id": "import_ventes_brutes"}
    )

    import_indicateurs = create_import_file_to_db_task(
        selecteur="indicateurs_ventes",
        load_strategy=LoadStrategy.FULL_LOAD,
        task_params={"task_id": "import_indicateurs"}
    )

    # === FINALISATION ===
    finalize_tables = copy_tmp_table_to_real_table()
    archive_files = copy_s3_files(bucket="data-lake")
    cleanup_files = del_s3_files(bucket="data-lake")

    # === ORCHESTRATION ===
    chain(
        validate_params(),
        detect_files,
        [extract_grist(), convert_files()],
        compute_indicators(),
        create_tables(),
        [import_ventes(), import_indicateurs()],
        finalize_tables(),
        archive_files(),
        cleanup_files()
    )

# Instanciation du DAG
dag_instance = pipeline_ventes_mensuelles()
```

## Bonnes Pratiques

### 1. Naming et Organisation

```python
# ✅ Bon : Utilisation de préfixes clairs
@dag("pipeline_ventes_mensuelles", ...)
def pipeline_ventes_mensuelles():
    extract_data = create_grist_etl_task(...)
    transform_data = create_parquet_converter_task(...)

# ❌ Éviter : Noms génériques
@dag("dag1", ...)
def my_dag():
    task1 = create_grist_etl_task(...)
```

### 2. Paramétrage

```python
# ✅ Bon : Utilisation des constantes
from utils.config.vars import DEFAULT_S3_BUCKET, DEFAULT_PG_DATA_CONN_ID

# ✅ Bon : Validation systématique
validate_params = create_validate_params_task(
    required_paths=["nom_projet", "db.prod_schema"],
    task_id="validate_params"
)
```

### 3. Gestion des Dépendances

```python
# ✅ Bon : Utilisation de chain pour la lisibilité
chain(
    validate_params(),
    extract_data(),
    [transform_data(), compute_metrics()],  # Parallélisation
    load_data(),
    cleanup()
)
```

### 4. Documentation

```python
# ✅ Bon : Documentation des fonctions
def calculer_taux_conversion(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule le taux de conversion par canal marketing.

    Args:
        df: DataFrame contenant les données de marketing

    Returns:
        DataFrame avec les taux de conversion calculés

    Business Logic:
        - Taux = (nb_conversions / nb_visiteurs) * 100
        - Filtrage des canaux avec moins de 100 visiteurs
    """
    # Implementation...
```

## Gestion des Erreurs

### 1. Exceptions Personnalisées

```python
from utils.exceptions import ConfigError

def ma_fonction_processing(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        raise ConfigError(
            "DataFrame vide reçu en entrée",
            context={"nb_rows": len(df)}
        )

    required_cols = ['date', 'montant', 'client_id']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ConfigError(
            f"Colonnes manquantes: {missing_cols}",
            missing_columns=missing_cols
        )

    return df
```

### 2. Callbacks de Notification

```python
from infra.mails.sender import create_airflow_callback, MailStatus

@dag(
    ...,
    on_failure_callback=create_airflow_callback(mail_status=MailStatus.ERROR),
    on_success_callback=create_airflow_callback(mail_status=MailStatus.SUCCESS)
)
def mon_dag():
    # Tasks avec callbacks individuels
    risky_task = create_etl_task(
        selecteur="data_source",
        on_failure_callback=create_airflow_callback(mail_status=MailStatus.ERROR)
    )
```

### 3. Retry et Timeout

```python
default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

# Task avec timeout spécifique
detect_files = S3KeySensor(
    ...,
    timeout=timedelta(hours=2),
    poke_interval=timedelta(minutes=5),
    soft_fail=True  # Continue même en cas d'échec
)
```

---

Ce guide vous permet de créer des DAGs robustes en utilisant les tâches pré-définies et vos propres fonctions de processing métier. Pour plus d'informations, consultez la [documentation de l'infrastructure](infra.md) et les [conventions du projet](convention.md).
