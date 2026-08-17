# Guide de création des pipelines (dags)

Ce guide explique comment créer des pipelines Airflow (appelées DAGs dans Airflow) en utilisant les tâches pré-définies disponibles dans le dossier `utils/` et/ou en créant ses propres tâches.

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

- **DAGs (`dags/`)** : Orchestration des traitements métiers
- **Tasks (`modules.common_tasks/`)** : Tâches génériques réutilisables
- **Infrastructure (`modules.infra/`)** : Interaction avec les systèmes externes (base de données, S3, HTTP, mails)
- **Enums (`modules.enums/`)** : Enums transverses nécessaires dans les dags, tâches, fonctions ...
- **Types (`modules.types/`)** : Types transverses nécessaires dans les dags, tâches, fonctions ...
- **Utilitaires (`modules.utils/`)** : Tâches réutilisables et configuration

Les dags doivent respecter [cette organisation](./convention.md#dags)

### Workflow Standard

Il existe deux worflows principaux génériques qui nécessitent d'être adapté à chaque pipeline.
Le premier workflow permet de réaliser un ETL classique. Il contient les étapes suivantes
1. **Validation des paramètres** : Vérification des paramètres requis du DAG
2. **Extraction** : Lecture des données depuis diverses sources (S3, Grist, base de données)
3. **Transformation** : Application de fonctions de processing personnalisées
4. **Chargement** : Sauvegarde des résultats (S3, base de données)
5. **Notification** : Envoi de mails de succès/échec

Le second workflow permet de réaliser des actions qui ne nécessitent pas nécessairement de données. Il contient les étapes suivantes
1. **Validation des paramètres** : Vérification des paramètres requis du DAG
2. **Actions**: Réalise une action définie (ping, envoi de mail, requête API ...)
5. **Notification** : Envoi de mails de succès/échec


Les workflows peuvent être plus complexes et mélanger des étapes de chacun de ces workflows. Les étapes à absolument conserver sont
- **Validation des paramètres**
- **Notification**

## Structure des Paramètres

Chaque DAG doit définir ses paramètres selon la structure suivante :

```python
from airflow.sdk import dag
from modules.enums.dags import DagStatus
from modules.infra.mails.default_smtp import create_send_mail_callback, MailStatus
from modules.types.dags import DBParams, FeatureFlagsEnable
from modules.utils.config.dag_params import create_dag_params, create_default_args

@dag(
    dag_id="id_unique_du_dag",
    schedule="*/15 8-19 * * 1-5",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["Tag1", "Tag2"],
    description="Description courte",  # noqa
    default_args=create_default_args(),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="schema"),
        feature_flags=FeatureFlagsEnable(
            db=True,
            mail=False,
            s3=False,
            convert_files=False,
            download_grist_doc=False,
        ),
    ),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
    # Autres arguments
)
```

Les FeatureFlagsEnable permettent d'activer/désactiver certaines fonctionnalités du dag et/ou des tâches sans avoir à modifier le code.  


## Tâches Pré-définies Disponibles

### 1. Validation des Paramètres

Une tâche générique est disponible: `from modules.common_tasks.validation import validate_dag_parameters`

### 2. Tâches ETL (Extract, Transform, Load)

#### ETL depuis Grist
```python
from functools import partial

from modules.common_tasks.grist import download_grist_doc_to_s3, generic_grist_processing
from modules.constants import DEFAULT_GRIST_HOST
from modules.types.dags import TaskConfig
from modules.types.readers import GristReaderStrategy
from modules.types.tasks import ETLTask, SingleInputStep
from modules.types.writers import FileWriterStrategy

# Télécharger le document Grist en début de DAG
grist_doc = download_grist_doc_to_s3(
    selecteur="grist_doc",
    workspace_id="grist_ws_id",
    grist_host=DEFAULT_GRIST_HOST,
    api_token_key="grist_secret_key",
    use_proxy=True,
)

# ETL Grist : traitement d'une table du document avec fonction de processing personnalisée
grist_etl = ETLTask(
    task_config=TaskConfig(task_id="ma_table"),
    target="ma_table",
    reader=GristReaderStrategy(),
    steps=[
        SingleInputStep(
            fn=partial(
                generic_grist_processing,
                cols_to_keep=["colonne_1", "colonne_2"],
                cols_mapping={"colonne_source": "colonne_cible"},
                txt_columns=["colonne_2"],
                custom_fn=ma_fonction_processing,  # Fonction de processing métier
            ),
            input_key="ma_table",
            output_key="ma_table",
        )
    ],
    writers=[FileWriterStrategy()],
    add_metadata=True,
)
```

#### ETL Générique
```python
from modules.common_tasks.etl import create_task
from modules.types.dags import TaskConfig, ETLStep

# ETL générique avec traitement personnalisé
etl_task = create_task(
    task_config=TaskConfig(
        task_id="my_task_id",
    ),
    output_selecteur="selecteur",
    steps=[
        ETLStep(
            fn=ma_fonction_processing,
            kwargs={"additional_fn_args": True},  # kwargs passés à la function
            use_context=True,
            read_data=True,
        ),
        ...
        ETLStep(
            fn=ma_fonction_processing_2,
            use_previous_output=True,
        ),
    ],
    input_selecteurs=["input_1", "input_2"],
    add_metadata=True,  # Ajoute import_timestamp et snapshot_id
    export_output=True,
)
```

### 3. Gestion des Fichiers

#### Conversion vers Parquet
```python
from modules.common_tasks.file import create_parquet_converter_task

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
from modules.common_tasks.projet import get_selecteur_config
from modules.common_tasks.sql import (
    copy_tmp_table_to_real_table,
    create_projet_snapshot,
    create_tmp_tables,
    delete_tmp_tables,
    ensure_partition,
    import_file_to_db,
)
from dags.config import nom_projet, storage_options

# Récupération des configurations de selecteurs
selecteur_configs = get_selecteur_config(storage_options=storage_options)

# Création du snapshot du projet
create_snapshot = create_projet_snapshot(nom_projet=nom_projet)

# Création des tables temporaires
create_tables = create_tmp_tables(
    storage_options=storage_options,
    reset_id_seq=False,
)

# Importer les données -- Tâche dynamique
import_task = import_file_to_db.expand(selecteur_config=selecteur_configs)

# Création de partition mensuelle -- Tâche dynamique
create_partition = ensure_partition.expand(selecteur_config=selecteur_configs)

# Copie des données vers production
copy_to_prod = copy_tmp_table_to_real_table(storage_options=storage_options)

# Suppression des tables temporaires
delete_tables = delete_tmp_tables()
```

### 5. Opérations S3

```python
from modules.common_tasks.s3 import copy_s3_files, del_s3_files

# Copie de fichiers S3
copy_files = copy_s3_files()

# Suppression de fichiers S3
delete_files = del_s3_files()
```

## Bonnes Pratiques

### 1. Naming et Organisation

```python
# ✅ Bon : Utilisation de préfixes clairs
@dag("pipeline_ventes_mensuelles", ...)
def pipeline_ventes_mensuelles():
    extract_data = ETLTask(...)  # ou create_task(...)
    transform_data = create_parquet_converter_task(...)

# ❌ Éviter : Noms génériques
@dag("dag1", ...)
def my_dag():
    task1 = ETLTask(...)  # ou create_task(...)
```

### 2. Paramétrage

```python
# ✅ Bon : Utilisation des constantes
from modules.constants import (
    DEFAULT_S3_BUCKET, DEFAULT_PG_DATA_CONN_ID, DEFAULT_S3_CONN_ID
)
```

### 3. Gestion des Dépendances

```python
# ✅ Bon : Utilisation de chain pour la lisibilité
chain(
    validate_dag_parameters(),
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
        taux: float.

    Returns:
        DataFrame avec les taux de conversion calculés

    Notes: (Optionnel)
        taux: s'exprime entre 0 et 1

    Logique métier: (Optionnel)
        - Taux = (nb_conversions / nb_visiteurs) * 100
        - Filtrage des canaux avec moins de 100 visiteurs
    """
    # Implementation...
```

## Gestion des Erreurs

### 1. Callbacks de Notification

```python
from modules.infra.mails.default_smtp import create_send_mail_callback, MailStatus

@dag(
    ...,
    on_failure_callback=create_send_mail_callback(mail_status=MailStatus.ERROR),
    on_success_callback=create_send_mail_callback(mail_status=MailStatus.SUCCESS)
)
def mon_dag():
    # Tasks avec callbacks individuels (via TaskConfig)
    risky_task = create_task(
        task_config=TaskConfig(
            task_id="risky_task",
            on_failure_callback=create_send_mail_callback(mail_status=MailStatus.ERROR),
        ),
        output_selecteur="data_source",
        steps=[...],
    )
```

### 2. Retry et Timeout

```python
from modules.utils.config.dag_params import create_default_args

# Retry au niveau du DAG
default_args = create_default_args(
    retries=2,
    retry_delay=timedelta(minutes=5),
)

# Retry au niveau d'une tâche (via TaskConfig)
task_with_retry = create_task(
    task_config=TaskConfig(
        task_id="ma_tache",
        retries=3,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=30),
    ),
    output_selecteur="selecteur",
    steps=[...],
)

# Task avec timeout spécifique
detect_files = S3KeySensor(
    ...,
    timeout=timedelta(hours=2),
    poke_interval=timedelta(minutes=5),
    soft_fail=True  # Continue même en cas d'échec
)
```

---

Ce guide vous permet de créer des DAGs robustes en utilisant les tâches pré-définies et vos propres fonctions de processing métier. Pour plus d'informations, consultez la [documentation de l'infrastructure](modules.infra.md) et les [conventions du projet](convention.md).
