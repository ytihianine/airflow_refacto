# Documentation du dossier `infra`


Un schéma de la solution complète est disponible à cette addresse : **ajouter_le_lien**
Le dossier `infra` contient les abstractions et wrappers pour toutes les interactions avec l'infrastructure externe : bases de données, système de fichiers, clients HTTP, envoi d'emails, catalogues de données, etc. Cette couche permet de découpler la logique métier des détails techniques d'implémentation.

## Vue d'ensemble

```
infra/
├── catalog/            # Gestion des catalogues de données (Iceberg, Polaris)
├── database/           # Intéragir avec les bases de données
├── file_system/        # Intéragir avec les gestionnaires de fichiers (local, S3, ...)
├── http_client/        # Intéragir avec les serveurs webs (HTTP, API, ...)
├── grist/              # Client spécialisé pour Grist
└── mails/              # Système d'envoi d'emails
```

## 1. Module Catalog (`infra/catalog/`)

### Description
Gestion des catalogues de données pour les tables Iceberg et l'administration avec Polaris.

### Classes principales

#### `IcebergCatalog` (Dataclass)
Client pour interagir avec un catalogue Iceberg :
- `create_namespace()` : Création de namespaces hiérarchiques
- `create_table()` : Création de table à partir d'un schéma DataFrame
- `update_table()` : Mise à jour du schéma avec de nouvelles colonnes
- `write_table()` : Écriture dans une table Iceberg à partir d'un DataFrame
- `write_table_and_namespace()` : Création du namespace et écriture
- `read_table()` : Chargement d'une table depuis le catalogue
- `read_table_as_df()` : Lecture d'une table sous forme de DataFrame pandas
- `drop_table()` : Suppression de table (avec purge optionnelle des données)
- `list_tables()` : Liste des tables dans un namespace (avec filtrage par pattern)

#### `PolarisCatalog` (Dataclass)
Client pour l'administration d'un catalogue Polaris :
- `get_root_access_token()` : Obtention d'un token OAuth
- `get_catalog_info()` : Récupération des métadonnées du catalogue
- `create_catalog()` : Création d'un catalogue avec configuration S3
- `delete_catalog()` : Suppression d'un catalogue
- `create_principal()` : Création d'un principal et récupération des credentials
- `create_principal_role()`, `create_catalog_role()` : Gestion des rôles
- `assign_principal_role()`, `assign_catalog_role()` : Attribution des rôles
- `grant_catalog_privileges()` : Attribution de privilèges de gestion

### Initialisation

```python
from modules.infra.catalog.iceberg import IcebergCatalog

catalog = IcebergCatalog(
    name="my_catalog",
    properties={
        "uri": "http://polaris:8181/api/catalog",
        "credential": "client_id:client_secret",
        "warehouse": "my_warehouse",
    }
)

# Écriture d'un DataFrame
catalog.write_table_and_namespace(
    namespace="my_namespace",
    table_name="my_table",
    df=my_dataframe
)

# Lecture
df = catalog.read_table_as_df(namespace="my_namespace", table_name="my_table")
```

### Fonctions utilitaires

```python
from modules.infra.catalog.iceberg import generate_catalog_properties

# Génération des propriétés de catalogue (SSL, credentials, options custom)
properties = generate_catalog_properties(
    uri="http://polaris:8181/api/catalog",
    credential="client_id:client_secret",
    warehouse="my_warehouse",
)
```

## 2. Module Database (`infra/database/`)

### Description
Fournit une interface unifiée pour les opérations de base de données.
Bases de données supportées :
- PostgreSQL
- SQLite
- Trino (lecture seule)

### Classes principales

#### `DBInterface` (Classe abstraite)
Interface de base définissant les méthodes communes :
- `get_uri()` : Récupération de l'URI de connexion
- `get_conn()` : Obtention de la connexion à la base
- `execute()` : Exécution de requêtes sans retour
- `fetch_one()`, `fetch_all()` : Récupération de lignes sous forme de dictionnaires
- `fetch_df()` : Récupération sous forme de DataFrame pandas
- `insert()`, `bulk_insert()` : Insertion de données
- `update()` : Mise à jour de lignes
- `delete()` : Suppression de lignes
- `begin()`, `commit()`, `rollback()` : Gestion des transactions
- `copy_expert()` : Opération COPY en masse (type PostgreSQL)

#### `PgAdapter`
Implémentation pour PostgreSQL. Supporte deux modes d'initialisation : via un Airflow connection ID ou des paramètres psycopg2 directs.

#### `SQLiteAdapter`
Implémentation pour SQLite via sqlite3.

#### `TrinoAdapter`
Implémentation en lecture seule pour Trino. Les opérations d'écriture (`insert`, `bulk_insert`, `update`, `delete`) lèvent `NotImplementedError`.

### Initialisation

```python
from modules.infra.database.factory import create_db_handler
from modules.enums.database import DatabaseType

# PostgreSQL (par défaut)
db = create_db_handler(connection_id="my_postgres_conn_id", db_type=DatabaseType.POSTGRES)

# SQLite
db = create_db_handler(connection_id="/path/to/database.db", db_type=DatabaseType.SQLITE)

# Trino (lecture seule)
db = create_db_handler(
    db_type=DatabaseType.TRINO,
    host="trino.example.com",
    user="my_user",
    catalog="my_catalog",
    port=443,
    schema="my_schema",
)

# Utilisation
results = db.fetch_all("SELECT * FROM my_table WHERE id = %s", (123,))
df = db.fetch_df("SELECT * FROM my_table")
```

### Exceptions
- `DatabaseError` : Erreur générale de base de données
- `ConnectionError` : Problème de connexion
- `QueryError` : Erreur d'exécution de requête
- `TransactionError` : Erreur lors d'une opération transactionnelle

## 3. Module File System (`infra/file_system/`)

### Description
Système unifié de gestion des fichiers supportant différents backends de stockage (local, S3/MinIO).

### Classes principales

#### `FSInterface` (Classe abstraite)
Interface de base pour les opérations sur fichiers :
- `read()` : Lire un fichier
- `write()` : Écrire un fichier
- `delete()` : Supprimer des fichiers
- `delete_single()` : Supprimer un fichier unique
- `exists()` : Vérifier la présence d'un fichier
- `get_metadata()` : Récupérer des métadonnées (`FileMetadata`)
- `list_files()` : Lister des fichiers (avec patterns glob)
- `move()` : Déplacer un fichier
- `copy()` : Copier un fichier
- `validate()` : Valider un fichier selon des critères
- `get_absolute_path()` : Convertir un chemin relatif → absolu

#### `LocalFS`
Gestion des fichiers sur le système de fichiers local. Supporte les écritures atomiques via fichiers temporaires.

#### `S3FS`
Gestion des fichiers sur S3/MinIO utilisant Airflow S3Hook ou un client boto3. Supporte plusieurs formats (CSV, Excel, Parquet, JSON), l'inférence de content-type et le listage paginé.

### Initialisation

```python
from modules.infra.file_system.factory import create_file_handler, create_default_s3_handler, create_local_handler
from modules.enums.filesystem import FileHandlerType

# Fichiers locaux
local_handler = create_file_handler(handler_type=FileHandlerType.LOCAL, base_path="/tmp")
# ou
local_handler = create_local_handler(base_path="/tmp")

# S3/MinIO
s3_handler = create_file_handler(
    handler_type=FileHandlerType.S3,
    connection_id="minio_conn",
    bucket="my-bucket"
)
# ou avec la configuration par défaut (bucket DSCI)
s3_handler = create_default_s3_handler()

# Utilisation
content = local_handler.read("path/to/file.txt")
s3_handler.write("remote/key/file.txt", "Hello World")
```

### Utilitaires DataFrame

Une fonction utilitaire est définie pour obtenir un DataFrame directement. Le format est auto-détecté à partir de l'extension du fichier (CSV, Excel, Parquet, JSON).

```python
from modules.infra.file_system.dataframe import read_dataframe

# Lecture d'un DataFrame depuis différents formats
df = read_dataframe(
    file_handler=s3_handler,
    file_path="data.csv",
    read_options={"sep": ";", "encoding": "utf-8"}
)
```

### Exceptions
- `FileHandlerError` : Erreur générale de gestion de fichier
- `FileNotFoundError` : Fichier introuvable
- `FilePermissionError` : Problème de permissions
- `FileValidationError` : Erreur de validation du fichier
- `FileTypeError` : Type de fichier non supporté
- `FileCorruptError` : Fichier corrompu

## 4. Module HTTP Client (`infra/http_client/`)

### Description
Clients HTTP génériques avec gestion des erreurs etconfiguration avancée.

### Classes principales

#### `HttpInterface` (Classe abstraite)
Interface de base pour les clients HTTP (supporte le context manager) :
- `request()` : Requête HTTP générique
- `get()`, `post()`, `put()`, `patch()`, `delete()` : Méthodes HTTP spécifiques
- `close()` : Libération des ressources

#### `HTTPResponse`
Wrapper pour les réponses HTTP :
- `json()` : Récupération de la réponse en JSON
- `text` : Propriété pour récupérer la réponse en texte
- `content` : Contenu brut de la réponse
- `headers` : Headers de la réponse
- `status_code` : Code de statut HTTP
- `raise_for_status()` : Lève une exception si le statut est en erreur

#### `ClientConfig` (Dataclass)
Configuration du client HTTP :
- URL de base, timeout, vérification SSL
- Authentification (token, Bearer/Basic)
- Retry (nombre max, statuts et méthodes de retry)
- Rate limiting (requêtes par seconde)
- Support proxy

#### Adaptateurs
- `RequestsClient` : Implémentation basée sur le package `requests`.
- `HttpxClient` : Implémentation basée sur `httpx`.

### Initialisation

```python
from modules.infra.http_client.factory import create_http_client
from modules.infra.http_client.config import ClientConfig
from modules.enums.http import HttpHandlerType

config = ClientConfig(
    timeout=30,
    headers={"Authorization": "Bearer token"}
)

# Requests
client = create_http_client(client_type=HttpHandlerType.REQUEST, config=config)

# HTTPx
client = create_http_client(client_type=HttpHandlerType.HTTPX, config=config)

# Utilisation
response = client.get("https://api.example.com/users")
if response.status_code == 200:
    users = response.json()
```

### Exceptions
- `HTTPClientError` : Erreur générale de client HTTP (stocke `status_code` et `response`)
- `ConnectionError` : Problème de connexion
- `TimeoutError` : Timeout de requête
- `RequestError` : Erreur lors de l'envoi de la requête
- `ResponseError` : Erreur dans la réponse
- `AuthenticationError` : Erreur d'authentification (401)
- `AuthorizationError` : Erreur d'autorisation (403)
- `APIError` : Erreur API (4xx, 5xx)
- `RateLimitError` : Limite de débit atteinte (429)

## 5. Module Grist (`infra/grist/`)

### Description
Client spécialisé pour interagir avec l'API Grist.

### Classe principale

#### `GristClient`
Client pour les opérations Grist

### Initialisation

```python
from modules.infra.grist.client import GristClient
from modules.infra.http_client.factory import create_http_client
from modules.infra.http_client.config import ClientConfig
from modules.enums.http import HttpHandlerType

config = ClientConfig(timeout=30)
http_client = create_http_client(client_type=HttpHandlerType.REQUEST, config=config)

grist = GristClient(
    http_client=http_client,
    grist_host="https://grist.example.com",
    api_token="your_token"
)

# Utilisation
doc_id = "your_doc_id"
df = grist.get_df_from_records(doc_id=doc_id, table_id="TableName")
grist.send_dataframe_to_grist(doc_id=doc_id, table_id="TableName", df=my_dataframe)
grist.post_records(doc_id=doc_id, table_id="TableName", records=records_data)
```

## 6. Module Mails (`infra/mails/`)

### Description
Système d'envoi d'emails avec support des templates Jinja2 et intégration Airflow SMTP.

### Classes principales

#### `MailMessage` (Dataclass)
Structure d'un message email :
- `to` : Destinataires principaux
- `mail_status` : Statut optionnel (`MailStatus`) pour génération automatique du template et du sujet
- `subject` : Sujet (auto-généré si `mail_status` est fourni)
- `html_content` : Contenu HTML (auto-généré si `mail_status` est fourni)
- `template_dir` : Répertoire personnalisé de templates
- `template_parameters` : Paramètres pour le rendu du template
- `cc`, `bcc` : Destinataires en copie
- `from_email` : Expéditeur
- `files` : Pièces jointes
- `custom_headers` : En-têtes personnalisés

Si `mail_status` est fourni, le template et le sujet sont automatiquement générés. Sinon, `html_content` et `subject` doivent être fournis manuellement.

### Fonctions principales

#### `send_mail()`
Envoi d'un email via Airflow `send_email_smtp`. Valide que le message possède un sujet et un contenu HTML.

#### `render_template()`
Rendu d'un template Jinja2 avec paramètres. Le répertoire par défaut est `infra/mails/templates/`.

#### `create_send_mail_callback()`
Crée une fonction callback Airflow pour les notifications automatiques de pipeline.

### Initialisation

```python
from modules.infra.mails.default_smtp import MailMessage, send_mail, create_send_mail_callback
from modules.enums.mail import MailStatus

# Envoi avec statut (template auto-généré)
message = MailMessage(
    to=["user@example.com"],
    mail_status=MailStatus.SUCCESS,
    template_parameters={"dag_id": "my_dag", "execution_date": "2025-01-01"}
)
send_mail(message)

# Envoi avec contenu personnalisé
message = MailMessage(
    to=["user@example.com"],
    subject="Notification",
    html_content="<p>Mon contenu HTML</p>"
)
send_mail(message)
```

### Templates intégrés
- `pipeline_start.html` : Début de pipeline
- `pipeline_end_success.html` : Succès de pipeline
- `pipeline_end_error.html` : Erreur de pipeline
- `logs_recap.html` : Récapitulatif des logs

### Callbacks Airflow

```python
from modules.infra.mails.default_smtp import create_send_mail_callback
from modules.enums.mail import MailStatus

# Création de callbacks pour notifications automatiques
on_failure = create_send_mail_callback(MailStatus.ERROR)
on_success = create_send_mail_callback(MailStatus.SUCCESS)

# Utilisation dans DAG
@dag(
    on_failure_callback=on_failure,
    on_success_callback=on_success
)
def my_dag():
    pass
```

## Bonnes pratiques

### 1. Utilisation des factory fonctions
Toujours utiliser les factory fonctions (`create_db_handler`, `create_file_handler`, `create_http_client`) pour créer les instances.

### 2. Gestion des erreurs
Capturer les exceptions spécifiques à chaque module plutôt que les exceptions génériques.

```python
from modules.infra.database.exceptions import DatabaseError
from modules.infra.file_system.exceptions import FileHandlerError

try:
    db.execute("INSERT INTO...")
except DatabaseError as e:
    logging.error(f"Database error: {e}")
```

### 3. Configuration centralisée
Utiliser les classes de configuration (`ClientConfig`) pour centraliser les paramètres.

### 4. Lazy loading
Les handlers utilisent l'initialisation paresseuse des connexions pour optimiser les performances.

### 5. Context managers
Utiliser les context managers quand disponibles pour la gestion automatique des ressources (ex. `HttpInterface`).

## Tests et mocking

Chaque handler peut être mocké facilement grâce aux interfaces abstraites :

```python
from unittest.mock import Mock
from modules.infra.database.base import DBInterface

# Mock pour les tests
mock_db = Mock(spec=DBInterface)
mock_db.fetch_all.return_value = [{"id": 1, "name": "test"}]
```

Cette architecture modulaire facilite les tests unitaires et l'évolution du code en isolant les dépendances externes.
