# Documentation du dossier `infra`


Un schéma de la solution complète est disponible à cette addresse : **ajouter_le_lien**
Le dossier `infra` contient les abstractions et wrappers pour toutes les interactions avec l'infrastructure externe : bases de données, système de fichiers, clients HTTP, envoi d'emails, etc. Cette couche permet de découpler la logique métier des détails techniques d'implémentation.

## Vue d'ensemble

```
infra/
├── database/           # Handlers pour bases de données
├── file_handling/      # Gestion des fichiers (local, S3)
├── http_client/        # Clients HTTP génériques
├── grist/             # Client spécialisé pour Grist
└── mails/             # Système d'envoi d'emails
```

## 1. Module Database (`infra/database/`)

### Description
Fournit une interface unifiée pour les opérations de base de données, avec support pour PostgreSQL et SQLite.

### Classes principales

#### `BaseDBHandler` (Classe abstraite)
Interface de base définissant les méthodes communes :
- `execute()` : Exécution de requêtes sans retour
- `fetch_one()`, `fetch_all()` : Récupération de lignes sous forme de dictionnaires
- `fetch_df()` : Récupération sous forme de DataFrame pandas
- `insert()`, `bulk_insert()` : Insertion de données
- `get_uri()` : Récupération de l'URI de connexion

#### `PostgresDBHandler`
Implémentation pour PostgreSQL utilisant Airflow PostgresHook.

#### `SQLiteDBHandler`
Implémentation pour SQLite (utile pour les tests et imports Grist).

### Initialisation

```python
from infra.database.factory import create_db_handler

# PostgreSQL (par défaut)
db = create_db_handler("my_postgres_conn_id")

# SQLite
db = create_db_handler("/path/to/database.db", db_type="sqlite")

# Utilisation
results = db.fetch_all("SELECT * FROM my_table WHERE id = %s", (123,))
df = db.fetch_df("SELECT * FROM my_table")
```

### Exceptions
- `DatabaseError` : Erreur générale de base de données
- `ConnectionError` : Problème de connexion

## 2. Module File Handling (`infra/file_handling/`)

### Description
Système unifié de gestion des fichiers supportant différents backends de stockage (local, S3/MinIO).

### Classes principales

#### `BaseFileHandler` (Classe abstraite)
Interface de base pour les opérations sur fichiers :
- `read()` : Lecture de fichier
- `write()` : Écriture de fichier
- `delete()` : Suppression de fichier
- `exists()` : Vérification d'existence
- `get_metadata()` : Récupération des métadonnées

#### `LocalFileHandler`
Gestion des fichiers sur le système de fichiers local.

#### `S3FileHandler`
Gestion des fichiers sur S3/MinIO utilisant Airflow S3Hook.

### Initialisation

```python
from infra.file_handling.factory import FileHandlerFactory

# Fichiers locaux
local_handler = FileHandlerFactory.create_handler("local", base_path="/tmp")

# S3/MinIO
s3_handler = FileHandlerFactory.create_handler(
    "s3",
    connection_id="minio_conn",
    bucket="my-bucket"
)

# Utilisation
content = local_handler.read("path/to/file.txt")
s3_handler.write("remote/file.txt", "Hello World")
```

### Utilitaires DataFrame

```python
from infra.file_handling.dataframe import read_dataframe, write_dataframe

# Lecture d'un DataFrame depuis différents formats
df = read_dataframe(
    file_handler=s3_handler,
    file_path="data.csv",
    read_options={"sep": ";", "encoding": "utf-8"}
)

# Écriture d'un DataFrame
write_dataframe(
    df=my_dataframe,
    file_handler=s3_handler,
    file_path="output.parquet",
    format="parquet"
)
```

### Exceptions
- `FileHandlerError` : Erreur générale de gestion de fichier
- `FileNotFoundError` : Fichier introuvable
- `FilePermissionError` : Problème de permissions
- `FileValidationError` : Erreur de validation du fichier

## 3. Module HTTP Client (`infra/http_client/`)

### Description
Clients HTTP génériques avec gestion des erreurs, configuration et adaptation.

### Classes principales

#### `AbstractHTTPClient` (Classe abstraite)
Interface de base pour les clients HTTP :
- `request()` : Requête HTTP générique
- `get()`, `post()`, `put()`, `delete()` : Méthodes HTTP spécifiques

#### `HTTPResponse`
Wrapper pour les réponses HTTP avec méthodes pratiques :
- `json` : Propriété pour récupérer la réponse en JSON
- `text` : Propriété pour récupérer la réponse en texte
- `headers` : Headers de la réponse
- `status_code` : Code de statut HTTP

### Initialisation

```python
from infra.http_client.factory import create_http_client
from infra.http_client.config import ClientConfig

config = ClientConfig(
    base_url="https://api.example.com",
    timeout=30,
    headers={"Authorization": "Bearer token"}
)

client = create_http_client("requests", config)

# Utilisation
response = client.get("/users")
if response.ok:
    users = response.json
```

### Exceptions
- `HTTPClientError` : Erreur générale de client HTTP
- `TimeoutError` : Timeout de requête
- `AuthenticationError` : Problème d'authentification

## 4. Module Grist (`infra/grist/`)

### Description
Client spécialisé pour interagir avec l'API Grist.

### Classe principale

#### `GristAPI`
Client pour les opérations Grist :
- Lecture de tables
- Écriture de données
- Gestion des documents et espaces de travail

### Initialisation

```python
from infra.grist.client import GristAPI
from infra.http_client.factory import create_http_client

http_client = create_http_client("requests", config)

grist = GristAPI(
    http_client=http_client,
    base_url="https://grist.example.com",
    doc_id="document_id",
    api_token="your_token"
)

# Utilisation
df = grist.get_table_data("TableName")
grist.update_records("TableName", records_data)
```

## 5. Module Mails (`infra/mails/`)

### Description
Système complet d'envoi d'emails avec support des templates, pièces jointes et intégration Airflow.

### Classes principales

#### `MailSender`
Gestionnaire principal pour l'envoi d'emails :
- Envoi avec templates HTML
- Gestion des pièces jointes
- Intégration avec Airflow SMTP

#### `MailConfig`
Configuration pour l'envoi d'emails :
- Connexion SMTP
- Répertoire des templates
- Configuration par défaut

#### `MailMessage`
Structure d'un message email :
- Destinataires (to, cc, bcc)
- Sujet et contenu
- Pièces jointes
- Priorité

### Initialisation

```python
from infra.mails.sender import MailSender
from infra.mails.config import MailConfig, MailMessage

config = MailConfig(
    smtp_conn_id="smtp_connection",
    templates_dir="templates/",
    from_email="noreply@example.com"
)

sender = MailSender(config)

message = MailMessage(
    to=["user@example.com"],
    subject="Notification",
    template_name="pipeline_success",
    context={"dag_id": "my_dag", "execution_date": "2025-01-01"}
)

sender.send(message)
```

### Templates intégrés
- `pipeline_start.html` : Début de pipeline
- `pipeline_end_success.html` : Succès de pipeline
- `pipeline_end_error.html` : Erreur de pipeline
- `dag_info.html` : Informations générales de DAG
- `logs_recap.html` : Récapitulatif des logs

### Fonctions utilitaires

```python
from infra.mails.sender import create_airflow_callback
from infra.mails.config import MailStatus

# Création de callbacks Airflow pour notifications automatiques
on_failure = create_airflow_callback(MailStatus.ERROR)
on_success = create_airflow_callback(MailStatus.SUCCESS)

# Utilisation dans DAG
@dag(
    on_failure_callback=on_failure,
    on_success_callback=on_success
)
def my_dag():
    pass
```

## Bonnes pratiques

### 1. Utilisation des fabriques
Toujours utiliser les fabriques (`create_db_handler`, `FileHandlerFactory.create_handler`) pour créer les instances.

### 2. Gestion des erreurs
Capturer les exceptions spécifiques à chaque module plutôt que les exceptions génériques.

```python
from infra.database.exceptions import DatabaseError
from infra.file_handling.exceptions import FileHandlerError

try:
    db.execute("INSERT INTO...")
except DatabaseError as e:
    logging.error(f"Database error: {e}")
```

### 3. Configuration centralisée
Utiliser les classes de configuration pour centraliser les paramètres.

### 4. Lazy loading
Les handlers utilisent l'initialisation paresseuse des connexions pour optimiser les performances.

### 5. Context managers
Utiliser les context managers quand disponibles pour la gestion automatique des ressources.

## Tests et mocking

Chaque handler peut être mocké facilement grâce aux interfaces abstraites :

```python
from unittest.mock import Mock
from infra.database.base import BaseDBHandler

# Mock pour les tests
mock_db = Mock(spec=BaseDBHandler)
mock_db.fetch_all.return_value = [{"id": 1, "name": "test"}]
```

Cette architecture modulaire facilite les tests unitaires et l'évolution du code en isolant les dépendances externes.
