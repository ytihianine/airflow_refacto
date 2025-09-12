# Documentation
### Informations générales
| Information | Valeur |
| -------- | -------- |
| Fichier source     | `files_dags.py`     |
| Description | Ce traitement vérifie la présence de fichiers sur MinIO et effectue le traitement. |
| Fréquence d'exécution | toutes les 10 minutes, du lundi au vendredi |
| Fonctionnement | Semi-automatisé |
| Propriétaires des données | MEF - SG |
| Mise en place de la pipeline | MEF - SG - DSCI - LdT |

### Données
| Information | Valeur |
| -------- | -------- |
| Données sources | Outil XXXX, extraction format xls |
| Données de sorties | Base de données |
| Données sources archivées | Oui > MinIO |
| Structure des données sources | N/A |
| Structure des données de sortie | N/A |

### Configuration
| Information | Valeur |
| -------- | -------- |
| Variables | X |
| Connexions | db_data_store, minio_bucket_dsci |

<br />

Pour plus d'informations, rendez-vous [ici](../../../README.md)
