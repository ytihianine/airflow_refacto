# Documentation
### Informations générales
| Information | Valeur |
| -------- | -------- |
| Fichier source     | `oad_dags.py`     |
| Description | Ce traitement permet de définir les caractéristiques principales d'un biens. |
| Fréquence d'exécution' | To define |
| Fonctionnement | Semi-automatisé |
| Propriétaires des données | MEF - SG - SIEP - MMSI |
| Mise en place de la pipeline | MEF - SG - DSCI - LdT |

### Données
| Information | Valeur |
| -------- | -------- |
| Données sources | Outil d'Aide au Diagnostic (OAD), extraction format xls |
| Données de sorties | Base de données |
| Données sources archivées | Oui > MinIO |
| Structure des données sources | N/A |
| Structure des données de sortie | N/A |

### Configuration
| Information | Valeur |
| -------- | -------- |
| Variables | X |
| Connexions | db_data_store, minio_dsci_bucket |

<br />

Pour plus d'informations, rendez-vous [ici](../../../README.md)
