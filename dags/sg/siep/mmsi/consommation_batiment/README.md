# Documentation
### Informations générales
| Information | Valeur |
| -------- | -------- |
| Fichier source     | `mmsi_dags.py`     |
| Description | Ce traitement permet de suivre la consommation des bâtiments du MEF |
| Fréquence de mise à jour | Mensuelle |
| Fonctionnement | Semi-automatisé |
| Propriétaires des données | MEF - SG - SIEP |
| Mise en place de la pipeline | MEF - SG - DSCI - LdT |

### Données
| Information | Valeur |
| -------- | -------- |
| Données sources | Outil de suivi des fluides interministériel (OSFi), extraction format xls |
| Données de sorties | Base de données |
| Données sources archivées | Oui, MinIO |
| Structure des données sources | [Non disponible]() |
| Structure des données de sortie | [Non disponible]() |

### Configuration
| Information | Valeur |
| -------- | -------- |
| Variables | X |
| Connexions | minio_bucket_dsci, db_data_store |

<br />

Pour plus d'informations, rendez-vous [ici](../../../../README.md)
