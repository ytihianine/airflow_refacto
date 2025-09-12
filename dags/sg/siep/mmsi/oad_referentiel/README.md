# Documentation
### Informations générales
| Information | Valeur |
| -------- | -------- |
| Fichier source     | `oad_refentiel_dags.py`     |
| Description | Ce traitement permet d'actualiser les référentiels propres à l'OAD. |
| Fréquence d'exécution' | To define |
| Fonctionnement | Semi-automatisé |
| Propriétaires des données | MEF - SG - SIEP - MMSI |
| Mise en place de la pipeline | MEF - SG - DSCI - LdT |

### Données
| Information | Valeur |
| -------- | -------- |
| Données sources | Outil d'Aide au Diagnostic (OAD), fichier maintenu à la main |
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
