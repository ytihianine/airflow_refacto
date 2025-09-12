# Documentation
### Informations générales
| Information | Valeur |
| -------- | -------- |
| Fichier source     | `tdb_interne_dags.py`     |
| Description | Ce traitement permet d'actualiser les données du tableau de bord interne du SIRCOM |
| Fréquence d'exécution | Toutes les 8 minutes de 7-13h et de 14-19h du lundi au vendredi |
| Fréquence de mise à jour | Multiples |
| Fonctionnement | Automatisé |
| Propriétaires des données | MEF - SG - SIRCOM |
| Mise en place de la pipeline | MEF - SG - DSCI - LdT |

### Données
| Information | Valeur |
| -------- | -------- |
| Données sources | Grist |
| Données de sorties | Base de donnnées |
| Données sources archivées | Non |
| Structure des données sources | [Non disponible]() |
| Structure des données de sortie | [Voir dans Grist]() |

### Configuration
| Information | Valeur |
| -------- | -------- |
| Variables | grist_secret_key |
| Connexions | db_data_store |

<br />

Pour plus d'informations, rendez-vous [ici](https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/airflow-demo)
