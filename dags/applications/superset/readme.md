# Documentation

### Objectifs
- [X] Mettre tous les admins propriétaires des graphiques
- [X] Mettre tous les admins propriétaires des tableaux de bord
- [] Sauvegarder la configuration des tableaux de bord

### Informations générales
| Information | Valeur |
| -------- | -------- |
| Fichier source     | `dags.py`     |
| Description | Sauvegarde automatisée des bases de données |
| Fréquence de mise à jour | `0 0,12 * * 1-5` => du lundi au vendredi, à 00h et 12h. |
| Fonctionnement | Automatisé |
| Propriétaires des données | MEF - SG - DSCI |
| Mise en place de la pipeline | MEF - SG - DSCI - LdT |

### Données
| Information | Valeur |
| -------- | -------- |
| Données sources | Base de donnnées |
| Données de sorties | Base de donnnées |
| Données sources archivées | Non |
| Structure des données sources | Aucune |
| Structure des données de sortie | Aucune |

### Configuration
| Information | Valeur |
| -------- | -------- |
| Variables | X |
| Connexions | db_data_store, postgresql_backup |

<br />

Pour plus d'informations, rendez-vous [ici](https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/airflow-demo)
