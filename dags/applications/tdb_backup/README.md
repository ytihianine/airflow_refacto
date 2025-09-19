# Documentation
### Informations générales
| Information | Valeur |
| -------- | -------- |
| Fichier source     | `sauvegarde_dags.py`     |
| Description | Cette pipeline sauvegarde la configuration des tableaux de bord de l'instance Chartsgouv. |
| Fréquence de mise à jour | Quotidienne |
| Fonctionnement | Automatisé |
| Propriétaires des données | MEF - SG - DSCI - LdT |
| Mise en place de la pipeline | MEF - SG - DSCI - LdT |

### Données
| Information | Valeur |
| -------- | -------- |
| Données sources | API instance Chartsgouv |
| Données de sorties | MinIO |
| Données sources archivées | X |
| Structure des données sources | [Documentation de l'API](https://chartsgouv-mef-sg.lab.incubateur.finances.rie.gouv.fr/swagger/v1) (nécessite des autorisations) |
| Structure des données de sortie | ZIP qui contient des YAML |

### Configuration
| Information | Valeur |
| -------- | -------- |
| Variables | chartsgouv_base_url, chartsgouv_admin_username, chartsgouv_admin_password |
| Connexions | X |

<br />

Pour plus d'informations, rendez-vous [ici](../../../README.md)
