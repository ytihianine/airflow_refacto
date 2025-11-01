# Configuration des projets

TODO:
- décrire la création des projets
- décrire comment l'utiliser
- Documenter les dags
- Ajouter la section RGPD

## Création des projets
La configuration des projets se fait via Grist pour les metadata du projet.
La configuration des dags se réalise dans la section `dags/`, dans le fichier `mon_projet_dags.py`. Pour configurer votre dag, vous devez suivre la documentation [creation_dags](creation_dags.md).

## Documenter les DAGS
Ci-dessous un template qui peut être copier/coller pour documenter les dags. Tous les champs sont obligatoires.

### Informations générales
| Information | Valeur |
| -------- | -------- |
| Fichier source     | `mmsi_dags.py`     |
| Description | Ce traitement permet de suivre la consommation des bâtiments du MEF |
| Fréquence de mise à jour | Mensuelle |
| Fonctionnement | Automatisé / Semi-automatisé |
| Propriétaires des données | MEF - SG - SIEP |
| Mise en place de la pipeline | MEF - SG - DSCI - LdT |

### Données
| Information | Valeur |
| -------- | -------- |
| Données sources | Outil de suivi des fluides interministériel (OSFi), extraction format xls |
| Données de sorties | Base de données |
| Données sources archivées | Oui, MinIO |
| Structure des données sources | [insérer un lien vers la documentation des données sources]() |
| Structure des données de sortie | [insérer un lien vers la documentation des données de sortie]() |

### Configuration
| Information | Valeur |
| -------- | -------- |
| Variables | username, password, base_url |
| Connexions | minio_sg |

<br />
<hr />

Description de ce qui est attendu pour chaque ligne:
- **Fichier source**: préciser le fichier qui contient le dag.
- **Description**: décrire l'objectif de la pipeline.
- **Fréquence de mise à jour**: peut être soir l'intervalle entre chaque lancement de dag, soit l'intervalle entre ajout de nouvelles données.
- **Fonctionnement**: Automatisé si le dag peut récupérer réalisé toute la chaîne sans intervention. Semi-automatisé si une action est nécessaire (exemple: extraire les données sources et les déposés dans un endroit spécifique).
- **Propriétaires des données**: de quel service provient les données.
- **Mise en place de la pipeline**: quel service a mis en place la pipeline.
- **Données sources**: d'où proviennent les données, préciser le format d'entrée.
- **Données de sortie**: sous quel format vont être stockées les données.
- **Données sources archivées**: si les données sont archivées. Si oui, préciser la façon dont elles sont archivées (Base de données, files storage...).
- **Structure des données sources**: si une documentation des données est disponible.
- **Structure des données  de sortie**: si une documentation des données est disponible.
- **Variables**: préciser le nom des variables utilisées dans la pipeline. Si aucune variable n'est utilisée, renseignez X.
- **Connexions**: préciser le nom des connexions utilisées dans la pipeline. Si aucune connexion n'est utilisée, renseignez X.

Les variables et les connexions sont définies et modifiables directement depuis l'interface web de l'instance Airflow
