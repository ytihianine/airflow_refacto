# Convention de nommage

## Table des matières
- [Convention de nommage](#convention-de-nommage)
  - [Table des matières](#table-des-matières)
  - [Dags](#dags)
  - [S3](#s3)
  - [SQL](#sql)
  - [Grist](#grist)
  - [Superset](#superset)


## Dags

Tous les dags doivent êtres organisés de la façon suivante:
```
.
├── src/dags
│   ├── projet_X
│   │   ├── grist
│   │   │   ├── sql   # Contient les scripts sql
│   │   │   │   └── projet_X.sql
│   │   │   ├── dags.py   # Définition du dag
│   │   │   ├── process.py  # Contient toutes les fonctions de processing
│   │   │   ├── actions.py   # Contient les actions à réaliser dans le dag
│   │   │   ├── enums.py   # (Optionnel) Enums propres au dag
│   │   │   ├── entities.py   # (Optionnel) Types propres au dag
│   │   │   ├── readme.md   # Documentation du dag
│   │   │   └── tasks.py  # Contient les tâches spécifiques au dag
```

## S3

Les buckets S3 sont organisés par niveau. Il existe un total de 4 niveaux:
- Niveau 1: FONCTION (ex: Finance, RH, Marketing)
- Niveau 2: ACTIVITÉ (ex: Comptabilité, Paie, Recrutement)
- Niveau 3: PROCESSUS/TRANSACTION (ex: Factures, Contrats)
- Niveau 4: FONCTION DOSSIER/SÉRIE (ex: 2024, Client_X)

```
bucket/niveau_1/niveau_2/niveau_3/niveau_4/file.ext
# Exemple 1: bucket/immobilier/energie/consommation/20250801/13h30/conso.parquet
# Exemple 2: bucket/finances/budget/ht2/20250801/13h30/achat.parquet
```
Dans les exemples, le niveau 4 est composé de la date et de l'heure au format `/AAAAMMDD/HHhMM`

Avec Iceberg, le niveau 4 n'est pas nécessaire car il sera géré via les partitions si nécessaires.  
Il rajoute une profondeur, automatiquement, entre le bucket et le niveau 1 qui correspond au nom du catalog
```
bucket/niveau_1/niveau_2/niveau_3/nom_table
# Exemple 1: bucket/catalog/immobilier/energie/consommation/conso
```

## SQL
1. Les tables

Application de ces [règles](https://www.postgresql.org/docs/7.0/syntax525.htm).
Le nom des tables doit définis avec les agents métiers.

Une table doit obligatoirement contenir les colonnes et le partitionnement suivant

```sql
CREATE TABLE IF NOT EXISTS schema.table (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    colonne_metier_1 TYPE,
    ...
    colonne_metier_N TYPE,
    import_timestamp TIMESTAMP,
    PRIMARY KEY (id_row, import_timestamp)
) PARTITION BY RANGE (import_timestamp);
```

2. Les vues

Les vues suivent les même règles que les tables.
En complément, le suffix `_vw` doit être rajouté pour faciliter la recherche.

3. Les colonnes

Les colonnes suivent les même règles que les tables.
En complément, si une même information est présente dans plusieurs tables, il faut s'assurer de conserver le même nom de colonne par cohérence.

## Grist
Ces conventions visent à faciliter le traitement des données issues de Grist.
- Les tables de documentations

Ajouter le préfix `doc_` => `doc_nom_de_la_table`

- Les tables métiers

Aucun préfix ou suffix nécessaire. Il faut néanmoins qu'elles possèdent un nom clair et concis qui découlent de la structuration des données définies dans le projet.

- Les tables de référence

Ajouter le préfix `ref_` => `ref_direction`

- Les tables d'onglets

Ajouter le préfix `onglet_` => `onglet_global_dsci`  
Les onglets permettent de hiérarchiser visuellement les tables sur le bandeau latéral gauche. Elles ne contiennent pas de données mais peuvent être utilisées pour de la documentation.

- Les colonnes intermédiaires

Il s'agit de toutes les colonnes qui n'ont pas de significations métiers mais qui servent à mettre en place certaines fonctionnalités.  
Si c'est une colonne qui servira pour un questionnaire => `quest_nom_colonne`  
Si c'est une colonne de traitement intermédiaire => `int_nom_colonne`

> Cette convention de nommage n'empêche pas l'utilisation de label plus explicite pour les utilisateurs finaux.

## Superset

Superset met à disposition un ensemble de permissions qui peuvent être regroupées par rôles puis par groupe.  
L'objectif de cette section est de fournir une convention pour nommer ces éléments.

**Nomemclature des rôles**  
La convention est la suivante: `ressource:nom_ressource:action`.  
Tous les éléments sont au singulieur.

Parmi les ressources possibles :
| Ressource | Description | Exemples |
| --------- | --------- | --------- |
| entity | représente un élément général | user, superset, db ... |
| dataset | représente un jeu de données |
| schema | représente un ensemble de jeu de données |
| dashboard | représente un tableau de bord |

Parmi les permissions possibles :
- `access` : peut accéder à la ressource
- `read` : peut lire des éléments de la ressource
- `create` : peut ajouter des éléments à la ressource
- `update` : peut modifier des éléments de la ressource
- `delete` : peut retirer des éléments à la ressource

Quelques exemples  
- Rôle pour accéder au SQLlab de Superset => `superset:sqllab:access`  
- Rôle pour créer des graphiques dans Superset => `superset:graphiques:access`  
- Rôle pour créer des datasets dans Superset => `superset:dataset:create`  
- Rôle pour lire les données du dataset `test_data` => `dataset:test_data:read`  
- Rôle pour importer des données => `superset:upload_file:access`  
- Rôle pour accéder à un tableau de bord `Mon TDB` => `dashboard:mon_tdb:access`  

**Nomenclature des groupes**  
_[à confirmer]_ La convention est la suivante: `ressource:nom_ressource:perimetre`.  

Quelques exemples  
- Groupe pour un utilisateur admin => `user:admin`  
- Groupe pour un utilisateur créateur => `user:createur`  
- Groupe pour un utilisateur créateur avec SQLlab => `user:createur_sqllab`  
- Groupe pour un utilisateur viewer => `user:viewer`  
- Groupe pour un utilisateur qui peut accéder à toutes les données du domaine `finance` => `data:schema:finance`  
- Groupe pour un utilisateur qui peut accéder à toutes les données du domaine `communication` => `data:schema:communication`   

## Bibliographies

- Superset (permissions)
  - [Gitlab Docs](https://docs.gitlab.com/development/permissions/conventions/#introducing-new-permissions)
  - [FOLIO Project](https://folio-org.atlassian.net/wiki/spaces/FOLIJET/pages/156368925/Permissions+naming+convention)