# Convention de nommage

## Table des matières
- [Convention de nommage](#convention-de-nommage)
  - [Table des matières](#table-des-matières)
  - [S3](#s3)
  - [SQL](#sql)
  - [Grist](#grist)


## S3
Tous les fichiers stockés doivent respecter le format suivant:
```
bucket/specific/path/AAAAMMDD/HHhMM/file.ext
# Exemple: dsci/SG/immobilier/consommation/20250801/13h30/conso.parquet
```
Le bucket, le prefix specific/path et le nom du fichier sont définis au niveau de la configuration du projet. Les deux segments
avec la date et l'heure sont ajoutés lors de la copie des fichiers via les tâches disponibles dans [utils/tasks/s3.py](../utils/tasks/s3.py).

## SQL
1. Les tables
Application de ces [règles](https://www.postgresql.org/docs/7.0/syntax525.htm).
Le nom des tables doit définis avec les agents métiers.

2. Les vues
Les vues suivent les même règles que les tables.
En complément, le suffix `_vw` doit être rajouté pour faciliter la recherche.

3. Les colonnes
Les colonnes suivent les même règles que les tables.
En complément, si une même information est présente dans plusieurs tables, il faut s'assurer de conserver le même nom de colonne pour conserver une cohérence.

## Grist
Ces conventions visent à faciliter le traitement des données issues de Grist.
- Les tables de documentations
Ajouter le préfix `doc_` => `doc_nom_de_la_table`

- Les tables métiers
Aucun préfix ou suffix nécessaire. Il faut néanmoins avoir qu'elles possèdent un nom clair et concis.

- Les tables de référence
Ajouter le préfix `ref_` => `ref_direction`

- Les tables d'onglets
Ajouter le préfix `onglet_` => `onglet_global_dsci`
Les onglets permettent de hiérarchiser visuellement les tables sur le bandeau latéral gauche. Elles ne contiennent pas de données.

- Les colonnes intermédiaires
Toutes les colonnes qui n'ont pas de significations métiers mais qui servent pour mettre en place certaines fonctionnalités
Si c'est une colonne qui servira pour un questionnaire => `quest_nom_colonne`
Si c'est une colonne de traitement intermédiaire => `int_nom_colonne`

> Cette convention de nommage n'empêche pas l'utilisation de label plus explicite pour les utilisateurs finaux.
