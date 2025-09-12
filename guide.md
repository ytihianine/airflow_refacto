## Stockage sur MinIO
Tous les fichiers stockés doivent respecter le format suivant:
```
bucket/specific/path/AAAAMMDD/HHhMM/file.ext
# Exemple: dsci/SG/immobilier/consommation/20250801/13h30/conso.parquet
```
Le bucket, le prefix specific/path et le nom du fichier sont définis au niveau de la configuration du projet. Les deux segments
avec la date et l'heure sont ajoutés lors de la copie des fichiers

## Convention de nommage des objets SQL
1. Les tables
Application de ces [règles](https://www.postgresql.org/docs/7.0/syntax525.htm).
Le nom des tables doit définis avec les agents métiers.

2. Les vues
Les vues suivent les même règles que les tables.
En complément, le suffix `_vw` doit être rajouté pour faciliter la recherche.


3. Les colonnes
Les colonnes suivent les même règles que les tables.
En complément, si une même information est présente dans plusieurs tables, il faut s'assurer de conserver le même nom de colonne pour conserver une cohérence.

## Convention de nommage des tables Grist
Ces conventions visent à faciliter le traitement des données issues de Grist.
- Les tables de documentations
Ajouter le préfix `doc_` => `doc_nom_de_la_table`

- Les tables métiers
Ajouter le préfix `struc_` => `struc_nom_de_la_table`

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

## Grandes étapes de fonctionnement

Ajouter le lien du schéma Figma

1. Récupérer les données
- **Cas 1: Données stockées dans Grist**

- **Cas 2: Données issues d'un SI métier**
Si une API est disponible sur le SI métier, il suffit de la requêter à interval régulier.
Sinon, il est nécessaire de passer par l'interface de dépôt de fichier.
Les étapes sont les suivantes:
- L'utilisateur réalise un export depuis son SI métier
- L'utilisateur se connecte sur l'interface de dépôt de fichiers
- L'utilisateur sélectionne le projet pour lequel il veut déposer un/des fichier(s)
- L'utilisateur téléverse l'ensemble des fichiers requis

L'implication de l'utilisateur prend fin.

2. Traitement par l'ETL
--- Dépose fichier
1) Récupérer tous les projets propre à un utilisateur
Get user_id
Get projets by user_id

2) Récupérer tous les fichiers sources propre à un projet
Get fichiers sources by projet_id

3) Déposer les fichiers sources dans leur dossier MinIO
Get s3_bucket / s3_key by projet_id


--- ETL
4) Vérifier si les fichiers sont présents dans le dossier MinIO
Get filenames & s3_bucket / s3_key by projet_id

5) Lire chaque fichier et créer son df associé
Get filename & s3_bucket / s3_key by projet_id and fichier_id

6) Renommer toutes les colonnes sources du fichier
Get colonnes sources and dest by fichier_id and project_id

7) Enregistrer l'état temporaire du df dans un fichier dans le folder spécifique MinIO
Get s3_tmp_path (=s3_bucket + s3_key + s3_tmp_filename) by projet_id and fichier_id

8) Envoyer chaque fichier temporaire en base
get table and s3_tmp_path by projet_id and fichier_id
## Config git
```
git config --global credential.helper 'cache --timeout=360000'
```

### Installer les packages
```
# Pour travailler en 'local' dans un service VSCode
sh setup.sh
```
Installer toutes les librairies ne permet pas pour autant d'exécuter des dags de A à Z.
Cela permet de réduire les erreurs affichés par votre linter.

### Export la structure des tables
On utilisera [dbdiagram](https://dbdiagram.io)
```
npm install -g @dbml/cli
db2dbml postgres 'postgresql://user:password@localhost:5432/dbname?schemas=schema1,schema2,schema3' -o database.dbml
```
