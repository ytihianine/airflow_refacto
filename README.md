# Traitement des données

Ce repo contient des scripts et dags permettant de traiter des données.

## Organisation du projet

```
.
├── dags: Contient l'ensemble des dags
├── docs: Contient toute la documentation du repo
├── modules: Contient du conde réutilisable
│   ├── common_tasks: tâches génériques réutilisables (S3, SQL, Grist, ...)
│   ├── enums: enums transverses
│   ├── infra: Code pour interagir avec l'infrastructure / systèmes externes
│   ├── types: types transverses
│   ├── utils: fonctions réutilisable (fonctions de traitements, récupération des configs ...)
│   └── constants.py: variables globales
├── scripts: Contient différents scripts utilitaires
├── test: Contient tous les tests unitaires
└── pyproject.toml: Contient la description du projet
```

Le code est organisé de façon modulaire et réutilisable. L'objectif est de développer uniquement les éléments spécifiques à chaque traitement (les logiques métiers).


## Documentation

L'ensemble de la documentation se trouve dans le dossier `docs/`.

```
.
├── docs
|    ├── configuration_projet.md
|    ├── contribuer.md
|    ├── convention.md
|    ├── dags.md
|    ├── grist.md
|    ├── modules.infra.md
|    └── utilitaires.md
```

## Mise en place de l'infrastructure

Le déploiement de l'infrastructure est en cours d'évolution. Les travaux sont disponibles sur ce [https://github.com/ytihianine/la-suite-donnee](https://github.com/ytihianine/la-suite-donnee)


## Environnement de développement

Si vous travaillez depuis Nubonyxia, je vous invite à suivre ce [guide](https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/environnement-nubonyxia) dans un premier temps.

Pour installer l'environnement python et les pre-commits
```bash
make setup-dev-env
```

Pour d'autres commandes
```bash
make help
```
