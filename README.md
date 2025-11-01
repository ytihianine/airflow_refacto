# Traitement des données

Ce repo contient des scripts et dags permettant de traiter des données.

## Organisation du repo

```
.
├── dags: Contient l'ensemble des dags
├── docs: Contient toute la documentation du repo
├── infra: Code pour interagir avec l'infrastructure / systèmes externes
├── scripts: Contient différents scripts utilitaires
└── utils: Code réutilisable (variables globales, tâches, fonctions ...)
```

Le code est organisé de façon modulaire et réutilisable. L'objectif est de développer uniquement les éléments spécifiques à chaque traitement (les logiques métiers).

Un guide du développeur est disponible dans [docs/contribuer.md](docs/contribuer.md)
