# Contribuer

## Table des matières
- [Contribuer](#contribuer)
  - [Table des matières](#table-des-matières)
  - [Contribuer](#contribuer-1)
  - [Les règles à respecter](#les-règles-à-respecter)



## Contribuer

- Environnements

Les principaux outils sont: git, python

- Implémentation

Si votre branche main venait à être désynchronisée avec la branche main du repo ChartsGouv, vous devez la resynchroniser avant de créer de nouvelle branche. Pour ce faire, les commandes suivantes vous seront utiles:
```bash
# Cloner le repo
git clone
# Pour mettre à jour votre branche locale
git pull origin main
# Créer une nouvelle branche
git checkout -b nom_de_votre_branche
git push --set-upstream origin nom_de_votre_branche
# Une fois vos modifications et commits réalisées
git push nom_de_votre_branche
```
A partir du merge, vous pouvez de nouveau créer une branche pour implémenter des modifications.
Une fois l'implémentation réalisée et votre code push, vous pouvez réaliser une Pull Request.

- Réaliser une Pull Request

Depuis la page des pull requests, vous pouvez en créer une nouvelle qui prend comme origine la branche de votre fork.
> **Important**
Le titre de la PR doit suivre les [conventional commits](https://gist.github.com/qoomon/5dfcdf8eec66a051ecd85625518cfd13#types). Vos commits intermédiaires doivent être simples et explicites.

## Les règles à respecter

- Pour les developpeurs

Assurez vous d'avoir exécuter le script de [setup.sh](../setup.sh).
```bash
# Depuis la racine
bash scripts/setup.sh
```

- Pour les maintainers

Les PR doivent être fusionnées uniquement sur la branche `pre-release` en utilisant **Squash and Merge**. _Le message du commit issu du squash **doit** respecter les conventionnels commits_.
L'utilisation des [conventional commits](https://gist.github.com/qoomon/5dfcdf8eec66a051ecd85625518cfd13#types) permet la création de releases et la mise à jour du CHANGELOG.md automatiquement.
Les maintainers doivent vérifier que le message de commit respecte les conventional commits avant de valider une PR.
Dès qu'une version est prête à être release, une PR peut être réalisée de `pre-release` vers main en utilisant l'option **Create a merge commit**.
