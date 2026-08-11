# Initialiser la configuration des projets

Les étapes ci-dessous permettent d'initialiser la configuration des projets dans une base postgres.

### Configurer le fichier .env
Dupliquer le fichier d'exemple .env.example et le renommer en .env avec la commande suivante:
```bash
# Depuis scripts/load_config
cp .env.example .env
```
Configurer toutes les variables.

### Récupérer le fichier de configuration

Réaliser un export des données depuis Grist sans l'historique. Le fichier exporté est au format sqlite.

### Exécuter le script
Le script peut être exécuté avec la commande suivante:
```bash
# Depuis scripts/load_config
bash load_config.bash
```
