
# Variables
PYTHON_VERSION := 3.12
AIRFLOW_VERSION := 3.2.2
ENV_NAME := env

# OS detection
ifeq ($(OS),Windows_NT)
    PYTHON := python
    VENV_BIN := $(ENV_NAME)/Scripts
else
    PYTHON := python3
    VENV_BIN := $(ENV_NAME)/bin
endif

UV := $(VENV_BIN)/uv

.PHONY: help install-sys-packages create-py-env install-airflow install-packages \
        install-pre-commit setup-git setup-dev-env init-env-files clean

help: ## Afficher l'aide
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-25s\033[0m %s\n", $$1, $$2}'

install-sys-packages: ## Installer les packages système nécessaires (libxml2-dev, libxmlsec1-dev, pkg-config)
	@echo "Ce script va mettre à jour votre système et installer les packages nécessaires."
	@echo "Packages à installer: libxml2-dev libxmlsec1-dev pkg-config"
	@echo ""
	@read -p "Voulez-vous continuer? (o/n): " REPLY; \
	echo ""; \
	if echo "$$REPLY" | grep -qiE '^[oOyY]$$'; then \
		echo "Mise à jour du système en cours..."; \
		sudo apt update; \
		sudo apt install -y libxml2-dev libxmlsec1-dev pkg-config; \
		echo "Système mis à jour avec succès!"; \
	else \
		echo "Mise à jour du système annulée."; \
	fi

create-py-env: ## Créer un nouvel environnement python
	@echo "Création d'un environnement"
	$(PYTHON) -m venv $(ENV_NAME)
	@echo "L'environnement a été créé"
	@echo "Exécuter dans votre terminal: source $(ENV_NAME)/bin/activate"

install-packages: ## Installer les packages python complémentaires
	@echo "Installation de uv et des dépendances depuis pyproject.toml"
	$(PYTHON) -m pip install uv
	$(UV) pip install \
		--python $(PYTHON) \
		--group default \
		--group airflow \
		--group airflow-providers \
		--group dev \
		--group iceberg

install-pre-commit: ## Installer les pre-commits
	@echo "Installation des pre-commits"
	$(VENV_BIN)/pre-commit install

setup-git: ## Initialiser la configuration git
	@echo "Init git config"
	git config --global credential.helper 'cache --timeout=360000'

setup-dev-env: create-py-env install-packages install-pre-commit setup-git ## Installer tout l'environnement de développement

init-env-files: ## Initialiser les fichiers d'environnement des scripts
	@echo "Initialisation des fichiers d'environnement"
	@chmod +x ./scripts/init_env.bash
	@./scripts/init_env.bash
	@echo "Tous les fichiers .env créés à partir de example.env. Veuillez les personnaliser avec vos propres valeurs."


run-pre-commit: ## Lancer pre-commit sur tous les fichiers
	$(VENV_BIN)/pre-commit run --all-files

clean: ## Nettoyer les fichiers temporaires Python
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	@echo "✓ Nettoyage terminé"
