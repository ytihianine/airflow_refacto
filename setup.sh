#! /bin/sh

PYTHON_VERSION="3.12"
AIRFLOW_VERSION="2.11.0"

# Pour régler certains problèmes d'installation des packages
echo Updating system ...
sudo apt update
sudo apt install -y libxml2-dev libxmlsec1-dev pkg-config
echo System updated !

# Installation des packages
# Pour travailler avec le projet en mode package
pip install -e .
# Être dans les même conditions que l'instance Airflow en cours
pip install "apache-airflow==$AIRFLOW_VERSION" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW_VERSION/constraints-$PYTHON_VERSION.txt"
pip install -r requirements.txt

# Installer les pre-commits
pre-commit install

echo Init git config
git config --global credential.helper 'cache --timeout=360000'
