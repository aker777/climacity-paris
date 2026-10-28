# ClimaCity Paris

Projet Spark / PySpark du module « Traitement de données massives » : prévoir la tension des
stations Vélib' à partir de leur historique, de la météo et de la qualité de l'air.
Énoncé : [CoursSpark / Projets / Cyclo](https://github.com/Septentrion/CoursSpark/tree/main/Projets/Cyclo).

## Lancer l'environnement

Prérequis : Docker Desktop (WSL 2 sous Windows), 8 Go de mémoire alloués à Docker.

```bash
cp .env.example .env      # adapter la mémoire à la machine
docker compose up -d --build
```

| Service | Adresse |
|---------|---------|
| JupyterLab | http://localhost:8888/lab?token=climacity |
| Spark UI | http://localhost:4040 (après création d'une SparkSession) |
| MLflow | http://localhost:5000 |

## Avancement

| Étape | Contenu | État |
|-------|---------|------|
| 1 | Socle : structure, environnement Docker (Spark 3.5, Delta 3.2, MLflow 2.17) | fait |
| 2 | Collecte des données (Vélib', GBFS, Open-Meteo, qualité de l'air EEA) | à faire |
| 3 | `00_configuration` : paramètres et vérification de l'environnement | à faire |
| 4 | `01_rdd_exploration` (Jour 1 matin) | à faire |
| 5 | `02_dataframe_pipeline` (Jour 1 après-midi) | à faire |
| 6 | `03_spark_sql_delta` (Jour 2 matin) | à faire |
| 7 | `04_streaming` (Jour 2 après-midi) | à faire |
| 8 | `05_mllib_prediction` + MLflow + bonus qualité de l'air (Jour 3 matin) | à faire |
| 9 | `06_optimisation_bilan` (Jour 3 après-midi) | à faire |

## Arborescence

```
notebooks/   carnets 00 à 06
scripts/     collecte des données, simulateur de flux
conf/        configuration Spark
docs/        captures (MLflow, Spark UI)
data/        données et sorties, hors dépôt
```
