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

## Exécuter les carnets

Tous les carnets importent `scripts/config.py` (chemins relatifs à la racine, graine, paramètres Spark,
`creer_session()`). Chacun tourne dans un noyau neuf et ne dépend que des fichiers de `data/` : on peut
les rejouer sans clic, de la première à la dernière cellule.

```bash
docker exec -w /home/jovyan climacity-aker-jupyter python scripts/executer.py notebooks/00_configuration.ipynb
```

## Données

Rien n'est versionné : `scripts/collecte.py` télécharge tout (~500 Mo, 4 min environ) et ne fait que de
l'acquisition. Le parsing et l'enrichissement se font ensuite en Spark dans les carnets.

```bash
docker exec -w /home/jovyan climacity-aker-jupyter python scripts/collecte.py --data data
```

| Source | Contenu | Dépôt dans `data/` |
|--------|---------|--------------------|
| [historique-velib-opendata](https://github.com/lovasoa/historique-velib-opendata) | 10 986 730 relevés, 1 399 stations, du 26/11/2020 au 09/04/2021, un relevé par station toutes les ~15 min | `velib/raw/velib_<quinzaine>.csv.gz` (10 fichiers) |
| API GBFS Vélib' Métropole | référentiel actuel des stations (code -> arrondissement) | `velib/station_information.json` |
| Open-Meteo (archive) | météo horaire de Paris-Montsouris, UTC | `meteo/meteo_montsouris_horaire.csv` |
| EEA, service de téléchargement | NO2 (33 points de mesure) et PM2.5 (8) horaires validés d'Airparif | `air/raw/*.parquet` (50 fichiers) |

**Écart avec l'énoncé.** L'extrait Vélib' 2022-2023 annoncé n'est pas public : le seul historique ouvert
couvre l'hiver 2020-2021 (couvre-feu puis confinement). L'apprentissage utilisera donc un découpage
temporel plutôt que 2022 / 2023. OpenAQ exigeant désormais une clé, la qualité de l'air vient de l'EEA.

## Avancement

| Étape | Contenu | État |
|-------|---------|------|
| 1 | Socle : structure, environnement Docker (Spark 3.5, Delta 3.2, MLflow 2.17) | fait |
| 2 | Collecte des données (Vélib', GBFS, Open-Meteo, qualité de l'air EEA) | fait |
| 3 | `00_configuration` : paramètres et vérification de l'environnement | fait |
| 4 | `01_rdd_exploration` (Jour 1 matin) : parsing RDD, comptage par station, trous de collecte, Pandas / Spark, DAG | fait |
| 5 | `02_dataframe_pipeline` (Jour 1 après-midi) : nettoyage, référentiel des stations, météo, air, jointures broadcast, cache, Parquet | fait |
| 6 | `03_spark_sql_delta` (Jour 2 matin) : 3 questions métier en SQL, LAG / LEAD et moyenne mobile 3 h, Delta (MERGE, time travel) | fait |
| 7 | `04_streaming` (Jour 2 après-midi) : simulateur de flux, `readStream` à schéma explicite, fenêtres glissantes 10 min / 2 min avec watermark, sink Delta, table `alertes`, rejet prouvé de la donnée tardive, reprise sur checkpoint | fait |
| 8 | `05_mllib_prediction` (Jour 3 matin) : features à décalages vérifiés en temps, Pipeline GBT et `CrossValidator` à plis temporels, MLflow (registre, alias `champion`), sauvegarde et rechargement, bonus qualité de l'air et `FMRegressor`, K-Means (k = 4) et carte Folium, cache sur calcul itératif | fait |
| 9 | `06_optimisation_bilan` (Jour 3 après-midi) | à faire |

## Arborescence

```
notebooks/   carnets 00 à 06
scripts/     config.py (configuration partagée), collecte.py, executer.py, simulateur de flux
conf/        configuration Spark
docs/        captures (MLflow, Spark UI)
data/        données et sorties, hors dépôt
```
