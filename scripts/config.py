"""Configuration partagée par tous les carnets ClimaCity Paris.

Chaque carnet commence par ``from config import *`` : les chemins, les paramètres Spark et la
graine aléatoire sont déclarés UNE seule fois, ici. Aucun chemin absolu n'est écrit en dur :
tout part de la racine du projet (dossier parent de ``scripts/``), qui vaut
``/home/jovyan`` dans le conteneur.
"""

from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession

# ── Chemins ──────────────────────────────────────────────────────────────────
RACINE = Path(os.environ.get("CLIMACITY_RACINE", Path(__file__).resolve().parents[1]))
DATA = RACINE / "data"

VELIB_RAW = DATA / "velib" / "raw"                       # 10 fichiers .csv.gz (Jour 1, RDD)
GBFS_JSON = DATA / "velib" / "station_information.json"  # référentiel actuel des stations
METEO_CSV = DATA / "meteo" / "meteo_montsouris_horaire.csv"
AIR_RAW = DATA / "air" / "raw"                           # Parquet EEA (NO2, PM2.5)

SORTIES = DATA / "output"
PARQUET_CONSOLIDE = SORTIES / "disponibilite_parquet"    # table consolidée du Jour 1
DELTA = SORTIES / "delta"                                # tables Delta (Jours 2 et 3)
STREAM = SORTIES / "stream"                              # entrée, checkpoints du flux
MODELES = SORTIES / "modeles"
MESURES_JSON = SORTIES / "mesures.json"                  # mesures de performance du projet

# ── Paramètres ───────────────────────────────────────────────────────────────
SEED = 42
SHUFFLE_PARTITIONS = 8          # au lieu de 200 : volumes du projet, machine locale
DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "4g")   # vient du .env
MLFLOW_URI = os.environ.get("MLFLOW_TRACKING_URI", f"file://{SORTIES / 'mlruns'}")
SPARK_UI = "http://localhost:4040"


def creer_session(nom: str, **options: str) -> SparkSession:
    """Crée (ou récupère) la SparkSession locale du projet.

    Args:
        nom: Nom de l'application, visible dans le Spark UI.
        **options: Réglages Spark supplémentaires, ``cle=valeur`` avec des points remplacés
            par des ``__`` (ex. ``spark__sql__adaptive__enabled="false"``).

    Returns:
        La session, en mode ``local[*]``, fuseau UTC, extensions Delta actives.

    Example:
        >>> spark = creer_session("ClimaCity-test")
        >>> spark.conf.get("spark.sql.session.timeZone")
        'UTC'
    """
    constructeur = (
        SparkSession.builder
        .appName(nom)
        .master("local[*]")
        .config("spark.driver.memory", DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.showConsoleProgress", "false")
    )
    for cle, valeur in options.items():
        constructeur = constructeur.config(cle.replace("__", "."), valeur)
    spark = constructeur.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir(str(SORTIES / "checkpoints_rdd"))
    return spark
