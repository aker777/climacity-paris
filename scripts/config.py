"""Configuration partagée par tous les carnets ClimaCity Paris.

Chaque carnet commence par ``from config import *`` : les chemins, les paramètres Spark et la
graine aléatoire sont déclarés UNE seule fois, ici. Aucun chemin absolu n'est écrit en dur :
tout part de la racine du projet (dossier parent de ``scripts/``), qui vaut
``/home/jovyan`` dans le conteneur.
"""

from __future__ import annotations

import json
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
STATIONS_PARQUET = SORTIES / "stations"                  # référentiel des stations (carnet 02)
METEO_PARQUET = SORTIES / "meteo_horaire"                # table meteo_horaire (carnet 02)
AIR_PARQUET = SORTIES / "air_horaire"                    # NO2 / PM2.5 horaires en UTC (carnet 02)
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


def enregistrer_mesures(section: str, mesures: dict) -> None:
    """Ajoute des mesures de performance au fichier commun ``MESURES_JSON``.

    Chaque carnet y range ses chronométrages sous sa propre section ; le carnet 06 les relit
    pour le bilan d'architecture. Une section déjà présente est remplacée, ce qui garde le
    fichier à jour quand on rejoue un carnet.

    Args:
        section: Nom de la section, en général celui du carnet (ex. ``"01_rdd_exploration"``).
        mesures: Dictionnaire sérialisable en JSON (durées en secondes, volumes...).

    Example:
        >>> enregistrer_mesures("01_rdd_exploration", {"pandas_s": 4.2, "spark_s": 2.1})
    """
    contenu = json.loads(MESURES_JSON.read_text(encoding="utf-8")) if MESURES_JSON.exists() else {}
    contenu[section] = mesures
    MESURES_JSON.parent.mkdir(parents=True, exist_ok=True)
    MESURES_JSON.write_text(json.dumps(contenu, indent=2, ensure_ascii=False), encoding="utf-8")


def stages_spark_ui(spark: SparkSession, description: str,
                    tous_les_jobs: bool = False) -> list[dict]:
    """Lit dans l'API REST du Spark UI les stages des jobs portant une description.

    Les carnets nomment leurs actions avec ``sc.setJobDescription()`` ; cette fonction
    retrouve le job correspondant et renvoie, pour chacun de ses stages, les chiffres de
    l'onglet *Stages* du Spark UI.

    Args:
        spark: Session active (son Spark UI doit être ouvert).
        description: Texte passé à ``sc.setJobDescription`` avant l'action.
        tous_les_jobs: Si False, seul le dernier job portant la description est lu ; si True,
            tous le sont (une requête DataFrame avec shuffle déclenche plusieurs jobs quand
            l'exécution adaptative, AQE, est active).

    Returns:
        Une liste de dictionnaires, un par stage exécuté : ``job``, ``stage``, ``statut``,
        ``nom``, ``taches``, ``calcul_s`` (temps cumulé des tâches), ``entree_mo``,
        ``shuffle_ecrit_ko`` et ``shuffle_lu_ko``.

    Example:
        >>> sc.setJobDescription("comptage"); spark.range(10).count()
        >>> stages_spark_ui(spark, "comptage")[0]["taches"]   # doctest: +SKIP
        8
    """
    import requests

    sc = spark.sparkContext
    api = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"
    jobs = requests.get(f"{api}/jobs", timeout=10).json()       # du plus récent au plus ancien
    retenus = [j for j in jobs if j.get("description") == description]
    if not tous_les_jobs:
        retenus = retenus[:1]
    lignes = []
    paires = sorted(((j, s) for j in retenus for s in j["stageIds"]), key=lambda p: (p[0]["jobId"], p[1]))
    for job, id_stage in paires:
        tentatives = requests.get(f"{api}/stages/{id_stage}", timeout=10).json()
        if not tentatives:
            continue
        s = tentatives[0]
        lignes.append({
            "job": job["jobId"], "stage": id_stage, "statut": s["status"], "nom": s["name"],
            "taches": s["numTasks"], "calcul_s": s["executorRunTime"] / 1000,
            "entree_mo": s["inputBytes"] / 1e6, "shuffle_ecrit_ko": s["shuffleWriteBytes"] / 1e3,
            "shuffle_lu_ko": s["shuffleReadBytes"] / 1e3,
        })
    return lignes
