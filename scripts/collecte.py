"""Collecte des données brutes du projet ClimaCity Paris.

Ce script ne fait QUE de l'acquisition : il télécharge les sources et les dépose dans
``data/`` sans les analyser. Tout le traitement (parsing, nettoyage, jointures,
agrégations) est fait avec Spark dans les carnets, conformément à la contrainte n°4.

Sources (toutes libres, sans clé) :

1. Historique Vélib' (``lovasoa/historique-velib-opendata``) : une archive de ~11 M de
   relevés, découpée ici en fichiers ``.csv.gz`` par quinzaine pour le Jour 1 (RDD).
2. Référentiel actuel des stations (API GBFS Vélib' Métropole) : codes de station,
   d'où l'on déduira l'arrondissement.
3. Météo horaire de Paris-Montsouris (archive Open-Meteo).
4. Qualité de l'air horaire, NO2 et PM2.5 (service de téléchargement de l'Agence
   européenne pour l'environnement, données Airparif validées) : bonus de l'énoncé.

Usage (dans le conteneur) :
    python scripts/collecte.py --data data
"""

from __future__ import annotations

import argparse
import gzip
import io
import json
import time
import zipfile
from pathlib import Path

import requests

HISTORIQUE_URL = (
    "https://github.com/lovasoa/historique-velib-opendata/releases/download/new/stations.zip"
)
GBFS_STATIONS_URL = (
    "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json"
)
OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"
MONTSOURIS = {"latitude": 48.8217, "longitude": 2.3378}       # station Météo-France 75114001
METEO_VARIABLES = [
    "temperature_2m", "relative_humidity_2m", "precipitation",
    "wind_speed_10m", "cloud_cover", "weather_code",
]
EEA_URLS_API = "https://eeadmz1-downloads-api-appservice.azurewebsites.net/ParquetFile/urls"
EEA_VILLE = "Paris (greater city)"
EEA_POLLUANTS = ["NO2", "PM2.5"]

# L'archive n'a pas d'en-tête : on l'ajoute à chaque fichier découpé.
ENTETE_HISTORIQUE = (
    "horodatage,capacite,velos_mecaniques,velos_electriques,nom_station,coordonnees,en_service"
)


def requete(methode: str, url: str, tentatives: int = 4, **kwargs) -> requests.Response:
    """Envoie une requête HTTP en réessayant en cas d'erreur réseau passagère.

    Args:
        methode: ``"GET"`` ou ``"POST"``.
        url: Adresse à interroger.
        tentatives: Nombre maximal d'essais (attente de 1, 2, 4... s entre deux essais).
        **kwargs: Arguments transmis à ``requests.request`` (``params``, ``json``, ``timeout``...).

    Returns:
        La réponse, dont le code de succès a été vérifié.

    Raises:
        requests.RequestException: Si tous les essais échouent.

    Example:
        >>> requete("GET", GBFS_STATIONS_URL, timeout=30).status_code
        200
    """
    kwargs.setdefault("timeout", 120)
    for essai in range(tentatives):
        try:
            reponse = requests.request(methode, url, **kwargs)
            reponse.raise_for_status()
            return reponse
        except requests.RequestException:
            if essai == tentatives - 1:
                raise
            time.sleep(2 ** essai)
    raise AssertionError("inatteignable")


def telecharger(url: str, destination: Path) -> Path:
    """Télécharge un fichier s'il n'est pas déjà présent.

    Le fichier est d'abord écrit sous ``<nom>.part`` puis renommé : une coupure réseau
    ne laisse jamais un fichier tronqué qui passerait pour complet.

    Args:
        url: Adresse du fichier.
        destination: Chemin du fichier à produire.

    Returns:
        ``destination``.

    Example:
        >>> telecharger(HISTORIQUE_URL, Path("data/velib/stations.zip")).exists()
        True
    """
    if destination.exists():
        return destination
    destination.parent.mkdir(parents=True, exist_ok=True)
    partiel = destination.with_name(destination.name + ".part")
    with requete("GET", url, stream=True) as reponse, open(partiel, "wb") as sortie:
        for bloc in reponse.iter_content(chunk_size=4 << 20):
            sortie.write(bloc)
    partiel.rename(destination)
    return destination


def quinzaine(horodatage: str) -> str:
    """Renvoie l'étiquette de quinzaine d'un horodatage ISO.

    Args:
        horodatage: Texte commençant par ``AAAA-MM-JJ``.

    Returns:
        ``AAAA-MM-A`` (jours 1 à 15) ou ``AAAA-MM-B`` (jours 16 et suivants).

    Example:
        >>> quinzaine("2021-01-16T08:00Z")
        '2021-01-B'
    """
    return f"{horodatage[:7]}-{'A' if int(horodatage[8:10]) <= 15 else 'B'}"


def decouper_historique(archive: Path, dossier: Path) -> tuple[list[Path], str, str]:
    """Découpe l'historique Vélib' en fichiers ``velib_<quinzaine>.csv.gz``.

    L'archive contient un seul CSV de ~800 Mo trié par date. Un fichier ``.gz`` ne se
    découpe pas : Spark en fait une seule partition. Une dizaine de fichiers donne donc
    du parallélisme au Jour 1. Les lignes sont recopiées telles quelles (seuls les 10
    premiers caractères sont lus, pour choisir le fichier).

    Args:
        archive: Le ``stations.zip`` téléchargé.
        dossier: Dossier de sortie.

    Returns:
        La liste des fichiers produits, la première et la dernière date (``AAAA-MM-JJ``).
    """
    dossier.mkdir(parents=True, exist_ok=True)
    existants = sorted(dossier.glob("velib_*.csv.gz"))
    marqueur = dossier / "_periode.json"
    if existants and marqueur.exists():
        periode = json.loads(marqueur.read_text())
        return existants, periode["debut"], periode["fin"]

    sorties: dict[str, gzip.GzipFile] = {}
    premiere = derniere = ""
    with zipfile.ZipFile(archive) as z, z.open(z.namelist()[0]) as brut:
        for ligne in io.TextIOWrapper(brut, encoding="utf-8"):
            date = ligne[:10]
            premiere = premiere or date
            derniere = date
            cle = quinzaine(ligne)
            if cle not in sorties:
                sorties[cle] = gzip.open(dossier / f"velib_{cle}.csv.gz", "wt", encoding="utf-8")
                sorties[cle].write(ENTETE_HISTORIQUE + "\n")
            sorties[cle].write(ligne)
    for f in sorties.values():
        f.close()
    marqueur.write_text(json.dumps({"debut": premiere, "fin": derniere}))
    return sorted(dossier.glob("velib_*.csv.gz")), premiere, derniere


def telecharger_meteo(debut: str, fin: str, destination: Path) -> Path:
    """Télécharge la météo horaire de Paris-Montsouris (archive Open-Meteo, UTC).

    Args:
        debut: Première date ``AAAA-MM-JJ``.
        fin: Dernière date ``AAAA-MM-JJ`` (incluse).
        destination: Fichier CSV à écrire (une ligne par heure, colonnes ``time`` puis
            ``METEO_VARIABLES``).

    Returns:
        ``destination``.

    Example:
        >>> telecharger_meteo("2021-01-01", "2021-01-02", Path("/tmp/m.csv")).exists()
        True
    """
    if destination.exists():
        return destination
    params = {**MONTSOURIS, "start_date": debut, "end_date": fin,
              "hourly": ",".join(METEO_VARIABLES), "timezone": "UTC"}
    horaire = requete("GET", OPEN_METEO_URL, params=params).json()["hourly"]
    colonnes = ["time"] + METEO_VARIABLES
    destination.parent.mkdir(parents=True, exist_ok=True)
    with open(destination, "w", encoding="utf-8") as sortie:
        sortie.write(",".join(colonnes) + "\n")
        for valeurs in zip(*(horaire[c] for c in colonnes)):
            sortie.write(",".join("" if v is None else str(v) for v in valeurs) + "\n")
    return destination


def telecharger_qualite_air(debut: str, fin: str, dossier: Path) -> list[Path]:
    """Télécharge les mesures horaires de NO2 et PM2.5 des stations de l'agglomération.

    Le service de l'EEA renvoie la liste des fichiers Parquet (un par point de mesure et
    par polluant, toutes années confondues) ; on les dépose tels quels, Spark filtrera la
    période et fera les moyennes. Jeu « E1a » : données validées par Airparif.

    Args:
        debut: Première date ``AAAA-MM-JJ``.
        fin: Dernière date ``AAAA-MM-JJ`` (incluse).
        dossier: Dossier de sortie des fichiers ``.parquet``.

    Returns:
        La liste des fichiers présents dans ``dossier``.

    Raises:
        ValueError: Si le service ne propose aucun fichier.
    """
    dossier.mkdir(parents=True, exist_ok=True)
    corps = {
        "countries": ["FR"], "cities": [EEA_VILLE], "pollutants": EEA_POLLUANTS,
        "dataset": 2, "source": "Api", "aggregationType": "hour",
        "dateTimeStart": f"{debut}T00:00:00Z", "dateTimeEnd": f"{fin}T23:59:59Z",
    }
    lignes = requete("POST", EEA_URLS_API, json=corps).text.splitlines()
    urls = [l.strip() for l in lignes if l.strip().startswith("http")]
    if not urls:
        raise ValueError(f"Aucun fichier EEA pour {EEA_VILLE} entre {debut} et {fin}")
    for url in urls:
        telecharger(url, dossier / url.rsplit("/", 1)[-1])
    return sorted(dossier.glob("*.parquet"))


def collecter(data: Path) -> dict:
    """Exécute toute la collecte (idempotente : rien n'est retéléchargé).

    Args:
        data: Racine des données.

    Returns:
        Un résumé : fichiers produits et période couverte.
    """
    archive = telecharger(HISTORIQUE_URL, data / "velib" / "stations.zip")
    fichiers, debut, fin = decouper_historique(archive, data / "velib" / "raw")
    gbfs = telecharger(GBFS_STATIONS_URL, data / "velib" / "station_information.json")
    meteo = telecharger_meteo(debut, fin, data / "meteo" / "meteo_montsouris_horaire.csv")
    air = telecharger_qualite_air(debut, fin, data / "air" / "raw")
    return {"periode": (debut, fin), "fichiers_velib": len(fichiers), "gbfs": gbfs,
            "meteo": meteo, "fichiers_air": len(air)}


if __name__ == "__main__":
    analyseur = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    analyseur.add_argument("--data", type=Path, default=Path("data"), help="racine des données")
    resume = collecter(analyseur.parse_args().data)
    for cle, valeur in resume.items():
        print(f"{cle:<15} {valeur}")
