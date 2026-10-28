"""Exécute des carnets de la première à la dernière cellule et enregistre leurs sorties.

Chaque carnet tourne dans un noyau neuf : aucun ne dépend de variables laissées en mémoire
par un autre, seulement des fichiers écrits dans ``data/``. C'est la vérification de
reproductibilité demandée par l'énoncé.

Usage (dans le conteneur, depuis /home/jovyan) :
    python scripts/executer.py notebooks/00_configuration.ipynb notebooks/01_rdd_exploration.ipynb
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import nbformat
from nbclient import NotebookClient
from nbclient.exceptions import CellExecutionError


def executer(carnet: Path, timeout: int) -> bool:
    """Exécute un carnet dans son dossier et le réécrit avec ses sorties.

    Args:
        carnet: Chemin du fichier ``.ipynb``.
        timeout: Durée maximale d'une cellule, en secondes.

    Returns:
        True si toutes les cellules ont réussi.
    """
    nb = nbformat.read(carnet, as_version=4)
    client = NotebookClient(nb, timeout=timeout, kernel_name="python3",
                            resources={"metadata": {"path": str(carnet.parent)}})
    t0 = time.perf_counter()
    try:
        client.execute()
        ok = True
    except CellExecutionError as erreur:
        print(str(erreur)[-2000:], file=sys.stderr)
        ok = False
    nbformat.write(nb, carnet)
    print(f"{'OK ' if ok else 'ÉCHEC'} {carnet.name} en {time.perf_counter() - t0:.0f} s")
    return ok


if __name__ == "__main__":
    analyseur = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    analyseur.add_argument("carnets", nargs="+", type=Path)
    analyseur.add_argument("--timeout", type=int, default=3600)
    arguments = analyseur.parse_args()
    for chemin in arguments.carnets:
        if not executer(chemin, arguments.timeout):
            sys.exit(1)
