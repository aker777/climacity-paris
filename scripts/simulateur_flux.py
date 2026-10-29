"""Simulateur de flux Vélib' : rejoue en accéléré des passages historiques de collecte.

Le carnet 04 prépare avec Spark un extrait de la table Delta ``disponibilite`` : un dossier
``passage=<n>`` par passage du collecteur (tous les relevés d'un passage partagent le même
horodatage), contenant un fichier JSON lines. Ce script joue le rôle du micro-service de
l'énoncé : il recopie ces passages un par un, à intervalle régulier, dans le répertoire
surveillé par ``readStream``. Il ne modifie aucune valeur et ne fait aucun calcul.

Chaque fichier est d'abord écrit sous un nom caché (``.passage_<n>.json.tmp``) puis renommé :
la source fichier de Spark ignore les noms qui commencent par ``.`` ou ``_``, et le renommage
est atomique, ce qui garantit que Spark ne lit jamais un fichier à moitié écrit.

L'option ``--retard`` introduit artificiellement de la donnée tardive : un passage est retenu
puis émis après un nombre donné de passages plus récents. Ses horodatages (heure de
l'événement) sont inchangés, seul l'ordre d'arrivée change.

Usage (dans le conteneur, depuis /home/jovyan) :
    python scripts/simulateur_flux.py --source data/output/stream/rejeu \\
        --sortie data/output/stream/entree --intervalle 2 --retard 12:1 --retard 18:4
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import time
from pathlib import Path


def lister_passages(source: Path) -> dict[int, Path]:
    """Recense les passages préparés par le carnet 04.

    Args:
        source: Dossier qui contient les sous-dossiers ``passage=<n>`` écrits par Spark.

    Returns:
        Un dictionnaire ``{numéro de passage: fichier JSON lines}``, trié par numéro.

    Example:
        >>> lister_passages(Path("data/output/stream/rejeu"))   # doctest: +SKIP
        {0: PosixPath('.../passage=0/part-00000-....json'), 1: ...}
    """
    passages = {}
    for dossier in source.glob("passage=*"):
        fichiers = sorted(dossier.glob("part-*.json"))
        if len(fichiers) != 1:
            raise ValueError(f"{dossier} contient {len(fichiers)} fichiers JSON au lieu d'un seul")
        passages[int(dossier.name.split("=")[1])] = fichiers[0]
    return dict(sorted(passages.items()))


def ordre_emission(numeros: list[int], retards: dict[int, int]) -> list[int]:
    """Calcule l'ordre d'arrivée des passages quand certains sont retenus.

    Args:
        numeros: Numéros des passages, dans l'ordre chronologique.
        retards: ``{passage retenu: nombre de passages émis avant lui}``.

    Returns:
        Les numéros dans l'ordre d'émission. Un passage retenu de ``r`` est émis juste après
        le ``r``-ième passage qui le suit (ou à la fin si le flux s'arrête avant).

    Example:
        >>> ordre_emission([0, 1, 2, 3, 4], {1: 2})
        [0, 2, 3, 1, 4]
    """
    ordre, en_attente = [], {}            # en_attente : {passage retenu: passages restant à émettre}
    for numero in numeros:
        if numero in retards and retards[numero] > 0:
            en_attente[numero] = retards[numero]
            continue
        ordre.append(numero)
        for retenu in list(en_attente):
            en_attente[retenu] -= 1
            if en_attente[retenu] == 0:
                ordre.append(retenu)
                del en_attente[retenu]
    return ordre + sorted(en_attente)


def emettre(fichier: Path, sortie: Path, numero: int) -> tuple[str, int]:
    """Dépose un passage dans le répertoire surveillé, de façon atomique.

    Args:
        fichier: Fichier JSON lines du passage.
        sortie: Répertoire surveillé par Spark.
        numero: Numéro du passage, repris dans le nom du fichier émis.

    Returns:
        L'horodatage du passage (lu sur la première ligne) et son nombre de relevés.

    Example:
        >>> emettre(Path(".../part-00000.json"), Path("data/output/stream/entree"), 3)   # doctest: +SKIP
        ('2021-03-25T04:51:00.000Z', 1367)
    """
    temporaire = sortie / f".passage_{numero:04d}.json.tmp"
    shutil.copyfile(fichier, temporaire)
    os.replace(temporaire, sortie / f"passage_{numero:04d}.json")
    with fichier.open(encoding="utf-8") as f:
        premiere = f.readline()
        n_lignes = 1 + sum(1 for _ in f)
    return json.loads(premiere)["horodatage"], n_lignes


def analyser_retard(texte: str) -> tuple[int, int]:
    """Lit une option ``--retard`` de la forme ``passage:nombre``.

    Args:
        texte: Par exemple ``"18:4"`` (le passage 18 arrive après les 4 suivants).

    Returns:
        Le couple ``(passage, nombre)``.

    Example:
        >>> analyser_retard("18:4")
        (18, 4)
    """
    passage, nombre = texte.split(":")
    return int(passage), int(nombre)


def main() -> None:
    analyseur = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    analyseur.add_argument("--source", type=Path, required=True,
                           help="dossier des passages préparés (sous-dossiers passage=<n>)")
    analyseur.add_argument("--sortie", type=Path, required=True,
                           help="répertoire surveillé par readStream")
    analyseur.add_argument("--intervalle", type=float, default=2.0,
                           help="secondes entre deux émissions (défaut : 2)")
    analyseur.add_argument("--retard", type=analyser_retard, action="append", default=[],
                           metavar="PASSAGE:N", help="retient PASSAGE jusqu'à l'émission de N passages suivants")
    analyseur.add_argument("--vider", action="store_true", help="vide le répertoire de sortie au départ")
    arguments = analyseur.parse_args()

    passages = lister_passages(arguments.source)
    retards = dict(arguments.retard)
    if arguments.vider and arguments.sortie.exists():
        shutil.rmtree(arguments.sortie)
    arguments.sortie.mkdir(parents=True, exist_ok=True)

    ordre = ordre_emission(list(passages), retards)
    print(f"{len(ordre)} passages à émettre, un toutes les {arguments.intervalle} s", flush=True)
    for rang, numero in enumerate(ordre):
        if rang:
            time.sleep(arguments.intervalle)
        horodatage, n_lignes = emettre(passages[numero], arguments.sortie, numero)
        note = f"  <- en retard de {retards[numero]} passages" if numero in retards else ""
        print(f"émission {rang:>2} : passage {numero:>2} ({horodatage}), {n_lignes} relevés{note}", flush=True)
    print("Fin du flux.", flush=True)


if __name__ == "__main__":
    main()
