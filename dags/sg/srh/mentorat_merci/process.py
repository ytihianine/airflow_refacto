from collections.abc import Mapping
from typing import Any

import pandas as pd
from dags.sg.srh.mentorat_merci.enums import ChoixDirection

correspondance_objectifs = {
    "Transmettre ma culture administrative et ministérielle": "Améliorer ma culture administrative et ministérielle",
    "Accompagner le mentoré dans la préparation des examens et concours": "Préparer un concours ou un examen professionnel",
    "Transmettre mes compétences professionnelles, notamment d’un point de vue managérial": "Obtenir un accompagnement dans la montée en compétences d’un point de vue managérial",  # noqa
    "Développer mon réseau professionnel": "Développer mon réseau professionnel",
    "Partager mes compétences numériques": "Partager mes compétences numériques",
}

critere_categorie = {
    "A+": ["A+"],
    "A": ["A+", "A"],
    "B": ["A", "B"],
    "C": ["B", "C"],
}


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df["27. Q9_Direction_Mentor"] = df["27. Q9_Direction_Mentor"].fillna("Pas de préférence")
    return df


def verifier_categorie_compatible(mentor: pd.Series, mentore: pd.Series) -> tuple[bool, str]:
    """
    Critère éliminatoire : Catégorie
    Vérifie si le mentor est dans la liste des catégories compatibles pour le mentoré
    Retourne (compatible, message)
    """
    cat_mentor = str(mentor["5. E_CATEGORIE"]).strip()
    cat_mentore = str(mentore["5. E_CATEGORIE"]).strip()

    # Vérifier si les catégories sont valides
    if cat_mentore not in critere_categorie:
        return False, f"✗ Catégorie mentoré '{cat_mentore}' inconnue"

    # Vérifier si le mentor est compatible avec le mentoré
    categories_compatibles = critere_categorie[cat_mentore]

    if cat_mentor in categories_compatibles:
        return True, f"✓ Catégorie OK (Mentoré {cat_mentore} ← Mentor {cat_mentor})"
    else:
        return (
            False,
            f"✗ Catégorie incompatible (Mentoré {cat_mentore} ✗ Mentor {cat_mentor})",
        )


def calculer_score_categorie(mentor: pd.Series, mentore: pd.Series) -> tuple[int, str]:
    """
    Critère 1: Catégorie (1000 points)
    Vérifie si le mentor est dans la liste des catégories compatibles pour le mentoré
    """
    cat_mentor = str(mentor["5. E_CATEGORIE"]).strip()
    cat_mentore = str(mentore["5. E_CATEGORIE"]).strip()

    # Vérifier si les catégories sont valides
    if cat_mentore not in critere_categorie:
        return 0, f"✗ Catégorie mentoré '{cat_mentore}' inconnue"

    # Vérifier si le mentor est compatible avec le mentoré
    categories_compatibles = critere_categorie[cat_mentore]

    if cat_mentor in categories_compatibles:
        return 1000, f"✓ Catégorie OK ( Mentor {cat_mentor} → Mentoré {cat_mentore})"
    else:
        return (
            0,
            f"✗ Catégorie incompatible (Mentor {cat_mentor} ✗ Mentoré {cat_mentore})",
        )


def extraire_objectifs(row: pd.Series, column: str) -> list[str]:
    """Extrait et ordonne les objectifs d'une personne"""
    objectifs = []

    if pd.notna(row.get(key=column)) is True:
        obj = str(row[column]).strip()

        if obj:
            obj = obj.split(sep=";")
            objectifs.extend(obj)

    # print(objectifs)
    return objectifs


def calculer_score_objectifs(mentor: pd.Series, mentore: pd.Series) -> tuple[int, str]:
    """
    Critère 2: Objectifs (935 points par objectif correspondant)
    Le 1er objectif du mentor doit correspondre au 1er du mentoré, etc.
    """
    obj_mentor = extraire_objectifs(row=mentor, column="28. Q3_Objectif_Mentor")
    obj_mentore = extraire_objectifs(row=mentore, column="25. Q8_Objectifs_Mentore")

    points_par_objectif = [500, 250, 125, 50, 10]
    correspondance = 0
    score = 0
    details = []

    # Compare les objectifs par ordre de priorité
    max_compare = min(len(obj_mentor), len(obj_mentore), 5)

    for i in range(max_compare):
        if correspondance_objectifs[obj_mentor[i]] == obj_mentore[i]:
            score += points_par_objectif[i]
            correspondance += 1
            details.append(f"  Obj {i+1}: ✓ '{obj_mentor[i]}' = '{obj_mentore[i]}'")
        else:
            details.append(f"  Obj {i+1}: ✗ '{obj_mentor[i]}' ≠ '{obj_mentore[i]}'")

    detail_str = f"Objectifs ({correspondance}/{max_compare} correspondances)\n" + "\n".join(details)
    return score, detail_str


def calculer_score_direction(mentor: pd.Series, mentore: pd.Series) -> tuple[int, str]:
    """
    Critère 3: Direction (200 points)
    Vérifie la préférence du mentor concernant la direction
    """
    dir_mentor = str(mentor["6. F_DIRECTION"]).strip()
    dir_mentore = str(mentore["6. F_DIRECTION"]).strip()
    pref_dir = str(mentor.get(key="27. Q9_Direction_Mentor", default="")).strip()

    # Si pas de données, score nul
    if not dir_mentor or not dir_mentore or dir_mentor == "nan" or dir_mentore == "nan":
        return 0, "Direction: données manquantes"

    # Pas de préférence = score automatique
    if pref_dir == ChoixDirection.SANS_PREF.value:
        return 200, "✓ Direction: pas de préférence"

    # Même direction
    if pref_dir == ChoixDirection.MEME_DIR.value:
        if dir_mentor == dir_mentore:
            return 200, f"✓ Direction: même direction ({dir_mentor})"
        else:
            return 0, f"✗ Direction: {dir_mentor} ≠ {dir_mentore}"

    # Autre direction
    if pref_dir == ChoixDirection.AUTRE_DIR.value:
        if dir_mentor != dir_mentore:
            return 200, "✓ Direction: directions différentes"
        else:
            return 0, "✗ Direction: même direction non souhaitée"

    return 0, f"Direction: préférence '{pref_dir}' non reconnue"


def calculer_score_geographie(mentor: pd.Series, mentore: pd.Series) -> tuple[int, str]:
    """
    Critère 4: Géographie (100 points)
    Même département
    """
    dept_mentor = str(mentor["15. H2_Departement"]).strip()
    dept_mentore = str(mentore["15. H2_Departement"]).strip()

    if dept_mentor and dept_mentore and dept_mentor != "nan" and dept_mentore != "nan":
        if dept_mentor == dept_mentore:
            return 100, f"✓ Géographie: même département ({dept_mentor})"
        else:
            return 0, f"✗ Géographie: {dept_mentor} ≠ {dept_mentore}"

    return 0, "Géographie: données manquantes"


def calculer_score_total(mentor: pd.Series, mentore: pd.Series) -> Mapping[str, Any]:
    """Calcule le score total et les détails pour un binôme"""
    score_cat, detail_cat = calculer_score_categorie(mentor=mentor, mentore=mentore)
    score_obj, detail_obj = calculer_score_objectifs(mentor=mentor, mentore=mentore)
    score_dir, detail_dir = calculer_score_direction(mentor=mentor, mentore=mentore)
    score_geo, detail_geo = calculer_score_geographie(mentor=mentor, mentore=mentore)

    score_total = score_cat + score_obj + score_dir + score_geo

    return {
        "score_total": score_total,
        "score_categorie": score_cat,
        "score_objectifs": score_obj,
        "score_direction": score_dir,
        "score_geographie": score_geo,
        "details": f"{detail_cat}\n{detail_obj}\n{detail_dir}\n{detail_geo}",
    }
