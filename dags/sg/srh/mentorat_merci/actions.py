import logging
import os
import pathlib
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pandas as pd
from dags.sg.srh.mentorat_merci import process
from modules.infra.mails.default_smtp import MailMessage, render_template, send_mail
from modules.utils.config.dag_params import get_execution_date, get_project_name
from modules.utils.config.tasks import get_list_contact


def trouver_meilleurs_binomes(df: pd.DataFrame) -> Mapping[str, pd.DataFrame]:
    """
    Trouve les meilleurs binômes en utilisant un algorithme glouton
    SEULS les binômes avec catégorie compatible sont évalués
    """
    df_mentors = pd.DataFrame(data=df[df["19. QA_STATUT"] == "Mentor"].copy())
    df_mentores = pd.DataFrame(data=df[df["19. QA_STATUT"] == "Mentoré"].copy())

    resultats = []
    mentores_disponibles = set(df_mentores.index)
    mentors_disponibles = set(df_mentors.index)

    # Calculer tous les scores possibles UNIQUEMENT pour les catégories compatibles
    tous_scores = []
    binomes_incompatibles = 0

    for idx_mentor in mentors_disponibles:
        mentor = df_mentors.loc[idx_mentor]
        for idx_mentore in mentores_disponibles:
            mentore = df_mentores.loc[idx_mentore]

            # # CRITÈRE ÉLIMINATOIRE : Vérifier d'abord la compatibilité des catégories
            # compatible, _ = self.verifier_categorie_compatible(mentor, mentore)

            # if not compatible:
            #     binomes_incompatibles += 1
            #     continue  # On ignore ce binôme, il n'est pas évalué

            # Si compatible, on calcule le score
            score_info = process.calculer_score_total(mentor=mentor, mentore=mentore)

            tous_scores.append(
                {
                    "idx_mentor": idx_mentor,
                    "idx_mentore": idx_mentore,
                    "mentor_nom": f"{mentor['2. B_PRENOM']} {mentor['1. A_NOM']}",
                    "mentore_nom": f"{mentore['2. B_PRENOM']} {mentore['1. A_NOM']}",
                    **score_info,
                }
            )

    print(f"\nBinômes éliminés pour incompatibilité de catégorie : {binomes_incompatibles}")
    print(f"Binômes éligibles évalués : {len(tous_scores)}")

    # Trier par score décroissant
    tous_scores.sort(key=lambda x: x["score_total"], reverse=True)

    # Sélectionner les meilleurs binômes (glouton)
    for score_info in tous_scores:
        idx_mentor = score_info["idx_mentor"]
        idx_mentore = score_info["idx_mentore"]

        if idx_mentor in mentors_disponibles and idx_mentore in mentores_disponibles:
            resultats.append(score_info)
            mentors_disponibles.remove(idx_mentor)
            mentores_disponibles.remove(idx_mentore)

    # Créer le DataFrame des résultats
    df_resultats = pd.DataFrame(data=resultats)

    # Ajouter les non-appariés
    cols_to_keep = [
        "2. B_PRENOM",
        "1. A_NOM",
        "5. E_CATEGORIE",
        "6. F_DIRECTION",
        "15. H2_Departement",
        "27. Q9_Direction_Mentor",
        "28. Q3_Objectif_Mentor",
    ]
    df_mentores_non_apparies = pd.DataFrame(data=df_mentores.loc[list(mentores_disponibles)][cols_to_keep])
    df_mentors_non_apparies = pd.DataFrame(data=df_mentors.loc[list(mentors_disponibles)][cols_to_keep])

    return {
        "df_binomes": df_resultats,
        "df_mentors": df_mentors,
        "df_mentores": df_mentores,
        "df_mentores_non_apparies": df_mentores_non_apparies,
        "df_mentors_non_apparies": df_mentors_non_apparies,
    }


def generer_rapport(dfs: Mapping[str, pd.DataFrame]) -> str:
    """Génère un rapport des binômes avec onglets séparés pour les non-appariés"""
    # Préparer les résultats principaux
    df_binomes = dfs["df_binomes"]
    rapport = []

    # Statistiques
    rapport.append("\n" + "=" * 80)
    rapport.append("RAPPORT DE MATCHING MENTOR-MENTORÉ")
    rapport.append("=" * 80)
    rapport.append(f"\nNombre total de mentors: {len(dfs["df_mentors"])}")
    rapport.append(f"Nombre total de mentorés: {len(dfs["df_mentores"])}")
    rapport.append(f"Nombre de binômes créés: {len(df_binomes)}")
    rapport.append(f"Mentors non appariés: {len(dfs["df_mentors_non_apparies"])}")
    rapport.append(f"Mentorés non appariés: {len(dfs["df_mentores_non_apparies"])}")

    if len(df_binomes) > 0:
        rapport.append(f"\nScore moyen: {df_binomes['score_total'].mean():.1f}")
        rapport.append(f"Score minimum: {df_binomes['score_total'].min()}")
        rapport.append(f"Score maximum: {df_binomes['score_total'].max()}")

        rapport.append(
            f"\nBinômes avec score parfait (2475p total): {len(df_binomes[df_binomes['score_total'] == 2475])}"
        )
        rapport.append(f"Binômes avec catégorie OK: {len(df_binomes[df_binomes['score_categorie'] == 1000])}")

    rapport.append("=" * 80)

    return "\n".join(rapport)


def send_result(dfs: Mapping[str, pd.DataFrame], context: Mapping[str, Any]) -> None:
    execution_date = get_execution_date(context=context, use_tz=True)
    nom_projet = get_project_name(context=context)
    projet_contact = get_list_contact(nom_projet=nom_projet)
    mail_to = [contact.contact_mail for contact in projet_contact if contact.is_mail_generic]
    mail_cc = [contact.contact_mail for contact in projet_contact if not contact.is_mail_generic]
    tmp_path = Path(f"/tmp/binomes_v{execution_date.strftime(format="%Y%m%d_%Hh%M")}.xlsx")

    # Sauvegarder dans un fichier Excel avec plusieurs onglets
    print(f"Saving file locally to {tmp_path}")
    with pd.ExcelWriter(path=tmp_path, engine="openpyxl") as writer:
        dfs["df_binomes"].to_excel(excel_writer=writer, sheet_name="Binômes", index=False)
        dfs["df_mentors_non_apparies"].to_excel(excel_writer=writer, sheet_name="Mentors non appariés", index=False)
        dfs["df_mentores_non_apparies"].to_excel(excel_writer=writer, sheet_name="Mentorés non appariés", index=False)

    rapport = generer_rapport(dfs=dfs)
    print(rapport)

    html_content = render_template(
        template_dir=pathlib.Path(__file__).parent.resolve(),
        template_parameters={"rapport": rapport},
        template_name="mail.html",
    )

    mail_message = MailMessage(
        to=mail_to,
        cc=mail_cc,
        subject=f"[{nom_projet}] - Binômes générés",
        html_content=html_content,
        files=[str(tmp_path)],
    )

    send_mail(mail_message=mail_message)

    os.remove(path=tmp_path)


def action_generer_binomes_mentorat(df: pd.DataFrame, context: Mapping[str, Any]) -> None:
    """
    Action principale pour générer les binômes de mentorat et envoyer le rapport par mail.
    """
    logging.info(msg="Début de l'action : génération des binômes de mentorat")
    dfs_resultats = trouver_meilleurs_binomes(df=df)
    send_result(dfs=dfs_resultats, context=context)
    logging.info(msg="Fin de l'action : génération des binômes de mentorat")
