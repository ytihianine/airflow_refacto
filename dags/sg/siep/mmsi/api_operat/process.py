import json
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Union


def split_declaration_and_adresse_efa(declarations: list) -> Union[list, list]:
    rows_declaration = []
    rows_adresse_efa = []

    for declaration in declarations:
        rows_adresse_efa.append(
            declaration.pop("adresseEfa", None)
            | {"idOccupantEfa": declaration["idOccupantEfa"]}
        )
        rows_declaration.append(declaration)

    return rows_declaration, rows_adresse_efa


def process_declarations(df: pd.DataFrame) -> pd.DataFrame:
    colnames_mapping = {
        "idConsommation": "id_consommation",
        "anneeDeclaree": "annee_declaree",
        "refOperatEfa": "ref_operat_efa",
        "denominationOccupantEfa": "denomination_occupant_efa",
        "complementNomEfa": "complement_nom_efa",
        "typeOccupantEfa": "type_occupant_efa",
        "idOccupantEfa": "id_occupant_efa",
        "idImportConsommations": "id_import_consommations",
        "statut": "statut",
    }
    df = df.fillna(np.nan).replace([np.nan], [None])
    df = df.rename(columns=colnames_mapping)
    return df


def process_adresse_efa(df: pd.DataFrame) -> pd.DataFrame:
    colnames_mapping = {
        "idOccupantEfa": "id_occupant_efa",
        "numeroNomVoie": "numero_nom_voie",
        "codePostal": "code_postal",
        "commune": "commune",
    }
    df = df.fillna(np.nan).replace([np.nan], [None])
    df = df.drop_duplicates(subset=["idOccupantEfa"])
    df = df.rename(columns=colnames_mapping)
    return df


def process_detail_conso(raw_data: list[dict[str, any]]) -> pd.DataFrame:
    colnames_mapping = {
        "consoId": "id_consommation_api",
        "refOperatEfa": "ref_operat_efa",
        "anneeDeclaree": "annee_declaree",
        "dateDebutConsoReference": "date_debut_conso_reference",
        "categorieActivitePrincipale": "categorie_activite_principale",
        "vehiElecNbBornes": "vehi_elec_nb_bornes",
        "vehiElecConsoAvcIRVEKWh": "vehi_elec_conso_avc_irve_kwh",
        "consoIndiHorsIRVEKWh": "conso_indi_hors_irve_kwh",
        "consoIndiGazNatReseauxKWh": "conso_indi_gaz_nat_reseaux_kwh",
        "consoIndiGazNatLiquefieKg": "conso_indi_gaz_nat_liquefie_kg",
        "consoIndiGazPropaneKg": "conso_indi_gaz_propane_kg",
        "consoIndiGazPropaneM3": "conso_indi_gaz_propane_m3",
        "consoIndiGazButaneKg": "conso_indi_gaz_butane_kg",
        "consoIndiGazButaneM3": "conso_indi_gaz_butane_m3",
        "consoIndiFioulDomestiLitre": "conso_indi_fioul_domesti_litre",
        "consoIndiCharbonAggloBriqKg": "conso_indi_charbon_agglo_briq_kg",
        "consoIndiHouilleKg": "conso_indi_houille_kg",
        "consoIndiBoisPlaqIndustrieKg": "conso_indi_bois_plaq_industrie_kg",
        "consoIndiBoisPlaqForestKg": "conso_indi_bois_plaq_forest_kg",
        "consoIndiBoisGranBriqKg": "conso_indi_bois_gran_briq_kg",
        "consoIndiBoisBuchesStere": "conso_indi_bois_buches_stere",
        "consoIndiReseauChaleurKWh": "conso_indi_reseau_chaleur_kwh",
        "consoIndiReseauFroidKWh": "conso_indi_reseau_froid_kwh",
        "consoIndiGazoleNonRoutierLitre": "conso_indi_gazole_non_routier_litre",
        "consoRepHorsIRVEKWh": "conso_rep_hors_irve_kwh",
        "consoRepGazNatReseauxKWh": "conso_rep_gaz_nat_reseaux_kwh",
        "consoRepGazNatLiquefieKg": "conso_rep_gaz_nat_liquefie_kg",
        "consoRepGazPropaneKg": "conso_rep_gaz_propane_kg",
        "consoRepGazPropaneM3": "conso_rep_gaz_propane_m3",
        "consoRepGazButaneKg": "conso_rep_gaz_butane_kg",
        "consoRepGazButaneM3": "conso_rep_gaz_butane_m3",
        "consoRepFioulDomestiLitre": "conso_rep_fioul_domesti_litre",
        "consoRepCharbonAggloBriqKg": "conso_rep_charbon_agglo_briq_kg",
        "consoRepHouilleKg": "conso_rep_houille_kg",
        "consoRepBoisPlaqIndustrieKg": "conso_rep_bois_plaq_industrie_kg",
        "consoRepBoisPlaqForestKg": "conso_rep_bois_plaq_forest_kg",
        "consoRepBoisGranBriqKg": "conso_rep_bois_gran_briq_kg",
        "consoRepBoisBuchesStere": "conso_rep_bois_buches_stere",
        "consoRepReseauChaleurKWh": "conso_rep_reseau_chaleur_kwh",
        "consoRepReseauFroidKWh": "conso_rep_reseau_froid_kwh",
        "consoRepGazoleNonRoutierLitre": "conso_rep_gazole_non_routier_litre",
        "consoComHorsIRVEKWh": "conso_com_hors_irve_kwh",
        "consoComGazNatReseauxKWh": "conso_com_gaz_nat_reseaux_kwh",
        "consoComGazNatLiquefieKg": "conso_com_gaz_nat_liquefie_kg",
        "consoComGazPropaneKg": "conso_com_gaz_propane_kg",
        "consoComGazPropaneM3": "conso_com_gaz_propane_m3",
        "consoComGazButaneKg": "conso_com_gaz_butane_kg",
        "consoComGazButaneM3": "conso_com_gaz_butane_m3",
        "consoComFioulDomestiLitre": "conso_com_fioul_domesti_litre",
        "consoComCharbonAggloBriqKg": "conso_com_charbon_agglo_briq_kg",
        "consoComHouilleKg": "conso_com_houille_kg",
        "consoComBoisPlaqIndustrieKg": "conso_com_bois_plaq_industrie_kg",
        "consoComBoisPlaqForestKg": "conso_com_bois_plaq_forest_kg",
        "consoComBoisGranBriqKg": "conso_com_bois_gran_briq_kg",
        "consoComBoisBuchesStere": "conso_com_bois_buches_stere",
        "consoComReseauChaleurKWh": "conso_com_reseau_chaleur_kwh",
        "consoComReseauFroidKWh": "conso_com_reseau_froid_kwh",
        "consoComGazoleNonRoutierLitre": "conso_com_gazole_non_routier_litre",
        "stationMeteoEfa": "station_meteo_efa",
        "chauffageConsoElecKWh": "chauffage_conso_elec_kwh",
        "chauffageConsoGazNatReseauxKWh": "chauffage_conso_gaz_nat_reseaux_kwh",
        "chauffageConsoGazNatLiquefieKg": "chauffage_conso_gaz_nat_liquefie_kg",
        "chauffageConsoGazPropaneKg": "chauffage_conso_gaz_propane_kg",
        "chauffageConsoGazPropaneM3": "chauffage_conso_gaz_propane_m3",
        "chauffageConsoGazButaneKg": "chauffage_conso_gaz_butane_kg",
        "chauffageConsoGazButaneM3": "chauffage_conso_gaz_butane_m3",
        "chauffageConsoFioulDomestiLitre": "chauffage_conso_fioul_domesti_litre",
        "chauffageConsoCharbonAggloBriqKg": "chauffage_conso_charbon_agglo_briq_kg",
        "chauffageConsoHouilleKg": "chauffage_conso_houille_kg",
        "chauffageConsoBoisPlaqIndustrieKg": "chauffage_conso_bois_plaq_industrie_kg",
        "chauffageConsoBoisPlaqForestKg": "chauffage_conso_bois_plaq_forest_kg",
        "chauffageConsoBoisGranBriqKg": "chauffage_conso_bois_gran_briq_kg",
        "chauffageConsoBoisBuchesStere": "chauffage_conso_bois_buches_stere",
        "chauffageConsoReseauChaleurKWh": "chauffage_conso_reseau_chaleur_kwh",
        "chauffageConsoReseauFroidKWh": "chauffage_conso_reseau_froid_kwh",
        "chauffageConsoGazoleNonRoutierLitre": "chauffage_conso_gazole_non_routier_litre",
        "refroidissementConsoElecKWh": "refroidissement_conso_elec_kwh",
        "refroidissementConsoGazNatReseauxKWh": "refroidissement_conso_gaz_nat_reseaux_kwh",
        "refroidissementConsoGazNatLiquefieKg": "refroidissement_conso_gaz_nat_liquefie_kg",
        "refroidissementConsoGazPropaneKg": "refroidissement_conso_gaz_propane_kg",
        "refroidissementConsoGazPropaneM3": "refroidissement_conso_gaz_propane_m3",
        "refroidissementConsoGazButaneKg": "refroidissement_conso_gaz_butane_kg",
        "refroidissementConsoGazButaneM3": "refroidissement_conso_gaz_butane_m3",
        "refroidissementConsoFioulDomestiLitre": "refroidissement_conso_fioul_domesti_litre",
        "refroidissementConsoCharbonAggloBriqKg": "refroidissement_conso_charbon_agglo_briq_kg",
        "refroidissementConsoHouilleKg": "refroidissement_conso_houille_kg",
        "refroidissementConsoBoisPlaqIndustrieKg": "refroidissement_conso_bois_plaq_industrie_kg",
        "refroidissementConsoBoisPlaqForestKg": "refroidissement_conso_bois_plaq_forest_kg",
        "refroidissementConsoBoisGranBriqKg": "refroidissement_conso_bois_gran_briq_kg",
        "refroidissementConsoBoisBuchesStere": "refroidissement_conso_bois_buches_stere",
        "refroidissementConsoReseauChaleurKWh": "refroidissement_conso_reseau_chaleur_kwh",
        "refroidissementConsoReseauFroidKWh": "refroidissement_conso_reseau_froid_kwh",
        "refroidissementConsoGazoleNonRoutierLitre": "refroidissement_conso_gazole_non_routier_litre",
        "froidLogistHauteurRefroidie": "froid_logist_hauteur_refroidie",
        "froidLogistConsoElecKWh": "froid_logist_conso_elec_kwh",
        "froidLogistConsoGazNatReseauxKWh": "froid_logist_conso_gaz_nat_reseaux_kwh",
        "froidLogistConsoGazNatLiquefieKg": "froid_logist_conso_gaz_nat_liquefie_kg",
        "froidLogistConsoGazPropaneKg": "froid_logist_conso_gaz_propane_kg",
        "froidLogistConsoGazPropaneM3": "froid_logist_conso_gaz_propane_m3",
        "froidLogistConsoGazButaneKg": "froid_logist_conso_gaz_butane_kg",
        "froidLogistConsoGazButaneM3": "froid_logist_conso_gaz_butane_m3",
        "froidLogistConsoFioulDomestiLitre": "froid_logist_conso_fioul_domesti_litre",
        "froidLogistConsoCharbonAggloBriqKg": "froid_logist_conso_charbon_agglo_briq_kg",
        "froidLogistConsoHouilleKg": "froid_logist_conso_houille_kg",
        "froidLogistConsoBoisPlaqIndustrieKg": "froid_logist_conso_bois_plaq_industrie_kg",
        "froidLogistConsoBoisPlaqForestKg": "froid_logist_conso_bois_plaq_forest_kg",
        "froidLogistConsoBoisGranBriqKg": "froid_logist_conso_bois_gran_briq_kg",
        "froidLogistConsoBoisBuchesStere": "froid_logist_conso_bois_buches_stere",
        "froidLogistConsoReseauChaleurKWh": "froid_logist_conso_reseau_chaleur_kwh",
        "froidLogistConsoReseauFroidKWh": "froid_logist_conso_reseau_froid_kwh",
        "froidLogistConsoGazoleNonRoutierLitre": "froid_logist_conso_gazole_non_routier_litre",
        "froidComConsoElecKWh": "froid_com_conso_elec_kwh",
        "froidComConsoGazNatReseauxKWh": "froid_com_conso_gaz_nat_reseaux_kwh",
        "froidComConsoGazNatLiquefieKg": "froid_com_conso_gaz_nat_liquefie_kg",
        "froidComConsoGazPropaneKg": "froid_com_conso_gaz_propane_kg",
        "froidComConsoGazPropaneM3": "froid_com_conso_gaz_propane_m3",
        "froidComConsoGazButaneKg": "froid_com_conso_gaz_butane_kg",
        "froidComConsoGazButaneM3": "froid_com_conso_gaz_butane_m3",
        "froidComConsoFioulDomestiLitre": "froid_com_conso_fioul_domesti_litre",
        "froidComConsoCharbonAggloBriqKg": "froid_com_conso_charbon_agglo_briq_kg",
        "froidComConsoHouilleKg": "froid_com_conso_houille_kg",
        "froidComConsoBoisPlaqIndustrieKg": "froid_com_conso_bois_plaq_industrie_kg",
        "froidComConsoBoisPlaqForestKg": "froid_com_conso_bois_plaq_forest_kg",
        "froidComConsoBoisGranBriqKg": "froid_com_conso_bois_gran_briq_kg",
        "froidComConsoBoisBuchesStere": "froid_com_conso_bois_buches_stere",
        "froidComConsoReseauChaleurKWh": "froid_com_conso_reseau_chaleur_kwh",
        "froidComConsoReseauFroidKWh": "froid_com_conso_reseau_froid_kwh",
        "froidComConsoGazoleNonRoutierLitre": "froid_com_conso_gazole_non_routier_litre",
        "conservationDocConsoElecKWh": "conservation_doc_conso_elec_kwh",
        "conservationDocConsoGazNatReseauxKWh": "conservation_doc_conso_gaz_nat_reseaux_kwh",
        "conservationDocConsoGazNatLiquefieKg": "conservation_doc_conso_gaz_nat_liquefie_kg",
        "conservationDocConsoGazPropaneKg": "conservation_doc_conso_gaz_propane_kg",
        "conservationDocConsoGazPropaneM3": "conservation_doc_conso_gaz_propane_m3",
        "conservationDocConsoGazButaneKg": "conservation_doc_conso_gaz_butane_kg",
        "conservationDocConsoGazButaneM3": "conservation_doc_conso_gaz_butane_m3",
        "conservationDocConsoFioulDomestiLitre": "conservation_doc_conso_fioul_domesti_litre",
        "conservationDocConsoCharbonAggloBriqKg": "conservation_doc_conso_charbon_agglo_briq_kg",
        "conservationDocConsoHouilleKg": "conservation_doc_conso_houille_kg",
        "conservationDocConsoBoisPlaqIndustrieKg": "conservation_doc_conso_bois_plaq_industrie_kg",
        "conservationDocConsoBoisPlaqForestKg": "conservation_doc_conso_bois_plaq_forest_kg",
        "conservationDocConsoBoisGranBriqKg": "conservation_doc_conso_bois_gran_briq_kg",
        "conservationDocConsoBoisBuchesStere": "conservation_doc_conso_bois_buches_stere",
        "conservationDocConsoReseauChaleurKWh": "conservation_doc_conso_reseau_chaleur_kwh",
        "conservationDocConsoReseauFroidKWh": "conservation_doc_conso_reseau_froid_kwh",
        "conservationDocConsoGazoleNonRoutierLitre": "conservation_doc_conso_gazole_non_routier_litre",
    }

    df = pd.DataFrame(data=raw_data)
    df = df.rename(columns=colnames_mapping)
    df = df.fillna(np.nan).replace([np.nan], [None])

    # Convertir les données au bon format
    df["date_debut_conso_reference"] = pd.to_datetime(
        df["date_debut_conso_reference"], format="%d/%m/%Y"
    )
    default_cols = list(colnames_mapping.values())
    cols_to_convert = [
        col
        for col in default_cols
        if col
        not in [
            "id_consommation_api",
            "refOperatEfa",
            "annee_declaree",
            "date_debut_conso_reference",
            "categorie_activite_principale",
        ]
    ]

    for col in cols_to_convert:
        print(f"Colname: {col}")
        print(f"Avant: {df[col].unique()}")
        df[col] = df[col].str.replace(",", ".")
        df[col] = df[col].replace("", np.nan)
        print(f"Après: {df[col].unique()}")
        df[col] = pd.to_numeric(df[col], downcast="float", errors="ignore")
    df = df.fillna(np.nan).replace([np.nan], [None])

    return df


def process_detail_conso_activite(raw_data: list[dict[str, any]]) -> pd.DataFrame:
    colnames_mapping = {
        "sousCategorieActivite": "sous_categorie_activite",
        "surfacePlancherM2": "surface_plancher_m2",
        "dateDebutActivite": "date_debut_activite",
        "dateFinActivite": "date_fin_activite",
        "chauffage": "chauffage",
        "refroidissement": "refroidissement",
        "logistiqueDeFroid": "logistique_de_froid",
        "froidCommercial": "froid_commercial",
        "conservationDocCollections": "conservation_doc_collections",
        "nbJoursOccupesMois1": "nb_jours_occupes_mois_1",
        "nbJoursOccupesMois2": "nb_jours_occupes_mois_2",
        "nbJoursOccupesMois3": "nb_jours_occupes_mois_3",
        "nbJoursOccupesMois4": "nb_jours_occupes_mois_4",
        "nbJoursOccupesMois5": "nb_jours_occupes_mois_5",
        "nbJoursOccupesMois6": "nb_jours_occupes_mois_6",
        "nbJoursOccupesMois7": "nb_jours_occupes_mois_7",
        "nbJoursOccupesMois8": "nb_jours_occupes_mois_8",
        "nbJoursOccupesMois9": "nb_jours_occupes_mois_9",
        "nbJoursOccupesMois10": "nb_jours_occupes_mois_10",
        "nbJoursOccupesMois11": "nb_jours_occupes_mois_11",
        "nbJoursOccupesMois12": "nb_jours_occupes_mois_12",
        "nbJoursOccupesMois13": "nb_jours_occupes_mois_13",
    }
    default_value = {
        "nbJoursOccupesMois1": None,
        "nbJoursOccupesMois2": None,
        "nbJoursOccupesMois3": None,
        "nbJoursOccupesMois4": None,
        "nbJoursOccupesMois5": None,
        "nbJoursOccupesMois6": None,
        "nbJoursOccupesMois7": None,
        "nbJoursOccupesMois8": None,
        "nbJoursOccupesMois9": None,
        "nbJoursOccupesMois10": None,
        "nbJoursOccupesMois11": None,
        "nbJoursOccupesMois12": None,
        "nbJoursOccupesMois13": None,
    }

    for elem in raw_data:
        print("I am the elem: ", elem)
        nb_jours_occupes = elem.pop("nbJoursOccupes", default_value)
        elem = elem | nb_jours_occupes

    df = pd.DataFrame(data=raw_data)
    df = df.fillna(np.nan).replace([np.nan], [None])

    # Conversion des colonnes au data type attendu
    df["dateDebutActivite"] = pd.to_datetime(df["dateFinActivite"], format="%d/%m/%Y")
    df["dateFinActivite"] = pd.to_datetime(df["dateFinActivite"], format="%d/%m/%Y")
    df["surfacePlancherM2"] = df["surfacePlancherM2"].str.replace(",", ".")
    df["surfacePlancherM2"] = pd.to_numeric(
        df["surfacePlancherM2"], downcast="float", errors="coerce"
    )
    df = df.fillna(np.nan).replace([np.nan], [None])
    df = df.rename(columns=colnames_mapping)

    return df


def process_detail_conso_indicateur(raw_data: list[dict[str, any]]) -> pd.DataFrame:
    max_num = max(
        [
            int(key.replace("nomIndicateur", ""))
            for key in raw_data[0].keys()
            if key.startswith("nomIndicateur")
            and key.replace("nomIndicateur", "").isdigit()
        ]
    )

    raw_data_formated = []
    for elem in raw_data:
        for i in range(1, max_num):
            nom_key = f"nomIndicateur{i}"
            valeur_key = f"valeurIndicateur{i}"

            nom_indicateur = elem.get(nom_key, "").strip()
            valeur = elem.get(valeur_key, "").strip()
            id_consommation = elem.get("id_consommation", None)

            if nom_indicateur and valeur:
                raw_data_formated.append(
                    {
                        "num_indicateur": i,
                        "nom_indicateur": nom_indicateur,
                        "valeur_indicateur": valeur,
                        "id_consommation": id_consommation,
                    }
                )

    df = pd.DataFrame(data=raw_data_formated)
    df = df.fillna(np.nan).replace([np.nan], [None])

    return df
