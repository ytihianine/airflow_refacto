from typing import Union
from functools import partial
from datetime import datetime
import pandas as pd
import numpy as np


valeur_indeterminee = "N/D"

mapping_direction = {
    "ASSOCIATION": {
        "AAF": "AAF",
        "AEB": "AEB",
        "AGRAF": "AGRAF",
        "ALPAF": "ALPAF",
        "ANC COMB": "ANC COMB",
        "APAHF": "APAHF",
        "ARTS": "ARTS",
        "ATSCAF": "ATSCAF",
        "CLUB SPORT": "CLUB SPORTIF",
        "EPAF": "EPAF",
        "ASFV": "ASFV",
        "ASFL": "ASFL",
        "ASFR": "ASFR",
        "FASF": "FASF",
        "FEMMES BER": "FEMMES BER",
    },
    "AUTORITES": {
        "ANAFE": "ANAFE",
        "AC": "AC",
        "ACPR": "ACPR",
        "ANC": "ANC",
        "ANJ": "ANJ",
        "ASN": "ASN",
        "APCR": "APCR",
        "ARCEP": "ARCEP",
        "CPT": "CPT",
        "CIF": "CIF",
        "CRE": "CRE",
    },
    "CONSEIL": {
        "CHAI": "CHAI",
        "CICAI": "CICAI",
        "CIE": "CIE",
        "CNC": "CNC",
        "CNN": "CNN",
        "CNOCP": "CNOCP",
    },
    "COMMISSIONS": {
        "ANAFE": "ANAFE",
        "CEPC": "CEPC",
        "CICC": "ANAFE",
        "CIF": "CIF",
        "CCLP": "CCLP",
        "CCCOP": "CCCOP",
        "CNS": "CNS",
        "CSNP": "CCSNP",
    },
    "MISSIONS": {
        "AMLA": "AMLA",
        "BER": "BER",
        "DIAMMS": "DIAMMS",
        "GIP-GEN": "GIP-GEN",
        "MAI": "MISSION-MAI",
        "MEA": "MISSION-MEA",
        "MGE": "MISSION-MGE",
        "ML": "ML",
        "MRP": "MRP",
        "RDM": "RDM",
        "OFGL": "OFGL",
        "PRO": "MISSION-PRO",
        "DTI": "DTI",
        "SIMP": "SIMP",
        "PRODVAC": "PRODVAC",
    },
    "DIRECTIONS ET SERVICES": {
        "AFT": "AFT",
        "APIE": "APIE",
        "AFA": "AFA",
        "AIFE": "AIFE",
        "APE": "APE",
        "CBCM": "CBCM",
        "CGE": "CGE",
        "CGEFI": "CGEFI",
        "CGEIET": "CGE",
        "CISIRH": "CISIRH",
        "DAE": "DAE",
        "DAJ": "DAJ",
        "DB": "DB",
        "DGAFP": "DGAFP",
        "DGE": "DGE",
        "DINSIC": "DINSIC",
        "DINUM": "DINUM",
        "DIRE": "DIRE",
        "DITP": "DITP",
        "DNLF": "DNLF",
        "IGF": "IGF",
        "MEDIASUP": "MEDIASUP",
        "MICAF": "MICAF",
        "SGAE": "SGAE",
        "TRACFIN": "TRACFIN",
        "DG TRESOR": "DG TRESOR",
        "BC": "BC",
        "EXT": "EXT",
    },
    "DIVERS": {
        "COOP": "COOP",
        "INC": "INC",
    },
    "ETABLISSEMENTS PUBLICS": {
        "ANFR": "ANFR",
        "CADES": "CADES",
        "AFII": "AFII",
        "GENES": "GENES",
        "IMT": "IMT",
    },
    "SEC GEN": {
        "BDS": "SG-BDS",
        "CAB": "SG-CAB",
        "DES": "SG-DES",
        "DSCI": "SG-DSCI",
        "DSI": "SG-DSI",
        "HFTLF": "SG-HFTLF",
        "IGPDE": "SG-IGPDE",
        "MFR": "SG-MFR",
        "MRC": "SG-MRC",
        "SAFI": "SG-SAFI",
        "SHFDS": "SG-SHFDS",
        "SEP2": "SG-SIEP",
        "SIEP": "SG-SIEP",
        "SIRCOM": "SG-SIRCOM",
        "SEP1": "SG-SNUM",
        "SNUM": "SG-SNUM",
        "SRH1/": "SG-SRH1",
        "SRH2/": "SG-SRH2",
        "SRH3/": "SG-SRH3",
        "SRH": "SG-SRH",
        "SEC GEN": "SG-AUTRE",
    },
    "MEDIATEUR": "MEDIATEUR",
    "MINISTRES": "CABINETS",
    "DGTRESOR": "DG TRESOR",
    "SYNDICATS": "SYNDICATS",
    "AUT MIN": "Autres ministères",
}


def determiner_aip_direction(aip_group: str) -> str:
    aip_dir = valeur_indeterminee
    if pd.isna(aip_group) or aip_group == "":
        return aip_dir

    aip_dir_split = aip_group.split("_")

    if "SEC_GEN" in aip_group:
        sub_service = aip_dir_split[-1]
        aip_dir = "SG_" + sub_service
        return aip_dir
    else:
        if len(aip_dir_split) == 2:
            aip_dir = aip_dir_split[-1]
            return aip_dir

    return aip_dir


def find_certificat_dir_in_profile(profile: str) -> str:
    if pd.isna(profile):
        return valeur_indeterminee
    profile = profile.upper()
    mapping = {
        # Autres structures
        "PRESTAT": "ARTEMIS",
        "DGCCRF": "DGCCRF",
        "AIFE": "AIFE",
    }

    for key, value in mapping.items():
        if key in profile:
            return value

    return valeur_indeterminee


def find_certificat_dir_in_dn(dn: str) -> str:
    if pd.isna(dn):
        return valeur_indeterminee
    dn = dn.upper()
    mapping = {
        # Autres structures
        "DGFIP": "DGFIP",
        "SIRHIUS": "CISIRH",
        "AIFE": "AIFE",
        "OPENTRUST": "IGC",
        # SG
        "ALIZE": "SG_SNUM",
    }

    for key, value in mapping.items():
        if key in dn:
            return value

    return valeur_indeterminee


def find_certificat_dir_in_subjectid(subjectid: str) -> str:
    if pd.isna(subjectid):
        return valeur_indeterminee
    subjectid = subjectid.upper()
    mapping = {
        # Autres structures
        "TRACFIN": "TRACFIN",
        # SG
        "ARCADE": "SG_SNUM",
        "TEST": "SG_SNUM",
    }

    for key, value in mapping.items():
        if key in subjectid:
            return value

    return valeur_indeterminee


def find_certificat_dir_in_contact(contact: str) -> str:
    if pd.isna(contact):
        return valeur_indeterminee
    contact = contact.upper()
    mapping = {
        # Autres structures
        "SIUDA": "DB",
        "DGFIP": "DGFIP",
        "DOUANE": "DGDDI",
        "AIFE": "AIFE",
        "INSEE": "INSEE",
        "CISIRH": "CISIRH",
        "CARGOET": "CISIRH",
        "LE-QUELLEC": "CISIRH",
        "CCRF": "DGCCRF",
        "@AFT": "AFT",
        # SG
        "SAFI": "SG_SAFI",
        "SEP": "SG_SNUM",
        "BEAUVOIS": "SG_SNUM",
        "BRUAL": "SG_SNUM",
        "NIANG": "SG_SNUM",
        "DJOUNNADI": "SG_SNUM",
        "SPOT": "SG_SNUM",
    }

    for key, value in mapping.items():
        if key in contact:
            return value

    return valeur_indeterminee


def find_certificat_dir_in_mail(mail: str) -> str:
    if pd.isna(mail):
        return valeur_indeterminee
    mail = mail.upper()
    mapping = {
        # Autres structures
        "DGFIP": "DGFIP",
        "SGAE": "SGAE",
        "IGF": "IGF",
        "SYNDICATS": "SYNDICATS",
        "INDUSTRIE.GOUV": "CABINETS",
        "TRANSFORMATION.GOUV": "CABINETS",
        "NUMERIQUE.GOUV": "CABINETS",
        "INSEE": "INSEE",
    }

    for key, value in mapping.items():
        if key in mail:
            return value

    return valeur_indeterminee


def determiner_certificat_direction(row: pd.Series) -> str:
    certif_dir = find_certificat_dir_in_profile(profile=row.profile)
    if certif_dir != valeur_indeterminee:
        return certif_dir
    certif_dir = find_certificat_dir_in_dn(dn=row.dn)
    if certif_dir != valeur_indeterminee:
        return certif_dir
    certif_dir = find_certificat_dir_in_subjectid(subjectid=row.subjectid)
    if certif_dir != valeur_indeterminee:
        return certif_dir
    certif_dir = find_certificat_dir_in_contact(contact=row.contact)
    if certif_dir != valeur_indeterminee:
        return certif_dir
    certif_dir = find_certificat_dir_in_mail(mail=row.contact)
    if certif_dir != valeur_indeterminee:
        return certif_dir

    certif_dir = valeur_indeterminee
    return certif_dir


def _match_mapping(
    text: str,
    mapping: dict[Union[str, tuple[str, ...]], str],
    default: str = valeur_indeterminee,
) -> str:
    """
    Matches `text` against keys in mapping.
    Keys can be a string (single condition) or a tuple (all substrings required).
    Returns the mapped value or `default`.
    """
    if text is None or pd.isna(text):
        return default

    text = text.upper()

    for keys, value in mapping.items():
        if isinstance(keys, str):  # single key
            keys = (keys,)
        if all(k.upper() in text for k in keys):
            return value
    return default


def determiner_ac(profile: str) -> str:
    mapping = {
        "SERVICE": "SERVICE",
        "SERVEUR": "SERVEURS",
        "AGENT": "AGENTS",
        "PRESTATAIRE": "PRESTATAIRES",
        "OPERATEURS": "OPERATEURS",
        "TEST": "TEST",
        "technique": "technique",
        "interne": "IGC",
        "AUTH_MOBILE": "MOBILE",
        "": "IGC",
    }

    return _match_mapping(text=profile, mapping=mapping)


def determiner_type_offre(profile: str) -> str:
    # Key order is important
    mapping = {
        # Autres structures
        "AUTH_SIGN": "AUTHENTIFICATION_SIGNATURE",
        "sign": "SIGNATURE",
        "auth": "AUTHENTIFICATION",
        "conf": "CONFIDENTIALITE",
        "prestataire": "ARTEMIS",
        "clients_multi": "CLIENT MULTIDOMAINE",
        "veurs_multi": "AUTHENTIFICATION MULTIDOMAINE",
        "horo": "HORODATAGE",
        "cachet": "CACHET",
        "serveurs_clien": "CLIENT SERVEUR",
        "operateur": "OPERATEUR",
        # Match exact values for those
        "F_SERVEURS": "AUTHENTIFICATION SERVEUR",
        # "F_SERVEURS_2": "AUTHENTIFICATION SERVEUR",
        # "F_SERVEURS_3": "AUTHENTIFICATION SERVEUR",
        # "serveurs": "",
    }
    return _match_mapping(text=profile, mapping=mapping)


def determiner_support(profile: str) -> str:
    # Key order is important
    mapping = {
        # Autres structures
        "TPM": "TPM",
        "logi": "LOGICIEL",
        "mate": "MATERIEL",
        "conf": "MATERIEL",
        "agents": "MATERIEL",
        "serveur": "SERVEUR",
        "service": "SERVICE",
        "ac_adm_technique": "MATERIEL",
    }
    return _match_mapping(text=profile, mapping=mapping)


def determiner_etat(row: pd.Series, date_ajd: datetime) -> str:
    date_debut_validite = row.date_debut_validite
    date_fin_validite = row.date_fin_validite
    date_revocation = row.date_revocation

    if date_revocation is not None:
        return "REVOQUE"

    if date_ajd > date_fin_validite:
        return "EXPIRE"

    if date_debut_validite <= date_ajd and date_ajd <= date_fin_validite:
        return "VALIDE"

    return "Autre"


def determiner_version(profile: str) -> str:
    # Key order is important
    mapping = {
        ("SERVEURS", "_3"): "SERVEUR3",
        ("SERVICE", "_3"): "SERVICE3",
        ("CONFIDENTIALITE",): "CRYPT",
        ("FSG3_",): "AC3",
        ("SERVEURS",): "SERVEUR",
        ("service",): "SERVICE",
        ("FSG_",): "AC2",
    }
    return _match_mapping(text=profile, mapping=mapping)


def determiner_version_serveur(profile: str) -> str:
    # Key order is important
    mapping = {
        ("3072",): "3072",
        ("serveur", "_2"): "2048",
        ("service", "_2"): "2048",
        ("_3",): "2048",
    }
    return _match_mapping(text=profile, mapping=mapping)


def map_agent_direction(row: pd.Series, mapping: dict) -> str | None:
    affectation = row.ou_sigle
    if pd.isna(affectation):
        return valeur_indeterminee

    affectation = affectation.strip().upper()

    if affectation.startswith("DGTRESOR"):
        return "DG TRESOR"

    affectation_split = affectation.split("/")

    direction = mapping.get(affectation_split[-1], None)

    if direction is None:
        return valeur_indeterminee

    if isinstance(direction, str):
        return direction.upper()

    if isinstance(direction, dict):
        for k, v in direction.items():
            if k in affectation_split:
                return v

    return valeur_indeterminee


"""
    Fonctions de processing des fichiers sources
"""


def process_agents(df: pd.DataFrame) -> pd.DataFrame:
    df["ou_sigle"] = df["ou_sigle"].str.strip()
    df["agent_mail"] = df["agent_mail"].str.strip()
    df["grid_mail"] = df["grid_mail"].str.strip()
    df["agent_direction"] = list(
        map(
            partial(map_agent_direction, mapping=mapping_direction),
            df.itertuples(index=False),
        )
    )
    return df


def process_aip(df: pd.DataFrame) -> pd.DataFrame:
    df["aip_direction"] = list(map(determiner_aip_direction, df["groupe"]))
    df["mail"] = df["mail"].str.strip()
    return df


def process_certificats(df: pd.DataFrame) -> pd.DataFrame:
    # df = df.fillna(np.nan).replace([np.nan], [None])
    date_cols = ["date_debut_validite", "date_fin_validite", "date_revocation"]
    for date_col in date_cols:
        df[date_col] = pd.to_datetime(
            df[date_col], format="%d/%m/%Y %H:%M:%S", errors="raise"
        )
    df["certificat_direction"] = list(
        map(determiner_certificat_direction, df.itertuples(index=False))
    )
    date_ajd = datetime.now()
    df["ac"] = list(map(determiner_ac, df["profile"]))
    df["type_offre"] = list(map(determiner_type_offre, df["profile"]))
    df["supports"] = list(map(determiner_support, df["profile"]))
    df["etat"] = list(
        map(partial(determiner_etat, date_ajd=date_ajd), df.itertuples(index=False))
    )
    df["version"] = list(map(determiner_version, df["profile"]))
    df["version_serveur"] = list(map(determiner_version_serveur, df["profile"]))
    return df


def process_igc(df: pd.DataFrame) -> pd.DataFrame:
    df["mail"] = df["mail"].str.strip()
    df["aip_balf_mail"] = df["aip_balf_mail"].str.strip()
    return df


"""
    Fonctions de processing des fichiers finaux
"""


def process_liste_aip(df_aip: pd.DataFrame, df_agents: pd.DataFrame) -> pd.DataFrame:
    df_agents = df_agents[["agent_direction", "grid_mail"]].drop_duplicates()

    df = pd.merge(
        left=df_aip,
        right=df_agents,
        how="left",
        left_on=["aip_direction"],
        right_on=["agent_direction"],
    )

    cols_to_keep = {
        "mail": "aip_mail",
        "aip_direction": "aip_balf_mail",
        "grid_mail": "aip_direction",
    }
    df = df[list(cols_to_keep.keys())]
    df = df.rename(columns=cols_to_keep)

    return df


def process_liste_certificats(df_certificats: pd.DataFrame) -> pd.DataFrame:
    df = df_certificats
    return df
