from modules.enums.database import LoadStrategy
from modules.types.projet import SelecteurStorageOptions

nom_projet_operat = "API Opera"
dag_id_operat = "api_operat_ademe"

storage_options = {
    "activite": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND, tbl_order=1),
    "consommations": SelecteurStorageOptions(write_to_db=False),
    "declaration_ademe": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND, tbl_order=0),
    "declarations": SelecteurStorageOptions(write_to_db=False),
    "detail": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND, tbl_order=1),
    "indicateur": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND, tbl_order=1),
}


ID_STRUCTURES = [
    "ETAT_MIN_EF",
    # "ETAT_REG_ARA",
    # "ETAT_REG_BFC",
    # "ETAT_REG_BRE",
    # "ETAT_REG_COR",
    # "ETAT_REG_CVL",
    # "ETAT_REG_GES",
    # "ETAT_REG_GUA",
    # "ETAT_REG_GUF",  # => Aucun résultat
    # # "ETAT_REG_HDF", # => Problème d'authentification
    # "ETAT_REG_IDF",
    # "ETAT_REG_LRE",  # => Aucune résultat
    # "ETAT_REG_MAY",  # => Aucune résultat
    # "ETAT_REG_MTQ",
    # "ETAT_REG_NAQ",
    # "ETAT_REG_NOR",
    # "ETAT_REG_OCC",
    # "ETAT_REG_PAC",
    # "ETAT_REG_PDL",
    # "194416186",
    # "313320244",
    # "195726476",
    # "195936489",
    # "180092025",
    # "180080012",
    # "130014228",
    # "383181575",
    # "197534936",
    # "775665912",
    # "451930051",
    # "180053027",
]
