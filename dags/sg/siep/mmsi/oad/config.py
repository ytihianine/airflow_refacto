from modules.enums.database import LoadStrategy
from modules.types.projet import SelecteurStorageOptions

storage_options = {
    "accessibilite": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "accessibilite_detail": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "bacs": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "bails": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "biens": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND, tbl_order=1),
    "biens_gest": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "biens_occupants": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND, tbl_order=3),
    "couts": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "deet_energie_ges": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "effectif": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "etat_de_sante": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "exploitation": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "gestionnaires": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND, tbl_order=1),
    "localisation": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "note": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "oad_carac": SelecteurStorageOptions(write_to_db=False),
    "oad_indic": SelecteurStorageOptions(write_to_db=False),
    "proprietaire": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "reglementation": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "sites": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND, tbl_order=1),
    "strategie": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "surface": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "typologie": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "valeur": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
}
