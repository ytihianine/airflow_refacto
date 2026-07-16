from modules.enums.database import LoadStrategy
from modules.types.projet import SelecteurStorageOptions

storage_options = {
    "quota_par_entite": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "experimentateurs": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    # Questionnaire 1
    "questionnaire_1": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "questionnaire_1_cas_usage": SelecteurStorageOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    "questionnaire_1_besoins_accompagnement": SelecteurStorageOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    # Questionnaire 2
    "questionnaire_2": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "questionnaire_2_typologie_interaction": SelecteurStorageOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    "questionnaire_2_facteurs_progression": SelecteurStorageOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    "questionnaire_2_formation_suivie": SelecteurStorageOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    "questionnaire_2_freins": SelecteurStorageOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    "questionnaire_2_impact_identifie": SelecteurStorageOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    "questionnaire_2_impact_observe": SelecteurStorageOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    "questionnaire_2_participation": SelecteurStorageOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    "questionnaire_2_taches": SelecteurStorageOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    "questionnaire_2_typologie_erreurs": SelecteurStorageOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    # Questionnaire_2_bis
    "questionnaire_2_bis": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "questionnaire_2_bis_raisons_non_utilisation": SelecteurStorageOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    # Questionnaire_3
    "questionnaire_3": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND, keep_file_id_col=False),
    "questionnaire_3_formation_suivie": SelecteurStorageOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    "questionnaire_3_programme_rdv": SelecteurStorageOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    "questionnaire_3_leviers_progression": SelecteurStorageOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    "questionnaire_3_fonctionnalites": SelecteurStorageOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    "questionnaire_3_risques_identifies": SelecteurStorageOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    "questionnaire_3_besoins_prioritaires": SelecteurStorageOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
    "questionnaire_3_besoins_moindres": SelecteurStorageOptions(
        load_strategy=LoadStrategy.APPEND, tbl_order=5, keep_file_id_col=False
    ),
}
