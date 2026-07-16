from modules.enums.database import LoadStrategy
from modules.types.projet import (
    SelecteurStorageOptions,
)

storage_options = {
    "delai_global_paiement_sp_manuel": SelecteurStorageOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "demande_achat_sp_manuel": SelecteurStorageOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "demande_paiement_sp_manuel": SelecteurStorageOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "engagement_juridique_sp_manuel": SelecteurStorageOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "get_all_cf_cc": SelecteurStorageOptions(write_to_s3_with_iceberg=False, write_to_db=False),
    "get_delai_global_paiement": SelecteurStorageOptions(write_to_s3_with_iceberg=False, write_to_db=False),
    "get_demande_achat": SelecteurStorageOptions(write_to_s3_with_iceberg=False, write_to_db=False),
    "get_demande_paiement_complet": SelecteurStorageOptions(write_to_s3_with_iceberg=False, write_to_db=False),
    "get_engagement_juridique": SelecteurStorageOptions(write_to_s3_with_iceberg=False, write_to_db=False),
    "grist_doc": SelecteurStorageOptions(write_to_s3_with_iceberg=False, write_to_db=False),
    "load_delai_global_paiement": SelecteurStorageOptions(
        write_to_s3=False, write_to_s3_with_iceberg=False, write_to_db=False
    ),
    "load_demande_achat": SelecteurStorageOptions(write_to_s3=False, write_to_s3_with_iceberg=False, write_to_db=False),
    "load_demande_paiement_complet": SelecteurStorageOptions(
        write_to_s3=False, write_to_s3_with_iceberg=False, write_to_db=False
    ),
    "load_engagement_juridique": SelecteurStorageOptions(
        write_to_s3=False, write_to_s3_with_iceberg=False, write_to_db=False
    ),
    "load_new_cf_cc": SelecteurStorageOptions(write_to_s3=False, write_to_s3_with_iceberg=False, write_to_db=False),
    "ref_bop": SelecteurStorageOptions(),
    "ref_cc": SelecteurStorageOptions(),
    "ref_prog": SelecteurStorageOptions(),
    "ref_sdep": SelecteurStorageOptions(),
    "ref_sp_choisi": SelecteurStorageOptions(),
    "ref_sp_pilotage": SelecteurStorageOptions(),
    "ref_uo": SelecteurStorageOptions(),
    "sp": SelecteurStorageOptions(load_strategy=LoadStrategy.FULL_LOAD),
}
