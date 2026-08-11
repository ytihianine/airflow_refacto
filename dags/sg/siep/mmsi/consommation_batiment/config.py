from modules.types.projet import SelecteurStorageOptions

storage_options = {
    "conso_mens_source": SelecteurStorageOptions(
        write_to_db=False,
        write_to_s3=False,
    ),
    "conso_avant_2019": SelecteurStorageOptions(
        write_to_db=False,
        write_to_s3=False,
    ),
    "conso_statut_fluide_global": SelecteurStorageOptions(
        write_to_db=False,
        write_to_s3=False,
    ),
    "bien_info_complementaire": SelecteurStorageOptions(read_options={"sheet_name": 0}),
}
