from modules.enums.database import LoadStrategy
from modules.types.projet import SelecteurStorageOptions

nom_projet_georisque = "Géorisques"
dag_id_georisque = "georisques_batiments"

storage_options = {
    "bien_db": SelecteurStorageOptions(
        write_to_db=False,
    ),
    "georisques": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND),
}
