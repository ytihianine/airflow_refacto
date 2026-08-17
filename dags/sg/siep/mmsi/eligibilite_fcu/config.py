from modules.enums.database import LoadStrategy
from modules.types.projet import SelecteurStorageOptions

dag_id_fcu = "eligibilite_fcu"
nom_projet_fcu = "France Chaleur Urbaine (FCU)"

storage_options = {
    "fcu": SelecteurStorageOptions(
        write_to_db=False,
    ),
    "fcu_result": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND),
}
