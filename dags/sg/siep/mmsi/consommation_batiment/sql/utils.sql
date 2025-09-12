DROP TABLE IF EXISTS siep.logs;

CREATE TABLE IF NOT EXISTS siep.logs (
    id_log BIGSERIAL PRIMARY KEY,
    date_envoi TIMESTAMP,
    jour DATE,
    heure TIME,
    type_requete TEXT,
    url TEXT,
    route TEXT,
    code_http INTEGER,
    methode_http TEXT,
    body JSON,
    reponse JSON,
    level TEXT,
    sql_query TEXT,
    data_query TEXT,
    message TEXT,
    message_erreur TEXT,
    id_utilisateur INTEGER
);

-- Dans le cas où insérer une nouvelle ligne ne fonctionne à cause de la PK
-- cause: la séquence de PK n'est pas synchronisée avec les valeurs de la colonne_id PK
-- Si la valeur de la 2ème commande est inférieure à celle de la 1ère commande, il faut lancer la 3ème commande
SELECT MAX(id_sous_indicateur) FROM sous_indicateur;
SELECT nextval(pg_get_serial_sequence('sous_indicateur', 'id_sous_indicateur'));
SELECT SETVAL((SELECT PG_GET_SERIAL_SEQUENCE('"sous_indicateur"', 'id_sous_indicateur')), (SELECT (MAX("id_sous_indicateur") + 1) FROM "sous_indicateur"), FALSE);
