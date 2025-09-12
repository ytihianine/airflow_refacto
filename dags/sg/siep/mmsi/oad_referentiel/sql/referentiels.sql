DROP TABLE IF EXISTS temporaire.tmp_ref_typologie;
DROP TABLE IF EXISTS siep.ref_typologie;
CREATE TABLE IF NOT EXISTS siep.ref_typologie (
    id BIGSERIAL PRIMARY KEY,
    bati_non_bati TEXT,
    famille_de_bien_simplifiee TEXT,
    famille_de_bien TEXT,
    type_de_bien TEXT,
    usage_detaille_du_bien TEXT UNIQUE NOT NULL
);
