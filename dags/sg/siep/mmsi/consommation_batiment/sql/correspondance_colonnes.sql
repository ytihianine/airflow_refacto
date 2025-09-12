DROP TABLE IF EXISTS siep.correspondance_colonnes;
CREATE TABLE IF NOT EXISTS siep.correspondance_colonnes (
    id BIGSERIAL PRIMARY KEY,
    nom_table TEXT,
    nom_colonne_fichier_source TEXT,
    nom_colonne_table TEXT
);
