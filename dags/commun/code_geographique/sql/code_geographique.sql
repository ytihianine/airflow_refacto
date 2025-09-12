CREATE SCHEMA IF NOT EXISTS commun;

DROP TABLE IF EXISTS temporaire.tmp_region;
DROP TABLE IF EXISTS commun.region;
CREATE TABLE IF NOT EXISTS commun.region (
    id SERIAL PRIMARY KEY,
    reg TEXT,
    libelle TEXT,
    ncc TEXT,
    tncc INTEGER,
    nccenr TEXT,
    cheflieu TEXT
);

DROP TABLE IF EXISTS temporaire.tmp_departement;
DROP TABLE IF EXISTS commun.departement;
CREATE TABLE IF NOT EXISTS commun.departement (
    id SERIAL PRIMARY KEY,
    dep TEXT,
    reg TEXT,
    libelle TEXT,
    ncc TEXT,
    tncc INTEGER,
    nccenr TEXT,
    cheflieu TEXT
);


DROP TABLE IF EXISTS temporaire.tmp_commune;
DROP TABLE IF EXISTS commun.commune;
CREATE TABLE IF NOT EXISTS commun.commune (
    id SERIAL PRIMARY KEY,
    dep TEXT,
    reg TEXT,
    libelle TEXT,
    arr TEXT,
    can TEXT,
    com TEXT,
    ncc TEXT,
    ctcd TEXT,
    tncc INTEGER,
    nccenr TEXT,
    typecom TEXT,
    comparent INTEGER
);


/*
    Tables pour les Code ISO 3166-2
*/
DROP TABLE IF EXISTS temporaire.tmp_code_iso_region;
DROP TABLE IF EXISTS commun.code_iso_region;
CREATE TABLE IF NOT EXISTS commun.code_iso_region (
    id SERIAL PRIMARY KEY,
    libelle TEXT,
    code_iso_3166_2 TEXT,
    categorie_division TEXT
);

DROP TABLE IF EXISTS temporaire.tmp_code_iso_departement;
DROP TABLE IF EXISTS commun.code_iso_departement;
CREATE TABLE IF NOT EXISTS commun.code_iso_departement (
    id SERIAL PRIMARY KEY,
    libelle TEXT,
    code_iso_3166_2 TEXT,
    categorie_division TEXT
);

/*
    Tables avec les données GEOJSON
*/
DROP TABLE IF EXISTS temporaire.tmp_region_geojson;
DROP TABLE IF EXISTS commun.region_geojson;
CREATE TABLE commun.region_geojson (
    id SERIAL PRIMARY KEY,
    libelle TEXT,
    type_contour TEXT,
    coordonnees JSONB
);

DROP TABLE IF EXISTS temporaire.tmp_departement_geojson;
DROP TABLE IF EXISTS commun.departement_geojson;
CREATE TABLE commun.departement_geojson (
    id SERIAL PRIMARY KEY,
    libelle TEXT,
    type_contour TEXT,
    coordonnees JSONB
);


/*
    Vue avec toutes les infos
*/
CREATE MATERIALIZED VIEW commun.vw_commune_full_info AS
    SELECT cc.libelle as commune, cc.com as code_insee, cc.dep as departement_num, cc.reg as region_num,
        cdept.libelle as departement,  creg.libelle as region,
        -- Info Code ISO 3166-2
        ccidept.code_iso_3166_2 as departement_code_iso_3166_2, ccireg.code_iso_3166_2 as region_code_iso_3166_2,
        -- Info GEOJSON
        dpg.coordonnees::text as departement_geojson, crg.coordonnees::text as region_geojson
    FROM commun.commune cc
    LEFT JOIN commun.departement cdept ON cc.dep = cdept.dep
    LEFT JOIN commun.region creg ON cc.reg = creg.reg
    LEFT JOIN commun.code_iso_departement ccidept ON cdept.libelle = ccidept.libelle
    LEFT JOIN commun.code_iso_region ccireg ON creg.libelle = ccireg.libelle
    LEFT JOIN commun.region_geojson crg ON creg.libelle = crg.libelle
    LEFT JOIN commun.departement_geojson dpg ON cdept.libelle = dpg.libelle;
    ;
