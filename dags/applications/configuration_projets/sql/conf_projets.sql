-- Create
CREATE SCHEMA IF NOT EXISTS conf_projets;

DROP TABLE IF EXISTS conf_projets.ref_direction CASCADE;
DROP TABLE IF EXISTS conf_projets.ref_direction;
CREATE TABLE IF NOT EXISTS conf_projets.ref_direction (
  id bigserial PRIMARY KEY,
  direction TEXT
);

DROP TABLE IF EXISTS conf_projets.ref_service CASCADE;
DROP TABLE IF EXISTS conf_projets.ref_service;
CREATE TABLE IF NOT EXISTS conf_projets.ref_service (
  id bigserial PRIMARY KEY,
  id_direction INT NOT NULL REFERENCES conf_projets.ref_direction(id),
  service TEXT NOT NULL
);

DROP TABLE IF EXISTS conf_projets.projet CASCADE;
CREATE TABLE IF NOT EXISTS conf_projets.projet(
  id bigserial PRIMARY KEY,
  nom_projet text,
  id_direction INT NOT NULL REFERENCES conf_projets.ref_direction(id),
  id_service INT NOT NULL REFERENCES conf_projets.ref_service(id)
);

DROP TABLE conf_projets.projet_snapshot;
CREATE TABLE conf_projets.projet_snapshot (
  id BIGSERIAL,
  id_projet INTEGER,
  snapshot_id TEXT,
  creation_timestamp TIMESTAMP,
  PRIMARY KEY (id_projet, snapshot_id)
  -- FOREIGN KEY(id_projet) REFERENCES conf_projets.projet(id)
);

DROP TABLE IF EXISTS conf_projets.selecteur CASCADE;
CREATE TABLE IF NOT EXISTS conf_projets.selecteur(
  id bigserial PRIMARY KEY,
  id_projet int REFERENCES conf_projets.projet(id),
  type_selecteur text,
  selecteur text
);

DROP TABLE IF EXISTS conf_projets.source CASCADE;
CREATE TABLE IF NOT EXISTS conf_projets.source(
  id bigserial PRIMARY KEY,
  id_projet int REFERENCES conf_projets.projet(id),
  type_source text,
  id_selecteur int REFERENCES conf_projets.selecteur(id),
  nom_source text
);

DROP TABLE IF EXISTS conf_projets.correspondance_colonne CASCADE;
CREATE TABLE IF NOT EXISTS conf_projets.correspondance_colonne (
  id bigserial PRIMARY KEY,
  id_projet int REFERENCES conf_projets.projet(id),
  id_selecteur int REFERENCES conf_projets.selecteur(id),
  colname_source text,
  colname_dest text,
  commentaire text,
  statut text,
  nouvelle_proposition text
);

DROP TABLE IF EXISTS conf_projets.colonnes_requises CASCADE;
CREATE TABLE IF NOT EXISTS conf_projets.colonnes_requises (
  id bigserial PRIMARY KEY,
  id_projet int REFERENCES conf_projets.projet(id),
  id_selecteur int REFERENCES conf_projets.selecteur(id),
  id_correspondance_colonne int REFERENCES conf_projets.correspondance_colonne(id)
);

DROP TABLE IF EXISTS conf_projets.storage_path CASCADE;
CREATE TABLE IF NOT EXISTS conf_projets.storage_path (
  id bigserial PRIMARY KEY,
  id_projet int NOT NULL REFERENCES conf_projets.projet(id),
  id_selecteur int REFERENCES conf_projets.selecteur(id),
  filename text,
  local_tmp_dir text,
  s3_bucket text,
  s3_key text,
  s3_tmp_key text,
  tbl_name text,
  tbl_order int
);


DROP TABLE IF EXISTS conf_projets.user;
CREATE TABLE IF NOT EXISTS conf_projets.user (
  id bigserial PRIMARY KEY,
  direction text,
  service text,
  bureau text,
  username text,
  password hash,
  acces_depose_fichier boolean
);

-- View to simplify function usage !
CREATE OR REPLACE VIEW conf_projets.vue_conf_projets
AS SELECT cpp.nom_projet,
  cpselec.selecteur,
  cpsource.type_source,
  cpsource.nom_source,
  cpstorage.filename,
  cpstorage.local_tmp_dir,
  cpstorage.s3_bucket,
  cpstorage.s3_key,
  cpstorage.s3_tmp_key,
  cpstorage.tbl_name,
  cpstorage.tbl_order,
  concat(cpstorage.local_tmp_dir, '/', cpstorage.filename) AS filepath_local,
  concat(cpstorage.local_tmp_dir, '/', cpstorage.filename) AS filepath_tmp_local,
  concat(cpstorage.s3_tmp_key, '/', cpstorage.filename) AS filepath_tmp_s3,
  concat(cpstorage.s3_key, '/', cpstorage.filename) AS filepath_s3,
      CASE
          WHEN cpsource.nom_source IS NULL THEN NULL::text
          ELSE concat(cpstorage.s3_key, '/', cpsource.nom_source)
      END AS filepath_source_s3
  FROM conf_projets.projet cpp
    LEFT JOIN conf_projets.selecteur cpselec ON cpselec.id_projet = cpp.id
    LEFT JOIN conf_projets.source cpsource ON cpsource.id_projet = cpp.id AND cpsource.id_selecteur = cpselec.id
    LEFT JOIN conf_projets.storage_path cpstorage ON cpstorage.id_projet = cpp.id AND cpstorage.id_selecteur = cpselec.id;

CREATE VIEW conf_projets.vue_cols_mapping AS
  SELECT cpp.nom_projet, cpselec.selecteur,
      cpcc.colname_source, cpcc.colname_dest
  FROM conf_projets.projet cpp
  LEFT JOIN conf_projets.selecteur cpselec ON cpselec.id_projet = cpp.id
  LEFT JOIN conf_projets.correspondance_colonne cpcc ON cpcc.id_projet = cpp.id
      AND cpcc.id_selecteur = cpselec.id
  WHERE cpselec.type_selecteur = 'Source'
    AND cpcc.colname_dest IS NOT NULL
    AND cpcc.colname_dest <> 'Non-conservée'
;

CREATE VIEW conf_projets.vue_cols_requises AS
  SELECT cpp.id as id_projet, cpp.nom_projet, cpselec.id as id_selecteur, cpselec.selecteur,
      cpcr.id_correspondance_colonne, cpcc.colname_dest
  FROM conf_projets.colonnes_requises cpcr
  INNER JOIN conf_projets.projet cpp ON cpp.id = cpcr.id_projet
  INNER JOIN conf_projets.selecteur cpselec ON cpselec.id = cpcr.id_selecteur
  INNER JOIN conf_projets.correspondance_colonne cpcc ON cpcc.id = cpcr.id_correspondance_colonne
;
