-- Create
CREATE SCHEMA IF NOT EXISTS conf_projets;


DROP TABLE IF EXISTS conf_projets."ref_direction" CASCADE;
CREATE TABLE conf_projets."ref_direction" (
  "id_row" bigint GENERATED ALWAYS AS IDENTITY,
  "id_direction" int,
  "direction" text,
  "import_timestamp" TIMESTAMP NOT NULL,
  "snapshot_id" UUID NOT NULL,
  "snapshot_id_parent" UUID NULL,
  PRIMARY KEY ("id_row"),
  UNIQUE ("id_direction", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS conf_projets."ref_service" CASCADE;
CREATE TABLE conf_projets."ref_service" (
  "id_row" bigint GENERATED ALWAYS AS IDENTITY,
  "id_service" int,
  "id_direction" int,
  "service" text,
  "import_timestamp" TIMESTAMP NOT NULL,
  "snapshot_id" UUID NOT NULL,
  "snapshot_id_parent" UUID NULL,
  PRIMARY KEY ("id_row"),
  UNIQUE ("id_service", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);


DROP TABLE IF EXISTS conf_projets."projet" CASCADE;
CREATE TABLE conf_projets."projet" (
  "id_row" bigint GENERATED ALWAYS AS IDENTITY,
  "id_projet" int,
  "id_direction" int,
  "id_service" int,
  "projet" text,
  "import_timestamp" TIMESTAMP NOT NULL,
  "snapshot_id" UUID NOT NULL,
  "snapshot_id_parent" UUID NULL,
  PRIMARY KEY ("id_row"),
  UNIQUE ("id_projet", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);


DROP TABLE IF EXISTS conf_projets."projet_documentation" CASCADE;
CREATE TABLE conf_projets."projet_documentation" (
  "id_row" bigint GENERATED ALWAYS AS IDENTITY,
  "id_projet" int,
  "type_documentation" text,
  "lien" text,
  "import_timestamp" TIMESTAMP NOT NULL,
  "snapshot_id" UUID NOT NULL,
  "snapshot_id_parent" UUID NULL,
  PRIMARY KEY ("id_row"),
  UNIQUE ("id_projet", "type_documentation", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS conf_projets."projet_s3" CASCADE;
CREATE TABLE conf_projets."projet_s3" (
  "id_row" bigint GENERATED ALWAYS AS IDENTITY,
  "id_projet" int,
  "bucket" text,
  "key" text,
  "key_tmp" text,
  "import_timestamp" TIMESTAMP NOT NULL,
  "snapshot_id" UUID NOT NULL,
  "snapshot_id_parent" UUID NULL,
  PRIMARY KEY ("id_row"),
  UNIQUE ("id_projet", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);


DROP TABLE IF EXISTS conf_projets."projet_contact" CASCADE;
CREATE TABLE conf_projets."projet_contact" (
  "id_row" bigint GENERATED ALWAYS AS IDENTITY,
  "id_contact" int,
  "id_projet" int,
  "contact_mail" text,
  "is_mail_generic" bool,
  "import_timestamp" TIMESTAMP NOT NULL,
  "snapshot_id" UUID NOT NULL,
  "snapshot_id_parent" UUID NULL,
  PRIMARY KEY ("id_row"),
  UNIQUE ("id_projet", "id_contact", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS conf_projets."projet_selecteur" CASCADE;
CREATE TABLE conf_projets."projet_selecteur" (
  "id_row" bigint GENERATED ALWAYS AS IDENTITY,
  "id_selecteur" int,
  "id_projet" int,
  "type_selecteur" text,
  "selecteur" text,
  "import_timestamp" TIMESTAMP NOT NULL,
  "snapshot_id" UUID NOT NULL,
  "snapshot_id_parent" UUID NULL,
  PRIMARY KEY ("id_row"),
  UNIQUE ("id_projet", "id_selecteur", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS conf_projets."selecteur_source" CASCADE;
CREATE TABLE conf_projets."selecteur_source" (
  "id_row" bigint GENERATED ALWAYS AS IDENTITY,
  "id_projet" int,
  "id_selecteur" int,
  "type_source" text,
  "id_source" text,
  "import_timestamp" TIMESTAMP NOT NULL,
  "snapshot_id" UUID NOT NULL,
  "snapshot_id_parent" UUID NULL,
  PRIMARY KEY ("id_row"),
  UNIQUE ("id_projet", "id_selecteur", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);


DROP TABLE IF EXISTS conf_projets."selecteur_s3" CASCADE;
CREATE TABLE conf_projets."selecteur_s3" (
  "id_row" bigint GENERATED ALWAYS AS IDENTITY,
  "id_projet" int,
  "id_selecteur" int,
  "key" text,
  "filename" text,
  "import_timestamp" TIMESTAMP NOT NULL,
  "snapshot_id" UUID NOT NULL,
  "snapshot_id_parent" UUID NULL,
  PRIMARY KEY ("id_row"),
  UNIQUE ("id_projet", "id_selecteur", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);


DROP TABLE IF EXISTS conf_projets."selecteur_database" CASCADE;
CREATE TABLE conf_projets."selecteur_database" (
  "id_row" bigint GENERATED ALWAYS AS IDENTITY,
  "id_projet" int,
  "id_selecteur" int,
  "tbl_name" text,
  "import_timestamp" TIMESTAMP NOT NULL,
  "snapshot_id" UUID NOT NULL,
  "snapshot_id_parent" UUID NULL,
  PRIMARY KEY ("id_row"),
  UNIQUE ("id_projet", "id_selecteur", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);


DROP TABLE IF EXISTS conf_projets."selecteur_column_mapping" CASCADE;
CREATE TABLE conf_projets."selecteur_column_mapping" (
  "id_row" bigint GENERATED ALWAYS AS IDENTITY,
  "id_col_mapping" int,
  "id_projet" int,
  "id_selecteur" int,
  "colname_source" text,
  "colname_dest" text,
  "to_keep" bool,
  "date_archivage" date,
  "import_timestamp" TIMESTAMP NOT NULL,
  "snapshot_id" UUID NOT NULL,
  "snapshot_id_parent" UUID NULL,
  PRIMARY KEY ("id_row"),
  UNIQUE ("id_projet", "id_selecteur", "id_col_mapping", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);


-- Vue pour get_projet_s3_info()
DROP VIEW IF EXISTS conf_projets.projet_s3_vw;
CREATE OR REPLACE VIEW conf_projets.projet_s3_vw AS
SELECT
    cpp.projet,
    cpps.bucket,
    cpps.key,
    cpps.key_tmp,
    cpp.import_timestamp,
    DENSE_RANK() OVER (
      ORDER BY cpp.import_timestamp desc
  ) as rang
FROM conf_projets.projet cpp
INNER JOIN conf_projets.projet_s3 cpps ON cpp.id_projet = cpps.id_projet
  AND cpp.import_timestamp = cpps.import_timestamp;


-- Vue pour column_mapping_dataframe()
DROP VIEW IF EXISTS conf_projets.cols_mapping_vw;
CREATE OR REPLACE VIEW conf_projets.cols_mapping_vw AS
SELECT
    cpp.projet,
    cpps.selecteur,
    scm.colname_source,
    scm.colname_dest,
    cpp.import_timestamp,
    DENSE_RANK() OVER (
      ORDER BY cpp.import_timestamp desc
  ) as rang
FROM conf_projets.projet cpp
INNER JOIN conf_projets.projet_selecteur cpps ON cpp.id_projet = cpps.id_projet
  AND cpp.import_timestamp = cpps.import_timestamp
INNER JOIN conf_projets.selecteur_column_mapping scm
  ON cpps.id_selecteur = scm.id_selecteur
    AND cpps.id_projet = scm.id_projet
    AND cpp.import_timestamp = cpps.import_timestamp
WHERE scm.to_keep = true;


-- Vue pour get_list_documentation()
DROP VIEW IF EXISTS conf_projets.projet_documentation_vw;
CREATE OR REPLACE VIEW conf_projets.projet_documentation_vw AS
SELECT
    cpp.projet,
    cppd.type_documentation,
    cppd.lien,
  cpp.import_timestamp,
    DENSE_RANK() OVER (
      ORDER BY cppd.import_timestamp desc
  ) as rang
FROM conf_projets.projet cpp
INNER JOIN conf_projets.projet_documentation cppd ON cpp.id_projet = cppd.id_projet
  AND cpp.import_timestamp = cppd.import_timestamp;

-- Vue pour get_list_contact()
DROP VIEW IF EXISTS conf_projets.projet_contact_vw;
create or replace
view conf_projets.projet_contact_vw as
select
	cpp.projet,
	cppc.contact_mail,
	cppc.is_mail_generic,
	cpp.import_timestamp,
	dense_rank() over (
order by
	cpp.import_timestamp desc
  ) as rang
from
	conf_projets.projet cpp
inner join conf_projets.projet_contact cppc on
	cpp.id_projet = cppc.id_projet
	and cpp.import_timestamp = cppc.import_timestamp;


-- Vue pour _get_selecteur_storage_info()
DROP VIEW IF EXISTS conf_projets.selecteur_s3_db_vw;
CREATE OR REPLACE VIEW conf_projets.selecteur_s3_db_vw AS
SELECT
    cpp.projet,
    cpps.type_selecteur,
    cpps.selecteur,
    cpss.type_source,
    cpss.id_source,
    cpss3.filename,
    COALESCE(cpss3.key, cpps3.key) as s3_key,
    cpps3.bucket,
    cpps3.key as projet_s3_key,
    cpps3.key_tmp as projet_s3_key_tmp,
    CONCAT(COALESCE(cpss3.key, cpps3.key), '/', cpss3.filename) as filepath_s3,
    CONCAT(cpps3.key_tmp, '/', cpss3.filename) as filepath_tmp_s3,
    cpsd.tbl_name,
    cpp.import_timestamp,
    DENSE_RANK() OVER (
      ORDER BY cpp.import_timestamp DESC
  ) as rang
FROM conf_projets.projet cpp
INNER JOIN conf_projets.projet_selecteur cpps
  ON cpp.id_projet = cpps.id_projet
  AND cpp.import_timestamp = cpps.import_timestamp
LEFT JOIN conf_projets.selecteur_source cpss
  ON cpps.id_selecteur = cpss.id_selecteur
    AND cpps.id_projet = cpss.id_projet
  AND cpp.import_timestamp = cpss.import_timestamp
INNER JOIN conf_projets.projet_s3 cpps3
  ON cpp.id_projet = cpps3.id_projet
  AND cpp.import_timestamp = cpps3.import_timestamp
LEFT JOIN conf_projets.selecteur_s3 cpss3
  ON cpps.id_selecteur = cpss3.id_selecteur
    AND cpps.id_projet = cpss3.id_projet
  AND cpp.import_timestamp = cpss3.import_timestamp
LEFT JOIN conf_projets.selecteur_database cpsd
  ON cpps.id_selecteur = cpsd.id_selecteur
    AND cpps.id_projet = cpsd.id_projet
  AND cpp.import_timestamp = cpsd.import_timestamp
;

-- Vue pour _get_snapshot_id()
DROP VIEW IF EXISTS conf_projets.projet_snapshot_vw;
CREATE OR REPLACE VIEW conf_projets.projet_snapshot_vw AS
  WITH latest_projet AS (
    SELECT id_projet, projet, import_timestamp,
      DENSE_RANK() OVER (
        PARTITION BY id_projet
        ORDER BY import_timestamp ASC
    ) as rang
    FROM conf_projets.projet
  )
  SELECT
    cte_projet.id_projet,
    cte_projet.projet,
    ver_snap.import_timestamp,
    ver_snap.snapshot_id,
    DENSE_RANK() OVER (
        PARTITION BY cte_projet.id_projet
        ORDER BY ver_snap.import_timestamp DESC
    ) as rang,
    ver_snap.created_at
  FROM versioning.snapshot ver_snap
  INNER JOIN latest_projet cte_projet
    ON cte_projet.id_projet = ver_snap.id_projet
  WHERE cte_projet.rang = 1
  ORDER BY id_projet, import_timestamp;

-- [TO REFACTOR] vue_source pour l'interface de dépôt de fichier
drop view IF EXISTS conf_projets.vue_source;
create or replace
view conf_projets.vue_source
as
select
	cpp.snapshot_id,
	cpps.id_selecteur as "id",
	cpp.projet as "nom_projet",
	cpps.selecteur,
	cpps3.bucket as "s3_bucket",
	cpps3.key as "s3_key",
	cppsource.id_source as "nom_source",
	cpp.id_direction,
	cp_ref_dir.direction,
	cpp.id_service,
	cp_ref_service.service
from
	conf_projets.projet cpp
inner join conf_projets.projet_s3 cpps3
  on
	cpp.id_projet = cpps3.id_projet
	and cpp.snapshot_id = cpps3.snapshot_id
inner join conf_projets.projet_selecteur cpps
  on
	cpp.id_projet = cpps.id_projet
	and cpp.snapshot_id = cpps.snapshot_id
join conf_projets.selecteur_source cppsource
  on
	cpp.id_projet = cppsource.id_projet
	and cpps.id_selecteur = cppsource.id_selecteur
	and cpp.snapshot_id = cppsource.snapshot_id
	and cppsource.type_source = 'Fichier'
join conf_projets.ref_direction cp_ref_dir
  on
	cpp.id_direction = cp_ref_dir.id_direction
	and cpp.snapshot_id = cp_ref_dir.snapshot_id
join conf_projets.ref_service cp_ref_service
  on
	cpp.id_service = cp_ref_service.id_service
	and cpp.snapshot_id = cp_ref_service.snapshot_id
	-- Conserver uniquement la dernière configuration
where
	cpp.import_timestamp = (
	select
		MAX(import_timestamp)
	from
		conf_projets.projet
	limit 1
)
order by
	cpp.id_projet,
	cpp.import_timestamp desc
;
