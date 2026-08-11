CREATE SCHEMA IF NOT EXISTS versioning;

CREATE TABLE IF NOT EXISTS versioning.snapshot (
    id_row BIGINT GENERATED ALWAYS AS IDENTITY,
    id_projet INTEGER NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
    is_dag_completed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id_row),
    UNIQUE (id_projet, import_timestamp),
    UNIQUE (snapshot_id),
    FOREIGN KEY (snapshot_id_parent) REFERENCES versioning.snapshot(snapshot_id)
);
