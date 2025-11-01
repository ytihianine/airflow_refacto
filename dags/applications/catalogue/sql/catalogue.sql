DROP SCHEMA documentation CASCADE;
CREATE SCHEMA documentation;

CREATE TABLE documentation.dataset (
    id SERIAL,
    is_visible BOOLEAN,
    is_public BOOLEAN,
    title TEXT,
    description TEXT,
    keyword TEXT[],
    id_structure INTEGER REFERENCES documentation.ref_structures(id),
    id_service INTEGER REFERENCES documentation.ref_service(id),
    id_system_information INTEGER REFERENCES documentation.ref_systeme_info(id),
    id_contact_point INTEGER REFERENCES documentation.ref_contact(id),
    issued TIMESTAMP,
    modified TIMESTAMP,
    id_frequency INTEGER REFERENCES documentation.ref_frequence(id),
    id_spatiale INTEGER REFERENCES documentation.ref_couverture_geographique(id),
    landing_page TEXT,
    id_format INTEGER REFERENCES documentation.ref_source_format(id),
    id_licence INTEGER REFERENCES documentation.ref_licence(id),
    id_theme INTEGER REFERENCES documentation.ref_theme(id),
    donnees_ouvertes BOOLEAN,
    url_open_data TEXT,
    table_schema TEXT NOT NULL,
    table_name TEXT NOT NULL
);

CREATE TABLE documentation.dataset_dictionnaire(
    id SERIAL,
    id_dataset INTEGER REFERENCES documentation.dataset(id),
    variable TEXT,
    unite TEXT,
    commentaire TEXT,
    data_type INTEGER REFERENCES documentation.ref_theme(id)
);


CREATE TABLE documentation.dataset (
    id SERIAL UNIQUE,

    -- Basic identification
    title TEXT NOT NULL,
    description TEXT,

    -- Lifecycle metadata
    issued TIMESTAMP,       -- Date the dataset was first published
    modified TIMESTAMP,     -- Last modification date

    -- Publisher information
    publisher_name TEXT,
    publisher_uri TEXT,

    -- Access and distribution
    access_url TEXT,       -- General access URL
    download_url TEXT,     -- Direct download link

    -- Classification
    theme TEXT[],          -- Categories/tags (array of URIs or text)
    keyword TEXT[],        -- Free-form keywords
    language TEXT[],       -- Language codes (e.g., 'en', 'fr')

    -- Legal and licensing
    license TEXT,          -- URI of the license
    rights TEXT,           -- Human-readable rights statement

    -- Versioning and status
    version TEXT,
    status TEXT,           -- e.g., 'active', 'deprecated'

    -- Additional metadata
    spatial TEXT,          -- e.g., bounding box or URI
    temporal TEXT,         -- e.g., time period covered (can be refined)

    -- Links to identifiers or external references
    identifier TEXT UNIQUE,   -- Persistent identifier (e.g., DOI, UUID string)
    landing_page TEXT         -- Public-facing page for this dataset
);


CREATE TABLE documentation.dataset_dictionnaire (
    id SERIAL,
    id_dataset INTEGER REFERENCES documentation.dataset(id),
    variable TEXT,
    unite TEXT,
    commentaire TEXT,
    type INTEGER REFERENCES documentation.ref_type_donnees(id)
);
