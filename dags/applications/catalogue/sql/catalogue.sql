CREATE TABLE dataset (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Basic identification
    title TEXT NOT NULL,
    description TEXT,

    -- Lifecycle metadata
    issued TIMESTAMP WITH TIME ZONE,       -- Date the dataset was first published
    modified TIMESTAMP WITH TIME ZONE,     -- Last modification date

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
