CREATE TABLE IF NOT EXISTS sircom.budget_depense (
    id BIGSERIAL PRIMARY KEY,
    annee INTEGER,
    semestre TEXT,
    date DATE,
    type_depense TEXT,
    fonctionnement_ht2_payees DOUBLE PRECISION,
    masse_salariale_t2_payees DOUBLE PRECISION,
    unite TEXT,
    is_last_value BOOLEAN
);
