-- Résultats batch (chargés depuis batch-ca/)
CREATE TABLE IF NOT EXISTS batch_ca_pays_mois (
    pays  TEXT NOT NULL,
    mois  TEXT NOT NULL, -- format YYYY-MM
    ca    NUMERIC NOT NULL,
    PRIMARY KEY (pays, mois)
);

-- Résultats streaming : CA cumulé en direct (une seule ligne, mise à jour en continu)
CREATE TABLE IF NOT EXISTS streaming_ca_cumule (
    id          SMALLINT PRIMARY KEY DEFAULT 1,
    ca_total    NUMERIC NOT NULL,
    nb_lignes   BIGINT NOT NULL,
    updated_at  TIMESTAMP NOT NULL,
    CONSTRAINT single_row CHECK (id = 1)
);

-- Résultats streaming : top produits par fenêtre glissante
CREATE TABLE IF NOT EXISTS streaming_top_produits (
    window_start   TIMESTAMP NOT NULL,
    window_end     TIMESTAMP NOT NULL,
    stock_code     TEXT NOT NULL,
    description    TEXT,
    quantite_totale NUMERIC NOT NULL,
    ca_fenetre     NUMERIC NOT NULL,
    PRIMARY KEY (window_start, stock_code)
);

-- Résultats streaming : CA cumulé par pays (pour filtrage dashboard)
CREATE TABLE IF NOT EXISTS streaming_ca_pays (
    pays        TEXT PRIMARY KEY,
    ca_total    NUMERIC NOT NULL,
    updated_at  TIMESTAMP NOT NULL
);
