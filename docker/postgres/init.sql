CREATE TABLE IF NOT EXISTS jobs (
    id           TEXT PRIMARY KEY,
    input_key    TEXT NOT NULL,
    output_keys  TEXT[],
    status       TEXT NOT NULL,
    error        TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);