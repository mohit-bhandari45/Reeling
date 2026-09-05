CREATE TABLE IF NOT EXISTS jobs (
    id             TEXT PRIMARY KEY,
    input_key      TEXT NOT NULL,
    output_keys    TEXT[],
    thumbnail_key  TEXT,
    status         TEXT NOT NULL,
    error          TEXT,
    attempts       INT NOT NULL DEFAULT 0,
    webhook_url    TEXT,
    renditions     TEXT[],
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);