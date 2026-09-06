package job

import (
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/lib/pq"
)

func NewPostgresConn(connString string) (*sql.DB, error) {
	db, err := sql.Open("pgx", connString)
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping db: %w", err)
	}

	return db, nil
}

type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

func (s *PostgresStore) Save(j *Job) error {
	query := `
		INSERT INTO jobs (id, input_key, output_keys, thumbnail_key, status, error, attempts, webhook_url, renditions, duration, source_width, source_height, source_codec, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, now(), now())
		ON CONFLICT (id) DO UPDATE SET
			input_key = EXCLUDED.input_key,
			output_keys = EXCLUDED.output_keys,
			thumbnail_key = EXCLUDED.thumbnail_key,
			status = EXCLUDED.status,
			error = EXCLUDED.error,
			attempts = EXCLUDED.attempts,
			webhook_url = EXCLUDED.webhook_url,
			renditions = EXCLUDED.renditions,
			duration = EXCLUDED.duration,
			source_width = EXCLUDED.source_width,
			source_height = EXCLUDED.source_height,
			source_codec = EXCLUDED.source_codec,
			updated_at = now()
	`
	_, err := s.db.Exec(query,
		j.ID, j.InputKey, pq.Array(j.OutputKeys), j.ThumbnailKey, j.Status, j.Error, j.Attempts,
		j.WebhookURL, pq.Array(j.Renditions), j.Duration, j.SourceWidth, j.SourceHeight, j.SourceCodec,
	)
	if err != nil {
		return fmt.Errorf("failed to save job: %w", err)
	}
	return nil
}

func (s *PostgresStore) Get(id string) (*Job, error) {
	query := `
		SELECT id, input_key, output_keys, thumbnail_key, status, error, attempts, webhook_url, renditions, duration, source_width, source_height, source_codec, created_at, updated_at
		FROM jobs
		WHERE id = $1
	`

	var j Job
	var outputKeys []string
	var renditions []string
	var errText sql.NullString
	var webhookURL sql.NullString
	var thumbnailKey sql.NullString
	var duration sql.NullFloat64
	var sourceWidth sql.NullInt64
	var sourceHeight sql.NullInt64
	var sourceCodec sql.NullString

	err := s.db.QueryRow(query, id).Scan(
		&j.ID,
		&j.InputKey,
		pq.Array(&outputKeys),
		&thumbnailKey,
		&j.Status,
		&errText,
		&j.Attempts,
		&webhookURL,
		pq.Array(&renditions),
		&duration,
		&sourceWidth,
		&sourceHeight,
		&sourceCodec,
		&j.CreatedAt,
		&j.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("job %s not found", id)
		}
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	j.OutputKeys = outputKeys
	j.ThumbnailKey = thumbnailKey.String
	j.Error = errText.String
	j.WebhookURL = webhookURL.String
	j.Renditions = renditions
	j.Duration = duration.Float64
	j.SourceWidth = int(sourceWidth.Int64)
	j.SourceHeight = int(sourceHeight.Int64)
	j.SourceCodec = sourceCodec.String

	return &j, nil
}
