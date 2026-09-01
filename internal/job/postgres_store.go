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
		INSERT INTO jobs (id, input_key, output_keys, status, error, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, now(), now())
		ON CONFLICT (id) DO UPDATE SET
			input_key = EXCLUDED.input_key,
			output_keys = EXCLUDED.output_keys,
			status = EXCLUDED.status,
			error = EXCLUDED.error,
			updated_at = now()
	`
	_, err := s.db.Exec(query, j.ID, j.InputKey, pq.Array(j.OutputKeys), j.Status, j.Error)
	if err != nil {
		return fmt.Errorf("failed to save job: %w", err)
	}
	return nil
}

func (s *PostgresStore) Get(id string) (*Job, error) {
	query := `
		SELECT id, input_key, output_keys, status, error, created_at, updated_at
		FROM jobs
		WHERE id = $1
	`;
}
