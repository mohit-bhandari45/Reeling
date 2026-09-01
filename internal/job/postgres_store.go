package job

import (
	"database/sql"
	"fmt"
)

func NewPostgresConn(connString string) (*sql.DB, error){
	db, err := sql.Open("pgx", connString);
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping db: %w", err)
	}

	return db, nil;
}