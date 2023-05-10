package daggo

import (
	"errors"
	"github.com/jmoiron/sqlx"
)

// Daggo is a wrapper around sqlx.DB object
type Daggo struct {
	db *sqlx.DB
}

// NewDaggo creates a new Daggo object given a DSN
func NewDaggo(dsn string) (*Daggo, error) {
	if dsn == "" {
		return nil, errors.New("DSN cannot be empty")
	}

	db, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		return nil, err
	}

	return &Daggo{db: db}, nil
}

// Close closes the underlying database connection
func (d *Daggo) Close() error {
	return d.db.Close()
}
