package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
)

// ChangeVersion observes commits through one dedicated read-only connection.
type ChangeVersion struct {
	database   *sql.DB
	connection *sql.Conn
}

// OpenChangeVersion keeps commit observation independent of the Store's pool.
func (s *Store) OpenChangeVersion(ctx context.Context) (*ChangeVersion, error) {
	var filename string
	if err := s.db.QueryRowContext(ctx, "SELECT file FROM pragma_database_list WHERE name='main'").Scan(&filename); err != nil {
		return nil, fmt.Errorf("read change observer source: %w", err)
	}
	parameters := url.Values{"mode": {"ro"}, "_pragma": {fmt.Sprintf("busy_timeout(%d)", busyTimeout.Milliseconds())}}
	source := url.URL{Scheme: "file", Path: filename, RawQuery: parameters.Encode()}
	db, err := sql.Open("sqlite", source.String())
	if err != nil {
		return nil, fmt.Errorf("open change observer: %w", err)
	}
	db.SetMaxOpenConns(1)
	connection, err := db.Conn(ctx)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("connect change observer: %w", err), db.Close())
	}
	return &ChangeVersion{database: db, connection: connection}, nil
}

// Read returns a version comparable only with earlier reads from this observer.
func (v *ChangeVersion) Read(ctx context.Context) (int64, error) {
	var version int64
	if err := v.connection.QueryRowContext(ctx, "PRAGMA data_version").Scan(&version); err != nil {
		return 0, fmt.Errorf("read change version: %w", err)
	}
	return version, nil
}

// Close releases the observer's connection and its independent pool.
func (v *ChangeVersion) Close() error {
	return errors.Join(v.connection.Close(), v.database.Close())
}
