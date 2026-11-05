package store

import (
	"context"
	"fmt"
)

// ParityDigest sums what a replay may have changed: every cursor and every record.
func (s *Store) ParityDigest(ctx context.Context) (string, string, error) {
	var cursors, records string
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) || ':' || COALESCE(SUM(seq % 1000000007), 0) FROM cursors").Scan(&cursors)
	if err != nil {
		return "", "", fmt.Errorf("sum the cursors: %w", err)
	}
	err = s.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(seq), 0) FROM records").Scan(&records)
	if err != nil {
		return "", "", fmt.Errorf("read the newest record: %w", err)
	}
	return cursors, records, nil
}

// Reader names a lane that recently posted on a drive.
type Reader struct {
	Drive string
	Lane  string
}

// RecentReaders returns the limit lanes that posted most recently.
func (s *Store) RecentReaders(ctx context.Context, limit int) ([]Reader, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT drive, lane FROM (SELECT drive, lane, MAX(seq) AS newest FROM (SELECT drive, lane, seq FROM records ORDER BY seq DESC LIMIT 20000) GROUP BY drive, lane) ORDER BY newest DESC LIMIT ?", limit)
	if err != nil {
		return nil, fmt.Errorf("read recent lanes: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Reader
	for rows.Next() {
		var r Reader
		if err := rows.Scan(&r.Drive, &r.Lane); err != nil {
			return nil, fmt.Errorf("read recent lanes: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
