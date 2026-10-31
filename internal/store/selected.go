package store

import (
	"context"
	"encoding/json"
	"fmt"
)

const selectedRecordsSQL = `SELECT ` + columns + ` FROM
(SELECT DISTINCT value AS selected_seq FROM json_each(?) ORDER BY selected_seq LIMIT -1) selected
CROSS JOIN records ON records.seq=selected.selected_seq
WHERE records.drive=? ORDER BY selected.selected_seq`

// RecordsAmong returns present records from drive in internal sequence order,
// omitting duplicate and absent sequences while retaining expired records.
func (s *Store) RecordsAmong(ctx context.Context, drive string, seqs []int64) ([]Record, error) {
	if drive == "" {
		return nil, ErrNoDrive
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(seqs) == 0 {
		return []Record{}, nil
	}
	ids, _ := json.Marshal(seqs)
	rows, err := s.db.QueryContext(ctx, selectedRecordsSQL, string(ids), drive)
	if err != nil {
		return nil, fmt.Errorf("read selected records: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Record
	for rows.Next() {
		r, err := s.scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, s.numbered(ctx, out)
}
