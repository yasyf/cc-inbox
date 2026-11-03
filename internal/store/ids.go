package store

import (
	"context"
	"encoding/json"
	"fmt"
)

// TextsByID returns the text of each record of drive whose id is among ids, keyed by id.
func (s *Store) TextsByID(ctx context.Context, drive string, ids []string) (map[string]string, error) {
	list, _ := json.Marshal(ids)
	rows, err := s.db.QueryContext(ctx, `SELECT id, text FROM records WHERE drive = ? AND id IN (SELECT value FROM json_each(?))`, drive, string(list))
	if err != nil {
		return nil, fmt.Errorf("read records by id: %w", err)
	}
	defer func() { _ = rows.Close() }()
	texts := map[string]string{}
	for rows.Next() {
		var id, text string
		if err := rows.Scan(&id, &text); err != nil {
			return nil, err
		}
		texts[id] = text
	}
	return texts, rows.Err()
}
