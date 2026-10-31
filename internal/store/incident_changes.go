package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

type incidentChange struct {
	Seq         int64
	Revision    int64
	Record      Record
	Values      []any
	DecodeError string
	Words       map[string]bool
	References  map[string]bool
	Unsupported bool
}

func (s *Store) captureIncidentChanges(ctx context.Context) ([]incidentChange, error) {
	connection, err := s.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = connection.Close() }()
	if _, err := connection.ExecContext(ctx, "BEGIN DEFERRED"); err != nil {
		return nil, err
	}
	defer func() { _, _ = connection.ExecContext(context.WithoutCancel(ctx), "ROLLBACK") }()
	rows, err := connection.QueryContext(ctx, "SELECT source_seq,revision FROM incident_changes ORDER BY source_seq")
	if err != nil {
		return nil, err
	}
	var changes []incidentChange
	for rows.Next() {
		var change incidentChange
		if err := rows.Scan(&change.Seq, &change.Revision); err != nil {
			_ = rows.Close()
			return nil, err
		}
		changes = append(changes, change)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	for i := range changes {
		change := &changes[i]
		row := connection.QueryRowContext(ctx, "SELECT "+columns+" FROM records WHERE seq=? AND kind='incident' AND EXISTS(SELECT 1 FROM incident_scopes WHERE drive=records.drive)", change.Seq)
		capture := incidentCapture{row: row}
		record, err := s.scan(&capture)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if len(capture.values) == 0 {
			return nil, err
		}
		change.Values, change.Record = capture.values, record
		if err != nil {
			change.DecodeError = err.Error()
			continue
		}
		change.Unsupported = record.Refs.Path != "" && !filepath.IsAbs(record.Refs.Path)
		change.Words, _, err = incidentWords(ctx, strings.NewReader(strings.ToValidUTF8(record.Text, "\uFFFD")))
		if err != nil {
			return nil, err
		}
		change.References = map[string]bool{}
		for _, pr := range record.Refs.PRs {
			change.References[strconv.Itoa(pr.Number)] = true
		}
	}
	if _, err := connection.ExecContext(ctx, "COMMIT"); err != nil {
		return nil, err
	}
	return changes, nil
}

func (s *Store) publishIncidentChanges(ctx context.Context, changes []incidentChange) (map[string]bool, error) {
	paths := map[string]bool{}
	if len(changes) == 0 {
		return paths, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	for _, change := range changes {
		var oldPath string
		err := tx.QueryRowContext(ctx, "SELECT body_path FROM incident_records WHERE seq=?", change.Seq).Scan(&oldPath)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		if oldPath != "" {
			paths[oldPath] = true
			if _, err := tx.ExecContext(ctx, "INSERT INTO incident_body_changes(path) VALUES(?) ON CONFLICT(path) DO NOTHING", oldPath); err != nil {
				return nil, err
			}
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM incident_record_prs WHERE source_seq=?", change.Seq); err != nil {
			return nil, err
		}
		if len(change.Values) == 0 {
			if _, err := tx.ExecContext(ctx, "DELETE FROM incident_records WHERE seq=?", change.Seq); err != nil {
				return nil, err
			}
		} else {
			path := change.Record.Refs.Path
			if change.Unsupported {
				path = ""
			}
			if err := writeIncidentRecord(ctx, tx, change.Values, change.Revision, change.DecodeError, path, change.Unsupported); err != nil {
				return nil, err
			}
			if path != "" {
				paths[path] = true
				if _, err := tx.ExecContext(ctx, "INSERT INTO incident_body_changes(path) VALUES(?) ON CONFLICT(path) DO NOTHING", path); err != nil {
					return nil, err
				}
			}
			for axis, words := range map[string]map[string]bool{"summary": change.Words, "refs": change.References} {
				for pr := range words {
					if _, err := tx.ExecContext(ctx, "INSERT INTO incident_record_prs(source_seq,axis,pr) VALUES(?,?,?)", change.Seq, axis, pr); err != nil {
						return nil, err
					}
				}
			}
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM incident_changes WHERE source_seq=? AND revision=?", change.Seq, change.Revision); err != nil {
			return nil, fmt.Errorf("acknowledge incident revision: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return paths, nil
}
