package store

import (
	"context"
	"fmt"
)

func commitPRRows(where string) string {
	return "SELECT r.drive AS drive, json_extract(r.fields, '$.commit') AS commit_sha, CAST(j.value AS TEXT) AS pr, r.seq AS source_seq FROM records r, json_each(CASE WHEN json_valid(r.refs) THEN r.refs ELSE '{}' END, '$.prs') j WHERE " + where +
		" AND r.kind = 'github' AND r.topic = 'pull_request' AND json_valid(r.fields) AND COALESCE(json_extract(r.fields, '$.commit'), '') != ''"
}

var commitPRObjects = [...]physicalObject{
	{
		"table", "record_commit_prs",
		"CREATE TABLE record_commit_prs (drive TEXT NOT NULL, commit_sha TEXT NOT NULL, pr TEXT NOT NULL, source_seq INTEGER NOT NULL, PRIMARY KEY (drive, commit_sha, pr, source_seq)) WITHOUT ROWID",
		"INSERT INTO record_commit_prs (drive, commit_sha, pr, source_seq) SELECT * FROM (" + commitPRRows("1") + ") WHERE true ON CONFLICT (drive, commit_sha, pr, source_seq) DO NOTHING",
	},
	{"index", "record_commit_prs_source", "CREATE INDEX record_commit_prs_source ON record_commit_prs(source_seq)", ""},
	{"trigger", "record_commit_prs_insert", "CREATE TRIGGER record_commit_prs_insert AFTER INSERT ON records\nWHEN NEW.kind = 'github' AND NEW.topic = 'pull_request'\nBEGIN\nINSERT INTO record_commit_prs (drive, commit_sha, pr, source_seq) SELECT * FROM (" + commitPRRows("r.seq = NEW.seq") + ") WHERE true ON CONFLICT (drive, commit_sha, pr, source_seq) DO NOTHING;\nEND", ""},
	{"trigger", "record_commit_prs_update", "CREATE TRIGGER record_commit_prs_update AFTER UPDATE OF seq, drive, kind, topic, refs, fields ON records\nBEGIN\nDELETE FROM record_commit_prs WHERE source_seq = OLD.seq;\nINSERT INTO record_commit_prs (drive, commit_sha, pr, source_seq) SELECT * FROM (" + commitPRRows("r.seq = NEW.seq") + ") WHERE true ON CONFLICT (drive, commit_sha, pr, source_seq) DO NOTHING;\nEND", ""},
	{"trigger", "record_commit_prs_delete", "CREATE TRIGGER record_commit_prs_delete AFTER DELETE ON records\nBEGIN\nDELETE FROM record_commit_prs WHERE source_seq = OLD.seq;\nEND", ""},
}

func (s *Store) commitPRIndex(ctx context.Context) (bool, error) {
	var installed bool
	if err := s.db.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'record_commit_prs')").Scan(&installed); err != nil {
		return false, fmt.Errorf("look for the commit index: %w", err)
	}
	return installed, nil
}
