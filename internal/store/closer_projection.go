package store

import (
	"strings"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

func closerKeyRows(where string) string {
	closers := kinds.Closers()
	names := make([]string, 0, len(closers))
	for _, k := range closers {
		names = append(names, "'"+string(k)+"'")
	}
	return `WITH closers AS MATERIALIZED (
SELECT drive, seq, re, topic, refs FROM records WHERE ` + where + ` AND kind IN (` + strings.Join(names, ",") + `)
)
SELECT drive, 're', CAST(re AS TEXT), seq FROM closers
UNION ALL SELECT drive, 'topic', hex(topic), seq FROM closers WHERE topic != ''
UNION ALL SELECT c.drive, 'ref', j.atom, c.seq FROM closers c,
json_tree(CASE WHEN json_error_position(c.refs) = 0 THEN c.refs ELSE '{}' END) j WHERE j.type = 'text'
UNION ALL SELECT drive, 'ref-ambiguous', '', seq FROM closers WHERE json_error_position(refs) != 0 OR instr(lower(refs), '\ud') > 0 OR refs GLOB '*�*'`
}

var closerMemberships = [...]physicalObject{
	{
		"table", "record_closer_keys",
		"CREATE TABLE record_closer_keys (drive TEXT NOT NULL, axis TEXT NOT NULL, key TEXT NOT NULL, source_seq INTEGER NOT NULL, PRIMARY KEY (source_seq, axis, key)) WITHOUT ROWID",
		"INSERT INTO record_closer_keys (drive, axis, key, source_seq) SELECT * FROM (" + closerKeyRows("1") + ") WHERE true ON CONFLICT (source_seq, axis, key) DO NOTHING",
	},
	{"index", "record_closer_keys_lookup", "CREATE INDEX record_closer_keys_lookup ON record_closer_keys(drive, axis, key, source_seq)", ""},
	{"trigger", "record_closer_keys_insert", "CREATE TRIGGER record_closer_keys_insert AFTER INSERT ON records\nBEGIN\nINSERT INTO record_closer_keys (drive, axis, key, source_seq) SELECT * FROM (" + closerKeyRows("seq = NEW.seq") + ") WHERE true ON CONFLICT (source_seq, axis, key) DO NOTHING;\nEND", ""},
	{"trigger", "record_closer_keys_update", "CREATE TRIGGER record_closer_keys_update AFTER UPDATE OF seq, drive, kind, re, topic, refs ON records\nWHEN OLD.seq IS NOT NEW.seq OR OLD.drive IS NOT NEW.drive OR OLD.kind IS NOT NEW.kind OR OLD.re IS NOT NEW.re OR OLD.topic IS NOT NEW.topic OR OLD.refs IS NOT NEW.refs\nBEGIN\nDELETE FROM record_closer_keys WHERE source_seq = OLD.seq;\nINSERT INTO record_closer_keys (drive, axis, key, source_seq) SELECT * FROM (" + closerKeyRows("seq = NEW.seq") + ") WHERE true ON CONFLICT (source_seq, axis, key) DO NOTHING;\nEND", ""},
	{"trigger", "record_closer_keys_delete", "CREATE TRIGGER record_closer_keys_delete AFTER DELETE ON records\nBEGIN\nDELETE FROM record_closer_keys WHERE source_seq = OLD.seq;\nEND", ""},
}
