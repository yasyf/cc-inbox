package store

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

const (
	// ReAliasAttr holds the alias a record's --re was given as.
	ReAliasAttr = "re-alias"

	refCardChars   = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-_"
	refCandidates  = 5
	refExcerpt     = 60
	refPrefixUpper = "\U0010FFFF"
)

// ResolvesAliasAttr holds the alias the i-th record of Closes() was given as.
func ResolvesAliasAttr(i int) string {
	return fmt.Sprintf("resolves-alias-%d", i)
}

// ErrNoRefIndex is what an alias lookup returns on a store whose alias index
// cci maintain-indexes has not installed yet.
var ErrNoRefIndex = errors.New("this store has no alias index yet; cci maintain-indexes installs it")

func refKeyRows(where string) string {
	return `WITH RECURSIVE src(drive, seq, topic, line, refs) AS MATERIALIZED (
SELECT drive, seq, topic, text, CASE WHEN json_error_position(refs) = 0 THEN refs ELSE '{}' END FROM records WHERE ` + where + `
), cards(drive, seq, rest) AS (
SELECT drive, seq, substr(line, instr(line, 'card-') + 5) FROM src WHERE instr(line, 'card-') > 0
UNION ALL SELECT drive, seq, substr(rest, instr(rest, 'card-') + 5) FROM cards WHERE instr(rest, 'card-') > 0
)
SELECT drive, 'topic', topic, seq FROM src WHERE topic != ''
UNION ALL SELECT drive, 'ccn', json_extract(refs, '$.ccn'), seq FROM src WHERE json_type(refs, '$.ccn') = 'text'
UNION ALL SELECT drive, 'ccn-text', line, seq FROM src WHERE json_type(refs, '$.ccn') = 'text'
UNION ALL SELECT s.drive, 'pr', CASE j.type WHEN 'integer' THEN CAST(j.value AS TEXT) ELSE substr(j.value, instr(j.value, '#') + 1) END, s.seq FROM src s, json_each(s.refs, '$.prs') j
UNION ALL SELECT drive, 'card', key, seq FROM (SELECT drive, seq, rtrim(substr(rest, 1, length(rest) - length(ltrim(rest, '` + refCardChars + `'))), '-') AS key FROM cards) WHERE key != ''`
}

var refKeyObjects = [...]physicalObject{
	{
		"table", "record_ref_keys",
		"CREATE TABLE record_ref_keys (drive TEXT NOT NULL, axis TEXT NOT NULL, key TEXT NOT NULL, source_seq INTEGER NOT NULL, PRIMARY KEY (drive, axis, key, source_seq)) WITHOUT ROWID",
		"INSERT INTO record_ref_keys (drive, axis, key, source_seq) SELECT * FROM (" + refKeyRows("1") + ") WHERE true ON CONFLICT (drive, axis, key, source_seq) DO NOTHING",
	},
	{"index", "record_ref_keys_source", "CREATE INDEX record_ref_keys_source ON record_ref_keys(source_seq)", ""},
	{"trigger", "record_ref_keys_insert", "CREATE TRIGGER record_ref_keys_insert AFTER INSERT ON records\nBEGIN\nINSERT INTO record_ref_keys (drive, axis, key, source_seq) SELECT * FROM (" + refKeyRows("seq = NEW.seq") + ") WHERE true ON CONFLICT (drive, axis, key, source_seq) DO NOTHING;\nEND", ""},
	{"trigger", "record_ref_keys_update", "CREATE TRIGGER record_ref_keys_update AFTER UPDATE OF seq, drive, text, topic, refs ON records\nWHEN OLD.seq IS NOT NEW.seq OR OLD.drive IS NOT NEW.drive OR OLD.text IS NOT NEW.text OR OLD.topic IS NOT NEW.topic OR OLD.refs IS NOT NEW.refs\nBEGIN\nDELETE FROM record_ref_keys WHERE source_seq = OLD.seq;\nINSERT INTO record_ref_keys (drive, axis, key, source_seq) SELECT * FROM (" + refKeyRows("seq = NEW.seq") + ") WHERE true ON CONFLICT (drive, axis, key, source_seq) DO NOTHING;\nEND", ""},
	{"trigger", "record_ref_keys_delete", "CREATE TRIGGER record_ref_keys_delete AFTER DELETE ON records\nBEGIN\nDELETE FROM record_ref_keys WHERE source_seq = OLD.seq;\nEND", ""},
}

// NamedRef is an alias: a reference to records by what they carry, never by
// number. t:<topic>, ccn:<id or text prefix>, card:<id>, pr#<n>, and
// last:<kind>[:<lane>].
type NamedRef struct {
	text   string
	axes   []string
	key    string
	prefix bool
	kind   kinds.Kind
	lane   string
}

// IsNamedRef reports whether ref is an alias rather than a record number.
func IsNamedRef(ref string) bool {
	for _, p := range []string{"t:", "ccn:", "card:", "pr#", "last:"} {
		if strings.HasPrefix(ref, p) {
			return true
		}
	}
	return false
}

// ParseNamedRef reads an alias.
func ParseNamedRef(ref string) (NamedRef, error) {
	n := NamedRef{text: ref}
	scheme, value, _ := strings.Cut(ref, ":")
	if strings.HasPrefix(ref, "pr#") {
		scheme, value = "pr", strings.TrimPrefix(ref, "pr#")
	}
	if value == "" {
		return NamedRef{}, fmt.Errorf("%q names nothing after %s", ref, scheme)
	}
	switch scheme {
	case "t":
		n.axes, n.key = []string{"topic"}, value
	case "card":
		n.axes, n.key = []string{"card"}, value
	case "ccn":
		n.axes, n.key, n.prefix = []string{"ccn", "ccn-text"}, value, true
	case "pr":
		pr, err := ParsePR(value)
		if err != nil || pr.Repo != "" {
			return NamedRef{}, fmt.Errorf("%q: want pr#<number>", ref)
		}
		n.axes, n.key = []string{"pr"}, value
	case "last":
		kind, lane, _ := strings.Cut(value, ":")
		k, err := kinds.Parse(kind)
		if err != nil {
			return NamedRef{}, fmt.Errorf("%q: %w", ref, err)
		}
		n.kind, n.lane = k, lane
	default:
		return NamedRef{}, fmt.Errorf("%q is not an alias: want t:<topic>, ccn:<id or text prefix>, card:<id>, pr#<n>, or last:<kind>[:<lane>]", ref)
	}
	return n, nil
}

func (n NamedRef) String() string { return n.text }

func (n NamedRef) where(drive string, openers bool) (string, []any) {
	if n.kind != "" {
		if n.lane == "" {
			return "drive = ? AND kind = ?", []any{drive, string(n.kind)}
		}
		return "drive = ? AND kind = ? AND lane = ?", []any{drive, string(n.kind), n.lane}
	}
	args := []any{drive}
	for _, axis := range n.axes {
		args = append(args, axis)
	}
	match := "key = ?"
	args = append(args, n.key)
	if n.prefix {
		match = "key >= ? AND key < ?"
		args = append(args, n.key+refPrefixUpper)
	}
	where := "seq IN (SELECT source_seq FROM record_ref_keys WHERE drive = ? AND axis IN (" + placeholders(len(n.axes)) + ") AND " + match + ")"
	if openers {
		ks := kinds.Openers()
		where += " AND kind IN (" + placeholders(len(ks)) + ")"
		for _, k := range ks {
			args = append(args, string(k))
		}
	}
	return where, args
}

func (s *Store) refIndex(ctx context.Context) error {
	var installed bool
	if err := s.db.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'record_ref_keys')").Scan(&installed); err != nil {
		return fmt.Errorf("look for the alias index: %w", err)
	}
	if !installed {
		return ErrNoRefIndex
	}
	return nil
}

func (s *Store) named(ctx context.Context, n NamedRef, drive string, openers bool, limit int) ([]Record, error) {
	where, args := n.where(drive, openers)
	query := "SELECT " + columns + " FROM records WHERE " + where + " ORDER BY seq DESC"
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("find %s: %w", n, err)
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
		return nil, fmt.Errorf("find %s: %w", n, err)
	}
	return out, s.numbered(ctx, out)
}

// FindNamed returns the newest limit records on drive that n names.
func (s *Store) FindNamed(ctx context.Context, n NamedRef, drive string, limit int) ([]Record, error) {
	if n.kind == "" {
		if err := s.refIndex(ctx); err != nil {
			return nil, err
		}
	}
	return s.named(ctx, n, drive, false, limit)
}

// ResolveNamed picks the one record on drive that n names. last: names the
// newest record of its kind. Any other alias names its one open record when
// exactly one is open; else, unless resolving, its one record when it names
// exactly one. Anything else refuses, naming the candidates.
func (s *Store) ResolveNamed(ctx context.Context, n NamedRef, drive string, resolving bool) (int64, error) {
	if n.kind != "" {
		found, err := s.named(ctx, n, drive, false, 1)
		if err != nil {
			return 0, err
		}
		if len(found) == 0 {
			return 0, fmt.Errorf("%s names no record on drive %s", n, drive)
		}
		return found[0].Seq, nil
	}
	newest, err := s.FindNamed(ctx, n, drive, refCandidates)
	if err != nil {
		return 0, err
	}
	if len(newest) == 0 {
		return 0, fmt.Errorf("%s names no record on drive %s", n, drive)
	}
	if len(newest) == 1 && !resolving {
		return newest[0].Seq, nil
	}
	openers, err := s.named(ctx, n, drive, true, 0)
	if err != nil {
		return 0, err
	}
	open, err := s.unclosedAmong(ctx, drive, openers)
	if err != nil {
		return 0, err
	}
	if len(open) == 1 {
		return open[0].Seq, nil
	}
	if len(open) == 0 && resolving {
		return 0, fmt.Errorf("%s names no open record on drive %s; newest: %s", n, drive, candidates(newest))
	}
	if len(open) == 0 {
		return 0, fmt.Errorf("%s names several records on drive %s and none is open; pass one: %s", n, drive, candidates(newest))
	}
	if err := s.numbered(ctx, open); err != nil {
		return 0, err
	}
	return 0, fmt.Errorf("%s names %d open records on drive %s; pass one: %s", n, len(open), drive, candidates(open[:min(len(open), refCandidates)]))
}

func candidates(records []Record) string {
	lines := make([]string, len(records))
	for i, r := range records {
		text := []rune(strings.Join(strings.Fields(r.Text), " "))
		if len(text) > refExcerpt {
			text = append(text[:refExcerpt-1], '…')
		}
		lines[i] = fmt.Sprintf("%s %s %s %q", r.Name(), r.Kind, r.Lane, string(text))
	}
	return strings.Join(lines, "; ")
}

func withAlias(name, alias string) string {
	if alias == "" {
		return name
	}
	return name + "(" + alias + ")"
}
