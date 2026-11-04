package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"unicode/utf8"
)

const (
	trigramBatch   = 2000
	trigramNarrow  = 5000
	trigramMaxWord = 16
	trigramThrough = "record_trigrams_through"
	trigramUntil   = "record_trigrams_until"
	trigramColumns = "text || ' ' || lane || ' ' || topic || ' ' || kind || ' ' || recipients || ' ' || refs || ' ' || origin"
	trigramNew     = "NEW.text || ' ' || NEW.lane || ' ' || NEW.topic || ' ' || NEW.kind || ' ' || NEW.recipients || ' ' || NEW.refs || ' ' || NEW.origin"
)

var trigramObjects = []string{
	"CREATE VIRTUAL TABLE IF NOT EXISTS record_trigrams USING fts5(line, tokenize = 'trigram', content = '', contentless_delete = 1, detail = none)",
	"CREATE TRIGGER IF NOT EXISTS record_trigrams_insert AFTER INSERT ON records BEGIN INSERT INTO record_trigrams (rowid, line) VALUES (NEW.seq, " + trigramNew + "); END",
	"CREATE TRIGGER IF NOT EXISTS record_trigrams_delete AFTER DELETE ON records BEGIN DELETE FROM record_trigrams WHERE rowid = OLD.seq; END",
	"CREATE TRIGGER IF NOT EXISTS record_trigrams_update AFTER UPDATE OF text, lane, topic, kind, recipients, refs, origin ON records BEGIN INSERT OR REPLACE INTO record_trigrams (rowid, line) VALUES (NEW.seq, " + trigramNew + "); END",
	"INSERT INTO meta (key, value) VALUES ('" + trigramUntil + "', (SELECT COALESCE(MAX(seq), 0) FROM records)) ON CONFLICT (key) DO NOTHING",
	"INSERT INTO meta (key, value) VALUES ('" + trigramThrough + "', 0) ON CONFLICT (key) DO NOTHING",
}

// IndexTrigrams installs the index grep narrows a scan with and fills it from the
// records already stored, a batch at a time, with pause between batches. The
// triggers cover every record stored after it starts.
func (s *Store) IndexTrigrams(ctx context.Context, pause func(context.Context) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin trigram install: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, statement := range trigramObjects {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("install trigrams: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit trigram install: %w", err)
	}
	for {
		done, err := s.fillTrigrams(ctx)
		if err != nil || done {
			return err
		}
		if err := pause(ctx); err != nil {
			return err
		}
	}
}

func (s *Store) fillTrigrams(ctx context.Context) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin trigram fill: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var through, until int64
	if err := tx.QueryRowContext(ctx, "SELECT (SELECT value FROM meta WHERE key = '"+trigramThrough+"'), (SELECT value FROM meta WHERE key = '"+trigramUntil+"')").Scan(&through, &until); err != nil {
		return false, fmt.Errorf("read trigram progress: %w", err)
	}
	if through >= until {
		return true, nil
	}
	var last sql.NullInt64
	if err := tx.QueryRowContext(ctx, "SELECT MAX(seq) FROM (SELECT seq FROM records WHERE seq > ? AND seq <= ? ORDER BY seq LIMIT ?)", through, until, trigramBatch).Scan(&last); err != nil {
		return false, fmt.Errorf("read trigram batch: %w", err)
	}
	next := until
	if last.Valid {
		next = last.Int64
		if _, err := tx.ExecContext(ctx, "INSERT OR REPLACE INTO record_trigrams (rowid, line) SELECT seq, "+trigramColumns+" FROM records WHERE seq > ? AND seq <= ?", through, next); err != nil {
			return false, fmt.Errorf("fill trigrams: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, "UPDATE meta SET value = ? WHERE key = '"+trigramThrough+"'", next); err != nil {
		return false, fmt.Errorf("record trigram progress: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit trigram fill: %w", err)
	}
	return next >= until, nil
}

// TrigramsReady reports whether the trigram index covers every stored record.
func (s *Store) TrigramsReady(ctx context.Context) bool {
	var ready bool
	err := s.db.QueryRowContext(ctx, "SELECT (SELECT value FROM meta WHERE key = '"+trigramThrough+"') >= (SELECT value FROM meta WHERE key = '"+trigramUntil+"')").Scan(&ready)
	return err == nil && ready
}

// TrigramsNarrow is the MATCH expression for a record holding every word of all,
// or else one word of either, when the index picks few enough records that reading
// through it is cheaper than scanning the drive; otherwise it is empty. A common
// word re-reads every match on each page, which costs far more than the scan.
func (s *Store) TrigramsNarrow(ctx context.Context, either, all []string) string {
	match := trigramEvery(all)
	if match == "" {
		match = trigramEither(either)
	}
	if match == "" || !s.TrigramsReady(ctx) {
		return ""
	}
	var hits int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM (SELECT 1 FROM record_trigrams WHERE record_trigrams MATCH ? LIMIT ?)", match, trigramNarrow).Scan(&hits); err != nil || hits >= trigramNarrow {
		return ""
	}
	return match
}

func trigramIndexable(word string) bool {
	return utf8.RuneCountInString(word) >= 3 && strings.Trim(word, "0123456789") != ""
}

func trigramTerm(word string) string {
	runes := []rune(word)
	grams := make([]string, 0, min(len(runes)-2, trigramMaxWord))
	for i := 0; i+3 <= len(runes) && len(grams) < trigramMaxWord; i++ {
		grams = append(grams, `"`+strings.ReplaceAll(string(runes[i:i+3]), `"`, `""`)+`"`)
	}
	return strings.Join(grams, " AND ")
}

func trigramEvery(words []string) string {
	terms := make([]string, 0, len(words))
	for _, word := range words {
		if trigramIndexable(word) {
			terms = append(terms, trigramTerm(word))
		}
	}
	return strings.Join(terms, " AND ")
}

func trigramEither(words []string) string {
	terms := make([]string, 0, len(words))
	for _, word := range words {
		if !trigramIndexable(word) {
			return ""
		}
		terms = append(terms, "("+trigramTerm(word)+")")
	}
	return strings.Join(terms, " OR ")
}
