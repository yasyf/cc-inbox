package store

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

func rootAudienceRows(where string) string {
	return `SELECT r.seq, r.drive, r.kind,
CASE WHEN EXISTS (SELECT 1 FROM json_each(CASE WHEN json_error_position(r.recipients) = 0 THEN r.recipients ELSE '[]' END) WHERE value IN ('root', 'main'))
OR EXISTS (SELECT 1 FROM records own WHERE own.seq = r.re AND own.lane IN ('root', 'main')) THEN 1 ELSE 0 END
+ CASE WHEN (r.recipients = '[]' OR EXISTS (SELECT 1 FROM json_each(CASE WHEN json_error_position(r.recipients) = 0 THEN r.recipients ELSE '[]' END) WHERE value = 'all'))
AND r.lane NOT IN ('root', 'main') AND r.source NOT IN ('hook:root', 'hook:main') THEN 2 ELSE 0 END
FROM records r WHERE ` + where
}

func refreshRootAudience(where string) string {
	return "DELETE FROM record_root_audience WHERE source_seq IN (SELECT r.seq FROM records r WHERE " + where + ");\n" +
		"INSERT INTO record_root_audience (source_seq, drive, kind, audience) " + rootAudienceRows(where) + ";\n"
}

var rootAudienceObjects = [...]physicalObject{
	{
		"table", "record_root_audience",
		"CREATE TABLE record_root_audience (source_seq INTEGER PRIMARY KEY, drive TEXT NOT NULL, kind TEXT NOT NULL, audience INTEGER NOT NULL)",
		"INSERT INTO record_root_audience (source_seq, drive, kind, audience) " + rootAudienceRows("1"),
	},
	{"index", "record_root_audience_parent", "CREATE INDEX record_root_audience_parent ON records(re)", ""},
	{"index", "record_root_audience_stream", "CREATE INDEX record_root_audience_stream ON record_root_audience(drive, audience, source_seq)", ""},
	{"index", "record_root_audience_kind_stream", "CREATE INDEX record_root_audience_kind_stream ON record_root_audience(drive, audience, kind, source_seq)", ""},
	{
		"trigger", "record_root_audience_insert",
		"CREATE TRIGGER record_root_audience_insert AFTER INSERT ON records\nBEGIN\n" +
			refreshRootAudience("r.seq = NEW.seq") + refreshRootAudience("r.re = NEW.seq AND r.seq != NEW.seq") + "END", "",
	},
	{
		"trigger", "record_root_audience_update",
		"CREATE TRIGGER record_root_audience_update AFTER UPDATE OF seq, drive, kind, lane, source, recipients, re ON records\n" +
			"WHEN OLD.seq IS NOT NEW.seq OR OLD.drive IS NOT NEW.drive OR OLD.kind IS NOT NEW.kind OR OLD.lane IS NOT NEW.lane OR OLD.source IS NOT NEW.source OR OLD.recipients IS NOT NEW.recipients OR OLD.re IS NOT NEW.re\nBEGIN\n" +
			"DELETE FROM record_root_audience WHERE source_seq = OLD.seq;\n" + refreshRootAudience("r.seq = NEW.seq") +
			refreshRootAudience("r.re IN (OLD.seq, NEW.seq) AND r.seq != NEW.seq AND (OLD.seq IS NOT NEW.seq OR OLD.lane IS NOT NEW.lane)") + "END", "",
	},
	{
		"trigger", "record_root_audience_delete",
		"CREATE TRIGGER record_root_audience_delete AFTER DELETE ON records\nBEGIN\nDELETE FROM record_root_audience WHERE source_seq = OLD.seq;\n" +
			refreshRootAudience("r.re = OLD.seq") + "END", "",
	},
}

func (f Filter) rootAudienceScope() bool {
	return RootName(f.For) && f.Drive != "" && f.Since.IsZero() && f.Until.IsZero() &&
		f.To == "" && f.Addressed == "" && f.Origin == "" && f.Stack == "" && f.Target == "" &&
		f.PeerAfter == 0 && f.PR.Number == 0 && len(f.Lanes) == 0 && len(f.Topics) == 0 &&
		len(f.FirstOnly) == 0 && len(f.Follow) == 0 && len(f.TopicKinds) == 0 && f.LaneFloor == "" &&
		(f.RepeatWindow == 0 || f.RepeatWindow == UrgentRepeat)
}

func (s *Store) rootAudienceConnectionAt(ctx context.Context, f Filter, now time.Time) (*sql.Conn, error) {
	if !f.rootAudienceScope() {
		return nil, nil
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("open root audience reader: %w", err)
	}
	keep := false
	defer func() {
		if !keep {
			_ = conn.Close()
		}
	}()
	terms, err := sqlite.Limit(conn, sqlite3.SQLITE_LIMIT_COMPOUND_SELECT, -1)
	if err != nil {
		return nil, fmt.Errorf("inspect root audience stream limit: %w", err)
	}
	variables, err := sqlite.Limit(conn, sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, -1)
	if err != nil {
		return nil, fmt.Errorf("inspect root audience variable limit: %w", err)
	}
	arms, candidates := f.candidateArmsAudience(s.Self(), true)
	_, params := f.where(s.Self(), now)
	if len(arms) > terms || len(candidates)+len(params) > variables {
		return nil, nil
	}
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin root audience capability read: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	installed, err := projectionInstalled(ctx, tx, rootAudienceObjects[:], "root audience")
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("finish root audience capability read: %w", err)
	}
	if !installed {
		return nil, nil
	}
	keep = true
	return conn, nil
}

func (f Filter) rootAudienceArms(bounds func(string, int64) string, params *[]any) []string {
	var arms []string
	for _, stream := range []struct {
		classes []int
		kinds   []kinds.Kind
	}{{[]int{1, 3}, f.ToKinds}, {[]int{2, 3}, f.Kinds}} {
		ks := slices.Compact(slices.Sorted(slices.Values(stream.kinds)))
		for _, class := range stream.classes {
			appendArm := func(kind *kinds.Kind) {
				*params = append(*params, f.Drive, class)
				selector := ""
				if kind != nil {
					selector = " AND kind = ?"
					*params = append(*params, string(*kind))
				}
				arms = append(arms, "SELECT source_seq AS candidate_seq FROM record_root_audience WHERE drive = ? AND audience = ?"+selector+bounds("source_seq", f.After))
			}
			if len(ks) == 0 {
				appendArm(nil)
			} else {
				for _, kind := range ks {
					appendArm(&kind)
				}
			}
		}
	}
	return arms
}

func (s *Store) rootAudienceConnection(ctx context.Context, f Filter) (*sql.Conn, error) {
	if !f.rootAudienceScope() {
		return nil, nil
	}
	return s.rootAudienceConnectionAt(ctx, f, s.Now())
}
