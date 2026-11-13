package store

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// Wake tells a follower that a record passing its filter exists at or after
// First, at or below Head. A zero Head is a heartbeat.
type Wake struct {
	First int64 `json:"first"`
	Head  int64 `json:"head"`
}

// Feed reads the records a filter passes as they commit, from a process that
// evaluates every reader's filter in one place, so a quiet reader does no
// work. Follow blocks, calling each per wake. It answers ErrUnrouted when no
// process can serve f, or when the one serving it went away.
type Feed interface {
	Follow(ctx context.Context, f Filter, each func(Wake) error) error
}

// FollowThrough sends every later follow to feed.
func (s *Store) FollowThrough(feed Feed) {
	s.feed = feed
}

// Feed is the store's Feed, nil when it has none.
func (s *Store) Feed() Feed {
	return s.feed
}

const recordMutationsKey = "record_mutations"

var recordMutationObjects = [...]physicalObject{
	{
		"trigger", "record_mutations_update",
		"CREATE TRIGGER record_mutations_update AFTER UPDATE OF drive, lane, kind, text, topic, recipients, re, resolves, refs, fields, expires_at, source, also_resolves ON records\nBEGIN\n" +
			"INSERT INTO meta (key, value) VALUES ('" + recordMutationsKey + "', 1) ON CONFLICT (key) DO UPDATE SET value = value + 1;\nEND",
		"",
	},
}

func countRecordsDeleted(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, "INSERT INTO meta (key, value) VALUES (?, 1) ON CONFLICT (key) DO UPDATE SET value = value + 1", recordMutationsKey); err != nil {
		return fmt.Errorf("count deleted records: %w", err)
	}
	return nil
}

// Tip is where the store's records stand: Head is the newest seq, and
// Mutations moves when a stored record changed under a reader. Exact is false
// on a store whose trigger counting mutations is not installed yet, so any
// commit may have changed one.
type Tip struct {
	Head      int64
	Mutations int64
	Exact     bool
}

// RecordTip reads the store's Tip from one snapshot.
func (s *Store) RecordTip(ctx context.Context) (Tip, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Tip{}, fmt.Errorf("begin record tip: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var tip Tip
	var head sql.NullInt64
	if err := tx.QueryRowContext(ctx, "SELECT MAX(seq) FROM records").Scan(&head); err != nil {
		return Tip{}, fmt.Errorf("read max seq: %w", err)
	}
	tip.Head = head.Int64
	if err := tx.QueryRowContext(ctx, "SELECT COALESCE((SELECT value FROM meta WHERE key = ?), 0)", recordMutationsKey).Scan(&tip.Mutations); err != nil {
		return Tip{}, fmt.Errorf("read record mutations: %w", err)
	}
	if tip.Exact, err = projectionInstalled(ctx, tx, recordMutationObjects[:], "record mutation"); err != nil {
		return Tip{}, err
	}
	return tip, nil
}

// FirstMatch is what one filter passes up to a head: the lowest seq, 0 when
// none, and the aliases its reader held when it was read. A filter whose read
// failed names the first seq it could reach, so its reader's own read fails
// the same way.
type FirstMatch struct {
	Seq     int64
	Aliases []string
}

// FirstMatches reads each filter's FirstMatch in (f.After, head] from one
// snapshot on one connection, so a page the first filter loads serves the rest.
func (s *Store) FirstMatches(ctx context.Context, filters []Filter, head int64) ([]FirstMatch, error) {
	known, settled, err := s.aliasChanges(ctx)
	if err != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	held := make([][]string, len(filters))
	for i, f := range filters {
		reader := cmp.Or(f.To, f.For, f.Addressed)
		if !settled || reader == "" || f.Drive == "" {
			continue
		}
		if held[i], err = s.Aliases(ctx, f.Drive, reader); err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			settled = false
		}
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("open follow reader: %w", err)
	}
	defer func() { _ = conn.Close() }()
	terms, err := sqlite.Limit(conn, sqlite3.SQLITE_LIMIT_COMPOUND_SELECT, -1)
	if err != nil {
		return nil, fmt.Errorf("inspect follow compound limit: %w", err)
	}
	variables, err := sqlite.Limit(conn, sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, -1)
	if err != nil {
		return nil, fmt.Errorf("inspect follow variable limit: %w", err)
	}
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin follow read: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	rootInstalled, err := projectionInstalled(ctx, tx, rootAudienceObjects[:], "root audience")
	if err != nil {
		return nil, err
	}
	var registry, refs bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'lane_holds'), EXISTS (SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'record_ref_keys')").Scan(&registry, &refs); err != nil {
		return nil, fmt.Errorf("look for the follow indexes: %w", err)
	}
	if settled {
		current, err := readAliasChanges(ctx, tx)
		settled = err == nil && current == known
	}
	now := s.Now()
	out := make([]FirstMatch, len(filters))
	for i, f := range filters {
		f.Before, f.Limit, f.Descending = head+1, 1, false
		f.prKeyed = refs && f.PR.Number != 0
		if registry && f.LaneAliases && len(f.Lanes) > 0 {
			if f.Lanes, err = lanesAnswering(ctx, tx, f.Drive, f.Lanes); err != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				out[i].Seq = f.After + 1
				continue
			}
		}
		reader := cmp.Or(f.To, f.For, f.Addressed)
		if registry && reader != "" && f.Drive != "" {
			if settled {
				f.Aliases = held[i]
			} else if f.Aliases, err = aliases(ctx, tx, f.Drive, reader); err != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				out[i].Seq = f.After + 1
				continue
			}
		}
		if f.EveryDrive && reader != "" && f.Drive != "" {
			if f.readerDrives, err = addressedDrives(ctx, tx, f.addressees(reader)); err != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				out[i].Seq = f.After + 1
				continue
			}
		}
		out[i].Aliases = f.Aliases
		if m := s.reads.model.Load(); m != nil {
			if seq, ok := m.first(ctx, s, f, now); ok {
				out[i].Seq = seq
				continue
			}
		}
		rootAudience := rootInstalled && f.rootAudienceScope()
		if rootAudience {
			arms, candidates := f.candidateArmsAudience(s.Self(), true)
			_, params := f.where(s.Self(), now)
			rootAudience = len(arms) <= terms && len(candidates)+len(params) <= variables
		}
		q, params := s.selectionColumnsAudienceAt(f, "records.seq", rootAudience, now)
		switch err := tx.QueryRowContext(ctx, q, params...).Scan(&out[i].Seq); {
		case err == nil, errors.Is(err, sql.ErrNoRows):
		case ctx.Err() != nil:
			return nil, ctx.Err()
		default:
			out[i].Seq = f.After + 1
		}
	}
	return out, nil
}
