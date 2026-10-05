package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

type Filter struct {
	Drive          string
	Kinds          []kinds.Kind
	Lanes          []string
	To             string
	For            string
	Topics         []string
	After          int64
	Before         int64
	Since          time.Time
	Until          time.Time
	IncludeExpired bool
	Descending     bool
	Limit          int
}

func (f Filter) where(now time.Time) (string, []any) {
	clauses := []string{"drive = ?"}
	params := []any{f.Drive}
	var selectors []string
	var selected []any
	if len(f.Kinds) > 0 {
		selectors = append(selectors, "kind IN ("+placeholders(len(f.Kinds))+")")
		for _, k := range f.Kinds {
			selected = append(selected, string(k))
		}
	}
	if len(f.Lanes) > 0 {
		selectors = append(selectors, "lane IN ("+placeholders(len(f.Lanes))+")")
		for _, l := range f.Lanes {
			selected = append(selected, l)
		}
	}
	if len(f.Topics) > 0 {
		selectors = append(selectors, "topic IN ("+placeholders(len(f.Topics))+")")
		for _, t := range f.Topics {
			selected = append(selected, t)
		}
	}
	if f.For != "" {
		broadcast := append([]string{"recipients = '[]'", "lane != ?"}, selectors...)
		clauses = append(clauses, "(EXISTS (SELECT 1 FROM json_each(recipients) WHERE value = ?) OR ("+strings.Join(broadcast, " AND ")+"))")
		params = append(append(params, f.For, f.For), selected...)
	} else {
		clauses = append(clauses, selectors...)
		params = append(params, selected...)
	}
	if f.To != "" {
		clauses = append(clauses, "EXISTS (SELECT 1 FROM json_each(recipients) WHERE value = ?)")
		params = append(params, f.To)
	}
	if f.After > 0 {
		clauses = append(clauses, "seq > ?")
		params = append(params, f.After)
	}
	if f.Before > 0 {
		clauses = append(clauses, "seq < ?")
		params = append(params, f.Before)
	}
	if !f.Since.IsZero() {
		clauses = append(clauses, "at >= ?")
		params = append(params, f.Since.UnixMilli())
	}
	if !f.Until.IsZero() {
		clauses = append(clauses, "at < ?")
		params = append(params, f.Until.UnixMilli())
	}
	if !f.IncludeExpired {
		clauses = append(clauses, "(expires_at IS NULL OR expires_at > ?)")
		params = append(params, now.UnixMilli())
	}
	return strings.Join(clauses, " AND "), params
}

func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

func (s *Store) Query(ctx context.Context, f Filter) ([]Record, error) {
	if f.Drive == "" {
		return nil, ErrNoDrive
	}
	where, params := f.where(s.Now())
	order := "ASC"
	if f.Descending {
		order = "DESC"
	}
	q := "SELECT " + columns + " FROM records WHERE " + where + " ORDER BY seq " + order
	if f.Limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", f.Limit)
	}
	rows, err := s.db.QueryContext(ctx, q, params...)
	if err != nil {
		return nil, fmt.Errorf("query records: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Record
	for rows.Next() {
		r, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) Count(ctx context.Context, f Filter) (int, error) {
	where, params := f.where(s.Now())
	var n int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM records WHERE "+where, params...).Scan(&n); err != nil {
		return 0, fmt.Errorf("count records: %w", err)
	}
	return n, nil
}

func (s *Store) Cursor(ctx context.Context, name, drive string) (int64, bool, error) {
	var seq int64
	err := s.db.QueryRowContext(ctx, "SELECT seq FROM cursors WHERE name = ? AND drive = ?", name, drive).Scan(&seq)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("read cursor %s: %w", name, err)
	}
	return seq, true, nil
}

func (s *Store) SetCursor(ctx context.Context, name, drive string, seq int64) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO cursors (name, drive, seq) VALUES (?, ?, ?)
ON CONFLICT (name, drive) DO UPDATE SET seq = MAX(seq, excluded.seq)`, name, drive, seq)
	if err != nil {
		return fmt.Errorf("write cursor %s: %w", name, err)
	}
	return nil
}

type Binding struct {
	Drive string
	Root  bool
}

func (s *Store) Bind(ctx context.Context, session, drive string, root bool) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO sessions (session, drive, root) VALUES (?, ?, ?)
ON CONFLICT (session) DO UPDATE SET drive = excluded.drive, root = excluded.root`, session, drive, root)
	if err != nil {
		return fmt.Errorf("bind session: %w", err)
	}
	return nil
}

func (s *Store) Binding(ctx context.Context, session string) (Binding, bool, error) {
	var b Binding
	err := s.db.QueryRowContext(ctx, "SELECT drive, root FROM sessions WHERE session = ?", session).Scan(&b.Drive, &b.Root)
	if errors.Is(err, sql.ErrNoRows) {
		return Binding{}, false, nil
	}
	if err != nil {
		return Binding{}, false, fmt.Errorf("read session binding: %w", err)
	}
	return b, true, nil
}

type Source struct {
	Path   string
	Drive  string
	Lane   string
	Offset int64
}

func (s *Store) Sources(ctx context.Context) ([]Source, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT path, drive, lane, offset FROM imports ORDER BY path")
	if err != nil {
		return nil, fmt.Errorf("list import sources: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Source
	for rows.Next() {
		var src Source
		if err := rows.Scan(&src.Path, &src.Drive, &src.Lane, &src.Offset); err != nil {
			return nil, fmt.Errorf("scan import source: %w", err)
		}
		out = append(out, src)
	}
	return out, rows.Err()
}

func (s *Store) Source(ctx context.Context, path string) (Source, bool, error) {
	src := Source{Path: path}
	err := s.db.QueryRowContext(ctx, "SELECT drive, lane, offset FROM imports WHERE path = ?", path).Scan(&src.Drive, &src.Lane, &src.Offset)
	if errors.Is(err, sql.ErrNoRows) {
		return Source{}, false, nil
	}
	if err != nil {
		return Source{}, false, fmt.Errorf("read import source: %w", err)
	}
	return src, true, nil
}

func (s *Store) SaveSource(ctx context.Context, src Source) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO imports (path, drive, lane, offset) VALUES (?, ?, ?, ?)
ON CONFLICT (path) DO UPDATE SET drive = excluded.drive, lane = excluded.lane, offset = excluded.offset`, src.Path, src.Drive, src.Lane, src.Offset)
	if err != nil {
		return fmt.Errorf("save import source: %w", err)
	}
	return nil
}

func (s *Store) CountBy(ctx context.Context, f Filter, column string) (map[string]int, error) {
	if column != "kind" && column != "lane" {
		panic("CountBy column must be kind or lane, got " + column)
	}
	where, params := f.where(s.Now())
	rows, err := s.db.QueryContext(ctx, "SELECT "+column+", COUNT(*) FROM records WHERE "+where+" GROUP BY "+column, params...)
	if err != nil {
		return nil, fmt.Errorf("count by %s: %w", column, err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]int{}
	for rows.Next() {
		var (
			key string
			n   int
		)
		if err := rows.Scan(&key, &n); err != nil {
			return nil, fmt.Errorf("scan count: %w", err)
		}
		out[key] = n
	}
	return out, rows.Err()
}

func (s *Store) LatestPerLane(ctx context.Context, f Filter, n int) ([]Record, error) {
	where, params := f.where(s.Now())
	q := "SELECT " + columns + " FROM records WHERE seq IN (SELECT seq FROM (SELECT seq, MAX(at * 1000000 + seq) FROM records WHERE " + where + " AND kind != 'digest' GROUP BY lane)) ORDER BY at DESC, seq DESC LIMIT ?"
	rows, err := s.db.QueryContext(ctx, q, append(params, n)...)
	if err != nil {
		return nil, fmt.Errorf("latest per lane: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Record
	for rows.Next() {
		r, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) Replies(ctx context.Context, drive string, seqs []int64) (map[int64][]Record, error) {
	out := map[int64][]Record{}
	if len(seqs) == 0 {
		return out, nil
	}
	params := []any{drive}
	for _, seq := range seqs {
		params = append(params, seq)
	}
	rows, err := s.db.QueryContext(ctx, "SELECT "+columns+" FROM records WHERE drive = ? AND re IN ("+placeholders(len(seqs))+") ORDER BY seq", params...)
	if err != nil {
		return nil, fmt.Errorf("read replies: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		r, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out[r.Re] = append(out[r.Re], r)
	}
	return out, rows.Err()
}
