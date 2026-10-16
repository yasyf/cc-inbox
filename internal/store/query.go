package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

type Filter struct {
	Drive          string
	Origin         string
	Peer           string
	Kinds          []kinds.Kind
	Lanes          []string
	To             string
	For            string
	Topics         []string
	Stack          string
	Target         string
	PR             PR
	After          int64
	PeerAfter      int64
	Before         int64
	Since          time.Time
	Until          time.Time
	IncludeExpired bool
	Descending     bool
	Limit          int
}

// where renders the filter. Records in the drive pass the selectors;
// <reader>@<Drive> records, and peer:<Peer> records past PeerAfter, pass
// whatever drive they sit in.
func (f Filter) where(self string, now time.Time) (string, []any) {
	scoped := []string{"drive = ?"}
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
		names := addressees(f.For)
		in := "(" + placeholders(len(names)) + ")"
		broadcast := append([]string{"recipients = '[]'", "lane NOT IN " + in, "source NOT IN " + in}, selectors...)
		scoped = append(scoped, "(EXISTS (SELECT 1 FROM json_each(recipients) WHERE value IN "+in+") OR ("+strings.Join(broadcast, " AND ")+"))")
		params = append(append(append(append(params, names...), names...), hookSources(names)...), selected...)
	} else {
		scoped = append(scoped, selectors...)
		params = append(params, selected...)
	}
	if f.To != "" {
		names := addressees(f.To)
		scoped = append(scoped, "EXISTS (SELECT 1 FROM json_each(recipients) WHERE value IN ("+placeholders(len(names))+"))")
		params = append(params, names...)
	}
	branches := []string{"(" + strings.Join(scoped, " AND ") + ")"}
	if peers := f.peerAddresses(self); len(peers) > 0 {
		branch := "EXISTS (SELECT 1 FROM json_each(recipients) WHERE value IN (" + placeholders(len(peers)) + "))"
		params = append(params, peers...)
		if f.PeerAfter > 0 {
			branch = "(" + branch + " AND seq > ?)"
			params = append(params, f.PeerAfter)
		}
		branches = append(branches, branch)
	}
	if readers := f.readerAddresses(); len(readers) > 0 {
		branches = append(branches, "EXISTS (SELECT 1 FROM json_each(recipients) WHERE value IN ("+placeholders(len(readers))+"))")
		params = append(params, readers...)
	}
	clauses := []string{"(" + strings.Join(branches, " OR ") + ")"}
	if f.Origin != "" {
		clauses = append(clauses, "origin = ?")
		params = append(params, f.Origin)
	}
	for _, ref := range []struct {
		path  string
		value any
		set   bool
	}{{"$.stacks", f.Stack, f.Stack != ""}, {"$.targets", f.Target, f.Target != ""}, {"$.prs", f.PR.value(), f.PR.Number != 0}} {
		if ref.set {
			clauses = append(clauses, "EXISTS (SELECT 1 FROM json_each(refs, '"+ref.path+"') WHERE value = ?)")
			params = append(params, ref.value)
		}
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

func (f Filter) peerAddresses(self string) []any {
	if f.Peer == "" {
		return nil
	}
	return []any{PeerAddress(f.Peer), PeerAddressOn(self, f.Peer)}
}

func (f Filter) readerAddresses() []any {
	var out []any
	reader := f.To
	if reader == "" {
		reader = f.For
	}
	if reader != "" && f.Drive != "" {
		for _, n := range addressees(reader) {
			out = append(out, n.(string)+"@"+f.Drive)
		}
	}
	return out
}

// PeerAddress is the recipient form that reaches one session thread in any drive.
func PeerAddress(peer string) string {
	return "peer:" + peer
}

// PeerAddressOn reaches one session on another node, in any drive.
func PeerAddressOn(node, session string) string {
	return "peer:" + node + "/" + session
}

// SplitPeer splits the target of a peer: address into its node and the rest.
// A first segment of exactly 16 lowercase hex characters is a node id; a
// session prefix that long always holds a dash, so the forms never collide.
func SplitPeer(target string) (node, rest string) {
	first, after, ok := strings.Cut(target, "/")
	if ok && len(first) == 16 && strings.Trim(first, "0123456789abcdef") == "" {
		return first, after
	}
	return "", target
}

// CrossDrive reports whether any recipient reaches outside the record's drive.
func CrossDrive(to []string) bool {
	for _, t := range to {
		if strings.HasPrefix(t, "peer:") || strings.Contains(t, "@") {
			return true
		}
	}
	return false
}

var rootNames = []any{"root", "main"}

// RootName reports whether lane addresses a drive's root, which answers to both root and main.
func RootName(lane string) bool {
	return slices.Contains(rootNames, any(lane))
}

func addressees(lane string) []any {
	if RootName(lane) {
		return rootNames
	}
	return []any{lane}
}

func HookSource(reader string) string {
	return "hook:" + reader
}

func hookSources(names []any) []any {
	out := make([]any, len(names))
	for i, n := range names {
		out[i] = HookSource(n.(string))
	}
	return out
}

func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

func (s *Store) Query(ctx context.Context, f Filter) ([]Record, error) {
	if f.Drive == "" {
		return nil, ErrNoDrive
	}
	where, params := f.where(s.Self(), s.Now())
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
		r, err := s.scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) Count(ctx context.Context, f Filter) (int, error) {
	where, params := f.where(s.Self(), s.Now())
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

// CursorPeak is the furthest seq cursor name reached in any drive: every
// peer:<session> record at or before it was already delivered to its reader.
func (s *Store) CursorPeak(ctx context.Context, name string) (int64, error) {
	var seq int64
	if err := s.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(seq), 0) FROM cursors WHERE name = ?", name).Scan(&seq); err != nil {
		return 0, fmt.Errorf("read cursor %s peak: %w", name, err)
	}
	return seq, nil
}

// MoveCursor sets a cursor to seq exactly, backwards included, so a reader
// that restarts replays from the point it last committed.
func (s *Store) MoveCursor(ctx context.Context, name, drive string, seq int64) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO cursors (name, drive, seq) VALUES (?, ?, ?)
ON CONFLICT (name, drive) DO UPDATE SET seq = excluded.seq`, name, drive, seq)
	if err != nil {
		return fmt.Errorf("move cursor %s: %w", name, err)
	}
	return nil
}

const inferredRoles = "('', 'root', 'lane', 'agent', 'session')"

type Binding struct {
	Drive string
	Root  bool
	Lane  string
}

func (s *Store) Bind(ctx context.Context, k PeerKey, drive string, root bool) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("bind session: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.claimSession(ctx, tx, k.Session); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO sessions (session, agent, origin, drive, root, role) VALUES (?, ?, ?, ?, ?, CASE WHEN ? THEN 'root' ELSE 'session' END)
ON CONFLICT (session, agent) DO UPDATE SET origin = excluded.origin, drive = excluded.drive, root = excluded.root, lane = '',
	role = CASE WHEN excluded.root THEN 'root' WHEN sessions.role IN `+inferredRoles+` THEN 'session' ELSE sessions.role END`, k.Session, k.Agent, s.Self(), drive, root, root); err != nil {
		return fmt.Errorf("bind session: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO cursors (name, drive, seq) SELECT ?, ?, COALESCE(MAX(seq), 0) FROM records WHERE drive = ?
ON CONFLICT (name, drive) DO UPDATE SET seq = excluded.seq`, k.String(), drive, drive); err != nil {
		return fmt.Errorf("start session cursor: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("bind session: %w", err)
	}
	return nil
}

func LaneCursor(k PeerKey) string {
	return "lane-" + k.String()
}

// BindLane names the lane k writes as in drive and starts its lane cursor. A
// thread already bound to another drive keeps that binding: only Bind moves it.
func (s *Store) BindLane(ctx context.Context, k PeerKey, drive, lane string, started time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("bind lane: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.claimSession(ctx, tx, k.Session); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO sessions (session, agent, origin, drive, root, lane, role) VALUES (?, ?, ?, ?, 0, ?, 'lane')
ON CONFLICT (session, agent) DO UPDATE SET origin = excluded.origin, drive = excluded.drive, lane = excluded.lane,
	role = CASE WHEN sessions.role IN `+inferredRoles+` THEN 'lane' ELSE sessions.role END WHERE NOT sessions.root AND sessions.drive IN ('', excluded.drive)`, k.Session, k.Agent, s.Self(), drive, lane); err != nil {
		return fmt.Errorf("bind lane: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO cursors (name, drive, seq) SELECT ?, ?, COALESCE(
	(SELECT MIN(seq) - 1 FROM records WHERE at >= ?),
	(SELECT MAX(seq) FROM records),
	0)
ON CONFLICT (name, drive) DO NOTHING`, LaneCursor(k), drive, started.UnixMilli()); err != nil {
		return fmt.Errorf("start lane cursor: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("bind lane: %w", err)
	}
	return nil
}

func (s *Store) Binding(ctx context.Context, k PeerKey) (Binding, bool, error) {
	var b Binding
	err := s.db.QueryRowContext(ctx, "SELECT drive, root, lane FROM sessions WHERE session = ? AND agent = ? AND drive != '' AND origin = ?", k.Session, k.Agent, s.Self()).Scan(&b.Drive, &b.Root, &b.Lane)
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
	Inode  uint64
	Parser int
}

func (s *Store) Sources(ctx context.Context) ([]Source, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT path, drive, lane, offset, inode, parser FROM imports ORDER BY path")
	if err != nil {
		return nil, fmt.Errorf("list import sources: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Source
	for rows.Next() {
		var src Source
		if err := rows.Scan(&src.Path, &src.Drive, &src.Lane, &src.Offset, &src.Inode, &src.Parser); err != nil {
			return nil, fmt.Errorf("scan import source: %w", err)
		}
		out = append(out, src)
	}
	return out, rows.Err()
}

func (s *Store) Source(ctx context.Context, path string) (Source, bool, error) {
	src := Source{Path: path}
	err := s.db.QueryRowContext(ctx, "SELECT drive, lane, offset, inode, parser FROM imports WHERE path = ?", path).Scan(&src.Drive, &src.Lane, &src.Offset, &src.Inode, &src.Parser)
	if errors.Is(err, sql.ErrNoRows) {
		return Source{}, false, nil
	}
	if err != nil {
		return Source{}, false, fmt.Errorf("read import source: %w", err)
	}
	return src, true, nil
}

func (s *Store) SaveSource(ctx context.Context, src Source) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO imports (path, drive, lane, offset, inode, parser) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT (path) DO UPDATE SET drive = excluded.drive, lane = excluded.lane, offset = excluded.offset, inode = excluded.inode, parser = excluded.parser`, src.Path, src.Drive, src.Lane, src.Offset, src.Inode, src.Parser)
	if err != nil {
		return fmt.Errorf("save import source: %w", err)
	}
	return nil
}

func (s *Store) CountBy(ctx context.Context, f Filter, column string) (map[string]int, error) {
	if column != "kind" && column != "lane" {
		panic("CountBy column must be kind or lane, got " + column)
	}
	where, params := f.where(s.Self(), s.Now())
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
	where, params := f.where(s.Self(), s.Now())
	q := "SELECT " + columns + " FROM records WHERE seq IN (SELECT seq FROM (SELECT seq, MAX(at * 1000000 + seq) FROM records WHERE " + where + " AND kind != 'digest' GROUP BY lane)) ORDER BY seq DESC LIMIT ?"
	rows, err := s.db.QueryContext(ctx, q, append(params, n)...)
	if err != nil {
		return nil, fmt.Errorf("latest per lane: %w", err)
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
	return out, rows.Err()
}

func (s *Store) Replies(ctx context.Context, seqs []int64) (map[int64][]Record, error) {
	out := map[int64][]Record{}
	if len(seqs) == 0 {
		return out, nil
	}
	params := make([]any, 0, 2*len(seqs))
	for _, seq := range seqs {
		params = append(params, seq)
	}
	params = append(params, params...)
	rows, err := s.db.QueryContext(ctx, "SELECT "+columns+" FROM records WHERE re IN ("+placeholders(len(seqs))+") OR resolves IN ("+placeholders(len(seqs))+") ORDER BY seq", params...)
	if err != nil {
		return nil, fmt.Errorf("read replies: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		r, err := s.scan(rows)
		if err != nil {
			return nil, err
		}
		if r.Re != 0 {
			out[r.Re] = append(out[r.Re], r)
		}
		if r.Resolves != 0 && r.Resolves != r.Re {
			out[r.Resolves] = append(out[r.Resolves], r)
		}
	}
	return out, rows.Err()
}

func (s *Store) Resolvers(ctx context.Context, drive string) (map[int64]int64, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT resolves, seq FROM records WHERE resolves != 0 AND (drive = ? OR resolves IN (SELECT seq FROM records WHERE drive = ?))", drive, drive)
	if err != nil {
		return nil, fmt.Errorf("read resolvers: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[int64]int64{}
	for rows.Next() {
		var target, seq int64
		if err := rows.Scan(&target, &seq); err != nil {
			return nil, fmt.Errorf("scan resolver: %w", err)
		}
		out[target] = seq
	}
	return out, rows.Err()
}
