package store

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
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
	Mute           []kinds.Kind
	ToKinds        []kinds.Kind
	FirstOnly      []kinds.Kind
	RepeatWindow   time.Duration
	Lanes          []string
	To             string
	For            string
	Addressed      string
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
		selected = append(selected, kindArgs(f.Kinds)...)
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
		broadcast := append([]string{"(recipients = '[]' OR EXISTS (SELECT 1 FROM json_each(recipients) WHERE value = 'all'))", "lane NOT IN " + in, "source NOT IN " + in}, selectors...)
		addressed, kindParams := f.addressedKinds(addressedTo(in))
		scoped = append(scoped, "("+addressed+" OR ("+strings.Join(broadcast, " AND ")+"))")
		params = append(append(append(append(append(append(params, names...), names...), kindParams...), names...), hookSources(names)...), selected...)
	} else {
		scoped = append(scoped, selectors...)
		params = append(params, selected...)
	}
	if f.Addressed != "" {
		names := addressees(f.Addressed)
		scoped = append(scoped, addressedTo("("+placeholders(len(names))+")"))
		params = append(append(params, names...), names...)
	}
	if f.To != "" {
		names := addressees(f.To)
		addressed, kindParams := f.addressedKinds("EXISTS (SELECT 1 FROM json_each(recipients) WHERE value IN (" + placeholders(len(names)) + "))")
		scoped = append(scoped, addressed)
		params = append(append(params, names...), kindParams...)
	}
	branches := []string{"(" + strings.Join(scoped, " AND ") + ")"}
	if peers := f.peerAddresses(self); len(peers) > 0 {
		branch := "EXISTS (SELECT 1 FROM json_each(recipients) WHERE value IN (" + placeholders(len(peers)) + "))"
		params = append(params, peers...)
		if f.PeerAfter > 0 {
			branch = "(" + branch + " AND seq > ?)"
			params = append(params, f.PeerAfter)
		}
		branch, kindParams := f.addressedKinds(branch)
		branches = append(branches, branch)
		params = append(params, kindParams...)
	}
	if readers := f.readerAddresses(); len(readers) > 0 {
		branch, kindParams := f.addressedKinds("EXISTS (SELECT 1 FROM json_each(recipients) WHERE value IN (" + placeholders(len(readers)) + "))")
		branches = append(branches, branch)
		params = append(append(params, readers...), kindParams...)
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
	}{{"$.stacks", f.Stack, f.Stack != ""}, {"$.targets", f.Target, f.Target != ""}} {
		if ref.set {
			clauses = append(clauses, "EXISTS (SELECT 1 FROM json_each(refs, '"+ref.path+"') WHERE value = ?)")
			params = append(params, ref.value)
		}
	}
	if f.PR.Number != 0 {
		text, param := f.PR.textMatch("value")
		clauses = append(clauses, "EXISTS (SELECT 1 FROM json_each(refs, '$.prs') WHERE (type = 'integer' AND value = ?) OR (type = 'text' AND "+text+"))")
		params = append(params, f.PR.Number, param)
	}
	if len(f.Mute) > 0 {
		clauses = append(clauses, "kind NOT IN ("+placeholders(len(f.Mute))+")")
		params = append(params, kindArgs(f.Mute)...)
	}
	if len(f.FirstOnly) > 0 && f.To != "" {
		names := addressees(f.To)
		clauses = append(clauses, "NOT (kind IN ("+placeholders(len(f.FirstOnly))+") AND json_extract(recipients, '$[0]') NOT IN ("+placeholders(len(names))+"))")
		params = append(append(params, kindArgs(f.FirstOnly)...), names...)
	}
	if f.RepeatWindow > 0 {
		clauses = append(clauses, "NOT (kind = ? AND topic != '' AND EXISTS (SELECT 1 FROM records prior WHERE prior.drive = records.drive AND prior.kind = records.kind AND prior.lane = records.lane AND prior.topic = records.topic AND prior.seq < records.seq AND prior.at > records.at - ?))")
		params = append(params, string(kinds.Urgent), f.RepeatWindow.Milliseconds())
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

func (f Filter) bounded(self string, now time.Time) (string, []any) {
	where, params := f.where(self, now)
	arms, candidates := f.candidateArms(self)
	if len(arms) == 0 {
		return where, params
	}
	candidate := "seq"
	if f.Origin != "" || f.PeerAfter > 0 {
		// Unary plus retains records_origin and peer-after OR scans, including their native error order.
		candidate = "+seq"
	}
	return where + " AND " + candidate + " IN (" + strings.Join(arms, " UNION ALL ") + ")", append(params, candidates...)
}

func (f Filter) candidateArms(self string) ([]string, []any) {
	peers, readers := f.peerAddresses(self), f.readerAddresses()
	if len(peers) == 0 && len(readers) == 0 {
		return nil, nil
	}
	params := []any{f.Drive}
	bounds := func(column string, after int64) string {
		var clauses string
		if after > 0 {
			clauses += " AND " + column + " > ?"
			params = append(params, after)
		}
		if f.Before > 0 {
			clauses += " AND " + column + " < ?"
			params = append(params, f.Before)
		}
		return clauses
	}
	arms := []string{"SELECT seq AS candidate_seq FROM records WHERE drive = ?" + bounds("seq", f.After)}
	for _, addresses := range []struct {
		values []any
		after  int64
	}{{peers, max(f.After, f.PeerAfter)}, {readers, f.After}} {
		for _, address := range addresses.values {
			params = append(params, address)
			arms = append(arms, "SELECT source_seq FROM record_recipients WHERE address = ?"+bounds("source_seq", addresses.after))
		}
	}
	arms = append(arms, "SELECT source_seq FROM record_json_errors WHERE field = 'recipients'"+bounds("source_seq", f.After))
	return arms, params
}

func (f Filter) addressedKinds(clause string) (string, []any) {
	if len(f.ToKinds) == 0 {
		return clause, nil
	}
	return "(" + clause + " AND kind IN (" + placeholders(len(f.ToKinds)) + "))", kindArgs(f.ToKinds)
}

func kindArgs(ks []kinds.Kind) []any {
	out := make([]any, len(ks))
	for i, k := range ks {
		out[i] = string(k)
	}
	return out
}

func addressedTo(in string) string {
	return "(EXISTS (SELECT 1 FROM json_each(recipients) WHERE value IN " + in + ") OR EXISTS (SELECT 1 FROM records own WHERE own.seq = records.re AND own.lane IN " + in + "))"
}

func (f Filter) peerAddresses(self string) []any {
	if f.Peer == "" {
		return nil
	}
	return []any{PeerAddress(f.Peer), PeerAddressOn(self, f.Peer)}
}

func (f Filter) readerAddresses() []any {
	var out []any
	reader := cmp.Or(f.To, f.For, f.Addressed)
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

func SameLane(a, b string) bool {
	return a == b || RootName(a) && RootName(b)
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

// Query reads the records f selects. A filter with no drive must name a Peer,
// and then reads only the records addressed to it, from every drive.
func (s *Store) Query(ctx context.Context, f Filter) ([]Record, error) {
	if f.Drive == "" && f.Peer == "" {
		return nil, ErrNoDrive
	}
	q, params := s.selection(f)
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, s.numbered(ctx, out)
}

func (s *Store) selection(f Filter) (string, []any) {
	order := "ASC"
	if f.Descending {
		order = "DESC"
	}
	var where string
	var params []any
	from, ordered := "records", "seq"
	if f.Origin != "" {
		where, params = f.bounded(s.Self(), s.Now())
	} else {
		where, params = f.where(s.Self(), s.Now())
		arms, candidates := f.candidateArms(s.Self())
		if len(arms) > 0 {
			// LIMIT -1 prevents flattening from moving validation ahead of the ordered candidate stream.
			from = "(" + strings.Join(arms, " UNION ") + " ORDER BY candidate_seq " + order + " LIMIT -1) c CROSS JOIN records ON records.seq = c.candidate_seq"
			ordered = "c.candidate_seq"
			params = append(candidates, params...)
		}
	}
	q := "SELECT " + columns + " FROM " + from + " WHERE " + where + " ORDER BY " + ordered + " " + order
	if f.Limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", f.Limit)
	}
	return q, params
}

func (s *Store) Count(ctx context.Context, f Filter) (int, error) {
	where, params := f.bounded(s.Self(), s.Now())
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

func LaneMail(k PeerKey, b Binding) Filter {
	f := Filter{Drive: b.Drive, Peer: k.String(), To: b.Lane, RepeatWindow: UrgentRepeat}
	if b.Lane != "" {
		f.FirstOnly = kinds.Broadcasts()
	}
	return f
}

// BindLane names the lane k writes as in drive and starts its lane cursor. A
// thread already bound to another drive, or already bound as a lane, keeps
// that binding: only Bind moves it, so a post as another lane never rebinds it.
// The first thread to bind a lane nobody has bound or posted as on drive
// starts its cursor before the first record addressed to that lane, so the
// lane reads what was posted to it before it started.
func (s *Store) BindLane(ctx context.Context, k PeerKey, drive, lane string, started time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("bind lane: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.claimSession(ctx, tx, k.Session); err != nil {
		return err
	}
	waiting, err := waitingFor(ctx, tx, k, drive, lane)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO sessions (session, agent, origin, drive, root, lane, role) VALUES (?, ?, ?, ?, 0, ?, 'lane')
ON CONFLICT (session, agent) DO UPDATE SET origin = excluded.origin, drive = excluded.drive, lane = excluded.lane,
	role = CASE WHEN sessions.role IN `+inferredRoles+` THEN 'lane' ELSE sessions.role END WHERE NOT sessions.root AND sessions.drive IN ('', excluded.drive) AND sessions.lane = ''`, k.Session, k.Agent, s.Self(), drive, lane); err != nil {
		return fmt.Errorf("bind lane: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO cursors (name, drive, seq) SELECT ?, ?, MIN(?, COALESCE(
	(SELECT MIN(seq) - 1 FROM records WHERE at >= MAX(?, COALESCE((SELECT announced_at FROM sessions WHERE session = ? AND agent = ? AND origin = ?), 0))),
	(SELECT MAX(seq) FROM records),
	0))
WHERE NOT EXISTS (SELECT 1 FROM cursors WHERE name = ? AND drive = ?)
ON CONFLICT (name, drive) DO NOTHING`, LaneCursor(k), drive, waiting, started.UnixMilli(), k.Session, k.Agent, s.Self(), LaneCursor(k), drive); err != nil {
		return fmt.Errorf("start lane cursor: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("bind lane: %w", err)
	}
	return nil
}

func waitingFor(ctx context.Context, tx *sql.Tx, k PeerKey, drive, lane string) (int64, error) {
	var started bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM cursors WHERE name = ? AND drive = ?)", LaneCursor(k), drive).Scan(&started); err != nil || started {
		return math.MaxInt64, err
	}
	waiting := int64(math.MaxInt64)
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MIN(r.seq) - 1, ?) FROM record_recipients a JOIN records r ON r.seq = a.source_seq
WHERE a.address = ? AND r.drive = ? AND NOT EXISTS (SELECT 1 FROM sessions WHERE drive = ? AND lane = ?) AND NOT EXISTS (SELECT 1 FROM records WHERE drive = ? AND lane = ?)`,
		waiting, lane, drive, drive, lane, drive, lane).Scan(&waiting); err != nil {
		return 0, fmt.Errorf("find records waiting for lane %s: %w", lane, err)
	}
	return waiting, nil
}

// Registered reports whether lane is bound to a thread in drive or has posted there.
func (s *Store) Registered(ctx context.Context, drive, lane string) (bool, error) {
	var ok bool
	if err := s.db.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM sessions WHERE drive = ? AND lane = ?) OR EXISTS (SELECT 1 FROM records WHERE drive = ? AND lane = ?)", drive, lane, drive, lane).Scan(&ok); err != nil {
		return false, fmt.Errorf("read lane %s on %s: %w", lane, drive, err)
	}
	return ok, nil
}

// Successors follows the stand-down records of drive from lane, each naming the
// lane that took the last one over; stoodDown reports whether lane itself stood down.
func (s *Store) Successors(ctx context.Context, drive, lane string) (chain []string, stoodDown bool, err error) {
	successor, stoodDown, err := s.successor(ctx, drive, lane)
	for err == nil && successor != "" && successor != lane && !slices.Contains(chain, successor) {
		chain = append(chain, successor)
		successor, _, err = s.successor(ctx, drive, successor)
	}
	return chain, stoodDown, err
}

func (s *Store) successor(ctx context.Context, drive, lane string) (string, bool, error) {
	var successor string
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(json_extract(fields, '$.attrs.successor'), '') FROM records
WHERE drive = ? AND kind = ? AND seq IN (SELECT source_seq FROM record_recipients WHERE address = ?) ORDER BY seq DESC LIMIT 1`, drive, string(kinds.StandDown), lane).Scan(&successor)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("read the stand-down of %s on %s: %w", lane, drive, err)
	}
	return successor, true, nil
}

// NamedSuccessor reports whether a stand-down record in drive hands a lane over to lane.
func (s *Store) NamedSuccessor(ctx context.Context, drive, lane string) (bool, error) {
	var ok bool
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM records WHERE drive = ? AND kind = ? AND json_extract(fields, '$.attrs.successor') = ?)`, drive, string(kinds.StandDown), lane).Scan(&ok); err != nil {
		return false, fmt.Errorf("read successors on %s: %w", drive, err)
	}
	return ok, nil
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

// SetParent records the agent that spawned k, "" for the session's root thread.
func (s *Store) SetParent(ctx context.Context, k PeerKey, parent string) error {
	if _, err := s.db.ExecContext(ctx, "UPDATE sessions SET parent = ? WHERE session = ? AND agent = ? AND origin = ?", parent, k.Session, k.Agent, s.Self()); err != nil {
		return fmt.Errorf("record parent of %s: %w", k, err)
	}
	return nil
}

// Owner is the thread whose lane answers for k's work: k once it is bound as
// a lane or as its drive's root, else the nearest such thread among the
// agents that spawned it, up to the session's root thread, which answers
// whenever it is bound to a drive. A k with no such thread owns itself, unbound.
func (s *Store) Owner(ctx context.Context, k PeerKey) (PeerKey, Binding, bool, error) {
	for at := k; ; {
		b, ok, err := s.Binding(ctx, at)
		switch {
		case err != nil:
			return PeerKey{}, Binding{}, false, err
		case ok && (b.Lane != "" || b.Root || at.Agent == ""):
			return at, b, true, nil
		case at.Agent == "":
			return k, Binding{}, false, nil
		}
		err = s.db.QueryRowContext(ctx, "SELECT parent FROM sessions WHERE session = ? AND agent = ? AND origin = ?", at.Session, at.Agent, s.Self()).Scan(&at.Agent)
		if errors.Is(err, sql.ErrNoRows) {
			at.Agent = ""
		} else if err != nil {
			return PeerKey{}, Binding{}, false, fmt.Errorf("read parent of %s: %w", at, err)
		}
	}
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, s.numbered(ctx, out)
}

type ReplyPage struct {
	Replies map[int64][]Record
	Err     error
}

type replyScanner struct {
	row      scanner
	re       any
	resolves any
	also     any
	seq      int64
}

func (r *replyScanner) Scan(dest ...any) error {
	err := r.row.Scan(append(dest, &r.re, &r.resolves, &r.also, &r.seq)...)
	if err == nil {
		return nil
	}
	values := make([]any, len(dest))
	raw := make([]any, len(dest), len(dest)+4)
	for i := range values {
		raw[i] = &values[i]
	}
	if rawErr := r.row.Scan(append(raw, &r.re, &r.resolves, &r.also, &r.seq)...); rawErr != nil {
		return rawErr
	}
	return err
}

func (s *Store) Replies(ctx context.Context, seqs []int64) (map[int64][]Record, error) {
	pages, err := s.ReplyPages(ctx, [][]int64{seqs})
	if err != nil {
		return nil, err
	}
	if pages[0].Err != nil {
		return nil, pages[0].Err
	}
	return pages[0].Replies, nil
}

// ReplyPages reads selected replies once and attributes decoding errors to each affected page.
func (s *Store) ReplyPages(ctx context.Context, selected [][]int64) ([]ReplyPage, error) {
	pages := make([]ReplyPage, len(selected))
	firstReply := make([]int64, len(selected))
	owners := map[int64][]int{}
	var seqs []int64
	for page, ids := range selected {
		pages[page].Replies = map[int64][]Record{}
		for _, seq := range ids {
			if len(owners[seq]) == 0 {
				seqs = append(seqs, seq)
			}
			if previous := owners[seq]; len(previous) == 0 || previous[len(previous)-1] != page {
				owners[seq] = append(owners[seq], page)
			}
		}
	}
	if len(seqs) == 0 {
		return pages, nil
	}
	ids, _ := json.Marshal(seqs)
	in := "SELECT value FROM json_each(?)"
	rows, err := s.db.QueryContext(ctx, "SELECT "+columns+", re, resolves, also_resolves, seq FROM records WHERE re IN ("+in+") OR resolves IN ("+in+") OR EXISTS (SELECT 1 FROM json_each(also_resolves) WHERE value IN ("+in+")) ORDER BY seq", string(ids), string(ids), string(ids))
	if err != nil {
		return nil, fmt.Errorf("read replies: %w", err)
	}
	defer func() { _ = rows.Close() }()
	type failure struct {
		metadata replyScanner
		err      error
	}
	var failures []failure
	for rows.Next() {
		metadata := replyScanner{row: rows}
		r, decodeErr := s.scan(&metadata)
		if decodeErr != nil {
			failures = append(failures, failure{metadata: metadata, err: decodeErr})
			continue
		}
		targets := append([]int64{r.Re, r.Resolves}, r.AlsoResolves...)
		affected := map[int]bool{}
		for _, target := range targets {
			for _, page := range owners[target] {
				affected[page] = true
			}
		}
		for page := range affected {
			if firstReply[page] == 0 {
				firstReply[page] = r.Seq
			}
			if r.Re != 0 {
				pages[page].Replies[r.Re] = append(pages[page].Replies[r.Re], r)
			}
			for _, target := range r.Closes() {
				if target != r.Re {
					pages[page].Replies[target] = append(pages[page].Replies[target], r)
				}
			}
		}
	}
	queryErr := rows.Err()
	if err := rows.Close(); err != nil && queryErr == nil {
		queryErr = err
	}
	for _, failed := range failures {
		direct, err := s.replyErrorTargets(ctx, "SELECT value FROM json_each(?) WHERE value = ? OR value = ?", string(ids), failed.metadata.re, failed.metadata.resolves)
		if err != nil {
			return nil, err
		}
		also, err := s.replyErrorTargets(ctx, "SELECT value FROM json_each(?) WHERE value IN (SELECT value FROM json_each(?))", string(ids), failed.metadata.also)
		if err != nil {
			if pages[0].Err == nil {
				pages[0].Err = err
				if firstReply[0] == 0 || firstReply[0] > failed.metadata.seq {
					pages[0].Err = fmt.Errorf("read replies: %w", err)
				}
				for _, target := range direct {
					if slices.Contains(owners[target], 0) {
						pages[0].Err = failed.err
						break
					}
				}
			}
			continue
		}
		targets := append(direct, also...)
		if len(targets) == 0 {
			return nil, failed.err
		}
		for _, target := range targets {
			for _, page := range owners[target] {
				if firstReply[page] == 0 || firstReply[page] > failed.metadata.seq {
					firstReply[page] = failed.metadata.seq
				}
				if pages[page].Err == nil {
					pages[page].Err = failed.err
				}
			}
		}
	}
	if queryErr != nil {
		for page := range pages {
			if pages[page].Err == nil {
				pages[page].Err = queryErr
				if firstReply[page] == 0 {
					pages[page].Err = fmt.Errorf("read replies: %w", queryErr)
				}
			}
		}
	}
	for _, page := range pages {
		for _, replies := range page.Replies {
			if err := s.numbered(ctx, replies); err != nil {
				return nil, err
			}
		}
	}
	return pages, nil
}

func (s *Store) replyErrorTargets(ctx context.Context, query string, args ...any) ([]int64, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var targets []int64
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			return nil, err
		}
		targets = append(targets, seq)
	}
	return targets, rows.Err()
}

const resolversSQL = `SELECT resolves, seq FROM records WHERE resolves != 0 AND drive = ?
UNION ALL SELECT resolves, seq FROM records WHERE resolves != 0 AND drive != ? AND resolves IN (SELECT seq FROM records WHERE drive = ?)
UNION ALL SELECT j.value, r.seq FROM records r, json_each(r.also_resolves) j WHERE (r.drive = ? OR j.value IN (SELECT seq FROM records WHERE drive = ?))
AND r.seq IN (SELECT seq FROM records WHERE drive = ? UNION ALL SELECT source_seq FROM record_also_resolves WHERE target IN (SELECT seq FROM records WHERE drive = ?) UNION ALL SELECT source_seq FROM record_json_errors WHERE field = 'also_resolves')`

func (s *Store) Resolvers(ctx context.Context, drive string) (map[int64]int64, error) {
	rows, err := s.db.QueryContext(ctx, resolversSQL, slices.Repeat([]any{drive}, 7)...)
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
		out[target] = max(out[target], seq)
	}
	return out, rows.Err()
}
