package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	_ "modernc.org/sqlite"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/node"
	"github.com/yasyf/cc-inbox/internal/version"
)

const (
	MaxText     = 400
	MaxBody     = 64 << 10
	DedupWindow = 10 * time.Minute
)

var migrations = []string{
	`CREATE TABLE IF NOT EXISTS records (
	seq INTEGER PRIMARY KEY AUTOINCREMENT,
	drive TEXT NOT NULL,
	lane TEXT NOT NULL,
	kind TEXT NOT NULL,
	at INTEGER NOT NULL,
	text TEXT NOT NULL,
	topic TEXT NOT NULL DEFAULT '',
	recipients TEXT NOT NULL DEFAULT '[]',
	re INTEGER NOT NULL DEFAULT 0,
	refs TEXT NOT NULL DEFAULT '{}',
	fields TEXT NOT NULL DEFAULT '{}',
	expires_at INTEGER,
	source TEXT NOT NULL,
	hash TEXT NOT NULL,
	line_hash TEXT
);
CREATE INDEX IF NOT EXISTS records_drive_seq ON records(drive, seq);
CREATE INDEX IF NOT EXISTS records_drive_kind ON records(drive, kind, seq);
CREATE INDEX IF NOT EXISTS records_hash ON records(hash, at);
CREATE UNIQUE INDEX IF NOT EXISTS records_line_hash ON records(line_hash) WHERE line_hash IS NOT NULL;
CREATE TABLE IF NOT EXISTS cursors (
	name TEXT NOT NULL,
	drive TEXT NOT NULL,
	seq INTEGER NOT NULL,
	PRIMARY KEY (name, drive)
);
CREATE TABLE IF NOT EXISTS sessions (
	session TEXT PRIMARY KEY,
	drive TEXT NOT NULL,
	root INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS imports (
	path TEXT PRIMARY KEY,
	drive TEXT NOT NULL,
	lane TEXT NOT NULL,
	offset INTEGER NOT NULL
);`,
	`ALTER TABLE imports ADD COLUMN inode INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE records ADD COLUMN resolves INTEGER NOT NULL DEFAULT 0;
CREATE INDEX records_resolves ON records(drive, resolves) WHERE resolves != 0;
UPDATE records SET refs = json_set(json_remove(refs, '$.pr'), '$.prs', json_array(json_extract(refs, '$.pr'))) WHERE json_extract(refs, '$.pr') IS NOT NULL;
UPDATE records SET refs = json_set(json_remove(refs, '$.build'), '$.builds', json_array(json_extract(refs, '$.build'))) WHERE json_extract(refs, '$.build') IS NOT NULL;
UPDATE records SET refs = json_set(refs, '$.prs', (SELECT json_group_array(value) FROM (SELECT value FROM json_each(records.refs, '$.prs') UNION SELECT value FROM json_each(records.fields, '$.stack') ORDER BY value))), fields = json_remove(fields, '$.stack') WHERE json_extract(fields, '$.stack') IS NOT NULL;
UPDATE records SET fields = json_set(json_remove(fields, '$.env'), '$.envs', json_array(json_extract(fields, '$.env'))) WHERE json_extract(fields, '$.env') IS NOT NULL`,
	`ALTER TABLE imports ADD COLUMN parser INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE sessions ADD COLUMN lane TEXT NOT NULL DEFAULT ''`,
	`CREATE TABLE lines (hash TEXT PRIMARY KEY) WITHOUT ROWID;
INSERT INTO lines SELECT line_hash FROM records WHERE line_hash IS NOT NULL`,
	`ALTER TABLE sessions ADD COLUMN role TEXT NOT NULL DEFAULT '';
ALTER TABLE sessions ADD COLUMN description TEXT NOT NULL DEFAULT '';
ALTER TABLE sessions ADD COLUMN tags TEXT NOT NULL DEFAULT '[]';
ALTER TABLE sessions ADD COLUMN cwd TEXT NOT NULL DEFAULT '';
ALTER TABLE sessions ADD COLUMN repo TEXT NOT NULL DEFAULT '';
ALTER TABLE sessions ADD COLUMN worktree TEXT NOT NULL DEFAULT '';
ALTER TABLE sessions ADD COLUMN branch TEXT NOT NULL DEFAULT '';
ALTER TABLE sessions ADD COLUMN remote TEXT NOT NULL DEFAULT '';
ALTER TABLE sessions ADD COLUMN pid INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sessions ADD COLUMN pid_started INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sessions ADD COLUMN env TEXT NOT NULL DEFAULT '{}';
ALTER TABLE sessions ADD COLUMN announced_at INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sessions ADD COLUMN seen_at INTEGER NOT NULL DEFAULT 0`,
	`CREATE TABLE claims (
	session TEXT NOT NULL,
	rel TEXT NOT NULL,
	kind TEXT NOT NULL,
	key TEXT NOT NULL,
	at INTEGER NOT NULL,
	PRIMARY KEY (session, rel, kind, key)
);
CREATE INDEX claims_ref ON claims(kind, key)`,
	`ALTER TABLE sessions ADD COLUMN superseded INTEGER NOT NULL DEFAULT 0`,
	`CREATE TABLE sessions_by_agent (
	session TEXT NOT NULL,
	agent TEXT NOT NULL DEFAULT '',
	agent_name TEXT NOT NULL DEFAULT '',
	drive TEXT NOT NULL,
	root INTEGER NOT NULL,
	lane TEXT NOT NULL DEFAULT '',
	role TEXT NOT NULL DEFAULT '',
	description TEXT NOT NULL DEFAULT '',
	tags TEXT NOT NULL DEFAULT '[]',
	cwd TEXT NOT NULL DEFAULT '',
	repo TEXT NOT NULL DEFAULT '',
	worktree TEXT NOT NULL DEFAULT '',
	branch TEXT NOT NULL DEFAULT '',
	remote TEXT NOT NULL DEFAULT '',
	pid INTEGER NOT NULL DEFAULT 0,
	pid_started INTEGER NOT NULL DEFAULT 0,
	env TEXT NOT NULL DEFAULT '{}',
	announced_at INTEGER NOT NULL DEFAULT 0,
	seen_at INTEGER NOT NULL DEFAULT 0,
	superseded INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (session, agent)
);
INSERT INTO sessions_by_agent (session, drive, root, lane, role, description, tags, cwd, repo, worktree, branch, remote, pid, pid_started, env, announced_at, seen_at, superseded)
SELECT session, drive, root, lane, role, description, tags, cwd, repo, worktree, branch, remote, pid, pid_started, env, announced_at, seen_at, superseded FROM sessions;
DROP TABLE sessions;
ALTER TABLE sessions_by_agent RENAME TO sessions;
CREATE TABLE claims_by_agent (
	session TEXT NOT NULL,
	agent TEXT NOT NULL DEFAULT '',
	rel TEXT NOT NULL,
	kind TEXT NOT NULL,
	key TEXT NOT NULL,
	at INTEGER NOT NULL,
	PRIMARY KEY (session, agent, rel, kind, key)
);
INSERT INTO claims_by_agent (session, rel, kind, key, at) SELECT session, rel, kind, key, at FROM claims;
DROP TABLE claims;
ALTER TABLE claims_by_agent RENAME TO claims;
CREATE INDEX claims_ref ON claims(kind, key)`,
	`ALTER TABLE records ADD COLUMN origin TEXT NOT NULL DEFAULT '';
ALTER TABLE records ADD COLUMN origin_seq INTEGER NOT NULL DEFAULT 0;
ALTER TABLE records ADD COLUMN id TEXT;
ALTER TABLE records ADD COLUMN body TEXT;
ALTER TABLE records ADD COLUMN re_origin TEXT NOT NULL DEFAULT '';
ALTER TABLE records ADD COLUMN re_origin_seq INTEGER NOT NULL DEFAULT 0;
ALTER TABLE records ADD COLUMN resolves_origin TEXT NOT NULL DEFAULT '';
ALTER TABLE records ADD COLUMN resolves_origin_seq INTEGER NOT NULL DEFAULT 0;
UPDATE records SET origin = {self}, origin_seq = seq,
	re_origin = CASE WHEN re = 0 THEN '' ELSE {self} END, re_origin_seq = re,
	resolves_origin = CASE WHEN resolves = 0 THEN '' ELSE {self} END, resolves_origin_seq = resolves;
CREATE UNIQUE INDEX records_origin ON records(origin, origin_seq);
CREATE UNIQUE INDEX records_id ON records(drive, id) WHERE id IS NOT NULL;
CREATE TRIGGER records_identity AFTER INSERT ON records
WHEN NEW.origin_seq = 0 OR (NEW.re != 0 AND NEW.re_origin = '') OR (NEW.resolves != 0 AND NEW.resolves_origin = '')
BEGIN
UPDATE records SET
	origin_seq = CASE WHEN NEW.origin_seq = 0 THEN NEW.seq ELSE NEW.origin_seq END,
	re_origin = CASE WHEN NEW.re = 0 OR NEW.re_origin != '' THEN NEW.re_origin ELSE COALESCE((SELECT t.origin FROM records t WHERE t.seq = NEW.re), NEW.origin) END,
	re_origin_seq = CASE WHEN NEW.re = 0 OR NEW.re_origin != '' THEN NEW.re_origin_seq ELSE COALESCE((SELECT t.origin_seq FROM records t WHERE t.seq = NEW.re), NEW.re) END,
	resolves_origin = CASE WHEN NEW.resolves = 0 OR NEW.resolves_origin != '' THEN NEW.resolves_origin ELSE COALESCE((SELECT t.origin FROM records t WHERE t.seq = NEW.resolves), NEW.origin) END,
	resolves_origin_seq = CASE WHEN NEW.resolves = 0 OR NEW.resolves_origin != '' THEN NEW.resolves_origin_seq ELSE COALESCE((SELECT t.origin_seq FROM records t WHERE t.seq = NEW.resolves), NEW.resolves) END
WHERE seq = NEW.seq;
END;
ALTER TABLE sessions ADD COLUMN origin TEXT NOT NULL DEFAULT '';
UPDATE sessions SET origin = {self};
ALTER TABLE claims ADD COLUMN origin TEXT NOT NULL DEFAULT '';
UPDATE claims SET origin = {self};
CREATE TABLE receipts (
	channel TEXT NOT NULL,
	origin TEXT NOT NULL,
	change_id TEXT NOT NULL,
	revision TEXT NOT NULL,
	digest TEXT NOT NULL,
	PRIMARY KEY (channel, origin)
);
CREATE TABLE watermarks (peer TEXT PRIMARY KEY, origin_seq INTEGER NOT NULL);
CREATE TABLE members (
	node TEXT PRIMARY KEY,
	public_key BLOB NOT NULL,
	tier TEXT NOT NULL,
	via TEXT NOT NULL,
	host TEXT NOT NULL DEFAULT '',
	by TEXT NOT NULL,
	at INTEGER NOT NULL
);
CREATE TABLE pending (
	origin TEXT NOT NULL,
	origin_seq INTEGER NOT NULL,
	wait_origin TEXT NOT NULL,
	wait_seq INTEGER NOT NULL,
	row TEXT NOT NULL,
	PRIMARY KEY (origin, origin_seq)
);
CREATE INDEX pending_wait ON pending(wait_origin, wait_seq);
CREATE TABLE aliases (origin TEXT NOT NULL, origin_seq INTEGER NOT NULL, seq INTEGER NOT NULL, PRIMARY KEY (origin, origin_seq));
CREATE TABLE folded (origin TEXT NOT NULL, origin_seq INTEGER NOT NULL, PRIMARY KEY (origin, origin_seq)) WITHOUT ROWID;
CREATE TABLE meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL);
INSERT INTO meta (key, value) SELECT 'compacted_through', MAX(seq) FROM records WHERE kind = 'digest' HAVING MAX(seq) IS NOT NULL`,
}

var (
	ErrTextTooLong = errors.New("text exceeds 400 characters; write the body to a file and run `cci post --lane <lane> --kind <kind> --path <file>`, which posts the file's first line as the text")
	ErrNoDrive     = errors.New("no drive: pass --drive or run `cci drive use <drive>`")
)

type Refs struct {
	Path    string   `json:"path,omitempty"`
	CCN     string   `json:"ccn,omitempty"`
	URL     string   `json:"url,omitempty"`
	Board   string   `json:"board,omitempty"`
	PRs     []PR     `json:"prs,omitempty"`
	Builds  []string `json:"builds,omitempty"`
	Stacks  []string `json:"stacks,omitempty"`
	Targets []string `json:"targets,omitempty"`
	Lanes   []string `json:"lanes,omitempty"`
}

type Census struct {
	N           int      `json:"n"`
	Denominator int      `json:"denominator"`
	Head        string   `json:"head,omitempty"`
	Drift       int      `json:"drift"`
	StacksClean []string `json:"stacks_clean,omitempty"`
}

type Fields struct {
	Attrs   map[string]string `json:"attrs,omitempty"`
	Envs    []string          `json:"envs,omitempty"`
	Mode    string            `json:"mode,omitempty"`
	Outcome string            `json:"outcome,omitempty"`
	Commit  string            `json:"commit,omitempty"`
	Census  *Census           `json:"census,omitempty"`
	Counts  map[string]int    `json:"counts,omitempty"`
}

type Record struct {
	Seq       int64           `json:"seq"`
	Origin    string          `json:"origin,omitempty"`
	OriginSeq int64           `json:"origin_seq,omitempty"`
	ID        string          `json:"id,omitempty"`
	Drive     string          `json:"drive"`
	Lane      string          `json:"lane"`
	Kind      kinds.Kind      `json:"kind"`
	At        time.Time       `json:"at"`
	Text      string          `json:"text"`
	Topic     string          `json:"topic,omitempty"`
	To        []string        `json:"to,omitempty"`
	Re        int64           `json:"re,omitempty"`
	Resolves  int64           `json:"resolves,omitempty"`
	Refs      Refs            `json:"refs"`
	Fields    Fields          `json:"fields"`
	ExpiresAt *time.Time      `json:"expires_at,omitempty"`
	Source    string          `json:"source"`
	Body      json.RawMessage `json:"body,omitempty"`
	Status    string          `json:"status,omitempty"`
}

func (r Record) Tracked() bool {
	return !strings.HasPrefix(r.Source, "import:") || len(r.Refs.Stacks) > 0 || len(r.Refs.Targets) > 0
}

type Store struct {
	db   *sql.DB
	home string
	node node.Node
	Now  func() time.Time
}

func Home() (string, error) {
	if h := os.Getenv("CCI_HOME"); h != "" {
		return h, nil
	}
	dir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home: %w", err)
	}
	return filepath.Join(dir, ".cc-inbox"), nil
}

func Open(ctx context.Context, home string) (*Store, error) {
	if err := os.MkdirAll(home, 0o700); err != nil {
		return nil, fmt.Errorf("create %s: %w", home, err)
	}
	n, err := node.Load(home)
	if err != nil {
		return nil, err
	}
	dsn := "file:" + filepath.Join(home, "inbox.db") + "?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=mmap_size(268435456)&_pragma=cache_size(-65536)&_txlock=immediate"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open store: %w", err)
	}
	s := &Store{db: db, home: home, node: n, Now: time.Now}
	if err := s.initialize(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) initialize(ctx context.Context) error {
	lock, err := os.OpenFile(filepath.Join(s.home, "init.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open init lock: %w", err)
	}
	defer func() { _ = lock.Close() }()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("take init lock: %w", err)
	}
	defer func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) }()
	return s.migrate(ctx)
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) HomeDir() string {
	return s.home
}

// SchemaFingerprint names the store schema this binary writes: the hex
// SHA-256 of its migrations, so two nodes replicate only on the same schema.
func (s *Store) SchemaFingerprint() string {
	sum := sha256.Sum256([]byte(strings.Join(migrations, "\x00")))
	return hex.EncodeToString(sum[:])
}

// Self is this node's id, the origin of every record Post writes.
func (s *Store) Self() string {
	return s.node.ID()
}

func (s *Store) Node() node.Node {
	return s.node
}

func (s *Store) local(origin string) string {
	if origin == s.Self() {
		return ""
	}
	return origin
}

func (s *Store) origin(local string) string {
	if local == "" {
		return s.Self()
	}
	return local
}

func (s *Store) migrate(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var schema int
	if err := tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&schema); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	if schema == len(migrations) {
		return nil
	}
	if schema > len(migrations) {
		return fmt.Errorf("store schema %d is newer than this cci %s (schema %d); upgrade it with: %s", schema, version.String(), len(migrations), version.UpgradeCommand())
	}
	for i, step := range migrations[schema:] {
		if _, err := tx.ExecContext(ctx, strings.ReplaceAll(step, "{self}", "'"+s.Self()+"'")); err != nil {
			return fmt.Errorf("migrate store to schema %d: %w", schema+i+1, err)
		}
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", len(migrations))); err != nil {
		return fmt.Errorf("record schema version: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit migration: %w", err)
	}
	return nil
}

func Hash(r Record) string {
	parts := []any{r.Drive, r.Lane, r.Kind, r.Topic, slices.Sorted(slices.Values(r.To)), r.Re, r.Resolves, strings.Fields(r.Text)}
	if r.Refs.Path != "" {
		parts = append(parts, r.Refs.Path)
	}
	if r.ID != "" {
		parts = append(parts, r.ID)
	}
	key, _ := json.Marshal(parts)
	sum := sha256.Sum256(key)
	return hex.EncodeToString(sum[:])
}

func Validate(r Record) error {
	if r.Drive == "" {
		return ErrNoDrive
	}
	if r.Lane == "" {
		return errors.New("lane is required")
	}
	if strings.TrimSpace(r.Text) == "" {
		return errors.New("text is required")
	}
	if utf8.RuneCountInString(r.Text) > MaxText {
		return ErrTextTooLong
	}
	spec := r.Kind.Spec()
	if spec.NeedsPR && len(r.Refs.PRs) == 0 {
		return fmt.Errorf("kind %s requires --pr", r.Kind)
	}
	if spec.NeedsRef && r.Re == 0 && r.Topic == "" && r.Resolves == 0 {
		return fmt.Errorf("kind %s requires --re, --resolves or --topic", r.Kind)
	}
	if err := validateFields(r.Fields); err != nil {
		return err
	}
	if len(r.Body) > MaxBody {
		return fmt.Errorf("body is %d bytes; the limit is %d", len(r.Body), MaxBody)
	}
	if len(r.Body) > 0 && !json.Valid(r.Body) {
		return errors.New("body is not valid JSON")
	}
	for _, stack := range r.Refs.Stacks {
		if project, env, ok := strings.Cut(stack, "/"); !ok || project == "" || env == "" || strings.Contains(env, "/") {
			return fmt.Errorf("stack %q is not <project>/<env>", stack)
		}
	}
	for _, to := range r.To {
		if lane, drive, ok := strings.Cut(to, "@"); ok && (lane == "" || drive == "") {
			return fmt.Errorf("recipient %q is not <lane>@<drive>", to)
		}
	}
	return nil
}

var (
	modes    = []string{"platy", "cli", "manual", "walker"}
	outcomes = []string{"passed", "failed", "pending", "cancelled"}
)

func validateFields(f Fields) error {
	if f.Mode != "" && !slices.Contains(modes, f.Mode) {
		return fmt.Errorf("mode %q is not one of %s", f.Mode, strings.Join(modes, ", "))
	}
	if f.Outcome != "" && !slices.Contains(outcomes, f.Outcome) {
		return fmt.Errorf("outcome %q is not one of %s", f.Outcome, strings.Join(outcomes, ", "))
	}
	return nil
}

func (s *Store) Post(ctx context.Context, r Record, ttl time.Duration) (Record, bool, error) {
	if r.Kind.Spec().Internal {
		return Record{}, false, fmt.Errorf("kind %s is written only by cci itself", r.Kind)
	}
	if err := Validate(r); err != nil {
		return Record{}, false, err
	}
	if r.Resolves != 0 {
		target, err := s.Get(ctx, r.Resolves)
		if errors.Is(err, sql.ErrNoRows) || err == nil && target.Drive != r.Drive && !CrossDrive(target.To) {
			return Record{}, false, fmt.Errorf("--resolves #%d names no record in drive %s", r.Resolves, r.Drive)
		}
		if err != nil {
			return Record{}, false, err
		}
	}
	r.Origin, r.OriginSeq = "", 0
	r.At = s.Now().UTC()
	r.ExpiresAt = expiry(r, ttl)
	hash := Hash(r)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Record{}, false, fmt.Errorf("begin post: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var existing int64
	if r.ID != "" {
		err = tx.QueryRowContext(ctx, "SELECT seq FROM records WHERE drive = ? AND id = ?", r.Drive, r.ID).Scan(&existing)
	} else {
		err = sql.ErrNoRows
	}
	if errors.Is(err, sql.ErrNoRows) {
		err = tx.QueryRowContext(ctx, "SELECT seq FROM records WHERE hash = ? AND at >= ? AND origin = ? ORDER BY seq DESC LIMIT 1",
			hash, r.At.Add(-DedupWindow).UnixMilli(), s.Self()).Scan(&existing)
	}
	switch {
	case err == nil:
		got, err := s.get(ctx, tx, existing)
		return got, true, err
	case !errors.Is(err, sql.ErrNoRows):
		return Record{}, false, fmt.Errorf("check duplicate: %w", err)
	}
	seq, err := s.insert(ctx, tx, r, hash, nil)
	if err != nil {
		return Record{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return Record{}, false, fmt.Errorf("commit post: %w", err)
	}
	r.Seq = seq
	return r, false, nil
}

type Ingestion struct {
	Record   Record
	LineHash string
	Line     string
}

func (s *Store) IngestAll(ctx context.Context, items []Ingestion) (int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin ingest: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	inserted := 0
	for _, it := range items {
		unseen, err := consume(ctx, tx, it.LineHash)
		if err != nil {
			return 0, err
		}
		if !unseen {
			continue
		}
		r := it.Record
		r.ExpiresAt = expiry(r, 0)
		res, err := tx.ExecContext(ctx, insertSQL+" ON CONFLICT DO NOTHING", s.args(r, Hash(r), &it.LineHash)...)
		if err != nil {
			return 0, fmt.Errorf("ingest record: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("ingest record: %w", err)
		}
		inserted += int(n)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit ingest: %w", err)
	}
	return inserted, nil
}

func (s *Store) Reparse(ctx context.Context, items []Ingestion, fresh bool) (updated, inserted int, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("begin reparse: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var (
		parentAt   time.Time
		parentText string
	)
	for _, it := range items {
		r := it.Record
		unseen, err := consume(ctx, tx, it.LineHash)
		if err != nil {
			return 0, 0, err
		}
		var (
			at  int64
			old string
		)
		err = tx.QueryRowContext(ctx, "SELECT at, text FROM records WHERE line_hash = ?", it.LineHash).Scan(&at, &old)
		switch {
		case err == nil:
			if _, err := tx.ExecContext(ctx, reparseSQL, reparseArgs(r, it.LineHash)...); err != nil {
				return 0, 0, fmt.Errorf("reparse record: %w", err)
			}
			parentAt, parentText = time.UnixMilli(at).UTC(), old
			updated++
			continue
		case !errors.Is(err, sql.ErrNoRows):
			return 0, 0, fmt.Errorf("reparse record: %w", err)
		}
		switch {
		case unseen && fresh:
		case unseen && parentText != "" && strings.Contains(parentText, strings.Join(strings.Fields(it.Line), " ")):
			r.At = parentAt
		default:
			parentText = ""
			continue
		}
		r.ExpiresAt = expiry(r, 0)
		res, err := tx.ExecContext(ctx, insertSQL+" ON CONFLICT DO NOTHING", s.args(r, Hash(r), &it.LineHash)...)
		if err != nil {
			return 0, 0, fmt.Errorf("reparse record: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return 0, 0, fmt.Errorf("reparse record: %w", err)
		}
		inserted += int(n)
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, fmt.Errorf("commit reparse: %w", err)
	}
	return updated, inserted, nil
}

func consume(ctx context.Context, tx *sql.Tx, lineHash string) (bool, error) {
	res, err := tx.ExecContext(ctx, "INSERT INTO lines (hash) VALUES (?) ON CONFLICT DO NOTHING", lineHash)
	if err != nil {
		return false, fmt.Errorf("record consumed line: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("record consumed line: %w", err)
	}
	return n == 1, nil
}

func reparseArgs(r Record, lineHash string) []any {
	to := r.To
	if to == nil {
		to = []string{}
	}
	recipients, _ := json.Marshal(to)
	refs, _ := json.Marshal(r.Refs)
	var ttl any
	if t := r.Kind.Spec().TTL; t > 0 {
		ttl = t.Milliseconds()
	}
	return []any{r.Lane, string(r.Kind), r.Text, r.Topic, string(recipients), string(refs), Hash(r), ttl, lineHash}
}

func expiry(r Record, ttl time.Duration) *time.Time {
	if ttl == 0 {
		ttl = r.Kind.Spec().TTL
	}
	if ttl == 0 {
		return nil
	}
	t := r.At.Add(ttl)
	return &t
}

const insertSQL = `INSERT INTO records (origin, origin_seq, id, drive, lane, kind, at, text, topic, recipients, re, resolves, refs, fields, expires_at, source, hash, line_hash, body)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

const reparseSQL = `UPDATE records SET lane = ?, kind = ?, text = ?, topic = ?, recipients = ?, refs = ?, hash = ?, expires_at = at + ?
WHERE line_hash = ?`

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func (s *Store) insert(ctx context.Context, x execer, r Record, hash string, lineHash *string) (int64, error) {
	res, err := x.ExecContext(ctx, insertSQL, s.args(r, hash, lineHash)...)
	if err != nil {
		return 0, fmt.Errorf("insert record: %w", err)
	}
	seq, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("insert record: %w", err)
	}
	return seq, nil
}

func (s *Store) args(r Record, hash string, lineHash *string) []any {
	to := r.To
	if to == nil {
		to = []string{}
	}
	recipients, _ := json.Marshal(to)
	refs, _ := json.Marshal(r.Refs)
	fields, _ := json.Marshal(r.Fields)
	var expires any
	if r.ExpiresAt != nil {
		expires = r.ExpiresAt.UnixMilli()
	}
	var line any
	if lineHash != nil {
		line = *lineHash
	}
	var id, body any
	if r.ID != "" {
		id = r.ID
	}
	if len(r.Body) > 0 {
		body = string(r.Body)
	}
	return []any{s.origin(r.Origin), r.OriginSeq, id, r.Drive, r.Lane, string(r.Kind), r.At.UnixMilli(), r.Text, r.Topic, string(recipients), r.Re, r.Resolves, string(refs), string(fields), expires, r.Source, hash, line, body}
}

const columns = "seq, origin, origin_seq, COALESCE(id, ''), drive, lane, kind, at, text, topic, recipients, re, resolves, refs, fields, expires_at, source, COALESCE(body, '')"

type scanner interface {
	Scan(dest ...any) error
}

func (s *Store) scan(row scanner) (Record, error) {
	var (
		r                                Record
		kind, recipients, refs, fs, body string
		at                               int64
		expires                          sql.NullInt64
	)
	if err := row.Scan(&r.Seq, &r.Origin, &r.OriginSeq, &r.ID, &r.Drive, &r.Lane, &kind, &at, &r.Text, &r.Topic, &recipients, &r.Re, &r.Resolves, &refs, &fs, &expires, &r.Source, &body); err != nil {
		return Record{}, fmt.Errorf("scan record: %w", err)
	}
	if r.Origin = s.local(r.Origin); r.Origin == "" {
		r.OriginSeq = 0
	}
	if body != "" {
		r.Body = json.RawMessage(body)
	}
	r.Kind = kinds.Kind(kind)
	r.At = time.UnixMilli(at).UTC()
	if expires.Valid {
		t := time.UnixMilli(expires.Int64).UTC()
		r.ExpiresAt = &t
	}
	if err := json.Unmarshal([]byte(recipients), &r.To); err != nil {
		return Record{}, fmt.Errorf("decode recipients of #%d: %w", r.Seq, err)
	}
	if len(r.To) == 0 {
		r.To = nil
	}
	if err := json.Unmarshal([]byte(refs), &r.Refs); err != nil {
		return Record{}, fmt.Errorf("decode refs of #%d: %w", r.Seq, err)
	}
	if err := json.Unmarshal([]byte(fs), &r.Fields); err != nil {
		return Record{}, fmt.Errorf("decode fields of #%d: %w", r.Seq, err)
	}
	return r, nil
}

type querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (s *Store) get(ctx context.Context, q querier, seq int64) (Record, error) {
	return s.scan(q.QueryRowContext(ctx, "SELECT "+columns+" FROM records WHERE seq = ?", seq))
}

func (s *Store) Get(ctx context.Context, seq int64) (Record, error) {
	return s.get(ctx, s.db, seq)
}

func (s *Store) MaxSeq(ctx context.Context) (int64, error) {
	var seq sql.NullInt64
	if err := s.db.QueryRowContext(ctx, "SELECT MAX(seq) FROM records").Scan(&seq); err != nil {
		return 0, fmt.Errorf("read max seq: %w", err)
	}
	return seq.Int64, nil
}

type DriveInfo struct {
	Drive   string    `json:"drive"`
	Records int       `json:"records"`
	Last    time.Time `json:"last"`
}

func (s *Store) Drives(ctx context.Context) ([]DriveInfo, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT drive, COUNT(*), MAX(at) FROM records GROUP BY drive ORDER BY MAX(at) DESC")
	if err != nil {
		return nil, fmt.Errorf("list drives: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []DriveInfo
	for rows.Next() {
		var (
			d    DriveInfo
			last int64
		)
		if err := rows.Scan(&d.Drive, &d.Records, &last); err != nil {
			return nil, fmt.Errorf("scan drive: %w", err)
		}
		d.Last = time.UnixMilli(last).UTC()
		out = append(out, d)
	}
	return out, rows.Err()
}

func Clip(text string) string {
	if r := []rune(text); len(r) > MaxText {
		return string(r[:MaxText-1]) + "…"
	}
	return text
}

func (s *Store) Fit(text string) (string, string, error) {
	if utf8.RuneCountInString(text) <= MaxText {
		return text, "", nil
	}
	sum := sha256.Sum256([]byte(text))
	dir := filepath.Join(s.home, "blobs")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", "", fmt.Errorf("create blob dir: %w", err)
	}
	path := filepath.Join(dir, hex.EncodeToString(sum[:8])+".txt")
	if err := os.WriteFile(path, []byte(text+"\n"), 0o600); err != nil {
		return "", "", fmt.Errorf("write blob: %w", err)
	}
	return Clip(text), path, nil
}
