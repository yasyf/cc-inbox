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
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	_ "modernc.org/sqlite"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

const (
	MaxText     = 400
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
}

var (
	ErrTextTooLong = errors.New("text exceeds 400 characters; put the body in a file and pass --path")
	ErrNoDrive     = errors.New("no drive: pass --drive or run `cci drive use <drive>`")
)

type Refs struct {
	Path  string `json:"path,omitempty"`
	CCN   string `json:"ccn,omitempty"`
	PR    int    `json:"pr,omitempty"`
	Build string `json:"build,omitempty"`
	URL   string `json:"url,omitempty"`
}

type Fields struct {
	Stack  []int          `json:"stack,omitempty"`
	Env    string         `json:"env,omitempty"`
	Counts map[string]int `json:"counts,omitempty"`
}

type Record struct {
	Seq       int64      `json:"seq"`
	Drive     string     `json:"drive"`
	Lane      string     `json:"lane"`
	Kind      kinds.Kind `json:"kind"`
	At        time.Time  `json:"at"`
	Text      string     `json:"text"`
	Topic     string     `json:"topic,omitempty"`
	To        []string   `json:"to,omitempty"`
	Re        int64      `json:"re,omitempty"`
	Refs      Refs       `json:"refs"`
	Fields    Fields     `json:"fields"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	Source    string     `json:"source"`
}

type Store struct {
	db   *sql.DB
	home string
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
	dsn := "file:" + filepath.Join(home, "inbox.db") + "?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_txlock=immediate"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open store: %w", err)
	}
	s := &Store{db: db, home: home, Now: time.Now}
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

func (s *Store) migrate(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var version int
	if err := tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	if version == len(migrations) {
		return nil
	}
	if version > len(migrations) {
		return fmt.Errorf("store schema %d is newer than this cci (%d); upgrade cci", version, len(migrations))
	}
	for i, step := range migrations[version:] {
		if _, err := tx.ExecContext(ctx, step); err != nil {
			return fmt.Errorf("migrate store to schema %d: %w", version+i+1, err)
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
	sum := sha256.Sum256([]byte(r.Drive + "\x00" + r.Lane + "\x00" + string(r.Kind) + "\x00" + strings.Join(strings.Fields(r.Text), " ")))
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
	if spec.NeedsPR && r.Refs.PR == 0 {
		return fmt.Errorf("kind %s requires --pr", r.Kind)
	}
	if spec.NeedsRef && r.Re == 0 && r.Topic == "" {
		return fmt.Errorf("kind %s requires --re or --topic", r.Kind)
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
	r.At = s.Now().UTC()
	r.ExpiresAt = expiry(r, ttl)
	hash := Hash(r)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Record{}, false, fmt.Errorf("begin post: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var existing int64
	err = tx.QueryRowContext(ctx, "SELECT seq FROM records WHERE hash = ? AND at >= ? ORDER BY seq DESC LIMIT 1",
		hash, r.At.Add(-DedupWindow).UnixMilli()).Scan(&existing)
	switch {
	case err == nil:
		got, err := s.get(ctx, tx, existing)
		return got, true, err
	case !errors.Is(err, sql.ErrNoRows):
		return Record{}, false, fmt.Errorf("check duplicate: %w", err)
	}
	seq, err := insert(ctx, tx, r, hash, nil)
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
}

func (s *Store) IngestAll(ctx context.Context, items []Ingestion) (int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin ingest: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	inserted := 0
	for _, it := range items {
		r := it.Record
		r.ExpiresAt = expiry(r, 0)
		res, err := tx.ExecContext(ctx, insertSQL+" ON CONFLICT DO NOTHING", args(r, Hash(r), &it.LineHash)...)
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

const insertSQL = `INSERT INTO records (drive, lane, kind, at, text, topic, recipients, re, refs, fields, expires_at, source, hash, line_hash)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func insert(ctx context.Context, x execer, r Record, hash string, lineHash *string) (int64, error) {
	res, err := x.ExecContext(ctx, insertSQL, args(r, hash, lineHash)...)
	if err != nil {
		return 0, fmt.Errorf("insert record: %w", err)
	}
	seq, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("insert record: %w", err)
	}
	return seq, nil
}

func args(r Record, hash string, lineHash *string) []any {
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
	return []any{r.Drive, r.Lane, string(r.Kind), r.At.UnixMilli(), r.Text, r.Topic, string(recipients), r.Re, string(refs), string(fields), expires, r.Source, hash, line}
}

const columns = "seq, drive, lane, kind, at, text, topic, recipients, re, refs, fields, expires_at, source"

type scanner interface {
	Scan(dest ...any) error
}

func scan(row scanner) (Record, error) {
	var (
		r                          Record
		kind, recipients, refs, fs string
		at                         int64
		expires                    sql.NullInt64
	)
	if err := row.Scan(&r.Seq, &r.Drive, &r.Lane, &kind, &at, &r.Text, &r.Topic, &recipients, &r.Re, &refs, &fs, &expires, &r.Source); err != nil {
		return Record{}, fmt.Errorf("scan record: %w", err)
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
	return scan(q.QueryRowContext(ctx, "SELECT "+columns+" FROM records WHERE seq = ?", seq))
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

func (s *Store) Fit(text string) (string, string, error) {
	r := []rune(text)
	if len(r) <= MaxText {
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
	return string(r[:MaxText-1]) + "…", path, nil
}
