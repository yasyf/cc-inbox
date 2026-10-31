package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/yasyf/cc-inbox/internal/store"
)

type probeResult struct {
	Fingerprint string `json:"fingerprint"`
	Schema      int    `json:"schema"`
	ReadSeq     int64  `json:"read_seq"`
	ReadText    string `json:"read_text"`
	PostedSeq   int64  `json:"posted_seq"`
	PostedText  string `json:"posted_text"`
}

type matrixResult struct {
	Ask      int64 `json:"ask"`
	Done     int64 `json:"done"`
	Imported int64 `json:"imported"`
	Remote   int64 `json:"remote"`
	Folded   int   `json:"folded"`
}

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) == 3 && os.Args[1] == "wait-write" {
		return waitingWriter(os.Args[2], store.Record{Drive: "concurrent", Lane: "old", Kind: "note", Text: "old writer during provisioning", To: []string{"peer:concurrent-old"}, Source: "post"})
	}
	if len(os.Args) == 3 && os.Args[1] == "wait-incident" {
		return waitingWriter(os.Args[2], store.Record{Drive: "concurrent", Lane: "old", Kind: "incident", Text: "old incident #31415", Source: "post"})
	}
	if len(os.Args) == 4 && os.Args[1] == "matrix" {
		return matrix(os.Args[2], os.Args[3])
	}
	if len(os.Args) != 2 && len(os.Args) != 3 {
		return fmt.Errorf("usage: compat-probe HOME [READ_SEQ] | compat-probe matrix HOME TARGET_SEQ")
	}
	ctx := context.Background()
	st, err := store.Open(ctx, os.Args[1])
	if err != nil {
		return fmt.Errorf("open old store: %w", err)
	}
	defer func() { _ = st.Close() }()
	result := probeResult{Fingerprint: st.SchemaFingerprint()}
	if len(os.Args) == 3 {
		seq, err := strconv.ParseInt(os.Args[2], 10, 64)
		if err != nil {
			return fmt.Errorf("parse read sequence: %w", err)
		}
		record, err := st.Get(ctx, seq)
		if err != nil {
			return fmt.Errorf("read candidate record: %w", err)
		}
		result.ReadSeq, result.ReadText = record.Seq, record.Text
	}
	record, _, err := st.Post(ctx, store.Record{
		Drive:  "compat",
		Lane:   "old",
		Kind:   "report",
		Text:   fmt.Sprintf("old fixture process %d", os.Getpid()),
		To:     []string{"peer:compat-old"},
		Source: "post",
	}, 0)
	if err != nil {
		return fmt.Errorf("post old fixture record: %w", err)
	}
	result.PostedSeq, result.PostedText = record.Seq, record.Text
	db, err := sql.Open("sqlite", "file:"+filepath.Join(os.Args[1], store.DBFile)+"?mode=ro")
	if err != nil {
		return fmt.Errorf("open schema metadata: %w", err)
	}
	defer func() { _ = db.Close() }()
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&result.Schema); err != nil {
		return fmt.Errorf("read schema metadata: %w", err)
	}
	if err := st.Close(); err != nil {
		return fmt.Errorf("close old store: %w", err)
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}

func waitingWriter(home string, row store.Record) error {
	ctx := context.Background()
	st, err := store.Open(ctx, home)
	if err != nil {
		return fmt.Errorf("open waiting old writer: %w", err)
	}
	defer func() { _ = st.Close() }()
	if _, err := fmt.Fprintln(os.Stdout, "ready"); err != nil {
		return err
	}
	var signal string
	if _, err := fmt.Fscanln(os.Stdin, &signal); err != nil {
		return fmt.Errorf("await old writer signal: %w", err)
	}
	if signal != "write" {
		return fmt.Errorf("old writer signal %q, want write", signal)
	}
	if _, err := fmt.Fprintln(os.Stdout, "writing"); err != nil {
		return err
	}
	record, _, err := st.Post(ctx, row, 0)
	if err != nil {
		return fmt.Errorf("post while memberships provision: %w", err)
	}
	return json.NewEncoder(os.Stdout).Encode(struct {
		Seq         int64  `json:"seq"`
		Fingerprint string `json:"fingerprint"`
	}{record.Seq, st.SchemaFingerprint()})
}

func matrix(home, rawTarget string) error {
	target, err := strconv.ParseInt(rawTarget, 10, 64)
	if err != nil {
		return fmt.Errorf("parse target sequence: %w", err)
	}
	ctx := context.Background()
	st, err := store.Open(ctx, home)
	if err != nil {
		return fmt.Errorf("open old store: %w", err)
	}
	defer func() { _ = st.Close() }()
	var result matrixResult
	ask, _, err := st.Post(ctx, store.Record{Drive: "compat", Lane: "old", Kind: "ask", Text: "old matrix ask", To: []string{"peer:old-matrix", "old@compat"}, Source: "post"}, 0)
	if err != nil {
		return fmt.Errorf("post old matrix ask: %w", err)
	}
	result.Ask = ask.Seq
	done, _, err := st.Post(ctx, store.Record{Drive: "compat", Lane: "old", Kind: "done", Text: "old matrix done", Resolves: ask.Seq, AlsoResolves: []int64{target}, To: []string{"peer:old-done"}, Source: "post"}, 0)
	if err != nil {
		return fmt.Errorf("post old matrix resolver: %w", err)
	}
	result.Done = done.Seq
	line := store.Ingestion{
		Record:   store.Record{Drive: "compat", Lane: "old-import", Kind: "note", At: time.Now(), Text: "old import", Source: "import:/old.md", To: []string{"peer:old-import"}},
		LineHash: "old-matrix-line",
		Line:     "old import",
	}
	if _, err := st.IngestAll(ctx, []store.Ingestion{line}); err != nil {
		return fmt.Errorf("ingest old matrix line: %w", err)
	}
	line.Record.To = []string{"peer:old-reparsed"}
	if _, _, err := st.Reparse(ctx, []store.Ingestion{line}, false); err != nil {
		return fmt.Errorf("reparse old matrix line: %w", err)
	}
	if result.Imported, err = laneSeq(ctx, st, "old-import"); err != nil {
		return err
	}
	if _, err := st.Apply(ctx, []store.Row{{
		Origin: "0123456789abcdef", OriginSeq: 1, Drive: "compat", Lane: "old-remote", Kind: "done", Text: "old remote", At: time.Now(),
		To: []string{"peer:old-remote"}, Resolves: &store.Pointer{Origin: st.Self(), Seq: ask.Seq}, AlsoResolves: []store.Pointer{{Origin: st.Self(), Seq: target}}, Source: "post",
	}}, nil); err != nil {
		return fmt.Errorf("apply old matrix row: %w", err)
	}
	if result.Remote, err = laneSeq(ctx, st, "old-remote"); err != nil {
		return err
	}
	for i := range 2 {
		if _, _, err := st.Post(ctx, store.Record{Drive: "old-compact", Lane: "old", Kind: "note", Text: fmt.Sprintf("old folded %d", i), To: []string{"peer:old-folded"}, Source: "post"}, 0); err != nil {
			return fmt.Errorf("post old folded record: %w", err)
		}
	}
	compacted, err := st.Compact(ctx, "old-compact", -time.Hour)
	if err != nil {
		return fmt.Errorf("compact old matrix drive: %w", err)
	}
	result.Folded = compacted.Folded
	if err := st.Close(); err != nil {
		return fmt.Errorf("close old store: %w", err)
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}

func laneSeq(ctx context.Context, st *store.Store, lane string) (int64, error) {
	records, err := st.Query(ctx, store.Filter{Drive: "compat", Lanes: []string{lane}, IncludeExpired: true})
	if err != nil {
		return 0, fmt.Errorf("read old matrix %s: %w", lane, err)
	}
	if len(records) != 1 {
		return 0, fmt.Errorf("old matrix %s has %d records", lane, len(records))
	}
	return records[0].Seq, nil
}
