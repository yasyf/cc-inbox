package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

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

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) != 2 && len(os.Args) != 3 {
		return fmt.Errorf("usage: compat-probe HOME [READ_SEQ]")
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
