package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

func TestOpenRetriesAStoreAnotherWriterHoldsPastTheBusyTimeout(t *testing.T) {
	ctx := context.Background()
	home := t.TempDir()
	st, err := Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	_ = st.Close()
	raw, err := sql.Open("sqlite", "file:"+filepath.Join(home, DBFile))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	conn, err := raw.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "PRAGMA journal_mode = DELETE"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	release := time.AfterFunc(2500*time.Millisecond, func() {
		_, _ = conn.ExecContext(ctx, "COMMIT")
		_ = conn.Close()
	})
	t.Cleanup(func() { release.Stop() })
	other, err := Open(ctx, home)
	if err != nil {
		t.Fatalf("a writer that held the store past the busy timeout failed the open: %v", err)
	}
	_ = other.Close()
}
