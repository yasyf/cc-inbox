package store

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

func postNote(ctx context.Context, t *testing.T, st *Store, text string) {
	t.Helper()
	if _, _, err := st.Post(ctx, Record{Drive: "d", Lane: "l", Kind: kinds.Note, Text: text}, 0); err != nil {
		t.Fatal(err)
	}
}

func TestANewStoreLogsOneRecordInKilobytePages(t *testing.T) {
	ctx := t.Context()
	st, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	postNote(ctx, t, st, "warms every tree a record touches")
	if done, err := st.checkpoint(ctx); err != nil || !done {
		t.Fatalf("checkpoint = %t, %v", done, err)
	}
	postNote(ctx, t, st, "one record, one commit")
	info, err := os.Stat(filepath.Join(st.HomeDir(), DBFile+"-wal"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() > 96<<10 {
		t.Fatalf("one record appended %d bytes to the log, want at most %d", info.Size(), 96<<10)
	}
	t.Logf("wal_bytes_per_record=%d", info.Size())
}

func TestAStoreBornWithFourKilobytePagesKeepsThemAndTakesWrites(t *testing.T) {
	ctx := t.Context()
	home := t.TempDir()
	st, err := Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	postNote(ctx, t, st, "written before the page size changed")
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := sql.Open("sqlite", "file:"+filepath.Join(home, DBFile))
	if err != nil {
		t.Fatal(err)
	}
	conn, err := raw.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{"PRAGMA journal_mode = DELETE", "PRAGMA page_size = 4096", "VACUUM", "PRAGMA journal_mode = WAL"} {
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	postNote(ctx, t, st, "written after reopening")
	if err := st.vacuumIncremental(ctx); err != nil {
		t.Fatal(err)
	}
	var pageSize, records int
	if err := st.db.QueryRowContext(ctx, "SELECT (SELECT page_size FROM pragma_page_size()), (SELECT COUNT(*) FROM records)").Scan(&pageSize, &records); err != nil {
		t.Fatal(err)
	}
	if pageSize != 4096 || records != 2 {
		t.Fatalf("page_size=%d records=%d, want 4096 and 2", pageSize, records)
	}
}
