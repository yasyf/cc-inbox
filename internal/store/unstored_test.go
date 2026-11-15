package store_test

import (
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/cc-inbox/internal/store"
)

func TestAWriteACorruptStoreFailsIsUnstoredAndOneBreakingAConstraintIsNot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "torn.db")
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=page_size(4096)")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), "CREATE TABLE records(id TEXT UNIQUE); INSERT INTO records VALUES ('a')"); err != nil {
		t.Fatal(err)
	}
	_, broken := db.ExecContext(t.Context(), "INSERT INTO records VALUES ('a')")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(bytes.Repeat([]byte{0xff}, 4096), 4096); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if db, err = sql.Open("sqlite", "file:"+path); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	_, torn := db.ExecContext(t.Context(), "INSERT INTO records VALUES ('b')")

	if torn == nil || !store.Unstored(fmt.Errorf("insert record: %w", torn)) {
		t.Fatalf("a write the corrupt store failed = %v; want it unstored, so its post is kept and resent", torn)
	}
	if broken == nil || store.Unstored(fmt.Errorf("insert record: %w", broken)) {
		t.Fatalf("a write breaking a constraint = %v; want it refused for what it holds, never resent", broken)
	}
}
