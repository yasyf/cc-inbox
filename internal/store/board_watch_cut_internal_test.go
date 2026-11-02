package store

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

func TestBoardClosingCutSeesWriteBeforeDBSnapshot(t *testing.T) {
	ctx := t.Context()
	st, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	body := filepath.Join(t.TempDir(), "ruling")
	if err := os.WriteFile(body, []byte("unrelated prior body"), 0o600); err != nil {
		t.Fatal(err)
	}
	watch, err := newIncidentFileWatch()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = watch.Close() }()
	if err := watch.Register(body); err != nil {
		t.Fatal(err)
	}
	captured, err := os.ReadFile(body)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := watch.DrainBoard(ctx)
	if err != nil || len(entry) != 0 {
		t.Fatalf("entry cut=%v, %v", entry, err)
	}
	if err := os.WriteFile(body, []byte("close #123"), 0o600); err != nil {
		t.Fatal(err)
	}
	late, _, err := st.Post(ctx, Record{Drive: "d", Lane: "root", Kind: kinds.Note, Text: "later record #456"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := st.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	if _, err := reader.ExecContext(ctx, "BEGIN DEFERRED"); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = reader.ExecContext(ctx, "ROLLBACK") }()
	var selected int64
	if err := reader.QueryRowContext(ctx, "SELECT seq FROM records WHERE seq=?", late.Seq).Scan(&selected); err != nil {
		t.Fatal(err)
	}
	if selected != late.Seq || bytes.Contains(captured, []byte("#123")) {
		t.Fatal("counterexample did not retain old body and later DB record")
	}
	closing, err := watch.DrainBoard(ctx)
	if err != nil || !closing[body] {
		t.Fatalf("closing cut missed intervening body change: %v, %v", closing, err)
	}
}
