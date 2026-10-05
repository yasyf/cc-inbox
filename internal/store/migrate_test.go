package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
)

func TestMigratesSchemaOneStores(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "inbox.db"))
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		migrations[0],
		`INSERT INTO records (drive, lane, kind, at, text, refs, fields, source, hash) VALUES ('d', 'l', 'opened', 1, 'x', '{"pr":30001,"build":"https://b/1"}', '{"stack":[1,2],"env":"plat"}', 'post', 'h')`,
		`INSERT INTO imports (path, drive, lane, offset) VALUES ('/x.md', 'd', 'x', 3)`,
		"PRAGMA user_version = 1",
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	_ = db.Close()
	st, err := Open(ctx, dir)
	if err != nil {
		t.Fatalf("open schema-1 store: %v", err)
	}
	defer func() { _ = st.Close() }()
	r, err := st.Get(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(r.Refs.PRs, r.Refs.Builds, r.Fields.Envs, r.Resolves); got != "[1 2 30001] [https://b/1] [plat] 0" {
		t.Fatalf("migrated record = %s (refs %+v fields %+v)", got, r.Refs, r.Fields)
	}
	src, ok, err := st.Source(ctx, "/x.md")
	if err != nil || !ok || src.Inode != 0 || src.Offset != 3 {
		t.Fatalf("migrated source = %+v %v %v", src, ok, err)
	}
}
