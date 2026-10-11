package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/version"
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

func TestNewerSchemaNamesTheUpgradeCommand(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "inbox.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", len(migrations)+1)); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	_, err = Open(ctx, dir)
	want := fmt.Sprintf("store schema %d is newer than this cci %s (schema %d); upgrade it with: %s", len(migrations)+1, version.String(), len(migrations), version.UpgradeCommand())
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("open newer store error = %v, want %q", err, want)
	}
}

func TestMigratesSessionsAndClaimsToAgentKeys(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "inbox.db"))
	if err != nil {
		t.Fatal(err)
	}
	stmts := append([]string{}, migrations[:9]...)
	stmts = append(stmts,
		`INSERT INTO sessions (session, drive, root, lane, role, pid, pid_started) VALUES ('s', 'd', 1, '', 'root', 42, 7)`,
		`INSERT INTO claims (session, rel, kind, key, at) VALUES ('s', 'owns', 'pr', '1', 5)`,
		"PRAGMA user_version = 9",
	)
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	_ = db.Close()
	st, err := Open(ctx, dir)
	if err != nil {
		t.Fatalf("open schema-9 store: %v", err)
	}
	defer func() { _ = st.Close() }()
	p, err := st.Peer(ctx, PeerKey{Session: "s"})
	if err != nil || p.Agent != "" || p.Drive != "d" || !p.Root || p.PID != 42 {
		t.Fatalf("migrated peer = %+v, %v", p, err)
	}
	claims, err := st.Claims(ctx, ClaimFilter{Kind: "pr", Key: "1"})
	if err != nil || len(claims) != 1 || claims[0].Peer() != (PeerKey{Session: "s"}) {
		t.Fatalf("migrated claims = %+v, %v", claims, err)
	}
	if _, err := st.Announce(ctx, Peer{Session: "s", Agent: "a1"}); err != nil {
		t.Fatalf("announce an agent beside a migrated root: %v", err)
	}
}

func TestMigratesSchemaNineRecordsToThisOrigin(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "inbox.db"))
	if err != nil {
		t.Fatal(err)
	}
	stmts := append(append([]string{}, migrations[:9]...),
		`INSERT INTO records (drive, lane, kind, at, text, source, hash) VALUES ('d', 'root', 'ask', 1, 'q', 'post', 'h1')`,
		`INSERT INTO records (drive, lane, kind, at, text, re, resolves, source, hash) VALUES ('d', 'api', 'answer', 2, 'a', 1, 1, 'post', 'h2')`,
		`INSERT INTO sessions (session, drive, root) VALUES ('s', 'd', 1)`,
		"PRAGMA user_version = 9")
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	_ = db.Close()
	st, err := Open(ctx, dir)
	if err != nil {
		t.Fatalf("open schema-9 store: %v", err)
	}
	defer func() { _ = st.Close() }()
	rows, err := st.Export(ctx, st.Self(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := Pointer{Origin: st.Self(), Seq: 1}
	if len(rows) != 2 || rows[1].OriginSeq != 2 || rows[1].Re == nil || *rows[1].Re != want || *rows[1].Resolves != want {
		t.Fatalf("migrated rows = %+v", rows)
	}
	if b, ok, err := st.Binding(ctx, PeerKey{Session: "s"}); err != nil || !ok || !b.Root {
		t.Fatalf("migrated binding = %+v %v %v", b, ok, err)
	}
	next, _, err := st.Post(ctx, Record{Drive: "d", Lane: "api", Kind: kinds.Answer, Text: "again", Re: 1}, 0)
	if err != nil {
		t.Fatal(err)
	}
	rows, err = st.Export(ctx, st.Self(), 2, 0)
	if err != nil || len(rows) != 1 || rows[0].OriginSeq != next.Seq || *rows[0].Re != want {
		t.Fatalf("post-migration export = %+v, %v", rows, err)
	}
}
