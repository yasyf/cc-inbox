package cli_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/store"
)

type tailSet struct {
	seq int64
	set string
}

var tailVariants = []struct {
	name string
	sets []tailSet
}{
	{"clean", nil},
	{"late malformed recipients", []tailSet{{14, `recipients = '['`}}},
	{"early malformed recipients", []tailSet{{3, `recipients = '['`}}},
	{"stale malformed recipients", []tailSet{{3, `recipients = '[', at = 0`}}},
	{"origin early malformed recipients", []tailSet{{11, `recipients = '[', origin = 'feedfacefeedface'`}}},
	{"drive malformed recipients", []tailSet{{12, `recipients = '['`}}},
	{"late typed peer recipients", []tailSet{{14, `recipients = '["peer:reader", 1]'`}}},
	{"late scalar peer recipients", []tailSet{{14, `recipients = '"peer:reader"'`}}},
	{"late json5 peer recipients", []tailSet{{14, `recipients = '[''peer:reader'']'`}}},
	{"late jsonb peer recipients", []tailSet{{14, `recipients = jsonb('["peer:reader"]')`}}},
	{"late malformed jsonb recipients", []tailSet{{14, `recipients = X'1B0D'`}}},
	{"late reader json5", []tailSet{{14, `recipients = '[''api@d'']'`}}},
	{"drive malformed refs", []tailSet{{12, `refs = '{'`}}},
	{"early malformed also_resolves", []tailSet{{3, `also_resolves = '['`}}},
	{"late malformed also_resolves", []tailSet{{14, `also_resolves = '['`}}},
	{"drive typed also_resolves", []tailSet{{4, `also_resolves = '["oops"]'`}}},
	{"drive null also_resolves element", []tailSet{{4, `also_resolves = '[null]'`}}},
	{"offdrive spaced target", []tailSet{{11, `also_resolves = '[" 9"]'`}}},
	{"offdrive text target", []tailSet{{11, `also_resolves = '["9"]'`}}},
	{"offdrive bool target", []tailSet{{11, `also_resolves = '[true]', resolves = 0`}, {7, `resolves = 0`}}},
	{"offdrive real target", []tailSet{{10, `also_resolves = '[9.0, 9.5]'`}}},
	{"corrupt drive resolves", []tailSet{{4, `resolves = 'b'`}, {12, `resolves = 'a'`}}},
	{"open openers", []tailSet{{7, `resolves = 0`}, {10, `also_resolves = '[]'`}}},
}

var tailCalls = []struct {
	name    string
	session bool
	args    []string
}{
	{"json peer newest", true, []string{"tail", "--drive", "d", "--since", "36h", "-n", "3", "--json", "--budget", "300000000"}},
	{"json peer all", true, []string{"tail", "--drive", "d", "--since", "36h", "-n", "80000", "--json", "--budget", "300000000"}},
	{"json nonpeer all", false, []string{"tail", "--drive", "d", "--since", "36h", "-n", "80000", "--json", "--budget", "300000000"}},
	{"json peer tiny budget", true, []string{"tail", "--drive", "d", "--since", "36h", "-n", "80000", "--json", "--budget", "1"}},
	{"json peer small budget", true, []string{"tail", "--drive", "d", "--since", "36h", "-n", "80000", "--json", "--budget", "700"}},
	{"text peer", true, []string{"tail", "--drive", "d", "--since", "36h", "-n", "80000", "--budget", "300000000"}},
	{"json to api", false, []string{"tail", "--drive", "d", "--to", "api", "--since", "36h", "-n", "80000", "--json", "--budget", "300000000"}},
	{"json reader root", true, []string{"tail", "--drive", "d", "--reader", "root", "--since", "36h", "-n", "80000", "--json", "--budget", "300000000"}},
	{"json origin", true, []string{"tail", "--drive", "d", "--origin", "feedfacefeedface", "--since", "36h", "-n", "2", "--json", "--budget", "300000000"}},
	{"json peer cursor", true, []string{"tail", "--drive", "d", "--json", "--budget", "300000000"}},
}

func TestTailPreservesNativeOutputAndErrors(t *testing.T) {
	if os.Getenv("CCI_TAIL_SNAPSHOTS") == "" {
		t.Skip("CCI_TAIL_SNAPSHOTS names the immutable-baseline snapshot directory CI compares against")
	}
	for _, variant := range tailVariants {
		for _, call := range tailCalls {
			t.Run(variant.name+"/"+call.name, func(t *testing.T) {
				home := t.TempDir()
				t.Setenv("CCI_HOME", home)
				t.Setenv("CLAUDE_CODE_SESSION_ID", "")
				t.Setenv("CODEX_THREAD_ID", "")
				t.Setenv("CCI_AGENT_ID", "")
				st, err := store.Open(context.Background(), home)
				if err != nil {
					t.Fatal(err)
				}
				node := st.Self()
				if err := st.Close(); err != nil {
					t.Fatal(err)
				}
				db, err := sql.Open("sqlite", filepath.Join(home, store.DBFile))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
				seedTailFixture(t, db, node)
				for _, set := range variant.sets {
					if _, err := db.Exec("UPDATE records SET "+set.set+" WHERE seq = ?", set.seq); err != nil {
						t.Fatalf("%s: %v", set.set, err)
					}
				}
				if call.session {
					t.Setenv("CLAUDE_CODE_SESSION_ID", "reader")
				}
				out, err := run(t, call.args...)
				var failure string
				if err != nil {
					failure = strings.ReplaceAll(err.Error(), node, "<node>")
				}
				out = strings.ReplaceAll(out, node, "<node>")
				checkTailSnapshot(t, out, failure)
				t.Logf("output_lines=%d error=%q", strings.Count(out, "\n"), failure)
			})
		}
	}
}

func seedTailFixture(t *testing.T, db *sql.DB, node string) {
	t.Helper()
	const at = 4102444800000
	for i, row := range []struct {
		drive, lane, kind, recipients string
		also                          any
		resolves                      int64
		refs                          string
	}{
		{"d", "root", "ask", `["api"]`, nil, 0, `{}`},
		{"f", "api", "note", `["peer:reader"]`, nil, 0, `{}`},
		{"x", "irrelevant", "note", `["peer:other"]`, `[900001]`, 0, `{}`},
		{"d", "api", "note", `[]`, nil, 0, `{"stacks":["p/e"]}`},
		{"f", "api", "note", `["` + store.PeerAddressOn(node, "reader") + `"]`, nil, 0, `{}`},
		{"f", "api", "note", `["api@d"]`, nil, 0, `{}`},
		{"f", "api", "done", `[]`, nil, 1, `{}`},
		{"f", "api", "note", `["root@d"]`, nil, 0, `{}`},
		{"d", "api", "ask", `[]`, nil, 0, `{}`},
		{"f", "api", "done", `[]`, `[9]`, 4, `{}`},
		{"x", "irrelevant", "note", `["peer:other"]`, `[900002]`, 900003, `{}`},
		{"d", "root", "note", `[]`, nil, 0, `{}`},
		{"d", "root", "note", `["peer:reader"]`, nil, 0, `{"stacks":["p/e"]}`},
		{"x", "irrelevant", "note", `["someone"]`, nil, 0, `{}`},
		{"d", "api", "note", `[]`, nil, 0, `{}`},
	} {
		res, err := db.Exec(`INSERT INTO records (drive, lane, kind, at, text, recipients, also_resolves, resolves, refs, source, hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'post', ?)`,
			row.drive, row.lane, row.kind, at, fmt.Sprintf("fixture %d", i+1), row.recipients, row.also, row.resolves, row.refs, fmt.Sprintf("fixture-%d", i+1))
		if err != nil {
			t.Fatal(err)
		}
		if seq, err := res.LastInsertId(); err != nil || seq != int64(i+1) {
			t.Fatalf("fixture seq = %d, %v; want %d", seq, err, i+1)
		}
	}
	if _, err := db.Exec("UPDATE records SET origin = 'feedfacefeedface' WHERE seq >= 12"); err != nil {
		t.Fatal(err)
	}
}

func checkTailSnapshot(t *testing.T, output, failure string) {
	t.Helper()
	path := filepath.Join(os.Getenv("CCI_TAIL_SNAPSHOTS"), strings.NewReplacer("/", "_", " ", "-").Replace(t.Name())+".json")
	snapshot := struct {
		Output string
		Error  string
	}{output, failure}
	if os.Getenv("CCI_TAIL_SNAPSHOT_WRITE") == "1" {
		raw, err := json.Marshal(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
		return
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := snapshot
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	if snapshot != want {
		t.Fatalf("immutable baseline mismatch:\noutput got %q\nwant %q\nerror got %q\nwant %q", snapshot.Output, want.Output, snapshot.Error, want.Error)
	}
}
