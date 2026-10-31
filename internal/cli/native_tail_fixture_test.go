package cli_test

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/yasyf/cc-inbox/internal/store"
)

type nativeTailSet struct {
	seq int64
	set string
}

var nativeTailVariants = []struct {
	name string
	sets []nativeTailSet
}{
	{"clean", nil},
	{"late malformed recipients", []nativeTailSet{{14, `recipients = '['`}}},
	{"early malformed recipients", []nativeTailSet{{3, `recipients = '['`}}},
	{"stale malformed recipients", []nativeTailSet{{3, `recipients = '[', at = 0`}}},
	{"origin early malformed recipients", []nativeTailSet{{11, `recipients = '[', origin = 'feedfacefeedface'`}}},
	{"drive malformed recipients", []nativeTailSet{{12, `recipients = '['`}}},
	{"late typed peer recipients", []nativeTailSet{{14, `recipients = '["peer:reader", 1]'`}}},
	{"late scalar peer recipients", []nativeTailSet{{14, `recipients = '"peer:reader"'`}}},
	{"late json5 peer recipients", []nativeTailSet{{14, `recipients = '[''peer:reader'']'`}}},
	{"late jsonb peer recipients", []nativeTailSet{{14, `recipients = jsonb('["peer:reader"]')`}}},
	{"late malformed jsonb recipients", []nativeTailSet{{14, `recipients = X'1B0D'`}}},
	{"late reader json5", []nativeTailSet{{14, `recipients = '[''api@d'']'`}}},
	{"drive malformed refs", []nativeTailSet{{12, `refs = '{'`}}},
	{"early malformed also_resolves", []nativeTailSet{{3, `also_resolves = '['`}}},
	{"late malformed also_resolves", []nativeTailSet{{14, `also_resolves = '['`}}},
	{"drive typed also_resolves", []nativeTailSet{{4, `also_resolves = '["oops"]'`}}},
	{"drive null also_resolves element", []nativeTailSet{{4, `also_resolves = '[null]'`}}},
	{"offdrive spaced target", []nativeTailSet{{11, `also_resolves = '[" 9"]'`}}},
	{"offdrive text target", []nativeTailSet{{11, `also_resolves = '["9"]'`}}},
	{"offdrive bool target", []nativeTailSet{{11, `also_resolves = '[true]', resolves = 0`}, {7, `resolves = 0`}}},
	{"offdrive real target", []nativeTailSet{{10, `also_resolves = '[9.0, 9.5]'`}}},
	{"corrupt drive resolves", []nativeTailSet{{4, `resolves = 'b'`}, {12, `resolves = 'a'`}}},
	{"open openers", []nativeTailSet{{7, `resolves = 0`}, {10, `also_resolves = '[]'`}}},
}

var nativeTailBaseCalls = []struct {
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

func seedNativeTailFixture(t *testing.T, db *sql.DB, node string) {
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
	if _, err := db.Exec("UPDATE records SET origin = 'feedfacefeedface', origin_seq = seq WHERE seq >= 12"); err != nil {
		t.Fatal(err)
	}
}
