package store

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

func TestIncidentHoldsTracksNativeBodyChangesWithoutWarmReads(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	path := filepath.Join(t.TempDir(), "bodies", "incident.md")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("body #29020"), 0o600); err != nil {
		t.Fatal(err)
	}
	record, _, err := st.Post(ctx, Record{Drive: "d", Lane: "root", Kind: kinds.Incident, Text: "summary #17", Source: "post", Refs: Refs{Path: path, PRs: []PR{{Number: 29000}}}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if report, err := st.ProvisionIncidentScope(ctx, "d"); err != nil || !report.Ready {
		t.Fatalf("provision=%+v, %v", report, err)
	}
	index, err := OpenIncidentIndex(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = index.Close() })
	check := func(want ...string) {
		t.Helper()
		got, err := index.Holds(ctx, st, "d", []string{"29020", "29021", "29022", "29000", "17"})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(want) {
			t.Fatalf("holds=%v, want %v", got, want)
		}
		for _, pr := range want {
			if got[pr].Seq != record.Seq || got[pr].Status != "open" {
				t.Fatalf("%s=%+v", pr, got[pr])
			}
		}
	}
	check("29020", "29000")
	before := index.Stats()
	var changesBefore, changesAfter int64
	st.db.SetMaxOpenConns(1)
	if err := st.db.QueryRowContext(ctx, "SELECT total_changes()").Scan(&changesBefore); err != nil {
		t.Fatal(err)
	}
	check("29020", "29000")
	check("29020", "29000")
	if err := st.db.QueryRowContext(ctx, "SELECT total_changes()").Scan(&changesAfter); err != nil {
		t.Fatal(err)
	}
	after := index.Stats()
	if before != after || changesBefore != changesAfter {
		t.Fatalf("warm work before=%+v after=%+v changes=%d/%d", before, after, changesBefore, changesAfter)
	}
	if err := os.WriteFile(path, []byte("body #29021"), 0o600); err != nil {
		t.Fatal(err)
	}
	check("29021", "29000")
	replacement := filepath.Join(filepath.Dir(path), "replacement")
	if err := os.WriteFile(replacement, []byte("body #29022"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, path); err != nil {
		t.Fatal(err)
	}
	check("29022", "29000")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Dir(path)); err != nil {
		t.Fatal(err)
	}
	check("17", "29000")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("body #29020"), 0o600); err != nil {
		t.Fatal(err)
	}
	check("29020", "29000")
	invalid := append([]byte("#29021"+strings.Repeat(" ", 70<<10)), 0xff)
	if err := os.WriteFile(path, invalid, 0o600); err != nil {
		t.Fatal(err)
	}
	check("17", "29000")
	if _, _, err := st.Post(ctx, Record{Drive: "d", Lane: "root", Kind: kinds.Note, Text: "resolved", Resolves: record.Seq, Source: "post"}, 0); err != nil {
		t.Fatal(err)
	}
	check()
}

func TestIncidentWordsMatchesUnicodeBoundariesAndDiscardsLateInvalidText(t *testing.T) {
	for _, text := range []string{"#42", "#42!", "#42\u0301"} {
		words, valid, err := incidentWords(context.Background(), strings.NewReader(text))
		if err != nil || !valid || !words["42"] {
			t.Fatalf("%q=%v/%v/%v", text, words, valid, err)
		}
	}
	for _, text := range []string{"#42é", "#42界", "#42²", "#42_"} {
		words, valid, err := incidentWords(context.Background(), strings.NewReader(text))
		if err != nil || !valid || words["42"] {
			t.Fatalf("%q=%v/%v/%v", text, words, valid, err)
		}
	}
	words, valid, err := incidentWords(context.Background(), strings.NewReader("#42 "+strings.Repeat("x", 70<<10)+"\xff"))
	if err != nil || valid || len(words) != 0 {
		t.Fatalf("invalid suffix=%v/%v/%v", words, valid, err)
	}
}

func TestIncidentHoldsUsesCurrentExpiryAndCanonicalRevisions(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	record, _, err := st.Post(ctx, Record{Drive: "d", Lane: "root", Kind: kinds.Incident, Text: "hold #42", Source: "post"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.ProvisionIncidentScope(ctx, "d"); err != nil {
		t.Fatal(err)
	}
	index, err := OpenIncidentIndex(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = index.Close() })
	if _, err := st.db.ExecContext(ctx, "UPDATE records SET text='hold #43' WHERE seq=?", record.Seq); err != nil {
		t.Fatal(err)
	}
	got, err := index.Holds(ctx, st, "d", []string{"42", "43"})
	if err != nil || len(got) != 1 || got["43"].Seq != record.Seq {
		t.Fatalf("revision=%v, %v", got, err)
	}
	if _, err := st.db.ExecContext(ctx, "UPDATE records SET expires_at=1 WHERE seq=?", record.Seq); err != nil {
		t.Fatal(err)
	}
	got, err = index.Holds(ctx, st, "d", []string{"43"})
	if err != nil || len(got) != 0 {
		t.Fatalf("expiry=%v, %v", got, err)
	}
	if _, err := st.db.ExecContext(ctx, "DELETE FROM records WHERE seq=?", record.Seq); err != nil {
		t.Fatal(err)
	}
	got, err = index.Holds(ctx, st, "d", []string{"43"})
	if err != nil || len(got) != 0 {
		t.Fatalf("deletion=%v, %v", got, err)
	}
}
