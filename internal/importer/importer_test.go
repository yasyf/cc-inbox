package importer_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/importer"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func write(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func appendTo(t *testing.T, path, body string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.WriteString(body); err != nil {
		t.Fatal(err)
	}
}

func TestImportIsIncrementalAndSurvivesRotation(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	dir := t.TempDir()
	inbox := filepath.Join(dir, "landing-desk.md")
	write(t, inbox, "OPENED lane-a (9:00 PM PT) #30001 first\nLANDED lane-a (9:05 PM PT) #30001 landed\n")
	res, err := importer.Import(ctx, st, inbox, "drive", "")
	if err != nil {
		t.Fatal(err)
	}
	if res.Entries != 2 || res.Inserted != 2 {
		t.Fatalf("first import = %+v", res)
	}
	appendTo(t, inbox, "GO root (9:10 PM PT) ship it\npartial line without newline")
	results, err := importer.Refresh(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Entries != 1 || results[0].Inserted != 1 {
		t.Fatalf("refresh after append = %+v", results)
	}
	if results, err = importer.Refresh(ctx, st); err != nil || len(results) != 1 || results[0].Entries != 0 {
		t.Fatalf("refresh with only a partial line = %+v, %v", results, err)
	}
	archive := filepath.Join(dir, "landing-desk.2026-10-04.md")
	write(t, archive, "OPENED lane-a (9:00 PM PT) #30001 first\nLANDED lane-a (9:05 PM PT) #30001 landed\nGO root (9:10 PM PT) ship it\n")
	write(t, inbox, "STATE lane-b (9:20 PM PT) green after rotation\n")
	if results, err = importer.Refresh(ctx, st); err != nil || len(results) != 1 || results[0].Inserted != 1 {
		t.Fatalf("refresh after rotation = %+v, %v", results, err)
	}
	if res, err = importer.Import(ctx, st, archive, "drive", "landing-desk"); err != nil || res.Entries != 3 || res.Inserted != 0 {
		t.Fatalf("archive import = %+v, %v; want 3 entries, 0 new", res, err)
	}
	all, err := st.Query(ctx, store.Filter{Drive: "drive", IncludeExpired: true})
	if err != nil {
		t.Fatal(err)
	}
	lines := make([]string, 0, len(all))
	for _, r := range all {
		lines = append(lines, string(r.Kind)+" "+r.Lane+" "+r.Text)
	}
	want := "opened lane-a #30001 first|landed lane-a #30001 landed|go root ship it|state lane-b green after rotation"
	if got := strings.Join(lines, "|"); got != want {
		t.Fatalf("records = %s\nwant      %s", got, want)
	}
	if all[0].Source != "import:"+inbox || all[0].Refs.PR != 30001 {
		t.Fatalf("first record = %+v", all[0])
	}
}

func TestImportStoresLongLinesAsBlobs(t *testing.T) {
	st, _ := testutil.Store(t)
	path := filepath.Join(t.TempDir(), "deploy-go.md")
	long := "GO root (9:00 PM PT) " + strings.Repeat("word ", 200)
	write(t, path, long+"\n")
	if _, err := importer.Import(context.Background(), st, path, "drive", ""); err != nil {
		t.Fatal(err)
	}
	all, err := st.Query(context.Background(), store.Filter{Drive: "drive"})
	if err != nil {
		t.Fatal(err)
	}
	r := all[0]
	if n := len([]rune(r.Text)); n != store.MaxText || !strings.HasSuffix(r.Text, "…") {
		t.Fatalf("text has %d runes: %q", n, r.Text)
	}
	body, err := os.ReadFile(r.Refs.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(body), "word word") || len(body) < 900 {
		t.Fatalf("blob = %q", body)
	}
}
