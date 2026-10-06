package importer_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/importer"
	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/kinds"
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

func rotate(t *testing.T, inbox, archived, live string) {
	t.Helper()
	archive := filepath.Join(inbox+".archive", "2026-10-06.md")
	if err := os.MkdirAll(filepath.Dir(archive), 0o700); err != nil {
		t.Fatal(err)
	}
	write(t, archive, archived)
	staged := inbox + ".new"
	write(t, staged, live)
	if err := os.Rename(staged, inbox); err != nil {
		t.Fatal(err)
	}
}

func raw(t *testing.T, st *store.Store) func(stmts ...string) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(st.HomeDir(), "inbox.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return func(stmts ...string) {
		t.Helper()
		for _, stmt := range stmts {
			if _, err := db.ExecContext(context.Background(), stmt); err != nil {
				t.Fatalf("%s: %v", stmt, err)
			}
		}
	}
}

func lanes(t *testing.T, st *store.Store) string {
	t.Helper()
	all, err := st.Query(context.Background(), store.Filter{Drive: "drive", IncludeExpired: true})
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(all))
	for _, r := range all {
		got = append(got, r.Lane)
	}
	return strings.Join(got, " ")
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
	if all[0].Source != "import:"+inbox || len(all[0].Refs.PRs) != 1 || all[0].Refs.PRs[0] != 30001 {
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

func TestRefreshFollowsRenameRotationIntoArchives(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	dir := t.TempDir()
	inbox := filepath.Join(dir, "deploy-go.md")
	write(t, inbox, "GO root (9:00 PM PT) one\nGO root (9:01 PM PT) two\n")
	if _, err := importer.Import(ctx, st, inbox, "drive", ""); err != nil {
		t.Fatal(err)
	}
	appendTo(t, inbox, "GO root (9:02 PM PT) three, written before rotation and never imported\n")
	rotate(t, inbox, "GO root (9:00 PM PT) one\nGO root (9:01 PM PT) two\nGO root (9:02 PM PT) three, written before rotation and never imported\n", "GO root (9:30 PM PT) four, after rotation "+strings.Repeat("x", 200)+"\n")
	if _, err := importer.Refresh(ctx, st); err != nil {
		t.Fatal(err)
	}
	all, err := st.Query(ctx, store.Filter{Drive: "drive"})
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(all))
	for _, r := range all {
		got = append(got, r.Lane+":"+strings.Fields(r.Text)[0])
	}
	if want := "root:one root:two root:three, root:four,"; strings.Join(got, " ") != want {
		t.Fatalf("records = %v, want %s", got, want)
	}
	if results, err := importer.Refresh(ctx, st); err != nil || len(results) != 0 {
		t.Fatalf("second refresh = %+v, %v; want nothing to do", results, err)
	}
}

func TestDefaultLane(t *testing.T) {
	tests := map[string]string{
		"/inbox/deploy-go.md":                       "deploy-go",
		"/inbox/deploy-go.md.archive/2026-10-04.md": "deploy-go",
		"/inbox/runner.md":                          "runner",
	}
	for path, want := range tests {
		if got := importer.DefaultLane(path); got != want {
			t.Errorf("DefaultLane(%s) = %s, want %s", path, got, want)
		}
	}
}

func TestImportReparsesLinesFromAnOlderParser(t *testing.T) {
	ctx := context.Background()
	home := t.TempDir()
	st, err := store.Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	inbox := filepath.Join(t.TempDir(), "deploy-go.md")
	write(t, inbox, "DONE (1:44 AM PT) tooling-ccx-5: defect 14 fixed\nOPENED (1:45 AM PT) release-simplify-3: #30541 stack grants\nLANDED (1:46 AM PT) inference-delete-3: #30543 compacted away\n")
	if _, err := importer.Import(ctx, st, inbox, "drive", ""); err != nil {
		t.Fatal(err)
	}
	first, err := st.Get(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+filepath.Join(home, "inbox.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	for _, stmt := range []string{
		"UPDATE records SET lane = 'deploy-go', kind = 'note', at = at - 3600000",
		"DELETE FROM records WHERE text LIKE '%compacted away%'",
		"UPDATE imports SET parser = 0",
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	appendTo(t, inbox, "DONE (1:47 AM PT) tooling-ccx-5: defect 15 fixed\n")
	res, err := importer.Import(ctx, st, inbox, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if res.Reparsed != 2 || res.Inserted != 1 {
		t.Fatalf("import = %+v, want 2 reparsed and 1 new", res)
	}
	all, err := st.Query(ctx, store.Filter{Drive: "drive", IncludeExpired: true})
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(all))
	for _, r := range all {
		got = append(got, string(r.Kind)+" "+r.Lane)
	}
	if want := "done tooling-ccx-5|opened release-simplify-3|done tooling-ccx-5"; strings.Join(got, "|") != want {
		t.Fatalf("records = %s, want %s", strings.Join(got, "|"), want)
	}
	if kept := first.At.Add(-time.Hour); !all[0].At.Equal(kept) || all[0].ExpiresAt != nil {
		t.Fatalf("reparsed record at %v expires %v, want the stored %v and no expiry", all[0].At, all[0].ExpiresAt, kept)
	}
	if results, err := importer.Refresh(ctx, st); err != nil || len(results) != 0 {
		t.Fatalf("refresh after reparse = %+v, %v; want nothing to do", results, err)
	}
}

func TestImportReparseSplitsContinuationsAndFollowsRotation(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	inbox := filepath.Join(t.TempDir(), "deploy-go.md")
	body := "DONE (9:40 PM PT) lane-a: first\nDONE (9:41 PM PT) lane-b: second\n"
	write(t, inbox, body)
	if _, err := importer.Import(ctx, st, inbox, "drive", ""); err != nil {
		t.Fatal(err)
	}
	exec := raw(t, st)
	exec(
		"UPDATE records SET lane = 'deploy-go', text = 'first DONE (9:41 PM PT) lane-b: second' WHERE seq = 1",
		"DELETE FROM lines WHERE hash = (SELECT line_hash FROM records WHERE seq = 2)",
		"DELETE FROM records WHERE seq = 2",
		"UPDATE imports SET parser = 0",
	)
	res, err := importer.Import(ctx, st, inbox, "", "")
	if err != nil || res.Reparsed != 1 || res.Inserted != 1 {
		t.Fatalf("reparse = %+v, %v; want 1 reparsed and 1 split out", res, err)
	}
	all, err := st.Query(ctx, store.Filter{Drive: "drive", IncludeExpired: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 || all[0].Lane != "lane-a" || all[0].Text != "first" || all[1].Lane != "lane-b" || !all[1].At.Equal(all[0].At) {
		t.Fatalf("records = %+v, want lane-a first and the split lane-b at its parent's time", all)
	}
	exec("UPDATE records SET lane = 'deploy-go'", "UPDATE imports SET parser = 0")
	staged := inbox + ".new"
	write(t, staged, body)
	if err := os.Rename(staged, inbox); err != nil {
		t.Fatal(err)
	}
	if res, err = importer.Import(ctx, st, inbox, "", ""); err != nil || res.Reparsed != 2 || res.Inserted != 0 {
		t.Fatalf("rotated import = %+v, %v; want 2 reparsed, 0 new", res, err)
	}
	if all, err = st.Query(ctx, store.Filter{Drive: "drive", IncludeExpired: true}); err != nil || all[0].Lane != "lane-a" || all[1].Lane != "lane-b" {
		t.Fatalf("records after rotation = %+v, %v", all, err)
	}
}

func TestRotationNeverReappendsALineAReparseSkipped(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	inbox := filepath.Join(t.TempDir(), "deploy-go.md")
	body := "DONE (9:40 PM PT) lane-a: first\nDONE (9:41 PM PT) lane-b: second\n"
	write(t, inbox, body)
	if _, err := importer.Import(ctx, st, inbox, "drive", ""); err != nil {
		t.Fatal(err)
	}
	raw(t, st)(
		"UPDATE records SET lane = 'deploy-go', text = 'first DONE (9:41 PM PT) lane-b: sec…' WHERE seq = 1",
		"DELETE FROM lines WHERE hash = (SELECT line_hash FROM records WHERE seq = 2)",
		"DELETE FROM records WHERE seq = 2",
		"UPDATE imports SET parser = 0",
	)
	if res, err := importer.Import(ctx, st, inbox, "", ""); err != nil || res.Reparsed != 1 || res.Inserted != 0 {
		t.Fatalf("reparse = %+v, %v; want 1 reparsed and the truncated continuation skipped", res, err)
	}
	rotate(t, inbox, body, "DONE (9:50 PM PT) lane-c: third\n")
	if _, err := importer.Refresh(ctx, st); err != nil {
		t.Fatal(err)
	}
	if got := lanes(t, st); got != "lane-a lane-c" {
		t.Fatalf("lanes = %s, want lane-a lane-c with lane-b never re-appended from the archive", got)
	}
}

func TestRotationNeverReappendsACompactedLine(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	inbox := filepath.Join(t.TempDir(), "deploy-go.md")
	body := "DONE (9:40 PM PT) lane-a: first\nDONE (9:41 PM PT) lane-b: second\n"
	write(t, inbox, body)
	written := time.Date(2026, 10, 1, 23, 0, 0, 0, time.Local)
	if err := os.Chtimes(inbox, written, written); err != nil {
		t.Fatal(err)
	}
	if _, err := importer.Import(ctx, st, inbox, "drive", ""); err != nil {
		t.Fatal(err)
	}
	if res, err := st.Compact(ctx, "drive", 48*time.Hour); err != nil || res.Folded != 2 {
		t.Fatalf("compact = %+v, %v; want both records folded", res, err)
	}
	rotate(t, inbox, body, "DONE (9:50 PM PT) lane-c: third\n")
	if _, err := importer.Refresh(ctx, st); err != nil {
		t.Fatal(err)
	}
	if got := lanes(t, st); got != "cci lane-c" {
		t.Fatalf("lanes = %s, want the digest and lane-c with no folded line re-appended", got)
	}
}

func TestRereadRecordsLinesAnOlderBinaryInserted(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	dir := t.TempDir()
	body := "DONE (9:40 PM PT) lane-a: first\nDONE (9:41 PM PT) lane-b: second\n"
	written := time.Date(2026, 10, 1, 23, 0, 0, 0, time.Local)
	for _, name := range []string{"deploy-go.md", "copy-1.md", "copy-2.md"} {
		write(t, filepath.Join(dir, name), body)
		if err := os.Chtimes(filepath.Join(dir, name), written, written); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := importer.Import(ctx, st, filepath.Join(dir, "deploy-go.md"), "drive", ""); err != nil {
		t.Fatal(err)
	}
	raw(t, st)("DELETE FROM lines")
	if res, err := importer.Import(ctx, st, filepath.Join(dir, "copy-1.md"), "drive", ""); err != nil || res.Reparsed != 2 || res.Inserted != 0 {
		t.Fatalf("first copy = %+v, %v; want 2 reparsed, 0 new", res, err)
	}
	if _, err := st.Compact(ctx, "drive", 48*time.Hour); err != nil {
		t.Fatal(err)
	}
	if res, err := importer.Import(ctx, st, filepath.Join(dir, "copy-2.md"), "drive", ""); err != nil || res.Inserted != 0 {
		t.Fatalf("second copy = %+v, %v; want the reparse to have recorded both lines", res, err)
	}
}

func TestImportedDeploymentBlocksStayOpenUntilClosedByRef(t *testing.T) {
	st, c := testutil.Store(t)
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "deploy-go.md")
	write(t, path, strings.Join([]string{
		"HOLD merge-walker (10:07 PM PT) stack:tunnel/tnt-usw2-1frg9c7 has 21 creates on HSBC",
		"DEFECT merge-walker-r2 (10:08 PM PT) -> root: storage/tnt-usw2-buckets plans 22 creates",
		"DEFECT slack-release-sweep-8 (10:09 PM PT) target:api preview red on every plan",
		"HOLD root (10:10 PM PT) names no deployment",
	}, "\n")+"\n")
	written := time.Date(2026, 10, 4, 23, 0, 0, 0, time.Local)
	open := func() string {
		t.Helper()
		if err := os.Chtimes(path, written, written); err != nil {
			t.Fatal(err)
		}
		if _, err := importer.Import(ctx, st, path, "drive", ""); err != nil {
			t.Fatal(err)
		}
		v, err := inbox.Digest(ctx, st, "drive", c.Now().Add(-48*time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		blocks := append(v.Holds, v.Defects...)
		got := make([]string, 0, len(blocks))
		for _, r := range blocks {
			got = append(got, string(r.Kind)+" "+r.Lane)
		}
		return strings.Join(got, ",")
	}
	if got := open(); got != "hold merge-walker,defect merge-walker-r2,defect slack-release-sweep-8" {
		t.Fatalf("open = %s", got)
	}
	appendTo(t, path, "FIX-LIVE merge-walker-r2 (10:20 PM PT) storage/tnt-usw2-buckets import landed\nLIFT root (10:21 PM PT) stack:tunnel/tnt-usw2-1frg9c7 creates approved\n")
	if got := open(); got != "defect slack-release-sweep-8" {
		t.Fatalf("open after closers = %s", got)
	}
}

func TestDigestNamesImportedHoldsNoRefCanClose(t *testing.T) {
	st, c := testutil.Store(t)
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "deploy-go.md")
	write(t, path, strings.Join([]string{
		"HOLD merge-walker (10:07 PM PT) stack:tunnel/tnt-usw2-1frg9c7 has 21 creates on HSBC",
		"HOLD reco-box-accounts (10:08 PM PT) tunnel/T apply waits on the box bake",
		"HOLD reco-target (10:09 PM PT) reco-box-1 replace pending",
	}, "\n")+"\n")
	written := time.Date(2026, 10, 4, 23, 0, 0, 0, time.Local)
	if err := os.Chtimes(path, written, written); err != nil {
		t.Fatal(err)
	}
	if _, err := importer.Import(ctx, st, path, "drive", ""); err != nil {
		t.Fatal(err)
	}
	v, err := inbox.Digest(ctx, st, "drive", c.Now().Add(-48*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Holds) != 1 || v.Holds[0].Lane != "merge-walker" {
		t.Fatalf("open holds = %+v", v.Holds)
	}
	var out bytes.Buffer
	v.Write(&out, c.Now(), 0, 0)
	want := fmt.Sprintf("untracked holds (2; name stack:<project>/<env> or target:<name> to track them): #%d reco-target, #%d reco-box-accounts", v.MaxSeq, v.MaxSeq-1)
	if !strings.Contains(out.String(), want) {
		t.Fatalf("digest missing %q:\n%s", want, out.String())
	}
}

func TestDigestDropsUntrackedHoldsAResolverCloses(t *testing.T) {
	st, c := testutil.Store(t)
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "deploy-go.md")
	write(t, path, strings.Join([]string{
		"HOLD merge-walker (10:07 PM PT) plan #1234 waits on the walker",
		"HOLD slack-release-sweep-7 (10:08 PM PT) 7-box re-roll waits on the owner",
		"HOLD slack-release-sweep-8 (10:09 PM PT) reco-e2e rerun re-rolls all 7 boxes",
	}, "\n")+"\n")
	written := time.Date(2026, 10, 4, 23, 0, 0, 0, time.Local)
	if err := os.Chtimes(path, written, written); err != nil {
		t.Fatal(err)
	}
	if _, err := importer.Import(ctx, st, path, "drive", ""); err != nil {
		t.Fatal(err)
	}
	since := c.Now().Add(-48 * time.Hour)
	v, err := inbox.Digest(ctx, st, "drive", since)
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Untracked) != 3 {
		t.Fatalf("untracked before resolvers = %+v", v.Untracked)
	}
	testutil.Post(t, st, store.Record{Drive: "drive", Lane: "unblock-sweep", Kind: kinds.Done, Text: "stale walker hold", Resolves: v.Untracked[0].Seq})
	testutil.Post(t, st, store.Record{Drive: "drive", Lane: "unblock-sweep", Kind: kinds.Lift, Text: "stale sweep-7 hold", Resolves: v.Untracked[1].Seq})
	testutil.Post(t, st, store.Record{Drive: "drive", Lane: "unblock-sweep", Kind: kinds.Lift, Text: "stale sweep-8 hold", Re: v.Untracked[2].Seq})
	if v, err = inbox.Digest(ctx, st, "drive", since); err != nil {
		t.Fatal(err)
	}
	if len(v.Untracked) != 0 {
		t.Fatalf("untracked after resolvers = %+v", v.Untracked)
	}
	var out bytes.Buffer
	v.Write(&out, c.Now(), 0, 0)
	if strings.Contains(out.String(), "untracked holds") {
		t.Fatalf("digest still lists untracked holds:\n%s", out.String())
	}
}
