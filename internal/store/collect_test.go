package store_test

import (
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func noPause(context.Context) error { return nil }

func exists(t *testing.T, st *store.Store, seq int64) bool {
	t.Helper()
	found, err := st.RecordsAmong(t.Context(), "d", []int64{seq})
	if err != nil {
		t.Fatal(err)
	}
	return len(found) == 1
}

func TestCollectDeletesOnlyWhatNothingDependsOn(t *testing.T) {
	st, clock := testutil.Store(t)
	stale := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "old note"})
	openAsk := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Ask, Text: "still open", To: []string{"root"}})
	answered := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Ask, Text: "answered", To: []string{"root"}})
	answer := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Answer, Text: "yes", Re: answered.Seq})
	hold := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Hold, Text: "hold", Refs: store.Refs{Targets: []string{"api"}}})
	clock.Advance(store.HotWindow + time.Hour)
	recentAsk := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Ask, Text: "recent answered", To: []string{"root"}})
	expiringCloser := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, Text: "closes the recent ask", Resolves: recentAsk.Seq})
	replacedState := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.State, Text: "first", Topic: "t"})
	latestState := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.State, Text: "second", Topic: "t"})
	clock.Advance(5 * 24 * time.Hour)
	fresh := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "fresh note"})
	res, err := st.Collect(t.Context(), noPause)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []struct {
		name string
		seq  int64
		kept bool
	}{
		{"note past the window", stale.Seq, false},
		{"open ask past the window", openAsk.Seq, true},
		{"answered ask past the window", answered.Seq, false},
		{"answer to a deleted ask", answer.Seq, false},
		{"open hold past the window", hold.Seq, true},
		{"recent opener", recentAsk.Seq, true},
		{"expired closer of a kept opener", expiringCloser.Seq, true},
		{"expired state another replaced", replacedState.Seq, false},
		{"expired newest state of its topic", latestState.Seq, true},
		{"fresh note", fresh.Seq, true},
	} {
		if got := exists(t, st, want.seq); got != want.kept {
			t.Errorf("%s #%d kept = %t, want %t", want.name, want.seq, got, want.kept)
		}
	}
	if res.Archived != 4 {
		t.Errorf("Archived = %d, want 4", res.Archived)
	}
	open, err := st.OpenItems(t.Context(), "d")
	if err != nil {
		t.Fatal(err)
	}
	if len(open) != 2 {
		t.Errorf("open items = %+v, want the open ask and the hold", open)
	}
	again, err := st.Collect(t.Context(), noPause)
	if err != nil {
		t.Fatal(err)
	}
	if again.Archived != 0 {
		t.Errorf("second pass archived %d, want 0", again.Archived)
	}
}

func TestCollectKeepsTheNewestHeadForThirtyDays(t *testing.T) {
	st, clock := testutil.Store(t)
	older := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Head, Text: "older", Topic: "t"})
	newest := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Head, Text: "newest", Topic: "t"})
	clock.Advance(store.HotWindow + time.Hour)
	if _, err := st.Collect(t.Context(), noPause); err != nil {
		t.Fatal(err)
	}
	if exists(t, st, older.Seq) || !exists(t, st, newest.Seq) {
		t.Fatalf("older kept = %t, newest kept = %t", exists(t, st, older.Seq), exists(t, st, newest.Seq))
	}
	clock.Advance(20 * 24 * time.Hour)
	if _, err := st.Collect(t.Context(), noPause); err != nil {
		t.Fatal(err)
	}
	if exists(t, st, newest.Seq) {
		t.Fatal("the newest head outlived its 30 days")
	}
}

func TestCollectDoesNotLetAForeignRowReturn(t *testing.T) {
	st, clock := testutil.Store(t)
	row := store.Row{Origin: "feedfacefeedface", OriginSeq: 7, Drive: "d", Lane: "elsewhere", Kind: kinds.Note, At: clock.Now(), Text: "from another node", Source: "post"}
	if _, err := st.Apply(t.Context(), []store.Row{row}, nil); err != nil {
		t.Fatal(err)
	}
	clock.Advance(store.HotWindow + time.Hour)
	res, err := st.Collect(t.Context(), noPause)
	if err != nil {
		t.Fatal(err)
	}
	if res.Archived != 1 {
		t.Fatalf("Archived = %d, want 1", res.Archived)
	}
	applied, err := st.Apply(t.Context(), []store.Row{row}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if applied.Duplicate != 1 {
		t.Fatalf("second Apply = %+v, want a duplicate", applied)
	}
}

func TestCollectReturnsSpaceToTheFileSystem(t *testing.T) {
	st, clock := testutil.Store(t)
	for range 600 {
		testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: strings.Repeat("padding ", 40)})
	}
	clock.Advance(store.HotWindow + time.Hour)
	res, err := st.Collect(t.Context(), noPause)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Vacuumed {
		t.Fatalf("Collect = %+v, want the vacuum that turns on incremental mode", res)
	}
	db, err := sql.Open("sqlite", "file:"+filepath.Join(st.HomeDir(), store.DBFile))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var mode, free int
	if err := db.QueryRowContext(t.Context(), "SELECT (SELECT auto_vacuum FROM pragma_auto_vacuum), (SELECT freelist_count FROM pragma_freelist_count)").Scan(&mode, &free); err != nil {
		t.Fatal(err)
	}
	if mode != 2 || free != 0 {
		t.Fatalf("auto_vacuum = %d, free pages = %d; want 2 and 0", mode, free)
	}
	testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "after the vacuum"})
}

func TestCollectArchivesEveryRecordItDeletes(t *testing.T) {
	st, clock := testutil.Store(t)
	note := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "keep a copy of this"})
	clock.Advance(store.HotWindow + time.Hour)
	if _, err := st.Collect(t.Context(), noPause); err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(st.HomeDir(), "archive", "d-*.jsonl.gz"))
	if err != nil || len(paths) != 1 {
		t.Fatalf("archives = %v, %v", paths, err)
	}
	f, err := os.Open(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	var line map[string]any
	if err := json.NewDecoder(gz).Decode(&line); err != nil {
		t.Fatal(err)
	}
	if line["text"] != "keep a copy of this" || line["seq"] != float64(note.Seq) {
		t.Fatalf("archived line = %v", line)
	}
}

func TestCollectLeavesWhatAPeerHasNotAcknowledged(t *testing.T) {
	st, clock := testutil.Store(t)
	acked := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "acknowledged"})
	unacked := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "not acknowledged"})
	if err := st.Ack(t.Context(), "peer", acked.Seq); err != nil {
		t.Fatal(err)
	}
	clock.Advance(store.HotWindow + time.Hour)
	if _, err := st.Collect(t.Context(), noPause); err != nil {
		t.Fatal(err)
	}
	if exists(t, st, acked.Seq) || !exists(t, st, unacked.Seq) {
		t.Fatalf("acked kept = %t, unacked kept = %t", exists(t, st, acked.Seq), exists(t, st, unacked.Seq))
	}
}

func TestSweepFilesKeepsNamedAndFreshBlobs(t *testing.T) {
	st, clock := testutil.Store(t)
	long := strings.Repeat("word ", store.MaxText)
	named := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Decide, Text: long})
	dir := filepath.Join(st.HomeDir(), "blobs")
	orphan := filepath.Join(dir, "orphan.txt")
	if err := os.WriteFile(orphan, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := clock.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(orphan, old, old); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(named.Refs.Path, old, old); err != nil {
		t.Fatal(err)
	}
	blobs, _, err := st.SweepFiles(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if blobs != 1 {
		t.Errorf("swept %d blobs, want 1", blobs)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Errorf("orphan blob survived: %v", err)
	}
	if _, err := os.Stat(named.Refs.Path); err != nil {
		t.Errorf("named blob removed: %v", err)
	}
}
