package store_test

import (
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
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

func TestCollectKeepsACloserOfAnOpenerThatStays(t *testing.T) {
	st, clock := testutil.Store(t)
	lift := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Lift, Text: "lift", Topic: "stack-a"})
	clock.Advance(store.HotWindow + time.Hour)
	hold := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Hold, Text: "later hold", Topic: "stack-a"})
	before, err := st.OpenItems(t.Context(), "d")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Collect(t.Context(), noPause); err != nil {
		t.Fatal(err)
	}
	after, err := st.OpenItems(t.Context(), "d")
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) || !exists(t, st, lift.Seq) || !exists(t, st, hold.Seq) {
		t.Fatalf("open items %d then %d; lift kept %t, hold kept %t", len(before), len(after), exists(t, st, lift.Seq), exists(t, st, hold.Seq))
	}
}

func TestCollectKeepsAnIncidentAndItsUpdates(t *testing.T) {
	st, clock := testutil.Store(t)
	first := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Incident, Text: "down"})
	update := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Incident, Text: "still down", Re: first.Seq})
	testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.FixLive, Text: "fixed", Re: first.Seq})
	clock.Advance(store.HotWindow + time.Hour)
	if _, err := st.Collect(t.Context(), noPause); err != nil {
		t.Fatal(err)
	}
	if !exists(t, st, first.Seq) || !exists(t, st, update.Seq) {
		t.Fatalf("incident kept = %t, update kept = %t", exists(t, st, first.Seq), exists(t, st, update.Seq))
	}
	open, err := st.OpenItems(t.Context(), "d")
	if err != nil {
		t.Fatal(err)
	}
	if len(open) != 0 {
		t.Fatalf("open items = %+v, want none", open)
	}
}

func TestCollectKeepsAParentAFreshReplyNames(t *testing.T) {
	st, clock := testutil.Store(t)
	parent := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, Text: "root says"})
	sibling := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "old reply", Re: parent.Seq})
	clock.Advance(store.HotWindow + time.Hour)
	reply := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "fresh reply", Re: parent.Seq})
	if _, err := st.Collect(t.Context(), noPause); err != nil {
		t.Fatal(err)
	}
	if !exists(t, st, parent.Seq) || !exists(t, st, sibling.Seq) || !exists(t, st, reply.Seq) {
		t.Fatalf("parent %t, old reply %t, fresh reply %t, want all kept", exists(t, st, parent.Seq), exists(t, st, sibling.Seq), exists(t, st, reply.Seq))
	}
}

func TestCollectSparesWhatARecordArrivingDuringThePassNames(t *testing.T) {
	st, clock := testutil.Store(t)
	var first store.Record
	for i := range 1001 {
		note := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "note " + strconv.Itoa(i)})
		if i == 0 {
			first = note
		}
	}
	clock.Advance(store.HotWindow + time.Hour)
	testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "newest"})
	var reply store.Record
	res, err := st.Collect(t.Context(), func(ctx context.Context) error {
		if reply.Seq != 0 {
			return nil
		}
		var err error
		reply, _, err = st.Post(ctx, store.Record{Drive: "d", Lane: "a", Kind: kinds.Note, Text: "late reply", Re: first.Seq}, 0)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if !exists(t, st, first.Seq) || res.Archived != 1000 {
		t.Fatalf("named record kept = %t, archived = %d", exists(t, st, first.Seq), res.Archived)
	}
}

func TestCollectKeepsAnUnlandedPRAndForgetsALandedOne(t *testing.T) {
	st, clock := testutil.Store(t)
	landedPR := store.Refs{PRs: []store.PR{{Repo: "o/r", Number: 7}}}
	opened := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Opened, Text: "opened 7", Refs: landedPR})
	testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Landed, Text: "landed 7", Refs: landedPR})
	unlanded := testutil.Post(t, st, store.Record{Lane: "b", Kind: kinds.Opened, Text: "opened 8", Refs: store.Refs{PRs: []store.PR{{Repo: "o/r", Number: 8}}}})
	testutil.Post(t, st, store.Record{Lane: "b", Kind: kinds.Note, Text: "newer"})
	clock.Advance(40 * 24 * time.Hour)
	if _, err := st.Collect(t.Context(), noPause); err != nil {
		t.Fatal(err)
	}
	if exists(t, st, opened.Seq) || !exists(t, st, unlanded.Seq) {
		t.Fatalf("landed opened kept = %t, unlanded opened kept = %t", exists(t, st, opened.Seq), exists(t, st, unlanded.Seq))
	}
	for lane, want := range map[string]bool{"a": false, "b": true} {
		got, err := st.OpenPR(t.Context(), "d", lane)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("OpenPR(%s) = %t, want %t", lane, got, want)
		}
	}
}

func TestCollectKeepsALaneNewestRecord(t *testing.T) {
	st, clock := testutil.Store(t)
	older := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "older"})
	newest := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "newest"})
	clock.Advance(40 * 24 * time.Hour)
	if _, err := st.Collect(t.Context(), noPause); err != nil {
		t.Fatal(err)
	}
	if exists(t, st, older.Seq) || !exists(t, st, newest.Seq) {
		t.Fatalf("older kept = %t, newest kept = %t", exists(t, st, older.Seq), exists(t, st, newest.Seq))
	}
}

func TestCollectKeepsARecordWhoseExpiryIsAhead(t *testing.T) {
	st, clock := testutil.Store(t)
	note, _, err := st.Post(t.Context(), store.Record{Drive: "d", Lane: "a", Kind: kinds.Note, Text: "long lived"}, 30*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "newer"})
	clock.Advance(store.HotWindow + time.Hour)
	testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "newest"})
	if _, err := st.Collect(t.Context(), noPause); err != nil {
		t.Fatal(err)
	}
	if !exists(t, st, note.Seq) {
		t.Fatal("a record went before its expiry")
	}
}

func TestCollectRetiresASilentDriveWhole(t *testing.T) {
	st, clock := testutil.Store(t)
	ask := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Ask, Text: "nobody came back", To: []string{"root"}})
	clock.Advance(91 * 24 * time.Hour)
	if _, err := st.Collect(t.Context(), noPause); err != nil {
		t.Fatal(err)
	}
	if exists(t, st, ask.Seq) {
		t.Fatal("a drive silent for 90 days kept its open ask")
	}
}

func TestCollectKeepsTheNewestSpawnOfEachLane(t *testing.T) {
	st, clock := testutil.Store(t)
	first := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Spawn, Text: "spawn x", Refs: store.Refs{Lanes: []string{"x"}}})
	second := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Spawn, Text: "spawn y", Refs: store.Refs{Lanes: []string{"y"}}})
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, Text: "newer"})
	clock.Advance(40 * 24 * time.Hour)
	if _, err := st.Collect(t.Context(), noPause); err != nil {
		t.Fatal(err)
	}
	if !exists(t, st, first.Seq) || !exists(t, st, second.Seq) {
		t.Fatalf("spawn x kept = %t, spawn y kept = %t", exists(t, st, first.Seq), exists(t, st, second.Seq))
	}
}

func TestCollectAdvancesCompactedThroughForLocalRecords(t *testing.T) {
	st, clock := testutil.Store(t)
	note := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "local"})
	clock.Advance(store.HotWindow + time.Hour)
	testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "newer"})
	if _, err := st.Collect(t.Context(), noPause); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+filepath.Join(st.HomeDir(), store.DBFile))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var through int64
	if err := db.QueryRowContext(t.Context(), "SELECT value FROM meta WHERE key = 'compacted_through'").Scan(&through); err != nil {
		t.Fatal(err)
	}
	if through != note.Seq {
		t.Fatalf("compacted_through = %d, want %d", through, note.Seq)
	}
}

func TestCollectPublishesASegmentPerPass(t *testing.T) {
	st, clock := testutil.Store(t)
	for _, text := range []string{"first pass", "second pass", "third pass"} {
		testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: text})
		clock.Advance(store.HotWindow + time.Hour)
		if _, err := st.Collect(t.Context(), noPause); err != nil {
			t.Fatal(err)
		}
	}
	paths, err := filepath.Glob(filepath.Join(st.HomeDir(), "archive", "d-*.jsonl.gz"))
	if err != nil || len(paths) != 2 {
		t.Fatalf("segments = %v, %v", paths, err)
	}
}

func TestCollectKeepsTheNewestHeadForThirtyDays(t *testing.T) {
	st, clock := testutil.Store(t)
	older := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Head, Text: "older", Topic: "t"})
	newest := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Head, Text: "newest", Topic: "t"})
	testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "newer"})
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
	testutil.Post(t, st, store.Record{Lane: "elsewhere", Kind: kinds.Note, Text: "newer"})
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
	for i := range 600 {
		if posted := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: strings.Repeat("padding ", 40) + strconv.Itoa(i)}); posted.Seq != int64(i+1) {
			t.Fatalf("post %d landed at #%d", i, posted.Seq)
		}
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
	testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "newer"})
	if _, err := st.Collect(t.Context(), noPause); err != nil {
		t.Fatal(err)
	}
	long := strings.Repeat("a long drive name ", 40)
	testutil.Post(t, st, store.Record{Drive: long, Lane: "a", Kind: kinds.Note, Text: "from a drive named past a file name's limit"})
	clock.Advance(store.HotWindow + time.Hour)
	testutil.Post(t, st, store.Record{Drive: long, Lane: "a", Kind: kinds.Note, Text: "newer"})
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

func TestCollectArchivesABlobWithItsRecord(t *testing.T) {
	st, clock := testutil.Store(t)
	body := strings.Repeat("word ", store.MaxText)
	excerpt, path, err := st.Fit(body)
	if err != nil {
		t.Fatal(err)
	}
	testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: excerpt, Refs: store.Refs{Path: path}})
	clock.Advance(store.HotWindow + time.Hour)
	testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "newer"})
	if _, err := st.Collect(t.Context(), noPause); err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(st.HomeDir(), "archive", "d-*.jsonl.gz"))
	if err != nil || len(paths) != 1 {
		t.Fatalf("segments = %v, %v", paths, err)
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
	if line["blob"] != body {
		t.Fatalf("archived blob = %.40v, want the body", line["blob"])
	}
}

func TestCollectLeavesWhatAPeerHasNotAcknowledged(t *testing.T) {
	st, clock := testutil.Store(t)
	acked := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "acknowledged"})
	unacked := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "not acknowledged"})
	testutil.Post(t, st, store.Record{Lane: "b", Kind: kinds.Note, Text: "other lane"})
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
	excerpt, path, err := st.Fit(strings.Repeat("word ", store.MaxText))
	if err != nil {
		t.Fatal(err)
	}
	named := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Decide, Text: excerpt, Refs: store.Refs{Path: path}})
	queuedBody, queuedPath, err := st.Fit(strings.Repeat("queued ", store.MaxText))
	if err != nil {
		t.Fatal(err)
	}
	queue := filepath.Join(st.HomeDir(), store.UnsentDir)
	if err := os.MkdirAll(queue, 0o700); err != nil {
		t.Fatal(err)
	}
	write, err := json.Marshal(store.Write{Records: []store.Record{{Text: queuedBody, Refs: store.Refs{Path: queuedPath}}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(queue, "1.write"), write, 0o600); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(st.HomeDir(), "blobs")
	orphan := filepath.Join(dir, "orphan.txt")
	if err := os.WriteFile(orphan, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := clock.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(orphan, old, old); err != nil {
		t.Fatal(err)
	}
	for _, blob := range []string{named.Refs.Path, queuedPath} {
		if err := os.Chtimes(blob, old, old); err != nil {
			t.Fatal(err)
		}
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
	if _, err := os.Stat(queuedPath); err != nil {
		t.Errorf("queued blob removed: %v", err)
	}
}
