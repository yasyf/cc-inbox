package store_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func noPace(context.Context) error { return nil }

func sameRecords(t *testing.T, got, want []store.Record) {
	t.Helper()
	a, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if string(a) != string(b) {
		t.Fatalf("model = %s\nstore = %s", a, b)
	}
}

func readModelFixture(t *testing.T, st *store.Store) {
	t.Helper()
	ask := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Ask, Text: "ship #41?", To: []string{"worker"}, Refs: store.Refs{PRs: []store.PR{{Repo: "a/b", Number: 41}}}})
	testutil.Post(t, st, store.Record{Lane: "worker", Kind: kinds.Note, Text: "ON IT", Re: ask.Seq})
	testutil.Post(t, st, store.Record{Lane: "worker", Kind: kinds.Decide, Text: "first to two", To: []string{"other", "worker"}})
	testutil.Post(t, st, store.Record{Lane: "worker", Kind: kinds.Note, Text: "broadcast", To: []string{"all"}})
	testutil.Post(t, st, store.Record{Lane: "other", Kind: kinds.Urgent, Text: "fire", Topic: "fire"})
	testutil.Post(t, st, store.Record{Lane: "other", Kind: kinds.Urgent, Text: "fire again", Topic: "fire"})
	testutil.Post(t, st, store.Record{Lane: "other", Kind: kinds.Note, Text: "stacked", Refs: store.Refs{Stacks: []string{"s/1"}, Targets: []string{"api"}, PRs: []store.PR{{Number: 41}}}})
	testutil.Post(t, st, store.Record{Drive: "e", Lane: "root", Kind: kinds.Note, Text: "elsewhere", To: []string{"worker@d", "peer:abc"}})
	testutil.Post(t, st, store.Record{Drive: "e", Lane: "root", Kind: kinds.Decide, Text: "followed"})
}

func TestReadModelMatchesTheStore(t *testing.T) {
	st, clock := testutil.Store(t)
	readModelFixture(t, st)
	model, err := st.BuildReadModel(t.Context(), noPace)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = model.Close() })
	asks := []kinds.Kind{kinds.Ask, kinds.Decide}
	tests := []struct {
		name string
		f    store.Filter
	}{
		{"drive", store.Filter{Drive: "d"}},
		{"newest", store.Filter{Drive: "d", Descending: true, Limit: 3}},
		{"for", store.Filter{Drive: "d", For: "worker"}},
		{"for with kinds", store.Filter{Drive: "d", For: "worker", Kinds: asks, ToKinds: asks}},
		{"to", store.Filter{Drive: "d", To: "worker"}},
		{"to first only", store.Filter{Drive: "d", To: "worker", FirstOnly: asks}},
		{"addressed through a reply", store.Filter{Drive: "d", Addressed: "root"}},
		{"lanes", store.Filter{Drive: "d", Lanes: []string{"other"}}},
		{"kinds", store.Filter{Drive: "d", Kinds: asks}},
		{"topics", store.Filter{Drive: "d", Topics: []string{"fire"}}},
		{"mute", store.Filter{Drive: "d", Mute: asks}},
		{"repeat", store.Filter{Drive: "d", RepeatWindow: time.Hour}},
		{"contains folds case", store.Filter{Drive: "d", Contains: []string{"on it"}}},
		{"contains a number", store.Filter{Drive: "d", Contains: []string{"41", "3"}}},
		{"pr by number", store.Filter{Drive: "d", PR: store.PR{Number: 41}}},
		{"pr in a repo", store.Filter{Drive: "d", PR: store.PR{Repo: "a/b", Number: 41}}},
		{"stack", store.Filter{Drive: "d", Stack: "s/1"}},
		{"target", store.Filter{Drive: "d", Target: "api"}},
		{"after and before", store.Filter{Drive: "d", After: 2, Before: 6}},
		{"since", store.Filter{Drive: "d", Since: clock.Now().Add(-time.Hour)}},
		{"until the past", store.Filter{Drive: "d", Until: clock.Now().Add(-time.Hour)}},
		{"reader address", store.Filter{Drive: "d", To: "worker", Descending: true}},
		{"peer", store.Filter{Drive: "d", Peer: "abc"}},
		{"follow", store.Filter{Drive: "d", Follow: []store.Follow{{Drive: "e", Kinds: asks}}}},
		{"none", store.Filter{Drive: "missing"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, answered, err := model.Answer(t.Context(), st, tt.f)
			if err != nil {
				t.Fatal(err)
			}
			if !answered {
				t.Fatal("the model did not answer")
			}
			want, err := st.Query(t.Context(), tt.f)
			if err != nil {
				t.Fatal(err)
			}
			sameRecords(t, got, want)
			wantCount, err := st.Count(t.Context(), tt.f)
			if err != nil {
				t.Fatal(err)
			}
			st.ServeReadModel(model)
			gotCount, err := st.Count(t.Context(), tt.f)
			st.ServeReadModel(nil)
			if err != nil {
				t.Fatal(err)
			}
			if gotCount != wantCount {
				t.Errorf("count = %d, want %d", gotCount, wantCount)
			}
		})
	}
}

func TestReadModelSkipsWhatARelayBackfilledAfterALaneBound(t *testing.T) {
	st, clock := testutil.Store(t)
	ctx := t.Context()
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, To: []string{"worker"}, Text: "posted before the bind"})
	k := store.PeerKey{Session: "s1"}
	if err := st.BindLane(ctx, k, "d", "worker", clock.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Apply(ctx, []store.Row{{Origin: "0123456789abcdef", OriginSeq: 1, Drive: "d", Lane: "root", Kind: kinds.Note, At: clock.Now().Add(-time.Hour), Text: "backfilled", To: []string{"worker"}, Source: "post"}}, nil); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Minute)
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, To: []string{"worker"}, Text: "posted since the bind"})
	model, err := st.BuildReadModel(ctx, noPace)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = model.Close() })
	f := store.LaneMail(k, store.Binding{Drive: "d", Lane: "worker"})
	got, answered, err := model.Answer(ctx, st, f)
	if err != nil || !answered {
		t.Fatalf("model answered %v, %v", answered, err)
	}
	want, err := st.Query(ctx, f)
	if err != nil {
		t.Fatal(err)
	}
	sameRecords(t, got, want)
	if len(want) != 2 || want[0].Text != "posted before the bind" || want[1].Text != "posted since the bind" {
		t.Fatalf("lane mail = %+v; want both posts and not the backfilled record", want)
	}
}

func TestReadModelKnowsWhatIsOpen(t *testing.T) {
	st, clock := testutil.Store(t)
	open := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Ask, Text: "still open?", To: []string{"worker"}})
	answered := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Ask, Text: "answered?", To: []string{"worker"}})
	testutil.Post(t, st, store.Record{Lane: "worker", Kind: kinds.Answer, Text: "yes", Re: answered.Seq})
	resolved := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Decide, Text: "resolved?", To: []string{"worker"}})
	testutil.Post(t, st, store.Record{Lane: "worker", Kind: kinds.Note, Text: "closing", Resolves: resolved.Seq})
	held := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Hold, Text: "hold", Refs: store.Refs{Stacks: []string{"s/1"}}})
	clock.Advance(time.Minute)
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Release, Text: "go", Refs: store.Refs{Stacks: []string{"s/1"}}})
	topical := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Blocker, Text: "blocked", Topic: "t"})
	note := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, Text: "not an opener"})
	seqs := []int64{open.Seq, answered.Seq, resolved.Seq, held.Seq, topical.Seq, note.Seq, 99999}
	want, err := st.OpenSeqsAmong(t.Context(), "d", seqs)
	if err != nil {
		t.Fatal(err)
	}
	model, err := st.BuildReadModel(t.Context(), noPace)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = model.Close() })
	st.ServeReadModel(model)
	t.Cleanup(func() { st.ServeReadModel(nil) })
	got, err := st.OpenSeqsAmong(t.Context(), "d", seqs)
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("open = %v, store says %v", got, want)
	}
	if stats := model.Stats(); stats.Hits == 0 {
		t.Error("the model did not answer")
	}
	records, err := st.OpenAmong(t.Context(), "d", seqs)
	if err != nil {
		t.Fatal(err)
	}
	st.ServeReadModel(nil)
	stored, err := st.OpenAmong(t.Context(), "d", seqs)
	if err != nil {
		t.Fatal(err)
	}
	sameRecords(t, records, stored)
}

func TestReadModelNamesTargets(t *testing.T) {
	st, _ := testutil.Store(t)
	readModelFixture(t, st)
	model, err := st.BuildReadModel(t.Context(), noPace)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = model.Close() })
	f := store.Filter{Drive: "d"}
	want, err := st.Query(t.Context(), f)
	if err != nil {
		t.Fatal(err)
	}
	st.ServeReadModel(model)
	t.Cleanup(func() { st.ServeReadModel(nil) })
	got, err := st.Query(t.Context(), f)
	if err != nil {
		t.Fatal(err)
	}
	sameRecords(t, got, want)
	for _, r := range want {
		seq, err := st.Resolve(t.Context(), r.Num)
		if err != nil || seq != r.Seq {
			t.Errorf("resolve %s = %d, %v, want %d", r.Num, seq, err, r.Seq)
		}
	}
}

func TestReadModelReadsItsOwnWrites(t *testing.T) {
	st, _ := testutil.Store(t)
	readModelFixture(t, st)
	model, err := st.BuildReadModel(t.Context(), noPace)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = model.Close() })
	posted := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, Text: "just committed"})
	got, answered, err := model.Answer(t.Context(), st, store.Filter{Drive: "d", After: posted.Seq - 1})
	if err != nil || !answered {
		t.Fatalf("answered = %t, err = %v", answered, err)
	}
	if len(got) != 1 || got[0].Seq != posted.Seq {
		t.Fatalf("got %+v, want the record just committed", got)
	}
}

func TestReadModelKeepsItsCapWithEveryTextInItsFile(t *testing.T) {
	st, clock := testutil.Store(t)
	first := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, Text: "first"})
	clock.Advance(37 * time.Hour)
	text := strings.Repeat("x", 390)
	for i := range 2000 {
		testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, Text: fmt.Sprint(i, text)})
	}
	const limit = 512 << 10
	model, err := st.BuildReadModelUnder(t.Context(), noPace, limit)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = model.Close() })
	stats := model.Stats()
	if stats.Bytes == 0 || stats.Bytes > limit {
		t.Errorf("bytes = %d, want some and at most %d", stats.Bytes, limit)
	}
	if stats.TextBytes <= limit {
		t.Errorf("text bytes = %d, want them past the cap of %d", stats.TextBytes, limit)
	}
	f := store.Filter{Drive: "d"}
	got, answered, err := model.Answer(t.Context(), st, f)
	if err != nil || !answered {
		t.Fatalf("answered = %t, err = %v", answered, err)
	}
	want, err := st.Query(t.Context(), f)
	if err != nil {
		t.Fatal(err)
	}
	sameRecords(t, got, want)
	if stats.SearchFloor <= first.Seq {
		t.Errorf("search floor = %d, want it above the first record %d", stats.SearchFloor, first.Seq)
	}
	if _, answered, err := model.Answer(t.Context(), st, store.Filter{Drive: "d", Contains: []string{"first"}, IncludeExpired: true}); err != nil || answered {
		t.Errorf("a search below the floor answered = %t, err = %v; want it left to the store", answered, err)
	}
}

func TestReadModelOverItsCapStaysOff(t *testing.T) {
	st, _ := testutil.Store(t)
	readModelFixture(t, st)
	if _, err := st.BuildReadModelUnder(t.Context(), noPace, 64); err == nil {
		t.Fatal("built a model whose selection columns cannot fit its cap")
	}
}

func TestReadModelLeavesAMalformedRowToTheStore(t *testing.T) {
	st, _ := testutil.Store(t)
	readModelFixture(t, st)
	bad := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, Text: "bad"})
	corrupt(t, st, "UPDATE records SET recipients='[' WHERE seq=?", bad.Seq)
	model, err := st.BuildReadModel(t.Context(), noPace)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = model.Close() })
	if _, answered, err := model.Answer(t.Context(), st, store.Filter{Drive: "d"}); err != nil || answered {
		t.Errorf("answered = %t, err = %v; want the store to judge the row", answered, err)
	}
	got, answered, err := model.Answer(t.Context(), st, store.Filter{Drive: "d", Before: bad.Seq})
	if err != nil || !answered || len(got) == 0 {
		t.Errorf("a read short of the row: answered = %t, err = %v, rows = %d", answered, err, len(got))
	}
}

func TestReadModelGoesStaleWhenARecordChanges(t *testing.T) {
	st, _ := testutil.Store(t)
	readModelFixture(t, st)
	model, err := st.BuildReadModel(t.Context(), noPace)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = model.Close() })
	corrupt(t, st, "UPDATE records SET text='changed' WHERE seq=?", 1)
	if _, answered, err := model.Answer(t.Context(), st, store.Filter{Drive: "d"}); err != nil || answered {
		t.Errorf("answered = %t, err = %v; want the store", answered, err)
	}
	if !model.Stale() {
		t.Error("the model did not notice the changed record")
	}
}

func TestReadModelSwitch(t *testing.T) {
	home := t.TempDir()
	if store.ReadModelOn(home) {
		t.Fatal("on before anyone turned it on")
	}
	if err := store.SetReadModel(home, true); err != nil || !store.ReadModelOn(home) {
		t.Fatalf("turn on: %v", err)
	}
	if err := store.SetReadModel(home, false); err != nil || store.ReadModelOn(home) {
		t.Fatalf("turn off: %v", err)
	}
}

func corrupt(t *testing.T, st *store.Store, statement string, seq int64) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(st.HomeDir(), store.DBFile))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(t.Context(), statement, seq); err != nil {
		t.Fatal(err)
	}
}

func TestReadModelLeavesWhatItCannotProveToTheStore(t *testing.T) {
	tests := []struct {
		name, set string
	}{
		{"fields that do not decode", `fields='{"attrs":123}'`},
		{"refs with a repeated key", `refs='{"stacks":["a"],"stacks":["b"]}'`},
		{"refs with a key in another case", `refs='{"STACKS":["a"]}'`},
		{"a surrogate escape in refs", `refs='{"path":"\ud83d\ude00"}'`},
		{"recipients that are not strings", `recipients='[1]'`},
		{"also_resolves that is not numbers", `also_resolves='["x"]'`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st, _ := testutil.Store(t)
			readModelFixture(t, st)
			bad := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, Text: "bad"})
			corrupt(t, st, "UPDATE records SET "+tt.set+" WHERE seq=?", bad.Seq)
			model, err := st.BuildReadModel(t.Context(), noPace)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = model.Close() })
			if _, answered, err := model.Answer(t.Context(), st, store.Filter{Drive: "d"}); err != nil || answered {
				t.Errorf("answered = %t, err = %v; want the store to judge the row", answered, err)
			}
		})
	}
}

func TestReadModelKeepsAnExpiryOfZero(t *testing.T) {
	st, _ := testutil.Store(t)
	expired := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, Text: "expired"})
	corrupt(t, st, "UPDATE records SET expires_at=0 WHERE seq=?", expired.Seq)
	model, err := st.BuildReadModel(t.Context(), noPace)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = model.Close() })
	for _, f := range []store.Filter{{Drive: "d"}, {Drive: "d", IncludeExpired: true}} {
		got, answered, err := model.Answer(t.Context(), st, f)
		if err != nil || !answered {
			t.Fatalf("answered = %t, err = %v", answered, err)
		}
		want, err := st.Query(t.Context(), f)
		if err != nil {
			t.Fatal(err)
		}
		sameRecords(t, got, want)
	}
}

func TestReadModelSearchReadsAnUnderscoreAsLikeDoes(t *testing.T) {
	st, _ := testutil.Store(t)
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, Text: "ci_fail here"})
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, Text: "ci-fail there"})
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, Text: "ciéfail wide"})
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, Text: "cifail none"})
	model, err := st.BuildReadModel(t.Context(), noPace)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = model.Close() })
	for _, word := range []string{"ci_fail", "_fail", "CI_FAIL", "ci__ail"} {
		f := store.Filter{Drive: "d", Contains: []string{word}}
		got, answered, err := model.Answer(t.Context(), st, f)
		if err != nil || !answered {
			t.Fatalf("%s: answered = %t, err = %v", word, answered, err)
		}
		want, err := st.Query(t.Context(), f)
		if err != nil {
			t.Fatal(err)
		}
		sameRecords(t, got, want)
	}
}

func TestReadModelSearchStopsAtANul(t *testing.T) {
	st, _ := testutil.Store(t)
	held := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, Text: "placeholder"})
	corrupt(t, st, "UPDATE records SET text='a'||char(0)||'needle' WHERE seq=?", held.Seq)
	model, err := st.BuildReadModel(t.Context(), noPace)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = model.Close() })
	f := store.Filter{Drive: "d", Contains: []string{"needle"}}
	got, answered, err := model.Answer(t.Context(), st, f)
	if err != nil || !answered {
		t.Fatalf("answered = %t, err = %v", answered, err)
	}
	want, err := st.Query(t.Context(), f)
	if err != nil {
		t.Fatal(err)
	}
	sameRecords(t, got, want)
}

func TestReadModelReadsUnderATrigramNarrowingStayOnTheStore(t *testing.T) {
	st, _ := testutil.Store(t)
	readModelFixture(t, st)
	model, err := st.BuildReadModel(t.Context(), noPace)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = model.Close() })
	if _, answered, err := model.Answer(t.Context(), st, store.Filter{Drive: "d", Trigrams: `"xyz"`}); err != nil || answered {
		t.Errorf("answered = %t, err = %v; want the store", answered, err)
	}
}

func TestReadModelOpenItemsMatchTheStore(t *testing.T) {
	st, clock := testutil.Store(t)
	readModelFixture(t, st)
	since := clock.Now().Add(-time.Hour)
	wantOpen, wantUntracked, wantOlder, err := st.OpenSince(t.Context(), "d", since)
	if err != nil {
		t.Fatal(err)
	}
	wantItems, err := st.OpenItems(t.Context(), "d")
	if err != nil {
		t.Fatal(err)
	}
	model, err := st.BuildReadModel(t.Context(), noPace)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = model.Close() })
	st.ServeReadModel(model)
	t.Cleanup(func() { st.ServeReadModel(nil) })
	open, untracked, older, err := st.OpenSince(t.Context(), "d", since)
	if err != nil {
		t.Fatal(err)
	}
	sameRecords(t, open, wantOpen)
	sameRecords(t, untracked, wantUntracked)
	if older != wantOlder {
		t.Errorf("older = %d, want %d", older, wantOlder)
	}
	items, err := st.OpenItems(t.Context(), "d")
	if err != nil {
		t.Fatal(err)
	}
	sameRecords(t, items, wantItems)
}

func TestReadModelDeclinesAPRPastThirtyTwoBits(t *testing.T) {
	st, _ := testutil.Store(t)
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, Text: "large", Refs: store.Refs{PRs: []store.PR{{Repo: "a/b", Number: 1 << 31}}}})
	model, err := st.BuildReadModel(t.Context(), noPace)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = model.Close() })
	if _, answered, err := model.Answer(t.Context(), st, store.Filter{Drive: "d", PR: store.PR{Number: 1 << 31}}); err != nil || answered {
		t.Errorf("answered = %t, err = %v; want the store", answered, err)
	}
}

func TestReadModelReadsItsOwnWritesUnderLoad(t *testing.T) {
	st, _ := testutil.Store(t)
	readModelFixture(t, st)
	model, err := st.BuildReadModelUnder(t.Context(), noPace, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = model.Close() })
	st.ServeReadModel(model)
	t.Cleanup(func() { st.ServeReadModel(nil) })
	var readers sync.WaitGroup
	stop := make(chan struct{})
	for range 4 {
		readers.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := st.Query(t.Context(), store.Filter{Drive: "d", Descending: true, Limit: 20, Contains: []string{"written"}}); err != nil {
					t.Error(err)
					return
				}
				if _, err := st.OpenSeqsAmong(t.Context(), "d", []int64{1, 2, 3}); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	text := strings.Repeat("y", 380)
	for i := range 300 {
		posted := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Ask, To: []string{"worker"}, Text: fmt.Sprint("written ", i, text)})
		got, err := st.Query(t.Context(), store.Filter{Drive: "d", After: posted.Seq - 1})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].Seq != posted.Seq || got[0].Text != posted.Text {
			t.Fatalf("read after commit = %+v, want #%d", got, posted.Seq)
		}
	}
	close(stop)
	readers.Wait()
	if stats := model.Stats(); stats.Hits == 0 || stats.Failures != 0 {
		t.Errorf("stats = %+v, want answers and no failures", stats)
	}
}
