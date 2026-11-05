package store_test

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

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

func TestReadModelKeepsItsCapAndReadsOlderTextFromTheStore(t *testing.T) {
	st, _ := testutil.Store(t)
	first := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, Text: "first"})
	text := strings.Repeat("x", 390)
	for i := range 2000 {
		testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, Text: fmt.Sprint(i, text)})
	}
	const limit = 1 << 20
	model, err := st.BuildReadModelUnder(t.Context(), noPace, limit)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = model.Close() })
	stats := model.Stats()
	if stats.Bytes == 0 || stats.Bytes > limit {
		t.Errorf("bytes = %d, want some and at most %d", stats.Bytes, limit)
	}
	if stats.HotFloor <= first.Seq {
		t.Errorf("hot floor = %d, want it above the first record %d", stats.HotFloor, first.Seq)
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
	if _, answered, err := model.Answer(t.Context(), st, store.Filter{Drive: "d", Contains: []string{"first"}}); err != nil || answered {
		t.Errorf("a search reaching older text answered = %t, err = %v; want it left to the store", answered, err)
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
