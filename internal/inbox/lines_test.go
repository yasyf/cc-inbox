package inbox_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func TestAnnotateMatchesFullHistoryAnnotation(t *testing.T) {
	st, clock := testutil.Store(t)
	ctx := context.Background()
	post := func(r store.Record) store.Record { return testutil.Post(t, st, r) }
	ask := post(store.Record{Drive: "a", Kind: kinds.Ask, Text: "ask answered by re"})
	post(store.Record{Drive: "a", Lane: "root", Kind: kinds.Answer, Text: "answer by re", Re: ask.Seq})
	post(store.Record{Drive: "a", Kind: kinds.Decide, Text: "decide closed by topic", Topic: "#1"})
	post(store.Record{Drive: "a", Lane: "root", Kind: kinds.Go, Text: "go on the topic", Topic: "#1"})
	post(store.Record{Drive: "a", Kind: kinds.Hold, Text: "hold still open", Topic: "#2"})
	post(store.Record{Drive: "a", Lane: "root", Kind: kinds.Lift, Text: "lift elsewhere", Topic: "#3"})
	held := post(store.Record{Drive: "a", Kind: kinds.Hold, Text: "hold done is the wrong pairing"})
	post(store.Record{Drive: "a", Lane: "root", Kind: kinds.Done, Text: "done on the hold", Re: held.Seq})
	post(store.Record{Drive: "a", Lane: "root", Kind: kinds.Lift, Text: "lift before its hold", Topic: "#4"})
	post(store.Record{Drive: "a", Kind: kinds.Hold, Text: "hold after its lift", Topic: "#4"})
	post(store.Record{Drive: "a", Kind: kinds.Incident, Text: "incident fixed on its stack", Refs: store.Refs{Stacks: []string{"api/prod"}}})
	post(store.Record{Drive: "a", Lane: "root", Kind: kinds.FixLive, Text: "fix live on api", Refs: store.Refs{Stacks: []string{"api/prod"}}})
	post(store.Record{Drive: "a", Kind: kinds.Defect, Text: "defect stack wins over target", Refs: store.Refs{Stacks: []string{"web/prod"}, Targets: []string{"web"}}})
	post(store.Record{Drive: "a", Lane: "root", Kind: kinds.FixLive, Text: "fix live on web stage", Refs: store.Refs{Stacks: []string{"web/stage"}, Targets: []string{"web"}}})
	post(store.Record{Drive: "a", Kind: kinds.Defect, Text: "defect closed by target", Refs: store.Refs{Targets: []string{"db"}}})
	post(store.Record{Drive: "a", Lane: "root", Kind: kinds.Done, Text: "done on db", Refs: store.Refs{Targets: []string{"db"}}})
	blocked := post(store.Record{Drive: "a", Kind: kinds.Blocked, Text: "blocked until unblocked"})
	post(store.Record{Drive: "a", Lane: "root", Kind: kinds.Unblock, Text: "unblocked", Re: blocked.Seq})
	crossAsk := post(store.Record{Drive: "a", Kind: kinds.Ask, Text: "ask resolved from drive b", To: []string{store.PeerAddress("sess-b")}})
	post(store.Record{Drive: "b", Lane: "root", Kind: kinds.Answer, Text: "answer from drive b", Resolves: crossAsk.Seq})
	noted := post(store.Record{Drive: "a", Kind: kinds.Ask, Text: "ask resolved by a state"})
	post(store.Record{Drive: "a", Lane: "root", Kind: kinds.State, Text: "state resolving the ask", Resolves: noted.Seq})
	post(store.Record{Drive: "a", Kind: kinds.Ask, Text: "ask for api on drive b", To: []string{"api@b"}})
	toB := post(store.Record{Drive: "a", Kind: kinds.Defect, Text: "defect for api on drive b", To: []string{"api@b"}})
	post(store.Record{Drive: "a", Lane: "root", Kind: kinds.FixLive, Text: "fix live for api", Re: toB.Seq})
	post(store.Record{Drive: "b", Kind: kinds.State, Text: "old state on b"})
	post(store.Record{Drive: "b", Kind: kinds.Hold, Text: "hold on b", Topic: "#b"})
	askC := post(store.Record{Drive: "c", Kind: kinds.Ask, Text: "ask on c answered"})
	post(store.Record{Drive: "c", Lane: "root", Kind: kinds.Answer, Text: "answer on c", Re: askC.Seq})
	post(store.Record{Drive: "c", Kind: kinds.Incident, Text: "incident on c still open"})
	at := clock.Now()
	imported := func(kind kinds.Kind, offset time.Duration, text string, refs store.Refs) store.Ingestion {
		return store.Ingestion{Record: store.Record{Drive: "a", Lane: "importer", Kind: kind, At: at.Add(offset), Text: text, Refs: refs, Source: "import:/inbox/a.md"}, LineHash: text}
	}
	reparsed := imported(kinds.Defect, 0, "imported defect reparsed onto a fixed stack", store.Refs{Stacks: []string{"old/prod"}})
	if _, err := st.IngestAll(ctx, []store.Ingestion{
		imported(kinds.Decide, 0, "imported decide untracked", store.Refs{}),
		imported(kinds.Ask, 0, "imported ask closed by stack", store.Refs{Stacks: []string{"svc/prod"}}),
		imported(kinds.FixLive, time.Minute, "imported fix live on svc", store.Refs{Stacks: []string{"svc/prod"}}),
		imported(kinds.Defect, time.Hour, "imported defect after its fix", store.Refs{Stacks: []string{"q/prod"}}),
		imported(kinds.FixLive, 0, "imported fix live before the defect", store.Refs{Stacks: []string{"q/prod"}}),
		reparsed,
	}); err != nil {
		t.Fatal(err)
	}
	post(store.Record{Drive: "a", Lane: "root", Kind: kinds.FixLive, Text: "fix live on new", Refs: store.Refs{Stacks: []string{"new/prod"}}})
	reparsed.Record.Refs.Stacks = []string{"new/prod"}
	if updated, _, err := st.Reparse(ctx, []store.Ingestion{reparsed}, false); err != nil || updated != 1 {
		t.Fatalf("Reparse() = %d, %v; want one record updated in place", updated, err)
	}
	clock.Advance(72 * time.Hour)
	for _, c := range []struct {
		drive string
		want  store.CompactResult
	}{{"b", store.CompactResult{Folded: 1, Digests: 1}}, {"c", store.CompactResult{Folded: 2, Digests: 1}}} {
		if res, err := st.Compact(ctx, c.drive, 48*time.Hour); err != nil || res != c.want {
			t.Fatalf("Compact(%s) = %+v, %v; want %+v", c.drive, res, err, c.want)
		}
	}
	statuses := map[string]string{
		"ask answered by re":                          "closed",
		"decide closed by topic":                      "closed",
		"hold still open":                             "open",
		"hold done is the wrong pairing":              "open",
		"hold after its lift":                         "open",
		"incident fixed on its stack":                 "closed",
		"defect stack wins over target":               "open",
		"defect closed by target":                     "closed",
		"blocked until unblocked":                     "closed",
		"ask resolved from drive b":                   "closed",
		"ask resolved by a state":                     "closed",
		"ask for api on drive b":                      "open",
		"defect for api on drive b":                   "closed",
		"hold on b":                                   "open",
		"incident on c still open":                    "open",
		"imported decide untracked":                   "imported",
		"imported ask closed by stack":                "closed",
		"imported defect after its fix":               "open",
		"imported defect reparsed onto a fixed stack": "closed",
	}
	for _, tt := range []struct {
		name   string
		filter store.Filter
		size   int
	}{
		{"whole drive", store.Filter{Drive: "a", IncludeExpired: true}, 31},
		{"first page", store.Filter{Drive: "a", Limit: 5}, 5},
		{"page after a seq", store.Filter{Drive: "a", After: held.Seq, Limit: 6}, 6},
		{"newest page", store.Filter{Drive: "a", Descending: true, Limit: 7}, 7},
		{"openers only", store.Filter{Drive: "a", Kinds: kinds.Openers()}, 17},
		{"one stack", store.Filter{Drive: "a", Stack: "api/prod"}, 2},
		{"imports", store.Filter{Drive: "a", Lanes: []string{"importer"}}, 6},
		{"reader on another drive", store.Filter{Drive: "b", For: "api"}, 5},
		{"compacted drive", store.Filter{Drive: "c", IncludeExpired: true}, 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			page, err := st.Query(ctx, tt.filter)
			if err != nil {
				t.Fatal(err)
			}
			if len(page) != tt.size {
				t.Fatalf("page holds %d records, want %d", len(page), tt.size)
			}
			want := fullHistoryAnnotation(ctx, t, st, page)
			if err := inbox.Annotate(ctx, st, page); err != nil {
				t.Fatal(err)
			}
			for i, r := range page {
				if got := render.JSON(r); got != render.JSON(want[i]) {
					t.Errorf("annotated #%d = %s, want %s", r.Seq, got, render.JSON(want[i]))
				}
				if r.Status != statuses[r.Text] {
					t.Errorf("#%d %q status = %q, want %q", r.Seq, r.Text, r.Status, statuses[r.Text])
				}
			}
		})
	}
}

func fullHistoryAnnotation(ctx context.Context, t *testing.T, st *store.Store, page []store.Record) []store.Record {
	t.Helper()
	open := map[int64]bool{}
	for _, r := range page {
		items, err := st.OpenItems(ctx, r.Drive)
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range items {
			open[o.Seq] = true
		}
	}
	want := slices.Clone(page)
	for i, r := range want {
		switch {
		case !r.Kind.Opens():
		case !r.Tracked():
			want[i].Status = "imported"
		case open[r.Seq]:
			want[i].Status = "open"
		default:
			want[i].Status = "closed"
		}
	}
	return want
}

func TestAnnotateReadsNoOpenerOffThePage(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	first := map[string]int64{}
	for _, drive := range []string{"a", "c", "e"} {
		for i := range 50 {
			r := testutil.Post(t, st, store.Record{Drive: drive, Lane: "history", Kind: kinds.Ask, Text: fmt.Sprintf("%s history ask %02d", drive, i)})
			if i == 0 {
				first[drive] = r.Seq
			}
		}
	}
	pageAsk := testutil.Post(t, st, store.Record{Drive: "a", Kind: kinds.Ask, Text: "page ask", To: []string{"reader"}})
	testutil.Post(t, st, store.Record{Drive: "a", Kind: kinds.Hold, Text: "page hold", Topic: "#p", To: []string{"reader"}})
	testutil.Post(t, st, store.Record{Drive: "a", Lane: "root", Kind: kinds.Lift, Text: "lift the page hold", Topic: "#p"})
	testutil.Post(t, st, store.Record{Drive: "c", Kind: kinds.State, Text: "state on c for the reader", To: []string{"reader@a"}})
	if _, err := st.IngestAll(ctx, []store.Ingestion{{Record: store.Record{Drive: "e", Lane: "importer", Kind: kinds.Decide, At: st.Now(), Text: "imported decide on e for the reader", To: []string{"reader@a"}, Source: "import:/inbox/e.md"}, LineHash: "e1"}}); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+filepath.Join(st.HomeDir(), "inbox.db")+"?_pragma=busy_timeout(10000)")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "UPDATE records SET refs = '{' WHERE lane = 'history'"); err != nil {
		t.Fatal(err)
	}
	page, err := st.Query(ctx, store.Filter{Drive: "a", To: "reader"})
	if err != nil {
		t.Fatal(err)
	}
	if err := inbox.Annotate(ctx, st, page); err != nil {
		t.Fatalf("Annotate() = %v; want the page annotated without reading a history opener", err)
	}
	got := make([]string, 0, len(page))
	for _, r := range page {
		got = append(got, fmt.Sprintf("%s:%s", r.Kind, r.Status))
	}
	if want := "ask:open hold:closed state: decide:imported"; strings.Join(got, " ") != want {
		t.Fatalf("page = %v, want %s", got, want)
	}
	for _, drive := range []string{"a", "c", "e"} {
		if _, err := st.OpenItems(ctx, drive); err == nil || !strings.Contains(err.Error(), fmt.Sprintf("decode refs of #%d", first[drive])) {
			t.Errorf("OpenItems(%s) = %v; want the full-history read to fail on poisoned #%d", drive, err, first[drive])
		}
	}
	if _, err := db.ExecContext(ctx, "UPDATE records SET refs = '{' WHERE seq = ?", pageAsk.Seq); err != nil {
		t.Fatal(err)
	}
	if err := inbox.Annotate(ctx, st, page); err == nil || !strings.Contains(err.Error(), fmt.Sprintf("decode refs of #%d", pageAsk.Seq)) {
		t.Errorf("Annotate() = %v; want the reread of page opener #%d to fail on its poisoned refs", err, pageAsk.Seq)
	}
}

func TestAnnotateRereadsARetainedPage(t *testing.T) {
	ctx := context.Background()
	ingest := func(t *testing.T, st *store.Store, refs store.Refs) store.Ingestion {
		t.Helper()
		defect := store.Ingestion{Record: store.Record{Drive: "d", Lane: "importer", Kind: kinds.Defect, At: st.Now(), Text: "imported defect", Refs: refs, Source: "import:/inbox/d.md"}, LineHash: "defect"}
		if _, err := st.IngestAll(ctx, []store.Ingestion{defect}); err != nil {
			t.Fatal(err)
		}
		return defect
	}
	reparse := func(t *testing.T, st *store.Store, defect store.Ingestion) {
		t.Helper()
		if updated, _, err := st.Reparse(ctx, []store.Ingestion{defect}, false); err != nil || updated != 1 {
			t.Fatalf("Reparse() = %d, %v; want one record updated in place", updated, err)
		}
	}
	fixLive := func(t *testing.T, st *store.Store, stack string) {
		t.Helper()
		testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.FixLive, Text: "fix live on " + stack, Refs: store.Refs{Stacks: []string{stack}}})
	}
	for _, tt := range []struct {
		name  string
		setup func(t *testing.T, st *store.Store, clock *testutil.Clock) func()
		want  string
	}{
		{"a reparse onto a fixed stack", func(t *testing.T, st *store.Store, _ *testutil.Clock) func() {
			defect := ingest(t, st, store.Refs{Stacks: []string{"old/prod"}})
			fixLive(t, st, "new/prod")
			return func() {
				defect.Record.Refs.Stacks = []string{"new/prod"}
				reparse(t, st, defect)
			}
		}, "defect:closed fix-live:"},
		{"a reparse off a fixed stack", func(t *testing.T, st *store.Store, _ *testutil.Clock) func() {
			defect := ingest(t, st, store.Refs{Stacks: []string{"new/prod"}})
			fixLive(t, st, "new/prod")
			return func() {
				defect.Record.Refs.Stacks = []string{"old/prod"}
				reparse(t, st, defect)
			}
		}, "defect:open fix-live:"},
		{"a reparse out of tracking", func(t *testing.T, st *store.Store, _ *testutil.Clock) func() {
			defect := ingest(t, st, store.Refs{Stacks: []string{"old/prod"}})
			return func() {
				defect.Record.Refs = store.Refs{}
				reparse(t, st, defect)
			}
		}, "defect:closed"},
		{"a reparse out of the opener kinds", func(t *testing.T, st *store.Store, _ *testutil.Clock) func() {
			defect := ingest(t, st, store.Refs{Stacks: []string{"old/prod"}})
			return func() {
				defect.Record.Kind = kinds.State
				reparse(t, st, defect)
			}
		}, "defect:closed"},
		{"a compaction folding the opener and its closer", func(t *testing.T, st *store.Store, clock *testutil.Clock) func() {
			ask := testutil.Post(t, st, store.Record{Kind: kinds.Ask, Text: "ask answered before compaction"})
			testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Answer, Text: "answer before compaction", Re: ask.Seq})
			return func() {
				clock.Advance(72 * time.Hour)
				if res, err := st.Compact(ctx, "d", 48*time.Hour); err != nil || res != (store.CompactResult{Folded: 2, Digests: 1}) {
					t.Fatalf("Compact() = %+v, %v; want the ask and its answer folded into one digest", res, err)
				}
			}
		}, "ask:closed answer:"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			st, clock := testutil.Store(t)
			mutate := tt.setup(t, st, clock)
			page, err := st.Query(ctx, store.Filter{Drive: "d", IncludeExpired: true})
			if err != nil {
				t.Fatal(err)
			}
			mutate()
			want := fullHistoryAnnotation(ctx, t, st, page)
			if err := inbox.Annotate(ctx, st, page); err != nil {
				t.Fatal(err)
			}
			got := make([]string, 0, len(page))
			for i, r := range page {
				got = append(got, fmt.Sprintf("%s:%s", r.Kind, r.Status))
				if render.JSON(r) != render.JSON(want[i]) {
					t.Errorf("annotated #%d = %s, want %s", r.Seq, render.JSON(r), render.JSON(want[i]))
				}
			}
			if strings.Join(got, " ") != tt.want {
				t.Fatalf("page = %v, want %s", got, tt.want)
			}
		})
	}
}

func TestAnnotateSeesNewClosersAndReparses(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	ask := testutil.Post(t, st, store.Record{Kind: kinds.Ask, Text: "fresh ask"})
	defect := store.Ingestion{Record: store.Record{Drive: "d", Lane: "importer", Kind: kinds.Defect, At: st.Now(), Text: "imported defect awaiting a reparse", Refs: store.Refs{Stacks: []string{"old/prod"}}, Source: "import:/inbox/fresh.md"}, LineHash: "fresh-defect"}
	if _, err := st.IngestAll(ctx, []store.Ingestion{defect}); err != nil {
		t.Fatal(err)
	}
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.FixLive, Text: "fix live on new", Refs: store.Refs{Stacks: []string{"new/prod"}}})
	for _, step := range []struct {
		name   string
		change func() error
		want   string
	}{
		{"before a matching closer", func() error { return nil }, "ask:open defect:open"},
		{"a new answer", func() error {
			_, _, err := st.Post(ctx, store.Record{Drive: "d", Lane: "root", Kind: kinds.Answer, Text: "answered", Re: ask.Seq}, 0)
			return err
		}, "ask:closed defect:open"},
		{"a reparse onto the fixed stack", func() error {
			defect.Record.Refs.Stacks = []string{"new/prod"}
			_, _, err := st.Reparse(ctx, []store.Ingestion{defect}, false)
			return err
		}, "ask:closed defect:closed"},
	} {
		if err := step.change(); err != nil {
			t.Fatalf("%s: %v", step.name, err)
		}
		page, err := st.Query(ctx, store.Filter{Drive: "d", Kinds: []kinds.Kind{kinds.Ask, kinds.Defect}})
		if err != nil {
			t.Fatal(err)
		}
		if err := inbox.Annotate(ctx, st, page); err != nil {
			t.Fatal(err)
		}
		got := make([]string, 0, len(page))
		for _, r := range page {
			got = append(got, fmt.Sprintf("%s:%s", r.Kind, r.Status))
		}
		if strings.Join(got, " ") != step.want {
			t.Fatalf("%s: page = %v, want %s", step.name, got, step.want)
		}
	}
}
