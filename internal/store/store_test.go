package store_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func open(t *testing.T) (*store.Store, *testutil.Clock) {
	t.Helper()
	return testutil.Store(t)
}

func post(t *testing.T, st *store.Store, r store.Record) store.Record {
	t.Helper()
	return testutil.Post(t, st, r)
}

func TestPostValidation(t *testing.T) {
	st, _ := open(t)
	tests := []struct {
		name string
		r    store.Record
		want string
	}{
		{"no drive", store.Record{Lane: "l", Kind: kinds.Note, Text: "x"}, "no drive"},
		{"no lane", store.Record{Drive: "d", Kind: kinds.Note, Text: "x"}, "lane is required"},
		{"empty text", store.Record{Drive: "d", Lane: "l", Kind: kinds.Note, Text: "  "}, "text is required"},
		{"too long", store.Record{Drive: "d", Lane: "l", Kind: kinds.Note, Text: strings.Repeat("é", 401)}, "exceeds 400"},
		{"opened needs pr", store.Record{Drive: "d", Lane: "l", Kind: kinds.Opened, Text: "x"}, "requires --pr"},
		{"answer needs re", store.Record{Drive: "d", Lane: "l", Kind: kinds.Answer, Text: "x"}, "requires --re or --topic"},
		{"digest is internal", store.Record{Drive: "d", Lane: "l", Kind: kinds.Digest, Text: "x"}, "written only by cci"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := st.Post(context.Background(), tt.r, 0)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Post() error = %v, want containing %q", err, tt.want)
			}
		})
	}
	if _, _, err := st.Post(context.Background(), store.Record{Drive: "d", Lane: "l", Kind: kinds.Note, Text: strings.Repeat("é", 400)}, 0); err != nil {
		t.Fatalf("400-rune text rejected: %v", err)
	}
}

func TestDedupeWindow(t *testing.T) {
	st, c := open(t)
	ctx := context.Background()
	first := post(t, st, store.Record{Kind: kinds.State, Text: "green at  abc"})
	c.Advance(5 * time.Minute)
	again, dup, err := st.Post(ctx, store.Record{Drive: "d", Lane: "lane-a", Kind: kinds.State, Text: "green at abc"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !dup || again.Seq != first.Seq {
		t.Fatalf("within window: dup=%v seq=%d, want dup of #%d", dup, again.Seq, first.Seq)
	}
	other := post(t, st, store.Record{Lane: "lane-b", Kind: kinds.State, Text: "green at abc"})
	if other.Seq == first.Seq {
		t.Fatal("another lane's identical text was deduped")
	}
	c.Advance(6 * time.Minute)
	later, dup, err := st.Post(ctx, store.Record{Drive: "d", Lane: "lane-a", Kind: kinds.State, Text: "green at abc"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if dup || later.Seq == first.Seq {
		t.Fatalf("after window: dup=%v seq=%d, want a new record", dup, later.Seq)
	}
}

func TestExpiry(t *testing.T) {
	st, c := open(t)
	ctx := context.Background()
	post(t, st, store.Record{Kind: kinds.Note, Text: "ephemeral"})
	post(t, st, store.Record{Kind: kinds.Go, Text: "durable"})
	short, _, err := st.Post(ctx, store.Record{Drive: "d", Lane: "lane-a", Kind: kinds.Go, Text: "short"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if short.ExpiresAt == nil {
		t.Fatal("--ttl did not set an expiry on a durable kind")
	}
	c.Advance(2 * time.Hour)
	visible, err := st.Query(ctx, store.Filter{Drive: "d"})
	if err != nil {
		t.Fatal(err)
	}
	if got := texts(visible); got != "ephemeral,durable" {
		t.Fatalf("after 2h visible = %s", got)
	}
	c.Advance(23 * time.Hour)
	visible, err = st.Query(ctx, store.Filter{Drive: "d"})
	if err != nil {
		t.Fatal(err)
	}
	if got := texts(visible); got != "durable" {
		t.Fatalf("after 25h visible = %s", got)
	}
	all, err := st.Query(ctx, store.Filter{Drive: "d", IncludeExpired: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("IncludeExpired returned %d records, want 3", len(all))
	}
}

func texts(rs []store.Record) string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r.Text
	}
	return strings.Join(out, ",")
}

func TestConcurrentWriters(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	const writers, each = 8, 25
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for w := range writers {
		wg.Go(func() {
			st, err := store.Open(ctx, dir)
			if err != nil {
				errs <- err
				return
			}
			defer func() { _ = st.Close() }()
			for i := range each {
				if _, _, err := st.Post(ctx, store.Record{Drive: "d", Lane: fmt.Sprintf("w%d", w), Kind: kinds.Note, Text: fmt.Sprintf("n%d", i)}, 0); err != nil {
					errs <- err
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	st, err := store.Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	n, err := st.Count(ctx, store.Filter{Drive: "d"})
	if err != nil {
		t.Fatal(err)
	}
	if n != writers*each {
		t.Fatalf("Count() = %d, want %d", n, writers*each)
	}
}

func TestCursorNeverMovesBack(t *testing.T) {
	st, _ := open(t)
	ctx := context.Background()
	if _, ok, err := st.Cursor(ctx, "s", "d"); err != nil || ok {
		t.Fatalf("fresh cursor ok=%v err=%v", ok, err)
	}
	for _, seq := range []int64{5, 3} {
		if err := st.SetCursor(ctx, "s", "d", seq); err != nil {
			t.Fatal(err)
		}
	}
	got, ok, err := st.Cursor(ctx, "s", "d")
	if err != nil || !ok || got != 5 {
		t.Fatalf("Cursor() = %d, %v, %v; want 5", got, ok, err)
	}
}

func TestOpenItems(t *testing.T) {
	st, _ := open(t)
	ctx := context.Background()
	ask := post(t, st, store.Record{Kind: kinds.Ask, Text: "which bucket?"})
	decide := post(t, st, store.Record{Kind: kinds.Decide, Text: "A or B", Topic: "#30427"})
	hold := post(t, st, store.Record{Kind: kinds.Hold, Text: "hold the queue", Topic: "#30431"})
	incident := post(t, st, store.Record{Kind: kinds.Incident, Text: "sanddb down", Topic: "hsbc-2147"})
	imported := []store.Ingestion{{Record: store.Record{Drive: "d", Lane: "runner", Kind: kinds.Decide, At: st.Now(), Text: "imported question", Source: "import:/inbox/runner.md"}, LineHash: "h1"}}
	if _, err := st.IngestAll(ctx, imported); err != nil {
		t.Fatal(err)
	}
	post(t, st, store.Record{Lane: "root", Kind: kinds.Answer, Text: "seeded bucket", Re: ask.Seq})
	post(t, st, store.Record{Lane: "root", Kind: kinds.Go, Text: "pick A", Topic: "#30427"})
	post(t, st, store.Record{Lane: "root", Kind: kinds.Lift, Text: "unrelated", Topic: "#99999"})
	post(t, st, store.Record{Lane: "root", Kind: kinds.Done, Text: "wrong pairing", Re: hold.Seq})
	open, err := st.OpenItems(ctx, "d")
	if err != nil {
		t.Fatal(err)
	}
	got := make([]int64, 0, len(open))
	for _, r := range open {
		got = append(got, r.Seq)
	}
	if fmt.Sprint(got) != fmt.Sprint([]int64{hold.Seq, incident.Seq}) {
		t.Fatalf("open = %v, want hold #%d and incident #%d (decide #%d closed by topic)", got, hold.Seq, incident.Seq, decide.Seq)
	}
}

func TestCompact(t *testing.T) {
	st, c := open(t)
	ctx := context.Background()
	post(t, st, store.Record{Kind: kinds.Go, Text: "old go"})
	post(t, st, store.Record{Lane: "lane-b", Kind: kinds.Note, Text: "old note"})
	hold := post(t, st, store.Record{Kind: kinds.Hold, Text: "still held", Topic: "#1"})
	c.Advance(72 * time.Hour)
	fresh := post(t, st, store.Record{Kind: kinds.Go, Text: "new go"})
	res, err := st.Compact(ctx, "d", 48*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if res != (store.CompactResult{Folded: 2, Digests: 1}) {
		t.Fatalf("Compact() = %+v", res)
	}
	again, err := st.Compact(ctx, "d", 48*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if again != (store.CompactResult{}) {
		t.Fatalf("second Compact() = %+v, want no-op", again)
	}
	all, err := st.Query(ctx, store.Filter{Drive: "d", IncludeExpired: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 || all[0].Seq != hold.Seq || all[1].Seq != fresh.Seq || all[2].Kind != kinds.Digest {
		t.Fatalf("after compact = %+v", all)
	}
	d := all[2]
	if d.Fields.Counts["total"] != 2 || d.Fields.Counts["kind:go"] != 1 || d.Fields.Counts["lane:lane-b"] != 1 {
		t.Fatalf("digest counts = %v", d.Fields.Counts)
	}
	if !strings.HasPrefix(d.Text, "2026-10-04: 2 records.") {
		t.Fatalf("digest text = %q", d.Text)
	}
}

func TestBindingSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	st, err := store.Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Bind(ctx, "s", "d", true); err != nil {
		t.Fatal(err)
	}
	b, ok, err := st.Binding(ctx, "s")
	if err != nil || !ok || b != (store.Binding{Drive: "d", Root: true}) {
		t.Fatalf("Binding() = %+v %v %v", b, ok, err)
	}
	_ = st.Close()
	if _, err := store.Open(ctx, dir); err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if !errors.Is(store.Validate(store.Record{}), store.ErrNoDrive) {
		t.Fatal("Validate on an empty record did not report ErrNoDrive")
	}
}
