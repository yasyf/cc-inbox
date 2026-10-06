package inbox_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func TestTailCursorAndBudget(t *testing.T) {
	st, c := testutil.Store(t)
	ctx := context.Background()
	for i := range 40 {
		testutil.Post(t, st, store.Record{Kind: kinds.State, Text: fmt.Sprintf("step %02d %s", i, strings.Repeat("x", 80))})
		c.Advance(time.Second)
	}
	opts := inbox.TailOptions{Filter: store.Filter{Drive: "d"}, Cursor: "root", Budget: 1000}
	var first bytes.Buffer
	res, err := inbox.Tail(ctx, st, opts, &first)
	if err != nil {
		t.Fatal(err)
	}
	if first.Len() > 1000 {
		t.Fatalf("first page is %d bytes, over the 1000-byte budget", first.Len())
	}
	if res.Printed == 0 || res.More != 40-res.Printed {
		t.Fatalf("first page = %+v", res)
	}
	if !strings.Contains(first.String(), fmt.Sprintf("... %d more; resume with cci tail", res.More)) {
		t.Fatalf("missing trailer:\n%s", first.String())
	}
	seen := res.Printed
	for res.More > 0 {
		var page bytes.Buffer
		if res, err = inbox.Tail(ctx, st, opts, &page); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(page.String(), fmt.Sprintf("step %02d", seen)) {
			t.Fatalf("page does not resume at step %02d:\n%s", seen, page.String())
		}
		seen += res.Printed
	}
	if seen != 40 {
		t.Fatalf("paged through %d records, want 40", seen)
	}
	var empty bytes.Buffer
	if res, err = inbox.Tail(ctx, st, opts, &empty); err != nil || res.Printed != 0 || empty.Len() != 0 {
		t.Fatalf("caught-up tail printed %q (%+v, %v)", empty.String(), res, err)
	}
}

func TestTailFreshCursorStartsAnHourBack(t *testing.T) {
	st, c := testutil.Store(t)
	testutil.Post(t, st, store.Record{Kind: kinds.Go, Text: "ancient"})
	c.Advance(3 * time.Hour)
	testutil.Post(t, st, store.Record{Kind: kinds.Go, Text: "recent"})
	var out bytes.Buffer
	if _, err := inbox.Tail(context.Background(), st, inbox.TailOptions{Filter: store.Filter{Drive: "d"}, Cursor: "new"}, &out); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "ancient") || !strings.Contains(out.String(), "recent") {
		t.Fatalf("fresh cursor output:\n%s", out.String())
	}
}

func TestTailBudgetGovernsWholeRecords(t *testing.T) {
	st, _ := testutil.Store(t)
	text := strings.Repeat("y", 390)
	for i := range 160 {
		testutil.Post(t, st, store.Record{Kind: kinds.State, Text: fmt.Sprintf("%03d %s", i, text)})
	}
	var out bytes.Buffer
	if _, err := inbox.Tail(context.Background(), st, inbox.TailOptions{Filter: store.Filter{Drive: "d"}, Budget: 60000}, &out); err != nil {
		t.Fatal(err)
	}
	if out.Len() > 60000 || out.Len() < 55000 {
		t.Fatalf("tail wrote %d bytes, want just under the 60000-byte budget", out.Len())
	}
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		if !strings.HasPrefix(line, "...") && !strings.Contains(line, text) {
			t.Fatalf("tail clipped a record within a 60000-byte budget: %s", line)
		}
	}
}

func TestReadsClipRecordsToWidth(t *testing.T) {
	const width = 400
	st, _ := testutil.Store(t)
	ctx := context.Background()
	text := strings.Repeat("z", 390)
	for i := range 60 {
		testutil.Post(t, st, store.Record{Kind: kinds.State, Text: fmt.Sprintf("%03d %s", i, text)})
	}
	testutil.Post(t, st, store.Record{Kind: kinds.Head, Text: "head " + text})
	var out bytes.Buffer
	if _, err := inbox.Tail(ctx, st, inbox.TailOptions{Filter: store.Filter{Drive: "d"}, Width: width}, &out); err != nil {
		t.Fatal(err)
	}
	if out.Len() > 6144 || out.Len() < 5000 {
		t.Fatalf("default tail wrote %d bytes, want just under 6144", out.Len())
	}
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		if n := len([]rune(line)); n != width && !strings.HasPrefix(line, "...") {
			t.Fatalf("line of %d characters, want clipped to %d: %s", n, width, line)
		}
	}
	whole := map[string]func(*bytes.Buffer) error{
		"tail --width 0": func(b *bytes.Buffer) error {
			_, err := inbox.Tail(ctx, st, inbox.TailOptions{Filter: store.Filter{Drive: "d"}, Budget: 16000}, b)
			return err
		},
		"tail --json": func(b *bytes.Buffer) error {
			_, err := inbox.Tail(ctx, st, inbox.TailOptions{Filter: store.Filter{Drive: "d"}, Budget: 16000, Width: width, JSON: true}, b)
			return err
		},
		"grep": func(b *bytes.Buffer) error {
			return inbox.Grep(ctx, st, store.Filter{Drive: "d"}, regexp.MustCompile("059 z"), 0, 0, false, b)
		},
		"state": func(b *bytes.Buffer) error {
			records, err := inbox.State(ctx, st, store.Filter{Drive: "d"})
			inbox.WriteState(b, records, st.Now(), 0, false)
			return err
		},
	}
	for name, read := range whole {
		var b bytes.Buffer
		if err := read(&b); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(b.String(), text) {
			t.Errorf("%s clipped record text:\n%s", name, b.String())
		}
	}
}

func TestDigestLatestIsByTimeNotSeq(t *testing.T) {
	st, c := testutil.Store(t)
	ctx := context.Background()
	imported := []store.Ingestion{
		{Record: store.Record{Drive: "d", Lane: "old-import", Kind: kinds.Note, At: c.Now().Add(-2 * time.Hour), Text: "older", Source: "import:/x.md"}, LineHash: "a"},
		{Record: store.Record{Drive: "d", Lane: "new-import", Kind: kinds.Note, At: c.Now().Add(-time.Minute), Text: "newer", Source: "import:/x.md"}, LineHash: "b"},
	}
	if _, err := st.IngestAll(ctx, imported); err != nil {
		t.Fatal(err)
	}
	testutil.Post(t, st, store.Record{Lane: "new-import", Kind: kinds.State, Text: "latest"})
	c.Advance(time.Second)
	v, err := inbox.Digest(ctx, st, "d", c.Now().Add(-inbox.DigestWindow))
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(v.Latest))
	for _, r := range v.Latest {
		got = append(got, r.Lane+":"+r.Text)
	}
	if strings.Join(got, ",") != "new-import:latest,old-import:older" {
		t.Fatalf("latest = %v", got)
	}
}

func TestGrep(t *testing.T) {
	st, _ := testutil.Store(t)
	testutil.Post(t, st, store.Record{Kind: kinds.Landed, Text: "landed #30427", Refs: store.Refs{PRs: []int{30427}}})
	testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "unrelated"})
	var out bytes.Buffer
	if err := inbox.Grep(context.Background(), st, store.Filter{Drive: "d"}, regexp.MustCompile("30427"), 0, 0, false, &out); err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(out.String()); !strings.Contains(got, "LANDED lane-a landed #30427 pr#30427") || strings.Contains(got, "unrelated") {
		t.Fatalf("grep output = %q", got)
	}
}

func TestDigest(t *testing.T) {
	st, c := testutil.Store(t)
	old := testutil.Post(t, st, store.Record{Kind: kinds.Ask, Text: "stale question"})
	c.Advance(30 * time.Hour)
	ask := testutil.Post(t, st, store.Record{Lane: "lane-b", Kind: kinds.Ask, Text: "which env?"})
	testutil.Post(t, st, store.Record{Kind: kinds.Hold, Text: "hold #1", Topic: "#1"})
	testutil.Post(t, st, store.Record{Lane: "lane-b", Kind: kinds.State, Text: "green"})
	v, err := inbox.Digest(context.Background(), st, "d", st.Now().Add(-inbox.DigestWindow))
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Asks) != 1 || v.Asks[0].Seq != ask.Seq || v.OlderOpen != 1 || len(v.Holds) != 1 {
		t.Fatalf("digest open items: asks=%v holds=%v older=%d (stale ask #%d)", v.Asks, v.Holds, v.OlderOpen, old.Seq)
	}
	if v.Total != 3 || v.Kinds["ask"] != 1 || v.Lanes["lane-b"] != 2 {
		t.Fatalf("digest counts total=%d kinds=%v lanes=%v", v.Total, v.Kinds, v.Lanes)
	}
	var out bytes.Buffer
	v.Write(&out, st.Now(), 0, 0)
	for _, want := range []string{"open asks (1, newest first):", "open holds (1, newest first):", "1 older open items", "latest per lane (2, newest first):", "STATE lane-b green"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("digest missing %q:\n%s", want, out.String())
		}
	}
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestWatchEmitsOnlyNewMatchingRecords(t *testing.T) {
	st, _ := testutil.Store(t)
	before := testutil.Post(t, st, store.Record{Kind: kinds.Go, Text: "before the watch"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var out syncBuffer
	done := make(chan error, 1)
	go func() {
		done <- inbox.Watch(ctx, st, inbox.WatchOptions{
			Filter:   store.Filter{Drive: "d", Kinds: []kinds.Kind{kinds.Go}, After: before.Seq},
			Cursor:   "monitor",
			Interval: 5 * time.Millisecond,
			For:      time.Minute,
		}, inbox.WriteLines(&out))
	}()
	time.Sleep(50 * time.Millisecond)
	testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "filtered out"})
	want := testutil.Post(t, st, store.Record{Kind: kinds.Go, Text: "after the watch"})
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(out.String(), "after the watch") && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := out.String(); strings.Count(got, "\n") != 1 || !strings.HasPrefix(got, fmt.Sprintf("#%d ", want.Seq)) {
		t.Fatalf("watch output = %q", got)
	}
	seq, ok, err := st.Cursor(context.Background(), "monitor", "d")
	if err != nil || !ok || seq != want.Seq {
		t.Fatalf("watch cursor = %d %v %v, want %d", seq, ok, err, want.Seq)
	}
}

func TestWatchAdvancesCursorOnlyPastDeliveredRecords(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	if err := st.SetCursor(ctx, "channel", "d", 0); err != nil {
		t.Fatal(err)
	}
	first := testutil.Post(t, st, store.Record{Kind: kinds.Go, Text: "delivered"})
	testutil.Post(t, st, store.Record{Kind: kinds.Go, Text: "the session went away"})
	refused := errors.New("channel closed")
	var got []int64
	err := inbox.Watch(ctx, st, inbox.WatchOptions{Filter: store.Filter{Drive: "d"}, Cursor: "channel", Interval: time.Millisecond}, func(r store.Record, _ string) error {
		if len(got) == 1 {
			return refused
		}
		got = append(got, r.Seq)
		return nil
	})
	if !errors.Is(err, refused) {
		t.Fatalf("watch err = %v, want %v", err, refused)
	}
	seq, ok, err := st.Cursor(ctx, "channel", "d")
	if err != nil || !ok || seq != first.Seq {
		t.Fatalf("cursor = %d %v %v, want %d", seq, ok, err, first.Seq)
	}
}

func TestWatchWithoutForRunsUntilCancelled(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- inbox.Watch(ctx, st, inbox.WatchOptions{Filter: store.Filter{Drive: "d"}, Interval: time.Millisecond}, func(store.Record, string) error { return nil })
	}()
	select {
	case err := <-done:
		t.Fatalf("watch returned early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestDigestListsLatestNewestSeqFirst(t *testing.T) {
	st, c := testutil.Store(t)
	ctx := context.Background()
	imported := []store.Ingestion{
		{Record: store.Record{Drive: "d", Lane: "merge-walker-r2", Kind: kinds.Note, At: c.Now().Add(-time.Minute), Text: "stamped 1:46", Source: "import:/x.md"}, LineHash: "a"},
		{Record: store.Record{Drive: "d", Lane: "landing-sweep-16", Kind: kinds.Note, At: c.Now().Add(-2 * time.Minute), Text: "stamped 1:45", Source: "import:/x.md"}, LineHash: "b"},
	}
	if _, err := st.IngestAll(ctx, imported); err != nil {
		t.Fatal(err)
	}
	v, err := inbox.Digest(ctx, st, "d", c.Now().Add(-inbox.DigestWindow))
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(v.Latest))
	for _, r := range v.Latest {
		got = append(got, r.Lane)
	}
	if strings.Join(got, ",") != "landing-sweep-16,merge-walker-r2" {
		t.Fatalf("latest = %v, want the higher seq first", got)
	}
	var out bytes.Buffer
	v.Write(&out, c.Now(), 0, 0)
	if i, j := strings.Index(out.String(), "#2 "), strings.Index(out.String(), "#1 "); i < 0 || j < i {
		t.Fatalf("digest prints #1 before #2:\n%s", out.String())
	}
}

func TestDigestDefaultBudgetFitsEverySection(t *testing.T) {
	st, _ := testutil.Store(t)
	text := strings.Repeat("w", 390)
	for i, k := range []kinds.Kind{kinds.Ask, kinds.Blocker, kinds.Defect, kinds.Hold, kinds.Incident} {
		for j := range 9 {
			testutil.Post(t, st, store.Record{Lane: fmt.Sprintf("open-lane-%d-%d", i, j), To: []string{"root"}, Kind: k, Text: text})
		}
	}
	for i := range 20 {
		testutil.Post(t, st, store.Record{Lane: fmt.Sprintf("latest-lane-%02d", i), Kind: kinds.State, Text: text})
	}
	v, err := inbox.Digest(context.Background(), st, "d", st.Now().Add(-inbox.DigestWindow))
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	v.Write(&out, st.Now(), inbox.DigestBudget, 0)
	for _, want := range []string{"open asks (9", "open blockers (9", "open defects (9", "open holds (9", "open incidents (9", "latest per lane (15", "latest-lane-05"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("default digest missing %q", want)
		}
	}
	if strings.Contains(out.String(), "digest truncated") {
		t.Fatalf("default digest truncated at %d bytes", out.Len())
	}
}
