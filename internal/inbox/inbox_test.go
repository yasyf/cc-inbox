package inbox_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
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

func TestTailSeedStartsAFreshCursorWhereTheSeedStands(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	read := testutil.Post(t, st, store.Record{Kind: kinds.Go, Text: "already read"})
	if err := st.SetCursor(ctx, "desk", "d", read.Seq); err != nil {
		t.Fatal(err)
	}
	testutil.Post(t, st, store.Record{Kind: kinds.Go, Text: "the assignment"})
	opts := inbox.TailOptions{Filter: store.Filter{Drive: "d"}, Cursor: "desk-wake", Seed: "desk"}
	var first, second bytes.Buffer
	if _, err := inbox.Tail(ctx, st, opts, &first); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(first.String(), "already read") || !strings.Contains(first.String(), "the assignment") {
		t.Fatalf("first seeded tail:\n%s", first.String())
	}
	testutil.Post(t, st, store.Record{Kind: kinds.Go, Text: "next"})
	if _, err := inbox.Tail(ctx, st, opts, &second); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(second.String(), "the assignment") || !strings.Contains(second.String(), "next") {
		t.Fatalf("second seeded tail:\n%s", second.String())
	}
}

func TestTailSeedSkipsWhatTheSeedCursorReadSinceTheLastTail(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	opts := inbox.TailOptions{Filter: store.Filter{Drive: "d"}, Cursor: "desk-wake", Seed: "desk"}
	if _, err := inbox.Tail(ctx, st, opts, io.Discard); err != nil {
		t.Fatal(err)
	}
	read := testutil.Post(t, st, store.Record{Kind: kinds.Go, Text: "read by the desk"})
	if err := st.SetCursor(ctx, "desk", "d", read.Seq); err != nil {
		t.Fatal(err)
	}
	var quiet, woke bytes.Buffer
	if _, err := inbox.Tail(ctx, st, opts, &quiet); err != nil || quiet.Len() != 0 {
		t.Fatalf("tail after the desk read printed %q (%v)", quiet.String(), err)
	}
	testutil.Post(t, st, store.Record{Kind: kinds.Go, Text: "arrived between waits"})
	if _, err := inbox.Tail(ctx, st, opts, &woke); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(woke.String(), "read by the desk") || !strings.Contains(woke.String(), "arrived between waits") {
		t.Fatalf("tail after a new record:\n%s", woke.String())
	}
}

func TestTailSeedWithNoSeedCursorStartsAtTheNewestRecord(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	testutil.Post(t, st, store.Record{Kind: kinds.Go, Text: "history"})
	opts := inbox.TailOptions{Filter: store.Filter{Drive: "d"}, Cursor: "desk-wake", Seed: "desk"}
	var first, second bytes.Buffer
	if _, err := inbox.Tail(ctx, st, opts, &first); err != nil || first.Len() != 0 {
		t.Fatalf("first seeded tail printed %q (%v)", first.String(), err)
	}
	testutil.Post(t, st, store.Record{Kind: kinds.Go, Text: "after the first wait"})
	if _, err := inbox.Tail(ctx, st, opts, &second); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(second.String(), "history") || !strings.Contains(second.String(), "after the first wait") {
		t.Fatalf("second seeded tail:\n%s", second.String())
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

func TestJSONTailNamesWhatItsBudgetCut(t *testing.T) {
	st, c := testutil.Store(t)
	ctx := context.Background()
	for i := range 40 {
		testutil.Post(t, st, store.Record{Kind: kinds.State, Text: fmt.Sprintf("step %02d %s", i, strings.Repeat("x", 80))})
		c.Advance(time.Second)
	}
	var out, notes bytes.Buffer
	res, err := inbox.Tail(ctx, st, inbox.TailOptions{Filter: store.Filter{Drive: "d"}, Budget: 1000, JSON: true, Notes: &notes}, &out)
	if err != nil {
		t.Fatal(err)
	}
	if res.More == 0 {
		t.Fatalf("a 1000-byte JSON tail of 40 records cut none: %+v", res)
	}
	if want := fmt.Sprintf("... %d more; resume with --since %d\n", res.More, res.Last); notes.String() != want {
		t.Errorf("JSON tail notes = %q, want %q", notes.String(), want)
	}
	if strings.Contains(out.String(), "...") {
		t.Errorf("JSON tail put its trailer among the records:\n%s", out.String())
	}
	read := strings.Count(out.String(), "\n")
	for res.More > 0 {
		var page bytes.Buffer
		if res, err = inbox.Tail(ctx, st, inbox.TailOptions{Filter: store.Filter{Drive: "d", After: res.Last}, Budget: 1000, JSON: true, Notes: io.Discard}, &page); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(page.String(), fmt.Sprintf("step %02d", read)) {
			t.Fatalf("JSON page after --since %d does not resume at step %02d, as a pager following its trailer needs:\n%s", res.Last, read, page.String())
		}
		read += strings.Count(page.String(), "\n")
	}
	if read != 40 {
		t.Fatalf("a JSON pager read %d of 40 records", read)
	}
}

func TestWindowTailKeepsTheNewestRecordsItsBudgetHolds(t *testing.T) {
	st, c := testutil.Store(t)
	ctx := context.Background()
	start := c.Now()
	for i := range 1200 {
		testutil.Post(t, st, store.Record{Kind: kinds.State, Text: fmt.Sprintf("step %04d %s", i, strings.Repeat("x", 80))})
		c.Advance(time.Second)
	}
	window := store.Filter{Drive: "d", Since: start}
	for _, tc := range []struct {
		limit int
		older int
	}{{0, 1200}, {900, 900}} {
		var out bytes.Buffer
		res, err := inbox.Tail(ctx, st, inbox.TailOptions{Filter: window, Limit: tc.limit, Budget: 6000}, &out)
		if err != nil {
			t.Fatal(err)
		}
		if res.Printed == 0 || res.Printed >= tc.older || res.More != 0 || out.Len() > 6000 {
			t.Fatalf("limit %d: tail = %+v in %d bytes, want a budget-cut read with nothing after it", tc.limit, res, out.Len())
		}
		lines := strings.Split(strings.TrimSpace(out.String()), "\n")
		if !strings.Contains(lines[len(lines)-2], "step 1199") || strings.Contains(out.String(), fmt.Sprintf("step %04d", 1200-tc.older)) {
			t.Fatalf("limit %d: budget kept the oldest records instead of the newest:\n%s", tc.limit, out.String())
		}
		want := fmt.Sprintf("... CUT %d older records before #%d; --oldest reads them first", tc.older-res.Printed, 1201-res.Printed)
		if lines[len(lines)-1] != want {
			t.Fatalf("limit %d: trailer = %q, want %q", tc.limit, lines[len(lines)-1], want)
		}
	}
	var oldest bytes.Buffer
	res, err := inbox.Tail(ctx, st, inbox.TailOptions{Filter: window, Budget: 6000, Oldest: true}, &oldest)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(oldest.String(), "step 0000") || strings.Contains(oldest.String(), "step 1199") || res.More != 1200-res.Printed {
		t.Fatalf("--oldest tail = %+v, want the window's first records and a resume trailer:\n%s", res, oldest.String())
	}
}

func TestTailReadsEveryRecordItsBudgetHoldsPastOnePage(t *testing.T) {
	st, _ := testutil.Store(t)
	for i := range 1200 {
		testutil.Post(t, st, store.Record{Kind: kinds.State, Text: fmt.Sprintf("step %04d", i)})
	}
	for _, asJSON := range []bool{false, true} {
		var out, notes bytes.Buffer
		res, err := inbox.Tail(context.Background(), st, inbox.TailOptions{Filter: store.Filter{Drive: "d"}, Budget: 4_000_000, JSON: asJSON, Notes: &notes}, &out)
		if err != nil {
			t.Fatal(err)
		}
		if res.Printed != 1200 || res.More != 0 || notes.Len() != 0 || !strings.Contains(out.String(), "step 1199") {
			t.Fatalf("json=%t tail printed %d, %d more, notes %q; want all 1200 records in one read", asJSON, res.Printed, res.More, notes.String())
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
			_, err := inbox.Tail(ctx, st, inbox.TailOptions{Filter: store.Filter{Drive: "d"}, Budget: 16000, Width: width, JSON: true, Notes: io.Discard}, b)
			return err
		},
		"grep": func(b *bytes.Buffer) error {
			return inbox.Grep(ctx, st, store.Filter{Drive: "d"}, regexp.MustCompile("059 z"), 0, 0, 0, false, b, nil)
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
	testutil.Post(t, st, store.Record{Kind: kinds.Landed, Text: "landed #30427", Refs: store.Refs{PRs: []store.PR{{Number: 30427}}}})
	testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "unrelated"})
	var out bytes.Buffer
	if err := inbox.Grep(context.Background(), st, store.Filter{Drive: "d"}, regexp.MustCompile("30427"), 0, 0, 0, false, &out, nil); err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(out.String()); !strings.Contains(got, "1 landed lane-a landed #30427") || strings.Contains(got, "unrelated") {
		t.Fatalf("grep output = %q", got)
	}
}

func TestDigest(t *testing.T) {
	st, c := testutil.Store(t)
	old := testutil.Post(t, st, store.Record{Kind: kinds.Ask, Text: "stale question"})
	c.Advance(30 * time.Hour)
	ask := testutil.Post(t, st, store.Record{Lane: "lane-b", Kind: kinds.Ask, Text: "which env?"})
	testutil.Post(t, st, store.Record{Kind: kinds.Hold, Text: "hold #1", Topic: "#1", Refs: store.Refs{PRs: []store.PR{{Number: 1}}}})
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
		}, inbox.WriteLines(&out, false))
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

func TestAWatchSinceNeverReplaysOlderAddressedMailOnAResumeOrAReArm(t *testing.T) {
	st, c := testutil.Store(t)
	ctx := context.Background()
	testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "handled hours ago", To: []string{"api"}})
	c.Advance(time.Hour)
	since := st.Now()
	watch := func(cursor string) []string {
		t.Helper()
		var got []string
		if err := inbox.Watch(ctx, st, inbox.WatchOptions{Filter: store.Filter{Drive: "d", For: "api", Since: since}, Cursor: cursor, Interval: time.Millisecond, For: 50 * time.Millisecond}, func(r store.Record, _ string) error {
			got = append(got, r.Text)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return got
	}
	resumed := "watch-1-1"
	if got := watch(resumed); len(got) != 0 {
		t.Fatalf("watch --since = %q; want no record older than --since", got)
	}
	if got := watch(resumed); len(got) != 0 {
		t.Fatalf("a watch resumed after a daemon restart = %q; want no record older than --since", got)
	}
	testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "after since", To: []string{"api"}})
	if got := watch("api-watch"); len(got) != 1 || got[0] != "after since" {
		t.Fatalf("watch --cursor api-watch --since = %q; want the one record inside --since", got)
	}
	if got := watch("api-watch"); len(got) != 0 {
		t.Fatalf("a re-armed watch on --cursor api-watch = %q; want nothing it already printed", got)
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

func TestReaderTailDeliversAddressedRecordsBeforeABacklogOfBroadcasts(t *testing.T) {
	st, c := testutil.Store(t)
	ctx := context.Background()
	own := testutil.Post(t, st, store.Record{Lane: "desk", Kind: kinds.Head, To: []string{"root"}, Text: "head of the desk's own PR"})
	broadcast := func(i int) {
		testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: fmt.Sprintf("broadcast %03d %s", i, strings.Repeat("x", 80))})
		c.Advance(time.Second)
	}
	for i := range 30 {
		broadcast(i)
	}
	order := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Decide, To: []string{"desk"}, Text: "enqueue the chain now"})
	reply := testutil.Post(t, st, store.Record{Lane: "fidelity", Kind: kinds.Review, To: []string{"root"}, Re: own.Seq, Text: "a review of the desk's head"})
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Decide, To: []string{"other"}, Text: "an order for another lane"})
	for i := range 30 {
		broadcast(30 + i)
	}
	opts := inbox.TailOptions{Filter: store.Filter{Drive: "d", For: "desk", Kinds: []kinds.Kind{kinds.Note}}, Cursor: "desk", Budget: 1200}
	var first bytes.Buffer
	res, err := inbox.Tail(ctx, st, opts, &first)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(first.String(), "\n")
	if !strings.HasPrefix(lines[0], fmt.Sprintf("#%d ", order.Seq)) || !strings.HasPrefix(lines[1], fmt.Sprintf("#%d ", reply.Seq)) {
		t.Fatalf("the records addressed to desk do not lead a capped page:\n%s", first.String())
	}
	if !strings.Contains(first.String(), "... 2 to desk first") {
		t.Fatalf("missing the addressed-first trailer:\n%s", first.String())
	}
	seen := map[string]int{}
	count := func(page string) {
		for _, line := range strings.Split(page, "\n") {
			if m := regexp.MustCompile(`^#\d+ .*?(broadcast \d{3}|enqueue the chain now|a review of the desk's head|an order for another lane)`).FindStringSubmatch(line); m != nil {
				seen[m[1]]++
			}
		}
	}
	count(first.String())
	for res.More > 0 {
		var page bytes.Buffer
		if res, err = inbox.Tail(ctx, st, opts, &page); err != nil {
			t.Fatal(err)
		}
		if res.Printed == 0 {
			t.Fatalf("a tail with %d more printed nothing:\n%s", res.More, page.String())
		}
		count(page.String())
	}
	for _, want := range []string{"enqueue the chain now", "a review of the desk's head"} {
		if seen[want] != 1 {
			t.Fatalf("%q delivered %d times, want once: %v", want, seen[want], seen)
		}
	}
	for i := range 60 {
		if key := fmt.Sprintf("broadcast %03d", i); seen[key] != 1 {
			t.Fatalf("%s delivered %d times, want once", key, seen[key])
		}
	}
	if seen["an order for another lane"] != 0 {
		t.Fatalf("delivered another lane's order: %v", seen)
	}

	var small bytes.Buffer
	testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "a broadcast before the order"})
	late := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Decide, To: []string{"desk"}, Text: "a second order"})
	if _, err := inbox.Tail(ctx, st, opts, &small); err != nil {
		t.Fatal(err)
	}
	if got := strings.Split(small.String(), "\n"); !strings.Contains(got[0], "a broadcast before the order") || !strings.HasPrefix(got[1], fmt.Sprintf("#%d ", late.Seq)) {
		t.Fatalf("a backlog that fits is not in seq order:\n%s", small.String())
	}
}

func TestLaneMailDeliversRootRecordsBeforeABacklogFromOtherLanes(t *testing.T) {
	st, c := testutil.Store(t)
	ctx := context.Background()
	ask := func(i int) {
		testutil.Post(t, st, store.Record{Lane: "desk-runner", Kind: kinds.Ask, To: []string{"landing"}, Text: fmt.Sprintf("merge ask %03d %s", i, strings.Repeat("x", 80))})
		c.Advance(time.Second)
	}
	for i := range 40 {
		ask(i)
	}
	order := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Decide, To: []string{"landing"}, Text: "retire enq.py now"})
	for i := range 20 {
		ask(40 + i)
	}
	opts := inbox.TailOptions{Filter: store.LaneMail(store.PeerKey{Session: "s1"}, store.Binding{Drive: "d", Lane: "landing"}), Cursor: "landing", Budget: 1200}
	var first bytes.Buffer
	res, err := inbox.Tail(ctx, st, opts, &first)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(first.String(), fmt.Sprintf("#%d ", order.Seq)) || !strings.Contains(first.String(), "from root first") {
		t.Fatalf("root's order does not lead a capped page of lane mail:\n%s", first.String())
	}
	seen := map[string]int{}
	count := func(page string) {
		for _, line := range strings.Split(page, "\n") {
			if m := regexp.MustCompile(`^#\d+ .*?(merge ask \d{3}|retire enq.py now)`).FindStringSubmatch(line); m != nil {
				seen[m[1]]++
			}
		}
	}
	count(first.String())
	for res.More > 0 {
		var page bytes.Buffer
		if res, err = inbox.Tail(ctx, st, opts, &page); err != nil {
			t.Fatal(err)
		}
		if res.Printed == 0 {
			t.Fatalf("a tail with %d more printed nothing:\n%s", res.More, page.String())
		}
		count(page.String())
	}
	if seen["retire enq.py now"] != 1 {
		t.Fatalf("root's order delivered %d times, want once: %v", seen["retire enq.py now"], seen)
	}
	for i := range 60 {
		if key := fmt.Sprintf("merge ask %03d", i); seen[key] != 1 {
			t.Fatalf("%s delivered %d times, want once", key, seen[key])
		}
	}
}

func TestATeammateBoundBeforeItsAnnounceHearsRecordsToAllOnlyFromItsBind(t *testing.T) {
	st, c := testutil.Store(t)
	ctx := context.Background()
	processStarted := c.Now()
	c.Advance(time.Hour)
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Go, To: []string{"landing"}, Text: "order waiting for the lane"})
	c.Advance(time.Hour)
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Correction, To: []string{"all"}, Text: "correction before the teammate"})
	c.Advance(time.Hour)
	k := store.PeerKey{Session: "s1", Agent: "alanding-2"}
	if err := st.BindLane(ctx, k, "d", "landing", processStarted); err != nil {
		t.Fatal(err)
	}
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Correction, To: []string{"all"}, Text: "correction after the bind"})
	var out bytes.Buffer
	if _, err := inbox.Tail(ctx, st, inbox.TailOptions{Filter: store.LaneMail(k, store.Binding{Drive: "d", Lane: "landing"}), Cursor: store.LaneCursor(k)}, &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); !strings.Contains(got, "order waiting for the lane") || !strings.Contains(got, "correction after the bind") || strings.Contains(got, "correction before the teammate") {
		t.Fatalf("a teammate bound on its first post replays records to all from its parent's process start, or drops its waiting order:\n%s", got)
	}
}

func TestARecordAddressedToALaneLeadsACappedPageOfBroadcasts(t *testing.T) {
	st, clock := testutil.Store(t)
	ctx := context.Background()
	for i := range 60 {
		testutil.Post(t, st, store.Record{Lane: "board-watch", Kind: kinds.Owner, To: []string{"all"}, Text: fmt.Sprintf("owner pick %02d, replayed", i)})
		clock.Advance(time.Second)
	}
	stand := testutil.Post(t, st, store.Record{Lane: "supervisor", Kind: kinds.StandDown, To: []string{"landing"}, Text: "landing stood down"})
	opts := inbox.TailOptions{Filter: store.LaneMail(store.PeerKey{Session: "s1"}, store.Binding{Drive: "d", Lane: "landing"}), Cursor: "landing", Budget: 400}
	var first bytes.Buffer
	if _, err := inbox.Tail(ctx, st, opts, &first); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(first.String(), fmt.Sprintf("#%d ", stand.Seq)) {
		t.Fatalf("the stand-down does not lead a capped page of broadcasts:\n%s", first.String())
	}
	picks := map[string]int{}
	stands := 0
	read := func(page string) {
		for _, line := range strings.Split(page, "\n") {
			if m := regexp.MustCompile(`owner pick \d\d`).FindString(line); m != "" && strings.HasPrefix(line, "#") {
				picks[m]++
			}
			if strings.HasPrefix(line, fmt.Sprintf("#%d ", stand.Seq)) {
				stands++
			}
		}
	}
	read(first.String())
	for range 80 {
		var next bytes.Buffer
		res, err := inbox.Tail(ctx, st, opts, &next)
		if err != nil {
			t.Fatal(err)
		}
		read(next.String())
		if res.Printed == 0 && res.More == 0 {
			break
		}
	}
	if stands != 1 || len(picks) != 60 {
		t.Fatalf("stand-down delivered %d times, %d of 60 broadcasts delivered", stands, len(picks))
	}
	for pick, n := range picks {
		if n != 1 {
			t.Fatalf("%s delivered %d times", pick, n)
		}
	}
}
