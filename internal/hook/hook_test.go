package hook_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/hook"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func TestReadPayload(t *testing.T) {
	p, err := hook.Read(strings.NewReader(`{"session_id":"s1","hook_event_name":"SessionStart","source":"compact"}`))
	if err != nil || p.SessionID != "s1" || p.Source != "compact" {
		t.Fatalf("Read() = %+v, %v", p, err)
	}
	if _, err := hook.Read(strings.NewReader("not json")); err == nil {
		t.Fatal("Read() accepted a non-JSON payload")
	}
}

func TestSessionStart(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	testutil.Post(t, st, store.Record{Kind: kinds.Ask, Text: "which env?"})
	var unbound bytes.Buffer
	if err := hook.SessionStart(ctx, st, hook.Payload{SessionID: "other"}, store.Window{}, &unbound); err != nil || unbound.Len() != 0 {
		t.Fatalf("unbound session wrote %q, %v", unbound.String(), err)
	}
	if err := st.Bind(ctx, store.PeerKey{Session: "s1"}, "d", true); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := hook.SessionStart(ctx, st, hook.Payload{SessionID: "s1", Source: "compact"}, store.Window{}, &out); err != nil {
		t.Fatal(err)
	}
	var got struct {
		HookSpecificOutput struct {
			HookEventName     string `json:"hookEventName"`
			AdditionalContext string `json:"additionalContext"`
		} `json:"hookSpecificOutput"`
	}
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("output %q: %v", out.String(), err)
	}
	ctxText := got.HookSpecificOutput.AdditionalContext
	if got.HookSpecificOutput.HookEventName != "SessionStart" || !strings.Contains(ctxText, "open asks (1, newest first):") || !strings.Contains(ctxText, "ASK lane-a which env?") {
		t.Fatalf("additionalContext = %q", ctxText)
	}
	if len(ctxText) > 4000 {
		t.Fatalf("additionalContext is %d bytes", len(ctxText))
	}
	seq, ok, err := st.Cursor(ctx, "s1", "d")
	if err != nil || !ok || seq == 0 {
		t.Fatalf("session cursor not advanced: %d %v %v", seq, ok, err)
	}
}

func TestPromptRecordsOwnerRulingsOnlyForRoot(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	if err := st.Bind(ctx, store.PeerKey{Session: "root"}, "d", true); err != nil {
		t.Fatal(err)
	}
	if err := st.Bind(ctx, store.PeerKey{Session: "lane"}, "d", false); err != nil {
		t.Fatal(err)
	}
	for _, p := range []hook.Payload{
		{SessionID: "lane", Prompt: "lane prompt"},
		{SessionID: "unbound", Prompt: "nobody"},
		{SessionID: "root", Prompt: "<teammate-message>relay</teammate-message>"},
		{SessionID: "root", Prompt: "/compact"},
		{SessionID: "root", Prompt: "  ship the wave fix now  "},
	} {
		if err := hook.Prompt(ctx, st, p, st.Now()); err != nil {
			t.Fatal(err)
		}
	}
	all, err := st.Query(ctx, store.Filter{Drive: "d"})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 || all[0].Kind != kinds.Owner || all[0].Lane != "owner" || all[0].Text != "ship the wave fix now" || all[0].Source != "hook:root" {
		t.Fatalf("records = %+v", all)
	}
}

func TestRootReaderSkipsTheOwnerPromptsItRecorded(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	if err := st.Bind(ctx, store.PeerKey{Session: "root"}, "d", true); err != nil {
		t.Fatal(err)
	}
	if err := hook.Prompt(ctx, st, hook.Payload{SessionID: "root", Prompt: "fix cci so my own messages are not echoed"}, st.Now()); err != nil {
		t.Fatal(err)
	}
	relayed := testutil.Post(t, st, store.Record{Lane: "owner", Kind: kinds.Owner, Text: "relayed from slack"})
	for reader, want := range map[string]int{"root": 1, "main": 1, "merge-walker-r2": 2} {
		got, err := st.Query(ctx, store.Filter{Drive: "d", For: reader})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != want || got[len(got)-1].Seq != relayed.Seq {
			t.Errorf("reader %s got %+v, want %d records ending at #%d", reader, got, want, relayed.Seq)
		}
	}
}

func TestSessionStartRepointsSubscription(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	launched := store.Window{PID: 100, Started: 1}
	resumed := store.Window{PID: 200, Started: 2}
	recycled := store.Window{PID: 200, Started: 3}
	if _, err := st.Subscribe(ctx, store.Subscription{Session: "s1", Window: launched, Drive: "d", Reader: "root", Cursor: "root-watch"}); err != nil {
		t.Fatal(err)
	}
	steps := []struct {
		name    string
		session string
		window  store.Window
		want    string
	}{
		{"resumed in a new process", "s1", resumed, "s1"},
		{"cleared in the same process", "s2", resumed, "s2"},
		{"compacted in the same process", "s2", resumed, "s2"},
		{"an unrelated session reuses the pid", "s3", recycled, ""},
	}
	for _, step := range steps {
		if err := hook.SessionStart(ctx, st, hook.Payload{SessionID: step.session}, step.window, &bytes.Buffer{}); err != nil {
			t.Fatal(err)
		}
		sub, ok, err := st.Subscription(ctx, step.window)
		if err != nil || ok != (step.want != "") || ok && (sub.Session != step.want || sub.Reader != "root" || sub.Cursor != "root-watch") {
			t.Fatalf("%s: subscription = %+v %v %v", step.name, sub, ok, err)
		}
	}
	for _, w := range []store.Window{launched, resumed} {
		if _, ok, _ := st.Subscription(ctx, w); ok {
			t.Fatalf("window %+v still holds a subscription", w)
		}
	}
}

func TestSessionStartSaysTheSubscriptionCameBack(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	launched := store.Window{PID: 100, Started: 1}
	resumed := store.Window{PID: 200, Started: 2}
	if _, err := st.Subscribe(ctx, store.Subscription{Session: "s1", Window: launched, Drive: "d", Reader: "owner-relay", Cursor: "channel-s1"}); err != nil {
		t.Fatal(err)
	}
	var back bytes.Buffer
	if err := hook.SessionStart(ctx, st, hook.Payload{SessionID: "s1"}, resumed, &back); err != nil {
		t.Fatal(err)
	}
	if got := hookContext(t, &back, "SessionStart"); !strings.Contains(got, "subscription back") || !strings.Contains(got, "owner-relay") {
		t.Fatalf("context = %q, want the reader that came back", got)
	}
	var again bytes.Buffer
	if err := hook.SessionStart(ctx, st, hook.Payload{SessionID: "s1"}, resumed, &again); err != nil {
		t.Fatal(err)
	}
	if got := hookContext(t, &again, "SessionStart"); strings.Contains(got, "subscription back") {
		t.Fatalf("a compaction in the same window said %q, want nothing", got)
	}
}

func hookContext(t *testing.T, out *bytes.Buffer, event string) string {
	t.Helper()
	if out.Len() == 0 {
		return ""
	}
	var got struct {
		HookSpecificOutput struct {
			HookEventName     string `json:"hookEventName"`
			AdditionalContext string `json:"additionalContext"`
		} `json:"hookSpecificOutput"`
	}
	if err := json.Unmarshal(out.Bytes(), &got); err != nil || got.HookSpecificOutput.HookEventName != event {
		t.Fatalf("output %q: %v", out.String(), err)
	}
	return got.HookSpecificOutput.AdditionalContext
}

func TestPostToolUseDeliversEveryKindAddressedToTheLane(t *testing.T) {
	st, clock := testutil.Store(t)
	ctx := context.Background()
	testutil.Post(t, st, store.Record{Lane: "lane-x", Kind: kinds.State, Text: "an earlier session of lane-x"})
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Stopped, To: []string{"lane-x"}, Text: "from before this session"})
	clock.Advance(time.Minute)
	started := clock.Now()
	clock.Advance(time.Minute)
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Stopped, To: []string{"lane-x"}, Text: "abandon this item"})
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Owner, To: []string{"lane-x", "lane-y"}, Text: "owner ruling"})
	testutil.Post(t, st, store.Record{Lane: "lane-b", Kind: kinds.Note, Text: "broadcast"})
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Go, To: []string{"lane-y"}, Text: "for another lane"})
	if err := st.BindLane(ctx, store.PeerKey{Session: "s1"}, "d", "lane-x", started); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := hook.PostToolUse(ctx, st, hook.Payload{SessionID: "s1", Event: "PostToolUse"}, noWindow, &out); err != nil {
		t.Fatal(err)
	}
	got := hookContext(t, &out, "PostToolUse")
	for _, want := range []string{"stopped root abandon this item", "owner root owner ruling"} {
		if !strings.Contains(got, want) {
			t.Fatalf("additionalContext lacks %q: %q", want, got)
		}
	}
	for _, unwanted := range []string{"from before this session", "broadcast", "for another lane"} {
		if strings.Contains(got, unwanted) {
			t.Fatalf("additionalContext carries %q: %q", unwanted, got)
		}
	}
	out.Reset()
	if err := hook.PostToolUse(ctx, st, hook.Payload{SessionID: "s1", Event: "PostToolUseFailure"}, noWindow, &out); err != nil || out.Len() != 0 {
		t.Fatalf("second round redelivered %q, %v", out.String(), err)
	}
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Defect, To: []string{"lane-x"}, Text: strings.Repeat("🛑", 399)})
	if err := hook.PostToolUse(ctx, st, hook.Payload{SessionID: "s1", Event: "PostToolUseFailure"}, noWindow, &out); err != nil {
		t.Fatal(err)
	}
	if got := hookContext(t, &out, "PostToolUseFailure"); !strings.Contains(got, "defect root 🛑") {
		t.Fatalf("an oversized record stalled delivery: %q", got)
	}
	out.Reset()
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Hold, To: []string{"lane-x"}, Text: "hold the apply"})
	if err := hook.SessionStart(ctx, st, hook.Payload{SessionID: "s1", Source: "compact"}, store.Window{}, &out); err != nil {
		t.Fatal(err)
	}
	if got := hookContext(t, &out, "SessionStart"); !strings.Contains(got, "hold root hold the apply") || strings.Contains(got, "open asks") {
		t.Fatalf("SessionStart additionalContext = %q", got)
	}
}

func TestPostToolUseDeliversBroadcastKindsOnlyToTheirFirstAddressee(t *testing.T) {
	st, clock := testutil.Store(t)
	ctx := context.Background()
	started := clock.Now()
	clock.Advance(time.Minute)
	for _, r := range []store.Record{
		{Lane: "desk", Kind: kinds.Review, To: []string{"lane-y", "lane-x"}, Text: "review copied to lane-x"},
		{Lane: "lane-y", Kind: kinds.Head, To: []string{"lane-z", "lane-x"}, Text: "head copied to lane-x"},
		{Lane: "desk", Kind: kinds.Review, To: []string{"lane-x", "lane-y"}, Text: "review for lane-x first"},
		{Lane: "root", Kind: kinds.Go, To: []string{"lane-y", "lane-x"}, Text: "go copied to lane-x"},
	} {
		testutil.Post(t, st, r)
	}
	if err := st.BindLane(ctx, store.PeerKey{Session: "s1"}, "d", "lane-x", started); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := hook.PostToolUse(ctx, st, hook.Payload{SessionID: "s1", Event: "PostToolUse"}, noWindow, &out); err != nil {
		t.Fatal(err)
	}
	got := hookContext(t, &out, "PostToolUse")
	if !strings.Contains(got, "review for lane-x first") || !strings.Contains(got, "go copied to lane-x") || strings.Contains(got, "review copied") || strings.Contains(got, "head copied") {
		t.Fatalf("additionalContext = %q; want the review addressed to lane-x first and the copied go, never a review or head lane-x is only copied on", got)
	}
}

func TestPostToolUseDeliversAReviewOfALanesOwnPREvenWhenItIsOnlyCopied(t *testing.T) {
	st, clock := testutil.Store(t)
	ctx := context.Background()
	started := clock.Now()
	clock.Advance(time.Minute)
	pr := func(n int) store.Refs { return store.Refs{PRs: []store.PR{{Repo: "o/r", Number: n}}} }
	for _, r := range []store.Record{
		{Lane: "lane-x", Kind: kinds.Opened, Refs: pr(7), Text: "opened o/r#7"},
		{Lane: "desk", Kind: kinds.Review, To: []string{"root", "lane-x"}, Refs: pr(7), Text: "fidelity missing on lane-x's own PR"},
		{Lane: "desk", Kind: kinds.Review, To: []string{"root", "lane-x"}, Refs: pr(8), Text: "review of a PR lane-x never opened"},
	} {
		testutil.Post(t, st, r)
	}
	if err := st.BindLane(ctx, store.PeerKey{Session: "s1"}, "d", "lane-x", started); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := hook.PostToolUse(ctx, st, hook.Payload{SessionID: "s1", Event: "PostToolUse"}, noWindow, &out); err != nil {
		t.Fatal(err)
	}
	got := hookContext(t, &out, "PostToolUse")
	if !strings.Contains(got, "fidelity missing on lane-x's own PR") || strings.Contains(got, "never opened") {
		t.Fatalf("additionalContext = %q; want the review of lane-x's own PR and not the one it is only copied on", got)
	}
}

func TestALaneBoundBeforeTheRelayFilledItsStoreHearsNoOlderBroadcast(t *testing.T) {
	st, clock := testutil.Store(t)
	ctx := context.Background()
	if err := st.BindLane(ctx, store.PeerKey{Session: "s1"}, "d", "lane-x", clock.Now()); err != nil {
		t.Fatal(err)
	}
	clock.Advance(-time.Hour)
	testutil.Post(t, st, store.Record{Lane: "board-watch", Kind: kinds.Owner, To: []string{"all"}, Text: "owner pick from before this lane"})
	clock.Advance(time.Hour + time.Minute)
	testutil.Post(t, st, store.Record{Lane: "board-watch", Kind: kinds.Owner, To: []string{"all"}, Text: "owner pick since this lane"})

	var out bytes.Buffer
	if err := hook.PostToolUse(ctx, st, hook.Payload{SessionID: "s1", Event: "PostToolUse"}, noWindow, &out); err != nil {
		t.Fatal(err)
	}
	got := hookContext(t, &out, "PostToolUse")
	if !strings.Contains(got, "owner pick since this lane") || strings.Contains(got, "owner pick from before this lane") {
		t.Fatalf("additionalContext = %q; want only the broadcast written since the lane started", got)
	}
}

func TestALaneReadsNoSeedBackfillAsMailButStillOwesTheAsksItBrought(t *testing.T) {
	st, clock := testutil.Store(t)
	ctx := context.Background()
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Ask, To: []string{"lane-x"}, Text: "posted before the lane bound"})
	clock.Advance(time.Minute)
	k := store.PeerKey{Session: "s1"}
	if err := st.BindLane(ctx, k, "d", "lane-x", clock.Now()); err != nil {
		t.Fatal(err)
	}
	backfilled := clock.Now().Add(-time.Hour)
	if _, err := st.Backfill(ctx, []store.Row{
		{Origin: "0123456789abcdef", OriginSeq: 1, Drive: "d", Lane: "root", Kind: kinds.Ask, At: backfilled, Text: "a backfilled ask", To: []string{"lane-x"}, Source: "post"},
		{Origin: "0123456789abcdef", OriginSeq: 2, Drive: "d", Lane: "root", Kind: kinds.Note, At: backfilled, Text: "a backfilled note", To: []string{"lane-x"}, Source: "post"},
	}, nil); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Minute)
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, To: []string{"lane-x"}, Text: "posted since the lane started"})

	var out bytes.Buffer
	if err := hook.PostToolUse(ctx, st, hook.Payload{SessionID: "s1", Event: "PostToolUse"}, noWindow, &out); err != nil {
		t.Fatal(err)
	}
	got := hookContext(t, &out, "PostToolUse")
	if !strings.Contains(got, "posted since the lane started") || !strings.Contains(got, "posted before the lane bound") || strings.Contains(got, "a backfilled note") {
		t.Fatalf("additionalContext = %q; want what was posted to the lane before it bound and since it started, and no note a relay backfilled after it bound", got)
	}
	owed, err := hook.Owed(ctx, st, "d", "lane-x")
	if err != nil {
		t.Fatal(err)
	}
	if len(owed) != 2 || owed[0].Text != "posted before the lane bound" || owed[1].Text != "a backfilled ask" {
		t.Fatalf("owed = %+v; want the ask posted before the lane bound and the backfilled ask, which is owed though it is not mail", owed)
	}
}

func TestARecordPostedWhileALaneSpawnedReachesItLiveAfterItBound(t *testing.T) {
	st, clock := testutil.Store(t)
	ctx := context.Background()
	posted := clock.Now()
	clock.Advance(10 * time.Minute)
	k := store.PeerKey{Session: "s1"}
	if err := st.BindLane(ctx, k, "d", "lane-x", clock.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Backfill(ctx, []store.Row{
		{Origin: "0123456789abcdef", OriginSeq: 1, Drive: "d", Lane: "root", Kind: kinds.Note, At: posted.Add(-time.Hour), Text: "a seeded note", To: []string{"lane-x"}, Source: "post"},
	}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Apply(ctx, []store.Row{
		{Origin: "0123456789abcdef", OriginSeq: 2, Drive: "d", Lane: "root", Kind: kinds.Ask, At: posted, Text: "instructions sent while the lane spawned", To: []string{"lane-x"}, Source: "post"},
	}, nil); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := hook.PostToolUse(ctx, st, hook.Payload{SessionID: "s1", Event: "PostToolUse"}, noWindow, &out); err != nil {
		t.Fatal(err)
	}
	if got := hookContext(t, &out, "PostToolUse"); !strings.Contains(got, "instructions sent while the lane spawned") || strings.Contains(got, "a seeded note") {
		t.Fatalf("additionalContext = %q; want the record a live delivery brought after the bind, and not the seeded one", got)
	}
	owed, err := hook.Owed(ctx, st, "d", "lane-x")
	if err != nil {
		t.Fatal(err)
	}
	if len(owed) != 1 || owed[0].Text != "instructions sent while the lane spawned" {
		t.Fatalf("owed = %+v; want the ask a live delivery brought after the bind", owed)
	}
}

func TestPostToolUseDeliversAReportToEveryLaneItNames(t *testing.T) {
	st, clock := testutil.Store(t)
	ctx := context.Background()
	if err := st.BindLane(ctx, store.PeerKey{Session: "s1"}, "d", "bugs-2", clock.Now()); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Minute)
	testutil.Post(t, st, store.Record{Lane: "regressions", Kind: kinds.Report, To: []string{"root", "bugs", "owner-relay"}, Text: "item 36 not fixed as seen"})
	testutil.Post(t, st, store.Record{Lane: "regressions", Kind: kinds.Review, To: []string{"root", "bugs"}, Text: "a review bugs is copied on"})

	var out bytes.Buffer
	if err := hook.PostToolUse(ctx, st, hook.Payload{SessionID: "s1", Event: "PostToolUse"}, noWindow, &out); err != nil {
		t.Fatal(err)
	}
	got := hookContext(t, &out, "PostToolUse")
	if !strings.Contains(got, "item 36 not fixed as seen") || strings.Contains(got, "a review bugs is copied on") {
		t.Fatalf("additionalContext = %q; want the report that names bugs after root, and never the copied review", got)
	}
}

func TestALanesHookFeedsNoAliasMailItsPredecessorCouldHaveRead(t *testing.T) {
	st, clock := testutil.Store(t)
	ctx := context.Background()
	note := func(lane, to, text string) {
		t.Helper()
		clock.Advance(time.Minute)
		testutil.Post(t, st, store.Record{Lane: lane, Kind: kinds.Note, To: []string{to}, Text: text})
	}
	clock.Advance(time.Minute)
	testutil.Post(t, st, store.Record{Lane: "desk-1", Kind: kinds.State, Text: "desk-1 up"})
	clock.Advance(10 * time.Minute)
	testutil.Post(t, st, store.Record{Lane: "desk-1", Kind: kinds.State, Text: "desk-1 still working"})
	clock.Advance(time.Minute)
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.StandDown, To: []string{"desk-1"}, Text: "desk-1 stands down"})
	note("root", "desk", "an order while no one held desk")
	note("root", "worker", "an order before any worker registered")
	clock.Advance(time.Minute)
	started := clock.Now()
	clock.Advance(-10 * time.Minute)
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, To: []string{"desk"}, Text: "an order desk-1 read"})
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Decide, To: []string{"desk"}, Text: "a decide desk-1 may have missed"})
	clock.Advance(-5 * time.Minute)
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Decide, To: []string{"desk"}, Text: "a decide from before desk-1"})
	clock.Advance(15 * time.Minute)
	if err := st.BindLane(ctx, store.PeerKey{Session: "s1"}, "d", "desk-2", started); err != nil {
		t.Fatal(err)
	}
	if err := st.BindLane(ctx, store.PeerKey{Session: "s2"}, "d", "worker-1", started); err != nil {
		t.Fatal(err)
	}
	clock.Advance(10 * time.Minute)
	note("root", "desk", "an order for desk-2")

	var out bytes.Buffer
	if err := hook.PostToolUse(ctx, st, hook.Payload{SessionID: "s1", Event: "PostToolUse"}, noWindow, &out); err != nil {
		t.Fatal(err)
	}
	got := hookContext(t, &out, "PostToolUse")
	if strings.Contains(got, "an order desk-1 read") || strings.Contains(got, "a decide from before desk-1") || !strings.Contains(got, "a decide desk-1 may have missed") || !strings.Contains(got, "an order while no one held desk") || !strings.Contains(got, "an order for desk-2") {
		t.Fatalf("desk-2's hook = %q; want no other mail from before desk-1's last post, no order from before desk-1 started, and the rest", got)
	}
	older, err := st.Query(ctx, store.Filter{Drive: "d", To: "desk-2", Kinds: []kinds.Kind{kinds.Note}})
	if err != nil || !slices.ContainsFunc(older, func(r store.Record) bool { return r.Text == "an order desk-1 read" }) {
		t.Fatalf("a read past the hook = %+v, %v; want the older mail still readable", older, err)
	}
	out.Reset()
	if err := hook.PostToolUse(ctx, st, hook.Payload{SessionID: "s2", Event: "PostToolUse"}, noWindow, &out); err != nil {
		t.Fatal(err)
	}
	if got := hookContext(t, &out, "PostToolUse"); !strings.Contains(got, "an order before any worker registered") {
		t.Fatalf("worker-1's hook = %q; want the order sent to a stem nobody ever held", got)
	}
}

func TestLaneCursorSurvivesOtherReadsAndLateImports(t *testing.T) {
	st, clock := testutil.Store(t)
	ctx := context.Background()
	started := clock.Now()
	clock.Advance(time.Minute)
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Stopped, To: []string{"lane-x"}, Text: "abandon this item"})
	testutil.Post(t, st, store.Record{Lane: "lane-b", Kind: kinds.State, Text: "state"})
	clock.Advance(-time.Hour)
	testutil.Post(t, st, store.Record{Lane: "lane-c", Kind: kinds.Note, Text: "imported late"})
	clock.Advance(time.Hour)
	if err := st.Bind(ctx, store.PeerKey{Session: "s1"}, "d", false); err != nil {
		t.Fatal(err)
	}
	if err := st.BindLane(ctx, store.PeerKey{Session: "s1"}, "d", "lane-x", started); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := hook.PostToolUse(ctx, st, hook.Payload{SessionID: "s1", Event: "PostToolUse"}, noWindow, &out); err != nil {
		t.Fatal(err)
	}
	if got := hookContext(t, &out, "PostToolUse"); !strings.Contains(got, "stopped root abandon this item") {
		t.Fatalf("additionalContext = %q", got)
	}
	if err := st.Bind(ctx, store.PeerKey{Session: "s1"}, "d", true); err != nil {
		t.Fatal(err)
	}
	if b, _, err := st.Binding(ctx, store.PeerKey{Session: "s1"}); err != nil || !b.Root || b.Lane != "" {
		t.Fatalf("binding after root rebind = %+v %v", b, err)
	}
}

func TestPostingAsALaneLeavesTheRootBinding(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	if err := st.Bind(ctx, store.PeerKey{Session: "root"}, "d", true); err != nil {
		t.Fatal(err)
	}
	if err := st.BindLane(ctx, store.PeerKey{Session: "root"}, "d", "runner", time.Time{}); err != nil {
		t.Fatal(err)
	}
	b, ok, err := st.Binding(ctx, store.PeerKey{Session: "root"})
	if err != nil || !ok || !b.Root || b.Lane != "" {
		t.Fatalf("root binding = %+v %v %v", b, ok, err)
	}
	testutil.Post(t, st, store.Record{Lane: "lane-a", Kind: kinds.Stopped, To: []string{"runner"}, Text: "not for root"})
	var out bytes.Buffer
	if err := hook.PostToolUse(ctx, st, hook.Payload{SessionID: "root", Event: "PostToolUse"}, noWindow, &out); err != nil || out.Len() != 0 {
		t.Fatalf("root PostToolUse wrote %q, %v", out.String(), err)
	}
}

func TestLaneHookDeliversPeerRecords(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	if err := st.BindLane(ctx, store.PeerKey{Session: "s-api"}, "d", "api", st.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	testutil.Post(t, st, store.Record{Drive: "other", Lane: "root", Kind: kinds.Go, Text: "cross-drive go", To: []string{store.PeerAddress("s-api")}})
	testutil.Post(t, st, store.Record{Drive: "other", Lane: "root", Kind: kinds.Go, Text: "lane at drive", To: []string{"api@d"}})
	testutil.Post(t, st, store.Record{Drive: "other", Lane: "root", Kind: kinds.Go, Text: "not for api", To: []string{"api@elsewhere"}})
	var out bytes.Buffer
	if err := hook.PostToolUse(ctx, st, hook.Payload{SessionID: "s-api", Event: "PostToolUse"}, noWindow, &out); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	if !strings.Contains(text, "cross-drive go") || !strings.Contains(text, "lane at drive") || strings.Contains(text, "not for api") {
		t.Fatalf("PostToolUse context = %q", text)
	}
}

func TestLaneRebindToAnotherDriveSkipsDeliveredPeerRecords(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	k := store.PeerKey{Session: "s-api"}
	started := st.Now().Add(-time.Hour)
	if err := st.BindLane(ctx, k, "a", "api", started); err != nil {
		t.Fatal(err)
	}
	testutil.Post(t, st, store.Record{Drive: "other", Lane: "root", Kind: kinds.Go, Text: "peer go", To: []string{store.PeerAddress("s-api")}})
	testutil.Post(t, st, store.Record{Drive: "b", Lane: "root", Kind: kinds.Hold, Text: "hold on b", To: []string{"api"}})
	var out bytes.Buffer
	if err := hook.PostToolUse(ctx, st, hook.Payload{SessionID: "s-api", Event: "PostToolUse"}, noWindow, &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); !strings.Contains(got, "peer go") || strings.Contains(got, "hold on b") {
		t.Fatalf("drive a delivery = %q", got)
	}
	if err := st.Bind(ctx, k, "b", false); err != nil {
		t.Fatal(err)
	}
	if err := st.BindLane(ctx, k, "b", "api", started); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := hook.PostToolUse(ctx, st, hook.Payload{SessionID: "s-api", Event: "PostToolUse"}, noWindow, &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); strings.Contains(got, "peer go") || !strings.Contains(got, "hold on b") {
		t.Fatalf("drive b delivery = %q", got)
	}
}

func TestPostToolUseRecordsTheAgentThatSpawnedASubagent(t *testing.T) {
	st, clock := testutil.Store(t)
	ctx := context.Background()
	projects := t.TempDir()
	transcript := filepath.Join(projects, "s1.jsonl")
	if err := os.MkdirAll(filepath.Join(projects, "s1", "subagents"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(projects, "s1", "subagents", "agent-aship-0001.meta.json"), []byte(`{"agentType":"ship-pr","parentAgentId":"aapi-0123"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	api := store.PeerKey{Session: "s1", Agent: "aapi-0123"}
	if err := st.BindLane(ctx, api, "d", "api", clock.Now()); err != nil {
		t.Fatal(err)
	}
	ship := hook.Payload{SessionID: "s1", AgentID: "aship-0001", TranscriptPath: transcript, Cwd: t.TempDir(), Event: "PostToolUse"}
	if err := hook.PostToolUse(ctx, st, ship, noWindow, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if owner, b, ok, err := st.Owner(ctx, ship.Key()); err != nil || !ok || owner != api || b.Lane != "api" {
		t.Fatalf("Owner(%s) = %s %+v %v, %v; want the spawning lane %s", ship.Key(), owner, b, ok, err, api)
	}
}

func noWindow() (store.Window, error) {
	return store.Window{}, nil
}

func TestPostToolUseAnnouncesAgentsAndDeliversTheirLanes(t *testing.T) {
	st, clock := testutil.Store(t)
	ctx := context.Background()
	projects := t.TempDir()
	transcript := filepath.Join(projects, "s1.jsonl")
	if err := os.MkdirAll(filepath.Join(projects, "s1", "subagents"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(projects, "s1", "subagents", "agent-aapi-0123.meta.json"), []byte(`{"agentType":"lane","name":"api-lane"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	api := hook.Payload{SessionID: "s1", AgentID: "aapi-0123", TranscriptPath: transcript, Cwd: t.TempDir(), Event: "PostToolUse"}
	web := hook.Payload{SessionID: "s1", AgentID: "aweb-4567", TranscriptPath: transcript, Cwd: t.TempDir(), Event: "PostToolUse"}
	window := func() (store.Window, error) { return store.Window{PID: 42, Started: 7}, nil }
	for _, p := range []hook.Payload{api, web} {
		if err := hook.PostToolUse(ctx, st, p, window, &bytes.Buffer{}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := st.Peer(ctx, api.Key())
	if err != nil || got.AgentName != "api-lane" || got.Role != "agent" || got.PID != 42 || got.Superseded {
		t.Fatalf("announced api agent = %+v, %v", got, err)
	}
	if got, err := st.Peer(ctx, web.Key()); err != nil || got.AgentName != "" {
		t.Fatalf("announced unnamed agent = %+v, %v", got, err)
	}
	if _, err := st.Peer(ctx, store.PeerKey{Session: "s1"}); err == nil {
		t.Fatal("an agent's tool call announced its root thread")
	}
	started := clock.Now()
	clock.Advance(time.Minute)
	if err := st.BindLane(ctx, api.Key(), "d", "api", started); err != nil {
		t.Fatal(err)
	}
	if err := st.BindLane(ctx, web.Key(), "d", "web", started); err != nil {
		t.Fatal(err)
	}
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Go, To: []string{"api"}, Text: "ship api"})
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Go, To: []string{"web"}, Text: "ship web"})
	var out bytes.Buffer
	if err := hook.PostToolUse(ctx, st, api, window, &out); err != nil {
		t.Fatal(err)
	}
	if got := hookContext(t, &out, "PostToolUse"); !strings.Contains(got, "go root ship api") || strings.Contains(got, "ship web") {
		t.Fatalf("api agent's delivery = %q", got)
	}
	out.Reset()
	if err := hook.PostToolUse(ctx, st, web, window, &out); err != nil {
		t.Fatal(err)
	}
	if got := hookContext(t, &out, "PostToolUse"); !strings.Contains(got, "go root ship web") || strings.Contains(got, "ship api") {
		t.Fatalf("web agent's delivery = %q", got)
	}
	if err := hook.SubagentStop(ctx, st, api, io.Discard); err != nil {
		t.Fatal(err)
	}
	if got, _ := st.Peer(ctx, api.Key()); !got.Superseded {
		t.Fatalf("stopped agent = %+v", got)
	}
	if err := hook.PostToolUse(ctx, st, api, window, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if got, _ := st.Peer(ctx, api.Key()); got.Superseded || got.Lane != "api" {
		t.Fatalf("agent after its next tool call = %+v", got)
	}
	resumed := func() (store.Window, error) { return store.Window{PID: 43, Started: 8}, nil }
	if err := hook.PostToolUse(ctx, st, api, resumed, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if got, _ := st.Peer(ctx, api.Key()); got.PID != 43 || got.PIDStarted != 8 {
		t.Fatalf("agent in a new process kept its old window: %+v", got)
	}
}

func TestASuccessorTeammateReadsFromItsSpawnAndNothingClosed(t *testing.T) {
	st, clock := testutil.Store(t)
	ctx := context.Background()
	rootStarted := clock.Now()
	clock.Advance(time.Hour)
	testutil.Post(t, st, store.Record{Lane: "lane-x", Kind: kinds.State, Text: "the predecessor at work"})
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, To: []string{"lane-x"}, Text: "for the predecessor"})
	clock.Advance(time.Minute)
	successor := hook.Payload{SessionID: "s1", AgentID: "alane-x-2", TranscriptPath: filepath.Join(t.TempDir(), "s1.jsonl"), Cwd: t.TempDir(), Event: "PostToolUse"}
	meta := filepath.Join(strings.TrimSuffix(successor.TranscriptPath, ".jsonl"), "subagents", "agent-alane-x-2.meta.json")
	if err := os.MkdirAll(filepath.Dir(meta), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(meta, []byte(`{"agentType":"lane","name":"lane-x-2"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(meta, clock.Now(), clock.Now()); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Minute)
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, To: []string{"lane-x"}, Text: "before its first tool call"})
	clock.Advance(time.Minute)
	rootWindow := func() (store.Window, error) { return store.Window{PID: 42, Started: rootStarted.UnixMilli()}, nil }
	if err := hook.PostToolUse(ctx, st, successor, rootWindow, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Minute)
	if err := st.BindLane(ctx, successor.Key(), "d", "lane-x", rootStarted); err != nil {
		t.Fatal(err)
	}
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Defect, To: []string{"lane-x"}, Text: "for the successor"})
	resolved := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Ask, To: []string{"lane-x"}, Text: "resolved already"})
	testutil.Post(t, st, store.Record{Lane: "lane-y", Kind: kinds.Answer, To: []string{"root"}, Text: "done", Resolves: resolved.Seq})
	answered := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Ask, To: []string{"lane-x"}, Text: "answered already"})
	testutil.Post(t, st, store.Record{Lane: "lane-y", Kind: kinds.Answer, To: []string{"root"}, Text: "yes", Re: answered.Seq})
	var out bytes.Buffer
	if err := hook.PostToolUse(ctx, st, successor, rootWindow, &out); err != nil {
		t.Fatal(err)
	}
	got := hookContext(t, &out, "PostToolUse")
	for _, want := range []string{"before its first tool call", "defect root for the successor"} {
		if !strings.Contains(got, want) {
			t.Errorf("successor's delivery = %q, want %q", got, want)
		}
	}
	for _, stale := range []string{"for the predecessor", "resolved already", "answered already"} {
		if strings.Contains(got, stale) {
			t.Errorf("successor's delivery = %q, want no %q", got, stale)
		}
	}
}

func TestPostToolUseNamesWorkflowAgentsAndDeliversTheirPeerRecords(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	projects := t.TempDir()
	transcript := filepath.Join(projects, "s1.jsonl")
	wf := filepath.Join(projects, "s1", "subagents", "workflows", "wf_0c95122f-2e2")
	if err := os.MkdirAll(wf, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(wf, "agent-a7bb-0123.meta.json"), []byte(`{"agentType":"workflow-subagent","description":"apply:box-and-capture","workflowPhase":"Apply"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	agent := hook.Payload{SessionID: "s1", AgentID: "a7bb-0123", TranscriptPath: transcript, Cwd: t.TempDir(), Event: "PostToolUse"}
	window := func() (store.Window, error) { return store.Window{PID: 42, Started: 7}, nil }
	var out bytes.Buffer
	if err := hook.PostToolUse(ctx, st, agent, window, &out); err != nil || out.Len() != 0 {
		t.Fatalf("first tool call wrote %q, %v", out.String(), err)
	}
	if got, err := st.Peer(ctx, agent.Key()); err != nil || got.AgentName != "apply:box-and-capture" || got.Role != "workflow" {
		t.Fatalf("announced workflow agent = %+v, %v", got, err)
	}
	testutil.Post(t, st, store.Record{Drive: "d", Lane: "root", Kind: kinds.Go, To: []string{store.PeerAddress(agent.Key().String())}, Text: "stop after this file"})
	testutil.Post(t, st, store.Record{Drive: "d", Lane: "root", Kind: kinds.Go, To: []string{"api"}, Text: "for a lane"})
	if err := hook.PostToolUse(ctx, st, agent, window, &out); err != nil {
		t.Fatal(err)
	}
	if got := hookContext(t, &out, "PostToolUse"); !strings.Contains(got, "addressed to peer:s1/a7bb-0123") || !strings.Contains(got, "stop after this file") || strings.Contains(got, "for a lane") {
		t.Fatalf("workflow agent's delivery = %q", got)
	}
	out.Reset()
	if err := hook.PostToolUse(ctx, st, agent, window, &out); err != nil || out.Len() != 0 {
		t.Fatalf("redelivered %q, %v", out.String(), err)
	}
	var root bytes.Buffer
	if err := hook.PostToolUse(ctx, st, hook.Payload{SessionID: "s1", Event: "PostToolUse"}, window, &root); err != nil || root.Len() != 0 {
		t.Fatalf("unbound root thread got %q, %v", root.String(), err)
	}
}

func bindRoot(t *testing.T, st *store.Store) hook.Payload {
	t.Helper()
	if err := st.BindLane(context.Background(), store.PeerKey{Session: "s1"}, "d", "root", time.Time{}); err != nil {
		t.Fatal(err)
	}
	return hook.Payload{SessionID: "s1", Event: "PostToolUse"}
}

func TestPostToolUseLeadsWithTheOpenAsksBehindABacklog(t *testing.T) {
	st, clock := testutil.Store(t)
	ctx := context.Background()
	p := bindRoot(t, st)
	answered := testutil.Post(t, st, store.Record{Kind: kinds.Ask, To: []string{"root"}, Text: "answered ask"})
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Answer, Re: answered.Seq, Text: "yes"})
	resolved := testutil.Post(t, st, store.Record{Kind: kinds.Blocker, To: []string{"main"}, Text: "resolved blocker"})
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, Resolves: resolved.Seq, Text: "cleared"})
	testutil.Post(t, st, store.Record{Kind: kinds.Ask, To: []string{"lane-b"}, Text: "ask for another lane"})
	testutil.Post(t, st, store.Record{Kind: kinds.Ask, To: []string{"lane-b", "root"}, Text: "ask copying root"})
	testutil.Post(t, st, store.Record{Lane: "watchdog", Kind: kinds.Decide, To: []string{"root", "lane-b"}, Text: "critical path status"})
	testutil.Post(t, st, store.Record{Lane: "main", Kind: kinds.Blocker, To: []string{"root"}, Text: "root's own blocker"})
	for i := range 5 {
		testutil.Post(t, st, store.Record{Kind: kinds.Ask, To: []string{"root"}, Text: fmt.Sprintf("ask %d", i)})
	}
	testutil.Post(t, st, store.Record{Kind: kinds.Decide, To: []string{"root@d"}, Text: "merge it now?"})
	for i := range 200 {
		testutil.Post(t, st, store.Record{Lane: "lane-b", Kind: kinds.State, To: []string{"root"}, Text: fmt.Sprintf("backlog %d", i)})
	}
	clock.Advance(3 * time.Minute)
	fresh := testutil.Post(t, st, store.Record{Lane: "lane-c", Kind: kinds.Ask, To: []string{"main"}, Text: "which branch?"})
	clock.Advance(2 * time.Minute)
	var out bytes.Buffer
	if err := hook.PostToolUse(ctx, st, p, noWindow, &out); err != nil {
		t.Fatal(err)
	}
	got := hookContext(t, &out, "PostToolUse")
	lines := strings.Split(got, "\n")
	if !strings.HasPrefix(lines[0], "open asks addressed to root, newest first") || !strings.HasPrefix(lines[1], fmt.Sprintf("#%d ", fresh.Seq)) || !strings.HasSuffix(lines[1], "which branch? (open 2m)") {
		t.Fatalf("the newest open ask does not lead: %q", got)
	}
	open, replay, ok := strings.Cut(got, "cci records addressed to root; act on each:\n")
	if !ok || strings.Contains(replay, "backlog 199") || strings.Contains(replay, "which branch?") {
		t.Fatalf("cursor replay changed: %q", got)
	}
	if !strings.Contains(open, "merge it now?") || !strings.Contains(open, "ask 2") || strings.Contains(open, "ask 1 ") || !strings.Contains(open, "... 2 older open asks") {
		t.Fatalf("open section is not capped at five: %q", open)
	}
	for _, closed := range []string{"answered ask", "resolved blocker", "ask for another lane", "ask copying root", "critical path status", "root's own blocker"} {
		if strings.Contains(open, closed) {
			t.Fatalf("open section carries %q: %q", closed, open)
		}
	}
}

func TestTheOpenAskHeaderPrintsOncePerAsk(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	p := bindRoot(t, st)
	deliver := func(event string) string {
		t.Helper()
		var out bytes.Buffer
		var err error
		if event == "SessionStart" {
			err = hook.SessionStart(ctx, st, p, store.Window{}, &out)
		} else {
			err = hook.PostToolUse(ctx, st, p, noWindow, &out)
		}
		if err != nil {
			t.Fatal(err)
		}
		return hookContext(t, &out, event)
	}
	const listed, unchanged = "open asks addressed to root, newest first", "cci records addressed to root; act on each:"
	asks := map[string]int64{}
	for _, tt := range []struct {
		event   string
		ask     string
		resolve string
		note    string
		want    string
	}{
		{"PostToolUse", "which branch?", "", "the first list", listed},
		{"PostToolUse", "", "", "a note changes no ask", unchanged},
		{"SessionStart", "", "", "after a compaction", listed},
		{"PostToolUse", "", "", "still the one ask", unchanged},
		{"PostToolUse", "which env?", "", "a second ask", "1 new open asks addressed to root (2 open); newest: #"},
		{"PostToolUse", "", "", "both asks printed", unchanged},
		{"PostToolUse", "", "which branch?", "one answered", unchanged},
		{"PostToolUse", "", "which env?", "every ask answered", unchanged},
		{"PostToolUse", "which region?", "", "an ask after an empty list", "1 new open asks addressed to root (1 open); newest: #"},
	} {
		if tt.ask != "" {
			asks[tt.ask] = testutil.Post(t, st, store.Record{Kind: kinds.Ask, To: []string{"root"}, Text: tt.ask}).Seq
		}
		if tt.resolve != "" {
			testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, Resolves: asks[tt.resolve], Text: "resolved"})
		}
		testutil.Post(t, st, store.Record{Kind: kinds.Note, To: []string{"root"}, Text: tt.note})
		got := deliver(tt.event)
		if !strings.HasPrefix(got, tt.want) || !strings.Contains(got, tt.note) {
			t.Fatalf("after %q: additionalContext = %q, want it to lead with %q", tt.note, got, tt.want)
		}
	}
}

func TestStopNudgesOnceAboutEachOverdueAsk(t *testing.T) {
	st, clock := testutil.Store(t)
	ctx := context.Background()
	p := bindRoot(t, st)
	p.Event = "Stop"
	stop := func() string {
		t.Helper()
		var out bytes.Buffer
		if err := hook.Stop(ctx, st, p, &out); err != nil {
			t.Fatal(err)
		}
		if out.Len() == 0 {
			return ""
		}
		var got struct{ Decision, Reason string }
		if err := json.Unmarshal(out.Bytes(), &got); err != nil || got.Decision != "block" {
			t.Fatalf("Stop output %q: %v", out.String(), err)
		}
		return got.Reason
	}
	testutil.Post(t, st, store.Record{Kind: kinds.Ask, To: []string{"root"}, Text: "asked before the hook ran"})
	clock.Advance(20 * time.Minute)
	if got := stop(); got != "" {
		t.Fatalf("the first stop blocked: %q", got)
	}
	first := testutil.Post(t, st, store.Record{Kind: kinds.Ask, To: []string{"root"}, Text: "where does the PR go?"})
	clock.Advance(9 * time.Minute)
	if got := stop(); got != "" {
		t.Fatalf("nudged before ten minutes: %q", got)
	}
	clock.Advance(2 * time.Minute)
	if got := stop(); !strings.Contains(got, fmt.Sprintf("cci #%d ASK from lane-a to root has waited 11m unanswered", first.Seq)) || !strings.Contains(got, fmt.Sprintf("--resolves %d", first.Seq)) || !strings.Contains(got, "where does the PR go?") {
		t.Fatalf("overdue nudge = %q", got)
	}
	if got := stop(); got != "" {
		t.Fatalf("nudged twice about one ask: %q", got)
	}
	second := testutil.Post(t, st, store.Record{Kind: kinds.Decide, To: []string{"main"}, Text: "merge now?"})
	clock.Advance(11 * time.Minute)
	if got := stop(); !strings.Contains(got, fmt.Sprintf("--resolves %d", second.Seq)) || strings.Contains(got, "older overdue") {
		t.Fatalf("second nudge = %q", got)
	}
	testutil.Post(t, st, store.Record{Kind: kinds.Ask, To: []string{"lane-b", "root"}, Text: "copied to root"})
	testutil.Post(t, st, store.Record{Kind: kinds.Decide, To: []string{"root"}, Text: "status with no question"})
	clock.Advance(11 * time.Minute)
	if got := stop(); got != "" {
		t.Fatalf("nudged about a record root does not owe: %q", got)
	}
}

func TestPostToolUseDeliversOnlyTheNewestStatusOnATopic(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	p := bindRoot(t, st)
	deliver := func() string {
		t.Helper()
		var out bytes.Buffer
		if err := hook.PostToolUse(ctx, st, p, noWindow, &out); err != nil {
			t.Fatal(err)
		}
		return hookContext(t, &out, "PostToolUse")
	}
	for i := range 3 {
		testutil.Post(t, st, store.Record{Lane: "runner", Kind: kinds.Report, Topic: "go-unqueued:42", To: []string{"root"}, Text: fmt.Sprintf("GO-UNQUEUED pass %d", i)})
		testutil.Post(t, st, store.Record{Lane: "runner", Kind: kinds.State, Topic: "orca", To: []string{"root"}, Text: fmt.Sprintf("orca alive pass %d", i)})
		testutil.Post(t, st, store.Record{Lane: "runner", Kind: kinds.State, To: []string{"root"}, Text: fmt.Sprintf("untopiced state %d", i)})
		testutil.Post(t, st, store.Record{Lane: "runner", Kind: kinds.Note, Topic: "orca", To: []string{"root"}, Text: fmt.Sprintf("note on the topic %d", i)})
	}
	testutil.Post(t, st, store.Record{Lane: "other", Kind: kinds.Report, Topic: "go-unqueued:42", To: []string{"root"}, Text: "another lane's report"})
	testutil.Post(t, st, store.Record{Lane: "runner", Kind: kinds.Report, Topic: "go-unqueued:42", To: []string{"root", "lane-b"}, Text: "copied to lane-b"})
	got := deliver() + deliver() + deliver()
	for _, want := range []string{"GO-UNQUEUED pass 2", "orca alive pass 2", "untopiced state 0", "untopiced state 2", "note on the topic 0", "note on the topic 2", "another lane's report", "copied to lane-b"} {
		if !strings.Contains(got, want) {
			t.Fatalf("delivery lacks %q: %q", want, got)
		}
	}
	for _, replaced := range []string{"GO-UNQUEUED pass 0", "GO-UNQUEUED pass 1", "orca alive pass 0", "orca alive pass 1"} {
		if strings.Contains(got, replaced) {
			t.Fatalf("delivery carries %q, which a newer record on its topic replaced: %q", replaced, got)
		}
	}
	testutil.Post(t, st, store.Record{Lane: "runner", Kind: kinds.State, Topic: "orca", To: []string{"root"}, Text: "orca alive pass 3"})
	if got := deliver(); !strings.Contains(got, "orca alive pass 3") {
		t.Fatalf("a status after a delivered one on its topic is missing: %q", got)
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("stdout closed") }

type cancellingWriter struct {
	bytes.Buffer
	cancel context.CancelFunc
}

func (w *cancellingWriter) Write(b []byte) (int, error) {
	w.cancel()
	return w.Buffer.Write(b)
}

func TestDeliveryRedeliversWhatTheHookNeverAcknowledged(t *testing.T) {
	for _, tt := range []struct {
		name  string
		event string
		bind  func(*testing.T, *store.Store) hook.Payload
		want  []string
	}{
		{"post-tool-use", "PostToolUse", bindRoot, []string{"open asks addressed to root, newest first", "which branch?", "deploy it"}},
		{"session-start lane", "SessionStart", bindRoot, []string{"open asks addressed to root, newest first", "which branch?", "deploy it"}},
		{"session-start digest", "SessionStart", func(t *testing.T, st *store.Store) hook.Payload {
			t.Helper()
			if err := st.Bind(context.Background(), store.PeerKey{Session: "s1"}, "d", true); err != nil {
				t.Fatal(err)
			}
			return hook.Payload{SessionID: "s1", Source: "compact"}
		}, []string{"unseen since this session's cursor", "deploy it"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			st, _ := testutil.Store(t)
			p := tt.bind(t, st)
			testutil.Post(t, st, store.Record{Lane: "lane-b", Kind: kinds.Ask, To: []string{"root"}, Text: "which branch?"})
			testutil.Post(t, st, store.Record{Lane: "lane-b", Kind: kinds.Go, To: []string{"root"}, Text: "deploy it"})
			run := func(ctx context.Context, w io.Writer) error {
				if tt.event == "SessionStart" {
					return hook.SessionStart(ctx, st, p, store.Window{}, w)
				}
				return hook.PostToolUse(ctx, st, p, noWindow, w)
			}
			cursors := func() map[string]int64 {
				t.Helper()
				got := map[string]int64{}
				for _, name := range []string{store.LaneCursor(p.Key()), store.LaneCursor(p.Key()) + "#addressed", "open-asks-seen-" + p.Key().String(), p.Key().String()} {
					seq, _, err := st.Cursor(context.Background(), name, "d")
					if err != nil {
						t.Fatal(err)
					}
					got[name] = seq
				}
				return got
			}
			if tt.event == "SessionStart" {
				var first bytes.Buffer
				if err := run(context.Background(), &first); err != nil {
					t.Fatal(err)
				}
				testutil.Post(t, st, store.Record{Lane: "lane-b", Kind: kinds.Go, To: []string{"root"}, Text: "deploy it again"})
			}
			before := cursors()
			if err := run(context.Background(), failingWriter{}); err == nil {
				t.Fatal("a failed output write reported success")
			}
			if got := cursors(); !maps.Equal(got, before) {
				t.Fatalf("a failed output write moved the cursors: %v, want %v", got, before)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			killed := &cancellingWriter{cancel: cancel}
			if err := run(ctx, killed); err == nil {
				t.Fatal("a hook cancelled before its acknowledgement reported success")
			}
			if got := cursors(); !maps.Equal(got, before) {
				t.Fatalf("a hook cancelled before its acknowledgement moved the cursors: %v, want %v", got, before)
			}
			var out bytes.Buffer
			if err := run(context.Background(), &out); err != nil {
				t.Fatal(err)
			}
			tail := func(text string) string {
				if _, after, ok := strings.Cut(text, "unseen since this session's cursor (cci tail for more):\n"); ok {
					return after
				}
				return text
			}
			got := hookContext(t, &out, tt.event)
			for _, want := range tt.want {
				if !strings.Contains(got, want) {
					t.Fatalf("redelivery lacks %q: %q", want, got)
				}
			}
			if got := cursors(); maps.Equal(got, before) {
				t.Fatalf("a delivered hook left the cursors at %v", got)
			}
			out.Reset()
			if err := run(context.Background(), &out); err != nil {
				t.Fatal(err)
			}
			if got := tail(hookContext(t, &out, tt.event)); strings.Contains(got, "deploy it") || strings.Contains(got, "newest first") {
				t.Fatalf("an acknowledged delivery came back: %q", got)
			}
		})
	}
}

func TestAClaimedLaneTakesTheThreadOffTheRootItsPostsBoundIt(t *testing.T) {
	st, clock := testutil.Store(t)
	ctx := context.Background()
	bridge := store.PeerKey{Session: "s1"}
	p := bindRoot(t, st)
	stop := func() string {
		t.Helper()
		var out bytes.Buffer
		if err := hook.Stop(ctx, st, hook.Payload{SessionID: "s1", Event: "Stop"}, &out); err != nil {
			t.Fatal(err)
		}
		return out.String()
	}
	if got := stop(); got != "" {
		t.Fatalf("the first stop blocked: %q", got)
	}
	if err := st.ClaimLane(ctx, bridge, "d", "owner-relay", time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := st.BindLane(ctx, bridge, "d", "root", time.Time{}); err != nil {
		t.Fatal(err)
	}
	if b, _, err := st.Binding(ctx, bridge); err != nil || b.Lane != "owner-relay" {
		t.Fatalf("a post as root rebound the claimed lane: %+v %v", b, err)
	}
	testutil.Post(t, st, store.Record{Kind: kinds.Ask, To: []string{"root"}, Text: "merge the stack?"})
	testutil.Post(t, st, store.Record{Kind: kinds.Go, To: []string{"main"}, Text: "for the coordinator"})
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Report, To: []string{"owner-relay"}, Text: "landed #12"})
	var out bytes.Buffer
	if err := hook.PostToolUse(ctx, st, p, noWindow, &out); err != nil {
		t.Fatal(err)
	}
	got := hookContext(t, &out, "PostToolUse")
	if !strings.Contains(got, "addressed to owner-relay") || !strings.Contains(got, "landed #12") {
		t.Fatalf("owner-relay mail missing: %q", got)
	}
	for _, unwanted := range []string{"merge the stack?", "for the coordinator", "addressed to root"} {
		if strings.Contains(got, unwanted) {
			t.Fatalf("root mail reached the owner-relay thread, %q: %q", unwanted, got)
		}
	}
	clock.Advance(11 * time.Minute)
	if got := stop(); got != "" {
		t.Fatalf("a root ask blocked the owner-relay thread: %q", got)
	}
	owed := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Ask, To: []string{"owner-relay"}, Text: "ask the owner?"})
	clock.Advance(11 * time.Minute)
	if got := stop(); !strings.Contains(got, fmt.Sprintf("cci #%d ASK from root to owner-relay", owed.Seq)) {
		t.Fatalf("stop = %q", got)
	}
}

func TestAClaimedLaneKeepsTheRootFlagAndARootReadingAsRootStays(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	root := store.PeerKey{Session: "root"}
	if err := st.Bind(ctx, root, "d", true); err != nil {
		t.Fatal(err)
	}
	if moves, err := st.MovesLane(ctx, store.Subscription{Session: "root", Drive: "d", Reader: "main"}); err != nil || moves {
		t.Fatalf("a root reading as main moves its lane: %v %v", moves, err)
	}
	if moves, err := st.MovesLane(ctx, store.Subscription{Session: "root", Drive: "d", Reader: "owner-relay"}); err != nil || !moves {
		t.Fatalf("a root reading as owner-relay keeps its lane: %v %v", moves, err)
	}
	if err := st.ClaimLane(ctx, root, "d", "owner-relay", time.Time{}); err != nil {
		t.Fatal(err)
	}
	if b, _, err := st.Binding(ctx, root); err != nil || !b.Root || b.Lane != "owner-relay" {
		t.Fatalf("binding = %+v %v", b, err)
	}
	if err := st.ClaimLane(ctx, store.PeerKey{Session: "coordinator"}, "d", "main", time.Time{}); err != nil {
		t.Fatal(err)
	}
	if b, _, err := st.Binding(ctx, store.PeerKey{Session: "coordinator"}); err != nil || b.Root || b.Lane != "root" {
		t.Fatalf("coordinator binding = %+v %v", b, err)
	}
}

func TestSessionStartKeepsTheLaneTheWindowSubscribedAs(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	window := store.Window{PID: 100, Started: 1}
	if err := st.BindLane(ctx, store.PeerKey{Session: "s1"}, "d", "root", time.Time{}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Subscribe(ctx, store.Subscription{Session: "s1", Window: window, Drive: "d", Reader: "owner-relay", Cursor: "relay"}); err != nil {
		t.Fatal(err)
	}
	if err := st.ClaimLane(ctx, store.PeerKey{Session: "s1"}, "d", "owner-relay", time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := hook.SessionStart(ctx, st, hook.Payload{SessionID: "s2", Source: "clear"}, window, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if b, ok, err := st.Binding(ctx, store.PeerKey{Session: "s2"}); err != nil || !ok || b.Drive != "d" || b.Lane != "owner-relay" {
		t.Fatalf("cleared session binding = %+v %v %v", b, ok, err)
	}
}

func TestAnOwnerPromptLeavesACredentialOutOfItsRecord(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	if err := st.Bind(ctx, store.PeerKey{Session: "root"}, "d", true); err != nil {
		t.Fatal(err)
	}
	value := "AS" + "IA" + strings.Repeat("A1B2", 4)
	if err := hook.Prompt(ctx, st, hook.Payload{SessionID: "root", Prompt: "use the key " + value + " for this"}, st.Now()); err != nil {
		t.Fatal(err)
	}
	got, err := st.Query(ctx, store.Filter{Drive: "d"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 || strings.Contains(got[len(got)-1].Text, value) || !strings.Contains(got[len(got)-1].Text, "use the key") {
		t.Fatalf("records = %+v; want the prompt with the value withheld", got)
	}
}
