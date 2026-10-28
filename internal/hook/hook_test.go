package hook_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
		if err := hook.Prompt(ctx, st, p); err != nil {
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
	if err := hook.Prompt(ctx, st, hook.Payload{SessionID: "root", Prompt: "fix cci so my own messages are not echoed"}); err != nil {
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
	for _, want := range []string{"STOPPED root -> lane-x abandon this item", "OWNER root -> lane-x,lane-y owner ruling"} {
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
	if got := hookContext(t, &out, "PostToolUseFailure"); !strings.Contains(got, "DEFECT root -> lane-x 🛑") {
		t.Fatalf("an oversized record stalled delivery: %q", got)
	}
	out.Reset()
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Hold, To: []string{"lane-x"}, Text: "hold the apply"})
	if err := hook.SessionStart(ctx, st, hook.Payload{SessionID: "s1", Source: "compact"}, store.Window{}, &out); err != nil {
		t.Fatal(err)
	}
	if got := hookContext(t, &out, "SessionStart"); !strings.Contains(got, "HOLD root -> lane-x hold the apply") || strings.Contains(got, "open asks") {
		t.Fatalf("SessionStart additionalContext = %q", got)
	}
}

func TestPostToolUseSkipsBroadcastKindsAddressedToSeveralLanes(t *testing.T) {
	st, clock := testutil.Store(t)
	ctx := context.Background()
	started := clock.Now()
	clock.Advance(time.Minute)
	for _, r := range []store.Record{
		{Lane: "desk", Kind: kinds.Review, To: []string{"lane-x", "lane-y"}, Text: "review for several"},
		{Lane: "lane-y", Kind: kinds.Head, To: []string{"lane-x", "lane-z"}, Text: "head for several"},
		{Lane: "desk", Kind: kinds.Review, To: []string{"lane-x"}, Text: "review for lane-x alone"},
		{Lane: "root", Kind: kinds.Go, To: []string{"lane-x", "lane-y"}, Text: "go for both"},
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
	if !strings.Contains(got, "review for lane-x alone") || !strings.Contains(got, "go for both") || strings.Contains(got, "for several") {
		t.Fatalf("additionalContext = %q; want the solo review and the go, never a review or head sent to several lanes", got)
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
	if got := hookContext(t, &out, "PostToolUse"); !strings.Contains(got, "STOPPED root -> lane-x abandon this item") {
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
	if got := hookContext(t, &out, "PostToolUse"); !strings.Contains(got, "GO root -> api ship api") || strings.Contains(got, "ship web") {
		t.Fatalf("api agent's delivery = %q", got)
	}
	out.Reset()
	if err := hook.PostToolUse(ctx, st, web, window, &out); err != nil {
		t.Fatal(err)
	}
	if got := hookContext(t, &out, "PostToolUse"); !strings.Contains(got, "GO root -> web ship web") || strings.Contains(got, "ship api") {
		t.Fatalf("web agent's delivery = %q", got)
	}
	if err := hook.SubagentStop(ctx, st, api); err != nil {
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
	for _, want := range []string{"before its first tool call", "DEFECT root -> lane-x for the successor"} {
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
	for i := range 40 {
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
	if !ok || strings.Contains(replay, "backlog 39") || strings.Contains(replay, "which branch?") {
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

func TestOpenAsksAreListedOnlyWhenTheyChange(t *testing.T) {
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
	const listed, unchanged, changed = "open asks addressed to root, newest first", "cci records addressed to root; act on each:", " open asks addressed to root, changed since the last list; newest: #"
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
		{"PostToolUse", "which env?", "", "a second ask", "2" + changed},
		{"PostToolUse", "", "which branch?", "one answered", "1" + changed},
		{"PostToolUse", "", "which env?", "every ask answered", unchanged},
		{"PostToolUse", "which region?", "", "an ask after an empty list", "1" + changed},
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
