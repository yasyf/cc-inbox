package hook_test

import (
	"bytes"
	"context"
	"encoding/json"
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
