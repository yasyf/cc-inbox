package hook_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

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
	if err := hook.SessionStart(ctx, st, hook.Payload{SessionID: "other"}, 0, &unbound); err != nil || unbound.Len() != 0 {
		t.Fatalf("unbound session wrote %q, %v", unbound.String(), err)
	}
	if err := st.Bind(ctx, "s1", "d", true); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := hook.SessionStart(ctx, st, hook.Payload{SessionID: "s1", Source: "compact"}, 0, &out); err != nil {
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
	if err := st.Bind(ctx, "root", "d", true); err != nil {
		t.Fatal(err)
	}
	if err := st.Bind(ctx, "lane", "d", false); err != nil {
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
	if len(all) != 1 || all[0].Kind != kinds.Owner || all[0].Lane != "owner" || all[0].Text != "ship the wave fix now" || all[0].Source != "hook" {
		t.Fatalf("records = %+v", all)
	}
}

func TestSessionStartRepointsSubscription(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	if _, err := st.Subscribe(ctx, store.Subscription{Session: "s1", Window: 100, Drive: "d", Reader: "root", Cursor: "root-watch"}); err != nil {
		t.Fatal(err)
	}
	steps := []struct {
		name    string
		session string
		window  int
	}{
		{"resumed in a new process", "s1", 200},
		{"cleared in the same process", "s2", 200},
	}
	for _, step := range steps {
		if err := hook.SessionStart(ctx, st, hook.Payload{SessionID: step.session}, step.window, &bytes.Buffer{}); err != nil {
			t.Fatal(err)
		}
		sub, ok, err := st.Subscription(ctx, step.window)
		if err != nil || !ok || sub.Session != step.session || sub.Reader != "root" || sub.Cursor != "root-watch" {
			t.Fatalf("%s: subscription = %+v %v %v", step.name, sub, ok, err)
		}
	}
	if _, ok, _ := st.Subscription(ctx, 100); ok {
		t.Fatal("the old window still holds the subscription")
	}
}
