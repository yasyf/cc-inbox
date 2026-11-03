package cli_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/peers"
	"github.com/yasyf/cc-inbox/internal/store"
)

func openRefusals(t *testing.T, home, lane string) []store.Record {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	open, err := st.OpenRefusals(ctx, "d", lane)
	if err != nil {
		t.Fatal(err)
	}
	return open
}

func rootsBlockers(t *testing.T, home string) []store.Record {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	got, err := st.Query(ctx, store.Filter{Drive: "d", Kinds: []kinds.Kind{kinds.Blocker}, To: "root", IncludeExpired: true})
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestARefusedPostIsTheLanesBlockerItsNextPostCloses(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	_, err := run(t, "post", "--drive", "d", "--lane", "fix-lane-1", "--kind", "note", "--to", "fix-lane-1", "--text", "only to myself")
	if err == nil || !strings.Contains(err.Error(), "addresses only fix-lane-1") || !strings.Contains(err.Error(), "went to fix-lane-1,") {
		t.Fatalf("refused post = %v; want the refusal and the blocker it left the lane", err)
	}
	open := openRefusals(t, home, "fix-lane-1")
	if len(open) != 1 || !slices.Equal(open[0].To, []string{"fix-lane-1"}) || !strings.Contains(open[0].Text, "only to myself") {
		t.Fatalf("open refusals = %+v; want one blocker to the lane naming the words it refused", open)
	}
	if _, err := run(t, "post", "--drive", "d", "--lane", "fix-lane-1", "--kind", "note", "--to", "meta-desk-1", "--text", "to the desk"); err != nil {
		t.Fatal(err)
	}
	if open := openRefusals(t, home, "fix-lane-1"); len(open) != 0 {
		t.Fatalf("after the lane's next post, open refusals = %+v", open)
	}
	if got := rootsBlockers(t, home); len(got) != 0 {
		t.Fatalf("root's blockers = %+v; a refusal the lane fixed reaches only the lane", got)
	}
}

func TestARefusalAfterARetryReachesRootNamingBoth(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	if _, err := run(t, "post", "--drive", "d", "--lane", "fix-lane-1", "--kind", "note", "--to", "fix-lane-1", "--text", "only to myself"); err == nil {
		t.Fatal("the first post went through")
	}
	if _, err := run(t, "post", "--drive", "d", "--lane", "fix-lane-1", "--kind", "note", "--to", "fix-lane-1", "--text", "still only to myself"); err == nil || !strings.Contains(err.Error(), "went to root,") {
		t.Fatalf("retry = %v; want its blocker sent to root", err)
	}
	first := openRefusals(t, home, "fix-lane-1")[0]
	got := rootsBlockers(t, home)
	if len(got) != 1 || !strings.Contains(got[0].Text, fmt.Sprintf("#%d", first.Seq)) || !strings.Contains(got[0].Text, "It said: note only to myself") || !strings.Contains(got[0].Text, "It said: note still only to myself") {
		t.Fatalf("root's blockers = %+v; want one naming both refusals", got)
	}
	if _, err := run(t, "post", "--drive", "d", "--lane", "fix-lane-1", "--kind", "note", "--to", "meta-desk-1", "--text", "to the desk"); err != nil {
		t.Fatal(err)
	}
	if open := openRefusals(t, home, "fix-lane-1"); len(open) != 0 {
		t.Fatalf("after the lane's next post, open refusals = %+v", open)
	}
}

func TestARefusalFoundParsingTheCommandLineReachesAnExistingStore(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	if _, err := run(t, "post", "--drive", "d", "--lane", "fix-lane-1", "--kind", "note", "--text", "up"); err != nil {
		t.Fatal(err)
	}
	if _, err := run(t, "post", "--drive", "d", "--lane", "fix-lane-1", "--kind", "unknown", "--text", "swallowed by a script"); err == nil || !strings.Contains(err.Error(), "unknown kind") {
		t.Fatalf("post = %v", err)
	}
	if open := openRefusals(t, home, "fix-lane-1"); len(open) != 1 {
		t.Fatalf("open refusals = %v; want one blocker", open)
	}
}

func TestALanesEndIsATypedRecordNeverASentence(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	for _, text := range []string{
		"STOPPED journal-refuse-fix: out of scope",
		"7:26 PM STOPPED journal-refuse-fix: out of scope",
		"journal-refuse-fix: STOPPED: out of scope",
		"DONE journal-refuse-fix",
		"WITHDRAWN journal-refuse-fix by root",
	} {
		for _, kind := range []string{"state", "report", "note"} {
			if _, err := run(t, "post", "--drive", "d", "--lane", "journal-refuse-fix", "--kind", kind, "--text", text); err == nil || !strings.Contains(err.Error(), "--kind stopped") {
				t.Errorf("%s %q = %v; want it refused naming the typed record", kind, text, err)
			}
		}
	}
	for _, args := range [][]string{
		{"--lane", "journal-refuse-fix", "--kind", "stopped", "--text", "STOPPED journal-refuse-fix: out of scope"},
		{"--lane", "journal-refuse-fix", "--kind", "report", "--text", "DONE with item 3 of 7"},
		{"--lane", "desk-runner", "--kind", "report", "--to", "root", "--text", "WITHDRAWN R84979 meta-lane-skill-1: dispatch stopped"},
		{"--lane", "root", "--kind", "report", "--to", "meta-desk-1", "--text", "STOPPED journal-refuse-fix is in the table"},
	} {
		if _, err := run(t, append([]string{"post", "--drive", "d"}, args...)...); err != nil {
			t.Errorf("%v = %v; want it stored", args, err)
		}
	}
}

func TestAThreadCannotPostAsALaneNothingLaunched(t *testing.T) {
	if lane := os.Getenv("CCI_TEST_UNREGISTERED"); lane != "" {
		_, err := run(t, "post", "--drive", "d", "--lane", lane, "--kind", "state", "--text", lane+" is working")
		fmt.Printf("err=%v\n", err)
		return
	}
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	ctx := context.Background()
	st, err := store.Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.Post(ctx, store.Record{Drive: "d", Lane: "root", Kind: kinds.Spawn, Text: "SPAWNED launched-lane", Refs: store.Refs{Lanes: []string{"launched-lane"}}, Fields: store.Fields{Attrs: map[string]string{"brief": "/briefs/a.md", "model": "opus"}}}, 0); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	thread := func(lane, id string, extra ...string) string {
		t.Helper()
		cmd := exec.Command(os.Args[0], "-test.run=^TestAThreadCannotPostAsALaneNothingLaunched$", "codex")
		cmd.Env = append(os.Environ(), "CCI_TEST_UNREGISTERED="+lane, "CCI_HOME="+home, "CLAUDE_CONFIG_DIR="+t.TempDir(), "CLAUDE_CODE_SESSION_ID=", "CCI_AGENT_ID=", "CLAUDE_LONG_RUNNING_LANE=", "CODEX_THREAD_ID="+id)
		for _, name := range peers.IdentityEnv {
			if name != "CODEX_THREAD_ID" {
				cmd.Env = append(cmd.Env, name+"=")
			}
		}
		cmd.Env = append(cmd.Env, extra...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s: %v\n%s", lane, err, out)
		}
		return string(out)
	}
	for range 2 {
		if out := thread("stray-lane", "019a3c3e-stray"); !strings.Contains(out, "has no registry row") || !strings.Contains(out, "--kind spawn --lane-ref stray-lane") {
			t.Fatalf("a lane nothing launched = %s; want it refused naming the fix", out)
		}
	}
	if out := thread("launched-lane", "019a3c3e-launched"); !strings.Contains(out, "err=<nil>") {
		t.Fatalf("a launched lane = %s", out)
	}
	if out := thread("window-lane", "019a3c3e-window", "CLAUDE_LONG_RUNNING_LANE=window-lane"); !strings.Contains(out, "err=<nil>") {
		t.Fatalf("a lane its launcher named in the window's environment = %s", out)
	}
}
