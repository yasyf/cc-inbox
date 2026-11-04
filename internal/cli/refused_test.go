package cli_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/peers"
	"github.com/yasyf/cc-inbox/internal/store"
)

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
