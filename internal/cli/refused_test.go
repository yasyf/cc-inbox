package cli_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/cli"
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
		return inWindow(t, "TestAThreadCannotPostAsALaneNothingLaunched", home, id, append([]string{"CCI_TEST_UNREGISTERED=" + lane}, extra...)...)
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

func TestAnLrVerbPostsAsItsOwnLaneAndBindsTheSessionThatRanItOnlyToThatSessionsLane(t *testing.T) {
	if lane := os.Getenv("CCI_TEST_EMBEDDED"); lane != "" {
		fmt.Printf("err=%v\n", embeddedPost(lane))
		return
	}
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	verb := func(lane, id string, extra ...string) {
		t.Helper()
		if out := inWindow(t, "TestAnLrVerbPostsAsItsOwnLaneAndBindsTheSessionThatRanItOnlyToThatSessionsLane", home, id, append([]string{"CCI_TEST_EMBEDDED=" + lane}, extra...)...); !strings.Contains(out, "err=<nil>") {
			t.Fatalf("a verb posting as %s = %s", lane, out)
		}
	}
	verb("lr-explain", "019a3c3e-explain")
	verb("grade-first", "019a3c3e-first")
	verb("window-lane", "019a3c3e-window", "CLAUDE_LONG_RUNNING_LANE=window-lane")
	verb("lr-explain", "019a3c3e-relay", "CLAUDE_LONG_RUNNING_LANE=owner-relay")

	ctx := context.Background()
	st, err := store.Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	for lane, want := range map[string]int{"lr-explain": 0, "grade-first": 0, "owner-relay": 0, "window-lane": 1} {
		if bound, err := st.LanePeers(ctx, []string{lane}); err != nil || len(bound) != want {
			t.Errorf("sessions bound as %s = %v, %v; want %d", lane, bound, err, want)
		}
	}
}

func inWindow(t *testing.T, test, home, id string, env ...string) string {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^"+test+"$", "codex")
	cmd.Env = append(os.Environ(), "CCI_HOME="+home, "CLAUDE_CONFIG_DIR="+t.TempDir(), "CLAUDE_CODE_SESSION_ID=", "CCI_AGENT_ID=", "CLAUDE_LONG_RUNNING_LANE=", "CODEX_THREAD_ID="+id)
	for _, name := range peers.IdentityEnv {
		if name != "CODEX_THREAD_ID" {
			cmd.Env = append(cmd.Env, name+"=")
		}
	}
	cmd.Env = append(cmd.Env, env...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s: %v\n%s", id, err, out)
	}
	return string(out)
}

func TestAPostUnderTheAliasItsSessionHoldsGoesOutAsThatSessionsLane(t *testing.T) {
	if lane := os.Getenv("CCI_TEST_ALIAS"); lane != "" {
		out, err := run(t, "post", "--drive", "d", "--lane", lane, "--kind", "state", "--text", "working", "--json")
		fmt.Printf("out=%s err=%v\n", strings.TrimSpace(out), err)
		return
	}
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	ctx := context.Background()
	st, err := store.Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.Post(ctx, store.Record{Drive: "d", Lane: "root", Kind: kinds.Spawn, Text: "SPAWNED meta-desk-3", Refs: store.Refs{Lanes: []string{"meta-desk-3"}}, Fields: store.Fields{Attrs: map[string]string{"brief": "/briefs/meta.md", "model": "opus"}}}, 0); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	post := func(lane, id string) string {
		t.Helper()
		return inWindow(t, "TestAPostUnderTheAliasItsSessionHoldsGoesOutAsThatSessionsLane", home, id, "CCI_TEST_ALIAS="+lane)
	}
	if out := post("meta-desk-3", "019a3c3e-desk"); !strings.Contains(out, "err=<nil>") {
		t.Fatalf("the launched lane's own post = %s", out)
	}
	if out := post("meta-desk", "019a3c3e-desk"); !strings.Contains(out, `"lane":"meta-desk-3"`) || !strings.Contains(out, "err=<nil>") {
		t.Fatalf("a post under the alias the session holds = %s; want it stored as meta-desk-3", out)
	}
	if out := post("meta-desk", "019a3c3e-other"); !strings.Contains(out, "has no registry row") {
		t.Fatalf("another session's post under that alias = %s; want it refused", out)
	}
}

func embeddedPost(lane string) error {
	ctx := context.Background()
	st, err := cli.OpenStore(ctx)
	if err != nil {
		return err
	}
	embedded, err := cli.WithStore(ctx, st)
	if err != nil {
		return err
	}
	root := cli.NewRootCmd()
	root.SetArgs([]string{"post", "--drive", "d", "--lane", lane, "--kind", "report", "--text", "one-pager for the owner"})
	root.SetOut(io.Discard)
	return root.ExecuteContext(embedded)
}
