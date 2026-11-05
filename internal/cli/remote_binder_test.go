package cli_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/cli"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/peers"
	"github.com/yasyf/cc-inbox/internal/store"
)

func TestALaneHeldLiveOnAnotherNodeRefusesASecondThread(t *testing.T) {
	const lane = "restate-redis-1"
	if os.Getenv("CCI_TEST_REMOTE_BINDER") != "" {
		out, err := run(t, "post", "--drive", "d", "--lane", lane, "--kind", "state", "--text", lane+" up")
		fmt.Printf("posted=%q exit=%d err=%v\n", out, cli.ExitCode(err), err)
		return
	}
	mac, sprite, config := t.TempDir(), t.TempDir(), t.TempDir()
	t.Setenv("CLAUDE_CONFIG_DIR", config)
	ctx := context.Background()
	open := func(home string) *store.Store {
		t.Helper()
		st, err := store.Open(ctx, home)
		if err != nil {
			t.Fatal(err)
		}
		return st
	}
	st := open(mac)
	if _, _, err := st.Post(ctx, store.Record{Drive: "d", Lane: "meta-desk-3", Kind: kinds.Spawn, Text: "SPAWNED " + lane, Refs: store.Refs{Lanes: []string{lane}}, Fields: store.Fields{Attrs: map[string]string{"brief": "/brief.md", "model": "opus"}}, Source: "post"}, 0); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	poster := func(home, thread string) string {
		t.Helper()
		cmd := exec.Command(os.Args[0], "-test.run=^TestALaneHeldLiveOnAnotherNodeRefusesASecondThread$", "codex")
		cmd.Env = append(os.Environ(), "CCI_TEST_REMOTE_BINDER=1", "CCI_HOME="+home, "CLAUDE_CODE_SESSION_ID=", "CCI_AGENT_ID=", "CODEX_THREAD_ID="+thread)
		for _, name := range peers.IdentityEnv {
			if name != "CODEX_THREAD_ID" {
				cmd.Env = append(cmd.Env, name+"=")
			}
		}
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s: %v\n%s", thread, err, out)
		}
		return string(out)
	}
	if out := poster(mac, "019a3c3e-mac-holder"); !strings.Contains(out, "err=<nil>") {
		t.Fatalf("the first thread posting as %s on the Mac = %s; want it bound", lane, out)
	}
	from, to := open(mac), open(sprite)
	rows, err := from.Export(ctx, from.Self(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := to.Apply(ctx, rows, nil); err != nil {
		t.Fatal(err)
	}
	node := from.Self()
	for _, s := range []*store.Store{from, to} {
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	out := poster(sprite, "019a3c3e-sprite-copy")
	for _, want := range []string{`posted=""`, fmt.Sprintf("exit=%d", cli.HeldLaneExit), "lane " + lane + " is held by live thread " + lane + " 019a3c3e-mac-holder on node " + node, "--kind stand-down --to " + lane + " --successor"} {
		if !strings.Contains(out, want) {
			t.Fatalf("a second thread posting as %s on the Sprite = %s; want it refused with %q", lane, out, want)
		}
	}
	t.Setenv("CCI_HOME", sprite)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "aaaa1111-root")
	t.Setenv("CCI_AGENT_ID", "")
	for _, name := range peers.IdentityEnv {
		if name != "CLAUDE_CODE_SESSION_ID" {
			t.Setenv(name, "")
		}
	}
	if _, err := run(t, "post", "--drive", "d", "--lane", "root", "--kind", "stand-down", "--to", lane, "--text", lane+" on the Mac stands down for the Sprite copy"); err != nil {
		t.Fatal(err)
	}
	if out := poster(sprite, "019a3c3e-sprite-copy"); !strings.Contains(out, "err=<nil>") {
		t.Fatalf("the Sprite copy posting as %s after it stood down = %s; want it bound", lane, out)
	}
}
