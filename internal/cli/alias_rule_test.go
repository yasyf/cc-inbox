package cli_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/peers"
	"github.com/yasyf/cc-inbox/internal/store"
)

func TestAReadOfAnAliasReadsWhatItsHolderReads(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())
	for _, name := range peers.IdentityEnv {
		t.Setenv(name, "")
	}
	for _, post := range [][]string{
		{"--lane", "sv-3", "--kind", "state", "--text", "sv-3 up"},
		{"--lane", "sv-4", "--kind", "state", "--text", "sv-4 up"},
		{"--lane", "root", "--kind", "note", "--to", "sv-3", "--text", "for the number"},
		{"--lane", "root", "--kind", "stand-down", "--to", "sv-4", "--text", "sv-4 stands down"},
		{"--lane", "root", "--kind", "note", "--to", "sv", "--text", "for the alias"},
	} {
		if _, err := run(t, append([]string{"post", "--drive", "d"}, post...)...); err != nil {
			t.Fatal(err)
		}
	}
	for _, read := range [][]string{
		{"tail", "--drive", "d", "--since", "0", "--to", "sv"},
		{"grep", "for the", "--drive", "d", "--to", "sv"},
	} {
		out, err := run(t, read...)
		if err != nil || !strings.Contains(out, "for the number") || !strings.Contains(out, "for the alias") {
			t.Fatalf("%v = %q, %v; want the holder's mail under either name", read, out, err)
		}
	}
}

func TestAPostToAnAliasIsNoSlipWhileANumberedLaneOnTheDriveAnswersToIt(t *testing.T) {
	ctx := context.Background()
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())
	for _, name := range peers.IdentityEnv {
		t.Setenv(name, "")
	}
	st, err := store.Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	k := store.PeerKey{Session: "session-diagnosis", Agent: "adiagnosis-1"}
	if err := st.BindLane(ctx, k, "d", "diagnosis-1", time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Announce(ctx, store.Peer{Session: k.Session, Agent: k.Agent}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := run(t, "post", "--drive", "d", "--lane", "root", "--kind", "stand-down", "--to", "diagnosis-1", "--text", "diagnosis-1 is done"); err != nil {
		t.Fatal(err)
	}
	if _, err := run(t, "post", "--drive", "d", "--lane", "fix", "--kind", "note", "--to", "diagnosis", "--text", "after it finished"); err != nil {
		t.Fatalf("post --to diagnosis: %v; want it stored for the alias", err)
	}
}
