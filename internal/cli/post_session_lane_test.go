package cli_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/peers"
	"github.com/yasyf/cc-inbox/internal/store"
)

func TestAnAgentPostingToItsSessionsLaneAsThatLaneIsToldToPostAsItsOwn(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())
	for _, name := range peers.IdentityEnv {
		t.Setenv(name, "")
	}
	ctx := context.Background()
	st, err := store.Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.BindLane(ctx, store.PeerKey{Session: "cccc3333-session"}, "d", "communicator", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "cccc3333-session")
	t.Setenv("CCI_AGENT_ID", "a0handler00000001")
	_, err = run(t, "post", "--drive", "d", "--lane", "communicator", "--kind", "note", "--to", "communicator@d", "--text", "the reply is in")
	if want := "--to communicator@d addresses only communicator, the lane posting this record, and communicator is the lane of this agent's session, which reads a record from its own lane as one it wrote: post it as this agent's own lane, such as --lane handler or --lane builder, keeping --to communicator@d"; err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("an agent's post as its session's lane to that lane = %v; want the refusal %q", err, want)
	}
	out, err := run(t, "post", "--json", "--drive", "d", "--lane", "handler", "--kind", "note", "--to", "communicator@d", "--text", "the reply is in")
	lines := strings.Split(strings.TrimSpace(out), "\n")
	var got store.Record
	if err != nil || json.Unmarshal([]byte(lines[len(lines)-1]), &got) != nil || got.Lane != "handler" {
		t.Fatalf("the same post as --lane handler = %q, %v; want it stored from handler", out, err)
	}
	t.Setenv("CCI_AGENT_ID", "")
	_, err = run(t, "post", "--drive", "d", "--lane", "communicator", "--kind", "note", "--to", "communicator@d", "--text", "the reply is in")
	if err == nil || !strings.Contains(err.Error(), "--to communicator@d addresses only communicator, the lane posting this record; name the lane that should read it") {
		t.Fatalf("the session's own post to its own lane = %v; want the refusal that names a reader", err)
	}
}
