package cli_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/shirou/gopsutil/v4/process"

	"github.com/yasyf/cc-inbox/internal/peers"
	"github.com/yasyf/cc-inbox/internal/store"
)

func TestWhoFoldsAgentAndIdleRowsIntoACount(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())
	for _, name := range peers.IdentityEnv {
		t.Setenv(name, "")
	}
	self, err := process.NewProcess(int32(os.Getpid())) //nolint:gosec // G115: OS pids fit in int32.
	if err != nil {
		t.Fatal(err)
	}
	started, err := self.CreateTime()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	st, err := store.Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	builder := store.PeerKey{Session: "aaaa1111-root", Agent: "abuilder-1"}
	if err := st.BindLane(ctx, builder, "d", "builder", time.Now()); err != nil {
		t.Fatal(err)
	}
	for _, p := range []store.Peer{
		{Session: "aaaa1111-root", Role: peers.RoleRoot},
		{Session: builder.Session, Agent: builder.Agent, AgentName: "builder", Role: peers.RoleAgent},
		{Session: "aaaa1111-root", Agent: "ahelper-1", Role: peers.RoleAgent},
		{Session: "aaaa1111-root", Agent: "aflow-1", AgentName: "apply:api", Role: peers.RoleWorkflow},
	} {
		p.PID, p.PIDStarted = os.Getpid(), started
		if _, err := st.Announce(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	st.Now = func() time.Time { return time.Now().Add(-2 * time.Hour) }
	if _, err := st.Announce(ctx, store.Peer{Session: "bbbb2222-idle", Role: peers.RoleSession, PID: os.Getpid(), PIDStarted: started}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	out, err := run(t, "who")
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	if err != nil || len(lines) != 3 || !strings.Contains(out, "session=aaaa1111\n") || !strings.Contains(out, " lane=builder ") ||
		lines[2] != "+3 more: 2 subagents or workflow agents holding no lane, 1 idle over an hour; --role agent, --role workflow or --all lists them" {
		t.Fatalf("who = %q, %v; want the root, the builder lane and one count line", out, err)
	}
	if out, err := run(t, "who", "--role", "agent"); err != nil || !strings.Contains(out, "/ahelper-1") || strings.Contains(out, "more:") {
		t.Fatalf("who --role agent = %q, %v", out, err)
	}
	if out, err := run(t, "who", "--all"); err != nil || strings.Count(out, "\n") != 5 || strings.Contains(out, "more:") {
		t.Fatalf("who --all = %q, %v; want every row", out, err)
	}
}
