package cli

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/hook"
	"github.com/yasyf/cc-inbox/internal/store"
)

func TestASubagentItsSessionStoppedNoLongerHoldsItsLane(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	window, err := windowOf(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	doctor := store.PeerKey{Session: "2fd5df50-root", Agent: "a66d159ec00411596"}
	if _, err := st.Announce(ctx, store.Peer{Session: doctor.Session, Agent: doctor.Agent, PID: window.PID, PIDStarted: window.Started}); err != nil {
		t.Fatal(err)
	}
	if err := st.BindLane(ctx, doctor, "d", "queue-92-mitigate-1", time.Now()); err != nil {
		t.Fatal(err)
	}
	worker := store.PeerKey{Session: "01a1b37c-worker"}
	err = refuseHeldLane(ctx, st, worker, store.Window{}, "d", "queue-92-mitigate-1", st.Now())
	if err == nil || !strings.Contains(err.Error(), "is held by live thread") {
		t.Fatalf("the worker beside a running subagent = %v, want it refused", err)
	}

	stop := hook.Payload{SessionID: doctor.Session, ToolName: "TaskStop", ToolInput: json.RawMessage(`{"task_id":"a66d159ec00411596"}`)}
	if err := hook.PostToolUse(ctx, st, stop, func() (store.Window, error) { return window, nil }, io.Discard); err != nil {
		t.Fatal(err)
	}

	if err := refuseHeldLane(ctx, st, worker, store.Window{}, "d", "queue-92-mitigate-1", st.Now()); err != nil {
		t.Fatalf("the worker after its session stopped the subagent = %v, want it to bind", err)
	}
}

func TestAHolderInTheSameWindowIsNotASecondBinderAfterARestart(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	window, err := windowOf(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	holder := store.PeerKey{Session: "d50e80e8-resumed"}
	if _, err := st.Announce(ctx, store.Peer{Session: holder.Session, PID: window.PID, PIDStarted: window.Started}); err != nil {
		t.Fatal(err)
	}
	if err := st.BindLane(ctx, holder, "d", "owner-relay", time.Now()); err != nil {
		t.Fatal(err)
	}
	mcpServer := store.PeerKey{Session: "ce184f34-mcp-server"}

	if err := refuseHeldLane(ctx, st, mcpServer, window, "d", "owner-relay", st.Now()); err != nil {
		t.Fatalf("the holder's own window was refused: %v", err)
	}
	err = refuseHeldLane(ctx, st, mcpServer, store.Window{PID: window.PID, Started: window.Started + 1}, "d", "owner-relay", st.Now())
	if err == nil || !strings.Contains(err.Error(), "is held by live thread") {
		t.Fatalf("a reused pid in another window = %v, want it refused", err)
	}
	err = refuseHeldLane(ctx, st, mcpServer, store.Window{}, "d", "owner-relay", st.Now())
	if err == nil || !strings.Contains(err.Error(), "is held by live thread") {
		t.Fatalf("no window = %v, want it refused", err)
	}
}
