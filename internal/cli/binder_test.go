package cli

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/store"
)

func TestAHolderInTheSameWindowIsNotASecondBinderAfterARestart(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	holder := store.PeerKey{Session: "d50e80e8-resumed"}
	if _, err := st.Announce(ctx, store.Peer{Session: holder.Session, PID: 4242, PIDStarted: 99}); err != nil {
		t.Fatal(err)
	}
	if err := st.BindLane(ctx, holder, "d", "owner-relay", time.Now()); err != nil {
		t.Fatal(err)
	}
	mcpServer := store.PeerKey{Session: "ce184f34-mcp-server"}

	if err := refuseHeldLane(ctx, st, mcpServer, store.Window{PID: 4242, Started: 99}, "d", "owner-relay", st.Now()); err != nil {
		t.Fatalf("the holder's own window was refused: %v", err)
	}
	err = refuseHeldLane(ctx, st, mcpServer, store.Window{PID: 4242, Started: 100}, "d", "owner-relay", st.Now())
	if err == nil || !strings.Contains(err.Error(), "is held by live thread") {
		t.Fatalf("a reused pid in another window = %v, want it refused", err)
	}
	err = refuseHeldLane(ctx, st, mcpServer, store.Window{}, "d", "owner-relay", st.Now())
	if err == nil || !strings.Contains(err.Error(), "is held by live thread") {
		t.Fatalf("no window = %v, want it refused", err)
	}
}
