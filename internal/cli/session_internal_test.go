package cli

import (
	"context"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func TestARegisteredSessionRefusesSubscribeAndUnsubscribe(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	if err := refuseRegistered(ctx, st, "s1"); err != nil {
		t.Fatalf("an unregistered session was refused: %v", err)
	}
	if _, err := st.Register(ctx, store.Registration{Subscription: store.Subscription{Alias: "release-demo-meta", Session: "s1", Drive: "d", Reader: "meta", Cursor: "meta"}}); err != nil {
		t.Fatal(err)
	}
	err := refuseRegistered(ctx, st, "s1")
	if err == nil || !strings.Contains(err.Error(), "registered as release-demo-meta") || !strings.Contains(err.Error(), "cci session rm release-demo-meta") {
		t.Fatalf("refuseRegistered = %v, want the alias that owns the subscription", err)
	}
	if err := refuseRegistered(ctx, st, "s2"); err != nil {
		t.Fatalf("another session was refused: %v", err)
	}
}
