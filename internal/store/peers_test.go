package store_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func TestAnnounceKeepsBinding(t *testing.T) {
	st, clock := testutil.Store(t)
	ctx := context.Background()
	if _, err := st.Peer(ctx, "s1"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("Peer(unknown) = %v, want ErrNoRows", err)
	}
	if err := st.Bind(ctx, "s1", "d", true); err != nil {
		t.Fatal(err)
	}
	got, err := st.Announce(ctx, store.Peer{Session: "s1", Role: "root", Description: "ship it", Tags: []string{"release"}, Branch: "main", PID: 42, PIDStarted: 7, Env: map[string]string{"ORCA_LAUNCH_RUN": "run_1"}})
	if err != nil {
		t.Fatal(err)
	}
	want := store.Peer{Session: "s1", Drive: "d", Root: true, Role: "root", Description: "ship it", Tags: []string{"release"}, Branch: "main", PID: 42, PIDStarted: 7, Env: map[string]string{"ORCA_LAUNCH_RUN": "run_1"}, AnnouncedAt: clock.Now(), SeenAt: clock.Now()}
	if got.Session != want.Session || got.Drive != want.Drive || got.Root != want.Root || got.Role != want.Role || got.Description != want.Description || got.Tags[0] != "release" || got.PID != 42 || got.PIDStarted != 7 || got.Env["ORCA_LAUNCH_RUN"] != "run_1" || !got.AnnouncedAt.Equal(want.AnnouncedAt) {
		t.Fatalf("Announce() = %+v, want %+v", got, want)
	}
	clock.Advance(time.Minute)
	if err := st.Touch(ctx, "s1"); err != nil {
		t.Fatal(err)
	}
	again, err := st.Announce(ctx, store.Peer{Session: "s1", Role: "lane"})
	if err != nil {
		t.Fatal(err)
	}
	if again.Drive != "d" || !again.Root || again.Role != "lane" || again.Description != "" || again.Tags != nil || !again.SeenAt.Equal(clock.Now()) {
		t.Fatalf("second Announce() = %+v", again)
	}
	b, ok, err := st.Binding(ctx, "s1")
	if err != nil || !ok || b.Drive != "d" || !b.Root {
		t.Fatalf("Binding() = %+v %v %v", b, ok, err)
	}
}

func TestAnnounceWithoutDriveIsNotABinding(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	if _, err := st.Announce(ctx, store.Peer{Session: "s2", Role: "session"}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := st.Binding(ctx, "s2"); err != nil || ok {
		t.Fatalf("Binding(announced only) = %v %v, want unbound", ok, err)
	}
	peers, err := st.Peers(ctx)
	if err != nil || len(peers) != 1 || peers[0].Session != "s2" || peers[0].Drive != "" {
		t.Fatalf("Peers() = %+v, %v", peers, err)
	}
	if err := st.Bind(ctx, "s2", "d", false); err != nil {
		t.Fatal(err)
	}
	if b, ok, err := st.Binding(ctx, "s2"); err != nil || !ok || b.Drive != "d" {
		t.Fatalf("Binding(after bind) = %+v %v %v", b, ok, err)
	}
}
