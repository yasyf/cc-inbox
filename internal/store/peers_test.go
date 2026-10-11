package store_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func TestAnnounceKeepsBinding(t *testing.T) {
	st, clock := testutil.Store(t)
	ctx := context.Background()
	if _, err := st.Peer(ctx, store.PeerKey{Session: "s1"}); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("Peer(unknown) = %v, want ErrNoRows", err)
	}
	if err := st.Bind(ctx, store.PeerKey{Session: "s1"}, "d", true); err != nil {
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
	if err := st.Touch(ctx, store.PeerKey{Session: "s1"}); err != nil {
		t.Fatal(err)
	}
	again, err := st.Announce(ctx, store.Peer{Session: "s1", Role: "lane"})
	if err != nil {
		t.Fatal(err)
	}
	if again.Drive != "d" || !again.Root || again.Role != "lane" || again.Description != "ship it" || len(again.Tags) != 1 || again.PID != 0 || !again.SeenAt.Equal(clock.Now()) {
		t.Fatalf("second Announce() = %+v", again)
	}
	b, ok, err := st.Binding(ctx, store.PeerKey{Session: "s1"})
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
	if _, ok, err := st.Binding(ctx, store.PeerKey{Session: "s2"}); err != nil || ok {
		t.Fatalf("Binding(announced only) = %v %v, want unbound", ok, err)
	}
	peers, err := st.Peers(ctx)
	if err != nil || len(peers) != 1 || peers[0].Session != "s2" || peers[0].Drive != "" {
		t.Fatalf("Peers() = %+v, %v", peers, err)
	}
	if err := st.Bind(ctx, store.PeerKey{Session: "s2"}, "d", false); err != nil {
		t.Fatal(err)
	}
	if b, ok, err := st.Binding(ctx, store.PeerKey{Session: "s2"}); err != nil || !ok || b.Drive != "d" {
		t.Fatalf("Binding(after bind) = %+v %v %v", b, ok, err)
	}
}

func TestAnnounceKeepsExplicitAttributesAndSupersedes(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	if _, err := st.Announce(ctx, store.Peer{Session: "old", Role: "watcher", Description: "watch release", Tags: []string{"release"}, PID: 7, PIDStarted: 1}); err != nil {
		t.Fatal(err)
	}
	auto, err := st.Announce(ctx, store.Peer{Session: "old", PID: 7, PIDStarted: 1, Branch: "main"})
	if err != nil {
		t.Fatal(err)
	}
	if auto.Role != "watcher" || auto.Description != "watch release" || len(auto.Tags) != 1 || auto.Branch != "main" || auto.Superseded {
		t.Fatalf("automatic announce changed explicit attributes: %+v", auto)
	}
	if _, err := st.Announce(ctx, store.Peer{Session: "new", PID: 7, PIDStarted: 1}); err != nil {
		t.Fatal(err)
	}
	old, err := st.Peer(ctx, store.PeerKey{Session: "old"})
	if err != nil || !old.Superseded {
		t.Fatalf("old session after /clear = %+v, %v; want superseded", old, err)
	}
	back, err := st.Announce(ctx, store.Peer{Session: "old", PID: 9, PIDStarted: 2})
	if err != nil || back.Superseded {
		t.Fatalf("resumed old session = %+v, %v; want live again", back, err)
	}
	if fresh, err := st.Peer(ctx, store.PeerKey{Session: "new"}); err != nil || fresh.Superseded {
		t.Fatalf("new session after a different pid announced = %+v, %v", fresh, err)
	}
}

func TestBindRefreshesInferredRoleOnly(t *testing.T) {
	st, clock := testutil.Store(t)
	ctx := context.Background()
	if _, err := st.Announce(ctx, store.Peer{Session: "s", Role: "session"}); err != nil {
		t.Fatal(err)
	}
	if err := st.BindLane(ctx, store.PeerKey{Session: "s"}, "d", "api", clock.Now()); err != nil {
		t.Fatal(err)
	}
	if p, _ := st.Peer(ctx, store.PeerKey{Session: "s"}); p.Role != "lane" || p.Lane != "api" {
		t.Fatalf("after BindLane = %+v", p)
	}
	if err := st.Bind(ctx, store.PeerKey{Session: "s"}, "d", true); err != nil {
		t.Fatal(err)
	}
	if p, _ := st.Peer(ctx, store.PeerKey{Session: "s"}); p.Role != "root" || !p.Root {
		t.Fatalf("after Bind --root = %+v", p)
	}
	if _, err := st.Announce(ctx, store.Peer{Session: "w", Role: "watcher"}); err != nil {
		t.Fatal(err)
	}
	if err := st.BindLane(ctx, store.PeerKey{Session: "w"}, "d", "pr-watch", clock.Now()); err != nil {
		t.Fatal(err)
	}
	if p, _ := st.Peer(ctx, store.PeerKey{Session: "w"}); p.Role != "watcher" || p.Lane != "pr-watch" {
		t.Fatalf("explicit role after BindLane = %+v", p)
	}
	if err := st.Bind(ctx, store.PeerKey{Session: "fresh"}, "d", false); err != nil {
		t.Fatal(err)
	}
	if p, _ := st.Peer(ctx, store.PeerKey{Session: "fresh"}); p.Role != "session" {
		t.Fatalf("Bind of a new session = %+v", p)
	}
}

func TestAgentsAreDistinctPeers(t *testing.T) {
	st, clock := testutil.Store(t)
	ctx := context.Background()
	root := store.PeerKey{Session: "s"}
	api := store.PeerKey{Session: "s", Agent: "aapi-1"}
	web := store.PeerKey{Session: "s", Agent: "aweb-2"}
	if err := st.Bind(ctx, root, "d", true); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Announce(ctx, store.Peer{Session: "s", PID: 42, PIDStarted: 7}); err != nil {
		t.Fatal(err)
	}
	for _, k := range []store.PeerKey{api, web} {
		if _, err := st.Announce(ctx, store.Peer{Session: k.Session, Agent: k.Agent, AgentName: k.Agent[1:4], PID: 42, PIDStarted: 7}); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.BindLane(ctx, api, "d", "api", clock.Now()); err != nil {
		t.Fatal(err)
	}
	if err := st.BindLane(ctx, web, "d", "web", clock.Now()); err != nil {
		t.Fatal(err)
	}
	all, err := st.Peers(ctx)
	if err != nil || len(all) != 3 {
		t.Fatalf("Peers() = %+v, %v; want root and two agents", all, err)
	}
	for _, tt := range []struct {
		key        store.PeerKey
		lane, role string
		root       bool
	}{{root, "", "root", true}, {api, "api", "lane", false}, {web, "web", "lane", false}} {
		p, err := st.Peer(ctx, tt.key)
		if err != nil || p.Lane != tt.lane || p.Role != tt.role || p.Root != tt.root || p.Superseded {
			t.Fatalf("Peer(%s) = %+v, %v", tt.key, p, err)
		}
	}
	if p, _ := st.Peer(ctx, api); p.AgentName != "api" || p.Key() != api {
		t.Fatalf("agent peer = %+v", p)
	}
	if _, err := st.Announce(ctx, store.Peer{Session: "s", Agent: "aapi-1"}); err != nil {
		t.Fatal(err)
	}
	if p, _ := st.Peer(ctx, api); p.AgentName != "api" {
		t.Fatalf("announce without a name erased it: %+v", p)
	}
	testutil.Post(t, st, store.Record{Lane: "web", Kind: kinds.Opened, Text: "fix", Refs: store.Refs{PRs: []int{7}}})
	if _, err := st.Claim(ctx, store.Claim{Session: "s", Agent: "aapi-1", Rel: "watches", Kind: "pr", Key: "7"}); err != nil {
		t.Fatal(err)
	}
	claims, err := st.Claims(ctx, store.ClaimFilter{Kind: "pr", Key: "7"})
	if err != nil || len(claims) != 2 {
		t.Fatalf("Claims(pr 7) = %+v, %v", claims, err)
	}
	holders := map[store.PeerKey]string{}
	for _, c := range claims {
		holders[c.Peer()] = c.Rel
	}
	if holders[api] != "watches" || holders[web] != "opened" || len(holders) != 2 {
		t.Fatalf("claim holders = %v, want api watches and web opened", holders)
	}
	if removed, err := st.Unclaim(ctx, store.Claim{Session: "s", Rel: "watches", Kind: "pr", Key: "7"}); err != nil || removed {
		t.Fatalf("root unclaimed the agent's claim: %v %v", removed, err)
	}
	if err := st.Retire(ctx, api); err != nil {
		t.Fatal(err)
	}
	if p, _ := st.Peer(ctx, api); !p.Superseded {
		t.Fatalf("retired agent = %+v", p)
	}
	if p, _ := st.Peer(ctx, root); p.Superseded {
		t.Fatalf("retiring an agent retired its root: %+v", p)
	}
	if p, err := st.Announce(ctx, store.Peer{Session: "s", Agent: "aapi-1"}); err != nil || p.Superseded {
		t.Fatalf("re-announced agent = %+v, %v", p, err)
	}
	if _, err := st.Announce(ctx, store.Peer{Session: "s", PID: 42, PIDStarted: 7}); err != nil {
		t.Fatal(err)
	}
	if p, _ := st.Peer(ctx, web); p.Superseded {
		t.Fatalf("root announce from the same process superseded its agent: %+v", p)
	}
}
