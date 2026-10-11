package peers_test

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/peers"
	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func TestResolveJoinsHarnessAndFilters(t *testing.T) {
	st, clock := testutil.Store(t)
	ctx := context.Background()
	cfg := t.TempDir()
	t.Setenv("CLAUDE_CONFIG_DIR", cfg)
	if err := os.MkdirAll(filepath.Join(cfg, "sessions"), 0o700); err != nil {
		t.Fatal(err)
	}
	statusAt := clock.Now().Add(-2 * time.Minute).UnixMilli()
	for name, body := range map[string]string{
		"11.json": `{"pid":11,"sessionId":"aaaa1111-root","cwd":"/w/alewife","startedAt":1,"messagingSocketPath":"/tmp/cc-socks/11.sock","name":"alewife-6d","status":"idle","statusUpdatedAt":` + itoa(statusAt) + `}`,
		"22.json": `{"pid":22,"sessionId":"bbbb2222-bare","cwd":"/w/sole","startedAt":1,"messagingSocketPath":"/tmp/cc-socks/22.sock","name":"sole-51","status":"busy","statusUpdatedAt":` + itoa(statusAt) + `}`,
	} {
		if err := os.WriteFile(filepath.Join(cfg, "sessions", name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Bind(ctx, store.PeerKey{Session: "aaaa1111-root"}, "b577ef71", true); err != nil {
		t.Fatal(err)
	}
	for _, p := range []store.Peer{
		{Session: "aaaa1111-root", Role: "root", Description: "cci mesh discovery", Tags: []string{"mesh"}, Branch: "yasyf/eng-1062", Worktree: "/w/alewife", Remote: "Forge-AI/monorepo"},
		{Session: "cccc3333-codex", Role: "codex", Description: "sweep", Branch: "sweep-1"},
	} {
		if _, err := st.Announce(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	clock.Advance(5 * time.Minute)
	tests := []struct {
		name string
		f    peers.Filter
		want []string
	}{
		{"all by last seen", peers.Filter{All: true}, []string{"cccc3333-codex", "aaaa1111-root", "bbbb2222-bare"}},
		{"live only keeps the hook-fresh codex lane", peers.Filter{}, []string{"cccc3333-codex"}},
		{"session prefix", peers.Filter{Session: "aaaa", All: true}, []string{"aaaa1111-root"}},
		{"harness name", peers.Filter{Name: "sole-51", All: true}, []string{"bbbb2222-bare"}},
		{"tag", peers.Filter{Tag: "mesh", All: true}, []string{"aaaa1111-root"}},
		{"drive", peers.Filter{Drive: "b577ef71", All: true}, []string{"aaaa1111-root"}},
		{"repo slug", peers.Filter{Repo: "Forge-AI/monorepo", All: true}, []string{"aaaa1111-root"}},
		{"worktree or cwd", peers.Filter{Worktree: "/w/sole", All: true}, []string{"bbbb2222-bare"}},
		{"branch", peers.Filter{Branch: "sweep-1", All: true}, []string{"cccc3333-codex"}},
		{"query over description", peers.Filter{Query: "MESH", All: true}, []string{"aaaa1111-root"}},
		{"query over cwd", peers.Filter{Query: "/w/sole", All: true}, []string{"bbbb2222-bare"}},
		{"nothing", peers.Filter{Role: "watcher", All: true}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			views, err := peers.Resolve(ctx, st, tt.f)
			if err != nil {
				t.Fatal(err)
			}
			got := make([]string, len(views))
			for i, v := range views {
				got[i] = v.Session
			}
			if len(got) != len(tt.want) {
				t.Fatalf("Resolve() = %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("Resolve() = %v, want %v", got, tt.want)
				}
			}
		})
	}
	views, err := peers.Resolve(ctx, st, peers.Filter{All: true})
	if err != nil {
		t.Fatal(err)
	}
	lines := map[string]string{}
	for _, v := range views {
		lines[v.Session] = v.Line(clock.Now())
	}
	announced := render.Stamp(clock.Now().Add(-5*time.Minute), clock.Now())
	status := render.Stamp(time.UnixMilli(statusAt), clock.Now())
	want := map[string]string{
		"aaaa1111-root":  `alewife-6d root drive=b577ef71 branch=yasyf/eng-1062 wt=/w/alewife gone, last seen ` + announced + ` "cci mesh discovery" #mesh session=aaaa1111 -> uds:/tmp/cc-socks/11.sock`,
		"bbbb2222-bare":  `sole-51 session cwd=/w/sole gone, last seen ` + status + ` session=bbbb2222 -> uds:/tmp/cc-socks/22.sock`,
		"cccc3333-codex": `- codex branch=sweep-1 seen 5m "sweep" session=cccc3333`,
	}
	for session, line := range want {
		if lines[session] != line {
			t.Errorf("Line(%s)\n got %q\nwant %q", session, lines[session], line)
		}
	}
	if _, err := peers.One(ctx, st, peers.Filter{Session: "zzz", All: true}); err == nil {
		t.Fatal("One() found a peer for an unknown prefix")
	}
	if _, err := peers.One(ctx, st, peers.Filter{All: true}); err == nil {
		t.Fatal("One() accepted an ambiguous filter")
	}
}

func itoa(n int64) string {
	return strconv.FormatInt(n, 10)
}

func TestAnnounceCanonicalizesPathsAndKeepsRole(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())
	dir := t.TempDir()
	real, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	first, err := peers.Announce(ctx, st, peers.Options{Session: "s", Cwd: dir, Role: "watcher"})
	if err != nil || first.Cwd != real || first.Role != "watcher" {
		t.Fatalf("Announce() = %+v, %v; want cwd %s", first, err, real)
	}
	again, err := peers.Announce(ctx, st, peers.Options{Session: "s", Cwd: dir})
	if err != nil || again.Role != "watcher" {
		t.Fatalf("automatic Announce() = %+v, %v; want role kept", again, err)
	}
	views, err := peers.Resolve(ctx, st, peers.Filter{Worktree: dir, All: true})
	if err != nil || len(views) != 1 {
		t.Fatalf("Resolve(--worktree %s) = %+v, %v", dir, views, err)
	}
}

func TestAgentViewsLeaveTheRootItsWindow(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	cfg := t.TempDir()
	t.Setenv("CLAUDE_CONFIG_DIR", cfg)
	if err := os.MkdirAll(filepath.Join(cfg, "sessions"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cfg, "sessions", "11.json"), []byte(`{"pid":11,"sessionId":"aaaa1111-root","cwd":"/w/alewife","startedAt":1,"messagingSocketPath":"/tmp/cc-socks/11.sock","name":"alewife-6d","status":"idle","statusUpdatedAt":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Announce(ctx, store.Peer{Session: "aaaa1111-root", Agent: "aapi-0123", AgentName: "api-lane"}); err != nil {
		t.Fatal(err)
	}
	views, err := peers.Resolve(ctx, st, peers.Filter{Session: "aaaa", All: true})
	if err != nil || len(views) != 2 {
		t.Fatalf("Resolve() = %+v, %v; want the root window and its agent", views, err)
	}
	byAgent := map[string]peers.View{}
	for _, v := range views {
		byAgent[v.Agent] = v
	}
	if root := byAgent[""]; root.Name != "alewife-6d" || root.Address == "" || root.Thread() != "aaaa1111" {
		t.Fatalf("root view = %+v", root)
	}
	if agent := byAgent["aapi-0123"]; agent.Name != "api-lane" || agent.Address != "" || agent.Thread() != "aaaa1111/api-lane" || !agent.Live {
		t.Fatalf("agent view = %+v", agent)
	}
	for _, tt := range []struct {
		f    peers.Filter
		want string
	}{
		{peers.Filter{Agent: "api-lane", All: true}, "aapi-0123"},
		{peers.Filter{Agent: "aapi-0123", All: true}, "aapi-0123"},
		{peers.KeyFilter(store.PeerKey{Session: "aaaa1111-root"}), ""},
	} {
		v, err := peers.One(ctx, st, tt.f)
		if err != nil || v.Agent != tt.want {
			t.Fatalf("One(%+v) = %+v, %v; want agent %q", tt.f, v, err, tt.want)
		}
	}
}
