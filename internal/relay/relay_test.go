package relay_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/e2e"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/node"
	"github.com/yasyf/cc-inbox/internal/relay"
	"github.com/yasyf/cc-inbox/internal/relay/relaytest"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

const secret = "the launch code is swordfish"

type peer struct {
	st   *store.Store
	id   node.Node
	tier node.Tier
	node *relay.Node
}

func newPeer(t *testing.T, srv *relaytest.Server, tier node.Tier) *peer {
	t.Helper()
	st, _ := testutil.Store(t)
	id, err := node.Load(st.HomeDir())
	if err != nil {
		t.Fatal(err)
	}
	srv.AddMember(id, string(tier))
	return &peer{st: st, id: id, tier: tier}
}

func pin(t *testing.T, on *peer, others ...*peer) {
	t.Helper()
	for _, o := range others {
		if err := on.st.PutMember(context.Background(), store.Member{
			Node: o.id.ID(), PublicKey: o.id.PublicKey(), Tier: o.tier, By: "test", At: time.Now(),
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func join(t *testing.T, srv *relaytest.Server, p *peer, routes ...string) {
	t.Helper()
	n, err := relay.Open(p.st.HomeDir(), p.st, p.id, relay.Config{URL: srv.URL, Mesh: relay.DefaultMesh, Tier: string(p.tier), Routes: routes})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Join(context.Background()); err != nil {
		t.Fatalf("join %s: %v", p.tier, err)
	}
	p.node = n
}

func configs(t *testing.T, p *peer) []relay.Config {
	t.Helper()
	cs, err := relay.Configs(p.st.HomeDir())
	if err != nil {
		t.Fatal(err)
	}
	return cs
}

func pull(t *testing.T, p *peer) store.Applied {
	t.Helper()
	a, err := p.node.Pull(context.Background(), 0, nil)
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	return a
}

func texts(t *testing.T, st *store.Store) []string {
	t.Helper()
	recs, err := st.Query(context.Background(), store.Filter{Drive: "d"})
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, r := range recs {
		out = append(out, r.Text)
	}
	return out
}

func mesh(t *testing.T) (*relaytest.Server, *peer, *peer, *peer) {
	t.Helper()
	srv := relaytest.NewServer(t)
	a := newPeer(t, srv, node.TierOwner)
	b := newPeer(t, srv, node.TierOwner)
	c := newPeer(t, srv, node.TierPaired)
	pin(t, a, b, c)
	pin(t, b, a)
	pin(t, c, a)
	join(t, srv, a)
	join(t, srv, b)
	join(t, srv, c)
	return srv, a, b, c
}

func TestPushPullEndToEnd(t *testing.T) {
	srv, a, b, c := mesh(t)
	if srv.Epoch() != 1 {
		t.Fatalf("first owner join left epoch %d, want 1", srv.Epoch())
	}
	testutil.Post(t, a.st, store.Record{Kind: kinds.Decision, Text: secret})
	pushed, err := a.node.Push(context.Background(), configs(t, a))
	if err != nil || pushed != 1 {
		t.Fatalf("push = %d, %v; want 1", pushed, err)
	}
	for _, p := range []*peer{b, c} {
		if got := pull(t, p); got.Inserted != 1 {
			t.Fatalf("%s pull inserted %d, want 1", p.tier, got.Inserted)
		}
		if got := texts(t, p.st); len(got) != 1 || got[0] != secret {
			t.Fatalf("%s texts = %q, want [%q]", p.tier, got, secret)
		}
	}
	for _, body := range srv.Pushes {
		if bytes.Contains(body, []byte("swordfish")) || bytes.Contains(body, []byte("launch")) {
			t.Fatalf("captured push body holds plaintext: %s", body)
		}
	}
	if again, _ := a.node.Push(context.Background(), configs(t, a)); again != 0 {
		t.Fatalf("second push resent %d records", again)
	}
}

func TestRedeliveryDedups(t *testing.T) {
	srv, a, b, _ := mesh(t)
	testutil.Post(t, a.st, store.Record{Kind: kinds.Note, Text: "once"})
	if _, err := a.node.Push(context.Background(), configs(t, a)); err != nil {
		t.Fatal(err)
	}
	pull(t, b)
	srv.Replay(b.id.ID())
	if got := pull(t, b); got.Inserted != 0 || got.Duplicate != 1 {
		t.Fatalf("redelivered pull = %+v, want 1 duplicate", got)
	}
}

func TestUnwrappedNodeCannotRead(t *testing.T) {
	srv := relaytest.NewServer(t)
	a := newPeer(t, srv, node.TierOwner)
	stranger := newPeer(t, srv, node.TierPaired)
	pin(t, stranger, a)
	join(t, srv, a)
	join(t, srv, stranger)
	testutil.Post(t, a.st, store.Record{Kind: kinds.Note, Text: secret})
	if _, err := a.node.Push(context.Background(), configs(t, a)); err != nil {
		t.Fatal(err)
	}
	if got := pull(t, stranger); got.Inserted != 0 {
		t.Fatalf("node without a wrap applied %d records", got.Inserted)
	}
	if got := texts(t, stranger.st); len(got) != 0 {
		t.Fatalf("node without a wrap reads %q", got)
	}
}

func TestRotationLocksOutDroppedMember(t *testing.T) {
	_, a, b, c := mesh(t)
	if _, err := a.st.DropMember(context.Background(), c.id.ID()); err != nil {
		t.Fatal(err)
	}
	epoch, err := a.node.Rotate(context.Background())
	if err != nil || epoch != 2 {
		t.Fatalf("rotate = %d, %v; want 2", epoch, err)
	}
	pull(t, b)
	pull(t, c)
	testutil.Post(t, a.st, store.Record{Kind: kinds.Note, Text: secret})
	if _, err := a.node.Push(context.Background(), configs(t, a)); err != nil {
		t.Fatal(err)
	}
	if got := pull(t, b); got.Inserted != 1 {
		t.Fatalf("remaining owner inserted %d after rotation, want 1", got.Inserted)
	}
	if got := pull(t, c); got.Inserted != 0 {
		t.Fatalf("dropped member inserted %d after rotation, want 0", got.Inserted)
	}
}

func TestPairedNodeNeverPushes(t *testing.T) {
	srv, _, _, c := mesh(t)
	testutil.Post(t, c.st, store.Record{Kind: kinds.Note, Text: "read-only"})
	if pushed, err := c.node.Push(context.Background(), configs(t, c)); err != nil || pushed != 0 {
		t.Fatalf("paired push = %d, %v; want 0, nil", pushed, err)
	}
	if len(srv.Pushes) != 0 {
		t.Fatalf("paired node sent %d push requests", len(srv.Pushes))
	}
}

func TestRoutesSplitDrivesAcrossMeshes(t *testing.T) {
	cs := []relay.Config{{Mesh: "default"}, {Mesh: "oncall", Routes: []string{"page:*"}}}
	for drive, want := range map[string]string{"page:123": "oncall", "cci-mesh": "default"} {
		if got := relay.Route(cs, drive); got != want {
			t.Errorf("Route(%s) = %s, want %s", drive, got, want)
		}
	}
}

func TestForgedControlIgnored(t *testing.T) {
	srv, _, b, _ := mesh(t)
	var seen []e2e.Control
	srv.Control(e2e.Control{Type: e2e.ControlUnpaired, Mesh: relay.DefaultMesh, Node: "x", Reason: "owner", By: "relay"})
	srv.ForgeControl(e2e.Control{Type: e2e.ControlUnpaired, Mesh: relay.DefaultMesh, Node: b.id.ID(), Reason: "owner", By: "relay"})
	if _, err := b.node.Pull(context.Background(), 0, func(_ context.Context, c e2e.Control) error {
		if c.Type == e2e.ControlUnpaired {
			seen = append(seen, c)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 1 || seen[0].Node != "x" {
		t.Fatalf("handler saw %+v, want only the relay-signed control", seen)
	}
}

func TestDelegatedInstancesWriteUnderTheirOwnOrigins(t *testing.T) {
	srv := relaytest.NewServer(t)
	a := newPeer(t, srv, node.TierOwner)
	svc := newPeer(t, srv, node.TierService)
	pin(t, a, svc)
	join(t, srv, a)
	pemKey, err := os.ReadFile(filepath.Join(node.Dir(svc.st.HomeDir()), "key.pem"))
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(relay.ServiceKeyEnv, string(pemKey))
	var jobs []*peer
	for range 2 {
		st, _ := testutil.Store(t)
		id, err := node.Load(st.HomeDir())
		if err != nil {
			t.Fatal(err)
		}
		job := &peer{st: st, id: id, tier: node.TierService}
		pin(t, job, a)
		n, err := relay.Open(st.HomeDir(), st, id, relay.Config{URL: srv.URL, Mesh: relay.DefaultMesh, Tier: string(node.TierService)})
		if err != nil {
			t.Fatal(err)
		}
		if err := n.Delegate([]string{"page:*"}, nil, time.Hour); err != nil {
			t.Fatal(err)
		}
		if err := n.Join(context.Background()); err != nil {
			t.Fatalf("instance join: %v", err)
		}
		job.node = n
		testutil.Post(t, st, store.Record{Drive: "page:1", Kind: kinds.Note, Text: "from job " + id.ID()})
		if pushed, err := n.Push(context.Background(), configs(t, job)); err != nil || pushed != 1 {
			t.Fatalf("instance push = %d, %v; want 1", pushed, err)
		}
		jobs = append(jobs, job)
	}
	if got := pull(t, a); got.Inserted != 2 {
		t.Fatalf("owner inserted %d instance records, want 2 (one per origin)", got.Inserted)
	}
	reopened, err := relay.Open(jobs[0].st.HomeDir(), jobs[0].st, jobs[0].id, mustConfig(t, jobs[0]))
	if err != nil {
		t.Fatal(err)
	}
	testutil.Post(t, a.st, store.Record{Drive: "page:1", Kind: kinds.Note, Text: secret})
	if _, err := a.node.Push(context.Background(), configs(t, a)); err != nil {
		t.Fatal(err)
	}
	if got, err := reopened.Pull(context.Background(), 0, nil); err != nil || got.Inserted != 2 {
		t.Fatalf("instance pull = %+v, %v; want the sibling's record and the owner's", got, err)
	}
}

func mustConfig(t *testing.T, p *peer) relay.Config {
	t.Helper()
	c, ok, err := relay.LoadConfig(p.st.HomeDir(), relay.DefaultMesh)
	if err != nil || !ok {
		t.Fatalf("load config: %v, %v", ok, err)
	}
	return c
}

func TestJoinRefusesARelayAssignedTier(t *testing.T) {
	srv := relaytest.NewServer(t)
	p := newPeer(t, srv, node.TierPaired)
	n, err := relay.Open(p.st.HomeDir(), p.st, p.id, relay.Config{URL: srv.URL, Mesh: relay.DefaultMesh, Tier: string(node.TierOwner)})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Join(context.Background()); err == nil {
		t.Fatal("join accepted a tier the relay contradicts")
	}
}

func TestForgedWrapDoesNotShadowTheRealOne(t *testing.T) {
	srv := relaytest.NewServer(t)
	a := newPeer(t, srv, node.TierOwner)
	b := newPeer(t, srv, node.TierOwner)
	pin(t, a, b)
	pin(t, b, a)
	join(t, srv, a)
	forger, err := node.Load(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	k, _ := e2e.NewGroupKey(1)
	forged, err := e2e.WrapKey(k, relay.DefaultMesh, forger, e2e.Member{Node: b.id.ID(), PublicKey: b.id.PublicKey(), Tier: "owner"})
	if err != nil {
		t.Fatal(err)
	}
	forged.From = a.id.ID()
	srv.InjectWrap(forged)
	join(t, srv, b)
	testutil.Post(t, a.st, store.Record{Kind: kinds.Note, Text: secret})
	if _, err := a.node.Push(context.Background(), configs(t, a)); err != nil {
		t.Fatal(err)
	}
	if got := pull(t, b); got.Inserted != 1 {
		t.Fatalf("owner with a forged wrap ahead of the real one inserted %d, want 1", got.Inserted)
	}
}
