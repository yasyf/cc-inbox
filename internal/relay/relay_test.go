package relay_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/e2e"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/node"
	"github.com/yasyf/cc-inbox/internal/relay"
	"github.com/yasyf/cc-inbox/internal/relay/relaytest"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
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
			Node: o.id.ID(), PublicKey: o.id.PublicKey(), Tier: o.tier, Via: "relay", By: "test", At: time.Now(),
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
	out := make([]string, 0, len(recs))
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
	res, err := a.node.Push(context.Background(), configs(t, a))
	if err != nil || res.Pushed != 1 {
		t.Fatalf("push = %+v, %v; want 1", res, err)
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
	if again, _ := a.node.Push(context.Background(), configs(t, a)); again.Pushed != 0 {
		t.Fatalf("second push resent %d records", again.Pushed)
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

func TestAnOwnerNeverOffersTheRelayADottedKind(t *testing.T) {
	srv, a, _, _ := mesh(t)
	before := len(srv.Pushes)
	testutil.Post(t, a.st, store.Record{Kind: kinds.Kind("lr.note"), Body: []byte(`{"text":"stays here"}`), Text: "stays here"})
	res, err := a.node.Push(context.Background(), configs(t, a))
	if err != nil || res.Pushed != 0 || len(res.Rejected) != 0 {
		t.Fatalf("push = %+v, %v; want nothing pushed and nothing refused", res, err)
	}
	if len(srv.Pushes) != before {
		t.Fatalf("a batch of only dotted kinds still called the relay %d times", len(srv.Pushes)-before)
	}
	testutil.Post(t, a.st, store.Record{Kind: kinds.Decision, Text: "crosses"})
	if res, err := a.node.Push(context.Background(), configs(t, a)); err != nil || res.Pushed != 1 {
		t.Fatalf("the next push = %+v, %v; want the decision", res, err)
	}
}

func TestStatusNamesTheKindsAnOwnerKeepsLocal(t *testing.T) {
	_, a, _, c := mesh(t)
	owner, err := a.node.Status(context.Background())
	if err != nil || !strings.Contains(owner.KeptLocal, "lr.note") || !strings.Contains(owner.KeptLocal, "board.card") {
		t.Fatalf("owner status = %+v, %v; want the dotted kinds it keeps local named", owner, err)
	}
	paired, err := c.node.Status(context.Background())
	if err != nil || paired.KeptLocal != "" {
		t.Fatalf("paired status = %+v, %v; want no kept-local line", paired, err)
	}
}

func TestPairedNodeNeverPushes(t *testing.T) {
	srv, _, _, c := mesh(t)
	testutil.Post(t, c.st, store.Record{Kind: kinds.Note, Text: "read-only"})
	if res, err := c.node.Push(context.Background(), configs(t, c)); err != nil || res.Pushed != 0 {
		t.Fatalf("paired push = %+v, %v; want 0, nil", res, err)
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
	jobs := make([]*peer, 0, 2)
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
		if res, err := n.Push(context.Background(), configs(t, job)); err != nil || res.Pushed != 1 {
			t.Fatalf("instance push = %+v, %v; want 1", res, err)
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

func TestScopeReportsWhatTheRelayLetsEachKeyReadAndWrite(t *testing.T) {
	srv := relaytest.NewServer(t)
	owner := newPeer(t, srv, node.TierOwner)
	join(t, srv, owner)
	st, _ := testutil.Store(t)
	svc, err := node.Load(st.HomeDir())
	if err != nil {
		t.Fatal(err)
	}
	srv.AddService(svc, relaytest.Allow{Drives: []string{"page:*", "github"}, Kinds: []string{"oncall.say"}})
	enrolled := &peer{st: st, id: svc, tier: node.TierService}
	join(t, srv, enrolled)
	pemKey, err := os.ReadFile(filepath.Join(node.Dir(st.HomeDir()), "key.pem"))
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(relay.ServiceKeyEnv, string(pemKey))
	instance, _ := testutil.Store(t)
	id, err := node.Load(instance.HomeDir())
	if err != nil {
		t.Fatal(err)
	}
	delegated, err := relay.Open(instance.HomeDir(), instance, id, relay.Config{URL: srv.URL, Mesh: relay.DefaultMesh, Tier: string(node.TierService)})
	if err != nil {
		t.Fatal(err)
	}
	if err := delegated.Delegate([]string{"page:*"}, nil, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := delegated.Join(context.Background()); err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		node         *relay.Node
		drives, kind []string
	}{
		"owner":              {owner.node, []string{"*"}, []string{"*"}},
		"enrolled service":   {enrolled.node, []string{"page:*", "github"}, []string{"oncall.say"}},
		"delegated instance": {delegated, []string{"page:*"}, []string{"oncall.say"}},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := tc.node.Scope(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got.Drives, tc.drives) || !slices.Equal(got.Kinds, tc.kind) {
				t.Errorf("scope = drives %v kinds %v, want drives %v kinds %v", got.Drives, got.Kinds, tc.drives, tc.kind)
			}
		})
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

func TestSyncBackfillsADriveFromTheLog(t *testing.T) {
	srv, a, b, _ := mesh(t)
	for i := range 3 {
		testutil.Post(t, a.st, store.Record{Kind: kinds.Note, Text: fmt.Sprintf("note %d", i)})
	}
	if _, err := a.node.Push(context.Background(), configs(t, a)); err != nil {
		t.Fatal(err)
	}
	late := newPeer(t, srv, node.TierOwner)
	pin(t, a, late)
	pin(t, late, a, b)
	join(t, srv, late)
	if err := a.node.Share(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := late.node.Sync(context.Background(), "d")
	if err != nil || got.Inserted != 3 {
		t.Fatalf("sync = %+v, %v; want 3 inserted", got, err)
	}
	if again, err := late.node.Sync(context.Background(), "d"); err != nil || again.Inserted != 0 || again.Duplicate != 0 {
		t.Fatalf("second sync = %+v, %v; want nothing read past its cursor", again, err)
	}
	for i := range 2 {
		testutil.Post(t, a.st, store.Record{Kind: kinds.Note, Text: fmt.Sprintf("after the restart %d", i)})
	}
	if _, err := a.node.Push(context.Background(), configs(t, a)); err != nil {
		t.Fatal(err)
	}
	restarted, err := relay.Open(late.st.HomeDir(), late.st, late.id, mustConfig(t, late))
	if err != nil {
		t.Fatal(err)
	}
	if resumed, err := restarted.Sync(context.Background(), "d"); err != nil || resumed.Inserted != 2 || resumed.Duplicate != 0 {
		t.Fatalf("sync after a restart = %+v, %v; want only the 2 new records", resumed, err)
	}
}

func TestSyncReadsALongSparseLogWithoutAHoleAndInOrder(t *testing.T) {
	srv, a, b, _ := mesh(t)
	const records = 2000
	for i := range records {
		drive := "d"
		if i%3 == 2 {
			drive = "other"
		}
		testutil.Post(t, a.st, store.Record{Drive: drive, Kind: kinds.Note, Text: fmt.Sprintf("note %d", i)})
	}
	if _, err := a.node.Push(context.Background(), configs(t, a)); err != nil {
		t.Fatal(err)
	}
	late := newPeer(t, srv, node.TierOwner)
	pin(t, a, late)
	pin(t, late, a, b)
	join(t, srv, late)
	if err := a.node.Share(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := late.node.Sync(context.Background(), "d"); err != nil {
		t.Fatal(err)
	}
	var want []string
	for i := range records {
		if i%3 != 2 {
			want = append(want, fmt.Sprintf("note %d", i))
		}
	}
	got := texts(t, late.st)
	if !slices.Equal(got, want) {
		t.Fatalf("synced %d records, want %d in origin order", len(got), len(want))
	}
}

func TestSyncOfASparseLogReadsFewDuplicates(t *testing.T) {
	srv, a, b, _ := mesh(t)
	const records = 3000
	for i := range records {
		drive := "d"
		if i%3 != 0 {
			drive = "other"
		}
		testutil.Post(t, a.st, store.Record{Drive: drive, Kind: kinds.Note, Text: fmt.Sprintf("note %d", i)})
	}
	if _, err := a.node.Push(context.Background(), configs(t, a)); err != nil {
		t.Fatal(err)
	}
	late := newPeer(t, srv, node.TierOwner)
	pin(t, a, late)
	pin(t, late, a, b)
	join(t, srv, late)
	if err := a.node.Share(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := late.node.Sync(context.Background(), "d")
	if err != nil {
		t.Fatal(err)
	}
	if got.Inserted != records/3 || got.Duplicate > got.Inserted/5 {
		t.Fatalf("sync = %+v; want %d inserted and few duplicates", got, records/3)
	}
}

func TestASeedCutOffPartwayKeepsWhatItReadAndSyncReadsOnlyTheRest(t *testing.T) {
	srv, a, b, _ := mesh(t)
	const records = 500
	for i := range records {
		testutil.Post(t, a.st, store.Record{Kind: kinds.Note, Text: fmt.Sprintf("note %d of a long drive with enough text to fill a gzip block %d", i, i*i)})
	}
	if _, err := a.node.Push(context.Background(), configs(t, a)); err != nil {
		t.Fatal(err)
	}
	var copied bytes.Buffer
	if err := relay.ExportDrive(context.Background(), a.st, "d", time.Time{}, &copied); err != nil {
		t.Fatal(err)
	}
	late := newPeer(t, srv, node.TierOwner)
	pin(t, a, late)
	pin(t, late, a, b)
	join(t, srv, late)
	if err := a.node.Share(context.Background()); err != nil {
		t.Fatal(err)
	}

	seeded, err := late.node.Seed(context.Background(), "d", bytes.NewReader(copied.Bytes()[:copied.Len()/2]))
	if err == nil || seeded.Inserted == 0 || seeded.Inserted == records {
		t.Fatalf("seed of half a copy = %+v, %v; want an error and some rows", seeded, err)
	}

	synced, err := late.node.Sync(context.Background(), "d")
	if err != nil || seeded.Inserted+synced.Inserted != records || synced.Duplicate >= seeded.Inserted {
		t.Fatalf("seed %+v then sync %+v, %v; want %d in all and few rows read twice", seeded, synced, err, records)
	}
}

func TestSeedLoadsAnExportedDriveSoSyncReadsOnlyWhatArrivedSince(t *testing.T) {
	srv, a, b, _ := mesh(t)
	for i := range 5 {
		testutil.Post(t, a.st, store.Record{Kind: kinds.Note, Text: fmt.Sprintf("note %d", i)})
	}
	testutil.Post(t, a.st, store.Record{Drive: "other", Kind: kinds.Note, Text: "another drive"})
	if _, err := a.node.Push(context.Background(), configs(t, a)); err != nil {
		t.Fatal(err)
	}
	var copied bytes.Buffer
	if err := relay.ExportDrive(context.Background(), a.st, "d", time.Time{}, &copied); err != nil {
		t.Fatal(err)
	}
	late := newPeer(t, srv, node.TierOwner)
	pin(t, a, late)
	pin(t, late, a, b)
	join(t, srv, late)
	if err := a.node.Share(context.Background()); err != nil {
		t.Fatal(err)
	}

	seeded, err := late.node.Seed(context.Background(), "d", &copied)
	if err != nil || seeded.Inserted != 5 {
		t.Fatalf("seed = %+v, %v; want 5 inserted", seeded, err)
	}
	testutil.Post(t, a.st, store.Record{Kind: kinds.Note, Text: "after the copy"})
	if _, err := a.node.Push(context.Background(), configs(t, a)); err != nil {
		t.Fatal(err)
	}

	synced, err := late.node.Sync(context.Background(), "d")
	if err != nil || synced.Inserted != 1 || synced.Duplicate != 0 {
		t.Fatalf("sync after the seed = %+v, %v; want only the 1 new record", synced, err)
	}
	if got := texts(t, late.st); len(got) != 6 || got[0] != "note 0" || got[5] != "after the copy" {
		t.Fatalf("store holds %q", got)
	}
}

func TestAnExportSinceALaunchSeedsOnlyWhatTheStoreTookFromThen(t *testing.T) {
	srv, a, b, _ := mesh(t)
	launch := a.st.Now().Add(time.Minute)
	testutil.Post(t, a.st, store.Record{Kind: kinds.Note, Text: "before the launch"})
	a.st.Now = func() time.Time { return launch }
	for _, text := range []string{"at the launch", "before the join"} {
		testutil.Post(t, a.st, store.Record{Kind: kinds.Note, Text: text})
	}
	testutil.Post(t, a.st, store.Record{Drive: "other", Kind: kinds.Note, Text: "another drive"})
	late := newPeer(t, srv, node.TierOwner)
	pin(t, late, a, b)
	join(t, srv, late)

	for _, step := range []struct {
		since time.Time
		want  []string
	}{
		{launch.Add(time.Hour), []string{}},
		{launch, []string{"at the launch", "before the join"}},
	} {
		var copied bytes.Buffer
		if err := relay.ExportDrive(context.Background(), a.st, "d", step.since, &copied); err != nil {
			t.Fatal(err)
		}
		if _, err := late.node.Seed(context.Background(), "d", &copied); err != nil {
			t.Fatal(err)
		}
		if got := texts(t, late.st); !slices.Equal(got, step.want) {
			t.Fatalf("a seed from an export since %s left the store holding %q, want %q", step.since, got, step.want)
		}
	}
}

var (
	measureTransactions sync.Once
	transactions        = struct {
		sync.Mutex
		largest map[string]int
	}{largest: map[string]int{}}
)

func largestTransactionUnder(home string) int {
	transactions.Lock()
	defer transactions.Unlock()
	most := 0
	for dsn, largest := range transactions.largest {
		if strings.Contains(dsn, home+string(filepath.Separator)) {
			most = max(most, largest)
		}
	}
	return most
}

type lockProbe struct {
	copy   io.Reader
	writer *sql.DB
	held   []error
}

func (p *lockProbe) Read(b []byte) (int, error) {
	tx, err := p.writer.BeginTx(context.Background(), nil)
	if err == nil {
		err = tx.Rollback()
	}
	if err != nil {
		p.held = append(p.held, err)
	}
	return p.copy.Read(b)
}

func TestASeedHoldsTheWriteLockAPageAtATimeAndNeverWhileItReadsItsCopy(t *testing.T) {
	measureTransactions.Do(func() {
		sqlite.RegisterConnectionHook(func(conn sqlite.ExecQuerierContext, dsn string) error {
			inserted := 0
			hooks := conn.(sqlite.HookRegisterer)
			hooks.RegisterPreUpdateHook(func(change sqlite.SQLitePreUpdateData) {
				if change.TableName == "records" && change.Op == sqlite3.SQLITE_INSERT {
					inserted++
				}
			})
			hooks.RegisterCommitHook(func() int32 {
				transactions.Lock()
				transactions.largest[dsn] = max(transactions.largest[dsn], inserted)
				transactions.Unlock()
				inserted = 0
				return 0
			})
			hooks.RegisterRollbackHook(func() { inserted = 0 })
			return nil
		})
	})
	largest := map[int]int{}
	for _, rows := range []int{250, 2500} {
		srv := relaytest.NewServer(t)
		late := newPeer(t, srv, node.TierOwner)
		join(t, srv, late)
		var copied bytes.Buffer
		zipped := gzip.NewWriter(&copied)
		encoder := json.NewEncoder(zipped)
		at := time.Now().Add(-time.Hour)
		for i := range rows {
			if err := encoder.Encode(store.Row{Origin: "0123456789abcdef", OriginSeq: int64(i + 1), Drive: "d", Lane: "root", Kind: kinds.Note, At: at, Text: fmt.Sprintf("note %d", i), Source: "post"}); err != nil {
				t.Fatal(err)
			}
		}
		if err := zipped.Close(); err != nil {
			t.Fatal(err)
		}
		writer, err := sql.Open("sqlite", "file:"+filepath.Join(late.st.HomeDir(), store.DBFile)+"?_pragma=busy_timeout(0)&_txlock=immediate")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = writer.Close() })
		probe := &lockProbe{copy: &copied, writer: writer}
		seeded, err := late.node.Seed(context.Background(), "d", probe)
		if err != nil || seeded.Inserted != rows {
			t.Fatalf("seed of %d rows = %+v, %v; want every row", rows, seeded, err)
		}
		if len(probe.held) > 0 {
			t.Fatalf("another writer found the store locked at %d reads of a %d-row copy: %v", len(probe.held), rows, probe.held)
		}
		largest[rows] = largestTransactionUnder(late.st.HomeDir())
	}
	if largest[250] != largest[2500] {
		t.Fatalf("the largest seed transaction stored %d records from a 250-row copy and %d from a 2,500-row copy; want the same page either way", largest[250], largest[2500])
	}
}

type unconfirmedCursors struct{}

func (unconfirmedCursors) Commit(_ context.Context, w store.Write) (store.Written, error) {
	if len(w.Cursors) > 0 {
		return store.Written{}, store.ErrUnconfirmed
	}
	return store.Written{}, store.ErrUnrouted
}

func TestSyncFinishesWhenItsCursorWriteIsUnconfirmed(t *testing.T) {
	srv, a, _, _ := mesh(t)
	for i := range 3 {
		testutil.Post(t, a.st, store.Record{Kind: kinds.Note, Text: fmt.Sprintf("note %d", i)})
	}
	if _, err := a.node.Push(context.Background(), configs(t, a)); err != nil {
		t.Fatal(err)
	}
	late := newPeer(t, srv, node.TierOwner)
	pin(t, a, late)
	pin(t, late, a)
	join(t, srv, late)
	if err := a.node.Share(context.Background()); err != nil {
		t.Fatal(err)
	}
	late.st.WriteThrough(unconfirmedCursors{})
	if got, err := late.node.Sync(context.Background(), "d"); err != nil || got.Inserted != 3 {
		t.Fatalf("sync = %+v, %v; want 3 inserted despite the cursor write", got, err)
	}
}

func TestSyncDoesNotMovePastARecordItCouldNotReadYet(t *testing.T) {
	srv, a, _, _ := mesh(t)
	for i := range 3 {
		testutil.Post(t, a.st, store.Record{Kind: kinds.Note, Text: fmt.Sprintf("note %d", i)})
	}
	if _, err := a.node.Push(context.Background(), configs(t, a)); err != nil {
		t.Fatal(err)
	}
	late := newPeer(t, srv, node.TierOwner)
	pin(t, a, late)
	pin(t, late, a)
	join(t, srv, late)
	if early, err := late.node.Sync(context.Background(), "d"); err != nil || early.Inserted != 0 {
		t.Fatalf("sync before the group key = %+v, %v; want nothing readable", early, err)
	}
	if err := a.node.Share(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, err := late.node.Sync(context.Background(), "d"); err != nil || got.Inserted != 3 {
		t.Fatalf("sync once the key arrived = %+v, %v; want the 3 records the cursor stayed behind", got, err)
	}
}

func TestRotateAfterIsConditional(t *testing.T) {
	srv, a, b, _ := mesh(t)
	if _, err := a.node.RotateAfter(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := b.node.RotateAfter(context.Background(), 1); !errors.Is(err, relay.ErrEpochTaken) || srv.Epoch() != 2 {
		t.Fatalf("second rotation after epoch 1 = %v at epoch %d, want ErrEpochTaken at 2", err, srv.Epoch())
	}
}

func TestPushResealsAfterAConcurrentRotation(t *testing.T) {
	srv, a, b, c := mesh(t)
	if _, err := b.st.DropMember(context.Background(), c.id.ID()); err != nil {
		t.Fatal(err)
	}
	if _, err := b.node.Rotate(context.Background()); err != nil {
		t.Fatal(err)
	}
	testutil.Post(t, a.st, store.Record{Kind: kinds.Note, Text: secret})
	if res, err := a.node.Push(context.Background(), configs(t, a)); err != nil || res.Pushed != 1 {
		t.Fatalf("push across a rotation = %+v, %v; want 1 resealed", res, err)
	}
	if got := pull(t, b); got.Inserted != 1 || srv.Epoch() != 2 {
		t.Fatalf("owner inserted %d at epoch %d, want 1 at 2", got.Inserted, srv.Epoch())
	}
}

func TestConcurrentPostsAndPushWaitOutAHeldWriteLock(t *testing.T) {
	_, a, _, _ := mesh(t)
	ctx := context.Background()
	testutil.Post(t, a.st, store.Record{Kind: kinds.Decision, Text: "before the lock"})
	cs := configs(t, a)
	holder, err := sql.Open("sqlite", "file:"+filepath.Join(a.st.HomeDir(), store.DBFile))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = holder.Close() })
	conn, err := holder.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	const writers = 16
	errs := make(chan error, writers+1)
	for i := range writers {
		go func() {
			st, err := store.Open(ctx, a.st.HomeDir())
			if err != nil {
				errs <- err
				return
			}
			_, _, err = st.Post(ctx, store.Record{Drive: "d", Lane: "lane-a", Kind: kinds.Decision, Text: fmt.Sprintf("writer %d", i)}, 0)
			errs <- errors.Join(err, st.Close())
		}()
	}
	go func() {
		res, err := a.node.Push(ctx, cs)
		if err == nil && res.Pushed != 1 {
			err = fmt.Errorf("push sent %d records, want 1", res.Pushed)
		}
		errs <- err
	}()
	time.Sleep(12 * time.Second)
	if _, err := conn.ExecContext(ctx, "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	for range writers + 1 {
		if err := <-errs; err != nil {
			t.Errorf("a writer gave up on the held lock: %v", err)
		}
	}
	if got := len(texts(t, a.st)); got != writers+1 {
		t.Fatalf("store holds %d records, want %d", got, writers+1)
	}
}

func TestConcurrentPushesFromOneHomeSendEachRecordOnce(t *testing.T) {
	_, a, b, _ := mesh(t)
	const records = 40
	for i := range records {
		testutil.Post(t, a.st, store.Record{Kind: kinds.Decision, Text: fmt.Sprintf("record %d", i)})
	}
	second, err := relay.Open(a.st.HomeDir(), a.st, a.id, mustConfig(t, a))
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan relay.PushResult, 2)
	errs := make(chan error, 2)
	for _, n := range []*relay.Node{a.node, second} {
		go func() {
			res, err := n.Push(context.Background(), configs(t, a))
			results <- res
			errs <- err
		}()
	}
	pushed := 0
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("push: %v", err)
		}
		res := <-results
		if len(res.Rejected) != 0 {
			t.Fatalf("a push racing another from the same home was refused %+v", res.Rejected)
		}
		pushed += res.Pushed
	}
	if pushed != records {
		t.Fatalf("the two pushes sent %d records, want %d", pushed, records)
	}
	inserted := 0
	for range records {
		inserted += pull(t, b).Inserted
	}
	if inserted != records {
		t.Fatalf("peer inserted %d records, want %d", inserted, records)
	}
}

func TestABacklogOverTheRelaysBodyLimitPushesInOrderedPagesUnderIt(t *testing.T) {
	srv, a, _, _ := mesh(t)
	const records = 230
	for i := range records {
		testutil.Post(t, a.st, store.Record{Kind: kinds.Note, Text: fmt.Sprintf("record %d", i), Fields: store.Fields{Attrs: map[string]string{"inputs": strings.Repeat("x", 40<<10)}}})
	}
	res, err := a.node.Push(context.Background(), configs(t, a))
	if err != nil || res.Pushed != records || len(res.Rejected) != 0 {
		t.Fatalf("push = %d pushed, %d refused, %v; want %d pushed", res.Pushed, len(res.Rejected), err, records)
	}
	seqs := make([]int64, 0, records)
	sent := 0
	for _, body := range srv.Pushes {
		if len(body) > 1<<20 {
			t.Fatalf("a page is %d bytes, over the relay's 1 MiB body limit", len(body))
		}
		sent += len(body)
		var page struct {
			Envelopes []e2e.Envelope `json:"envelopes"`
		}
		if err := json.Unmarshal(body, &page); err != nil {
			t.Fatal(err)
		}
		for _, e := range page.Envelopes {
			seqs = append(seqs, e.OriginSeq)
		}
	}
	if sent <= 1<<20 {
		t.Fatalf("the backlog sealed to %d bytes, want more than 1 MiB", sent)
	}
	if len(seqs) != records || !slices.IsSorted(seqs) || len(slices.Compact(slices.Clone(seqs))) != records {
		t.Fatalf("%d pages carried %d records out of seq order or twice; want each of %d once, in order", len(srv.Pushes), len(seqs), records)
	}
	if again, err := a.node.Push(context.Background(), configs(t, a)); err != nil || again.Pushed != 0 {
		t.Fatalf("second push = %+v, %v; want nothing left", again, err)
	}
}

func TestARecordNoPushCanCarryIsRefusedAloneAndTheRecordsBehindItPush(t *testing.T) {
	_, a, b, _ := mesh(t)
	testutil.Post(t, a.st, store.Record{Kind: kinds.Note, Text: "before"})
	big := testutil.Post(t, a.st, store.Record{Kind: kinds.Note, Text: "too large", Fields: store.Fields{Attrs: map[string]string{"inputs": strings.Repeat("x", 1<<20)}}})
	testutil.Post(t, a.st, store.Record{Kind: kinds.Note, Text: "after"})
	res, err := a.node.Push(context.Background(), configs(t, a))
	if err != nil || res.Pushed != 2 || len(res.Rejected) != 1 || res.Rejected[0].OriginSeq != big.Seq || !strings.HasPrefix(res.Rejected[0].Reason, "too_large: ") {
		t.Fatalf("push = %+v, %v; want 2 pushed and #%d refused as too_large", res, err, big.Seq)
	}
	pull(t, b)
	got := texts(t, b.st)
	slices.Sort(got)
	if !slices.Equal(got, []string{"after", "before"}) {
		t.Fatalf("peer holds %q, want the records around the refused one", got)
	}
	if again, err := a.node.Push(context.Background(), configs(t, a)); err != nil || again.Pushed != 0 || len(again.Rejected) != 0 {
		t.Fatalf("second push = %+v, %v; want the refused record acknowledged past", again, err)
	}
}

func TestDelegatedInstanceReopensWithoutTheServiceKeyInItsEnvironment(t *testing.T) {
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
	t.Setenv(relay.ServiceKeyEnv, "")
	kept, err := os.Stat(filepath.Join(st.HomeDir(), "relay", relay.DefaultMesh, "service.pem"))
	if err != nil || kept.Mode().Perm() != 0o600 {
		t.Fatalf("kept service key = %v, %v; want a 0600 file", kept, err)
	}
	reopened, err := relay.Open(st.HomeDir(), st, id, mustConfig(t, job))
	if err != nil {
		t.Fatalf("reopen without $%s: %v", relay.ServiceKeyEnv, err)
	}
	testutil.Post(t, st, store.Record{Drive: "page:1", Kind: kinds.Note, Text: "after the key left the environment"})
	if res, err := reopened.Push(context.Background(), configs(t, job)); err != nil || res.Pushed != 1 {
		t.Fatalf("instance push = %+v, %v; want 1", res, err)
	}
	if got := pull(t, a); got.Inserted != 1 {
		t.Fatalf("owner inserted %d instance records, want 1", got.Inserted)
	}
	first := reopened.Config.Delegation.Expires
	if err := reopened.Renew(context.Background()); err != nil {
		t.Fatalf("renew near expiry without $%s: %v", relay.ServiceKeyEnv, err)
	}
	renewed := mustConfig(t, job).Delegation
	if renewed.Expires == first || !slices.Equal(renewed.Drives, []string{"page:*"}) {
		t.Fatalf("renewed delegation = %+v; want a later expiry on the same drives", renewed)
	}
	if err := reopened.Renew(context.Background()); err != nil || mustConfig(t, job).Delegation.Expires != renewed.Expires {
		t.Fatalf("a fresh delegation renewed again: %v", err)
	}
	testutil.Post(t, st, store.Record{Drive: "page:1", Kind: kinds.Note, Text: "after the renewal"})
	again, err := relay.Open(st.HomeDir(), st, id, mustConfig(t, job))
	if err != nil {
		t.Fatal(err)
	}
	if res, err := again.Push(context.Background(), configs(t, job)); err != nil || res.Pushed != 1 {
		t.Fatalf("push on the renewed delegation = %+v, %v; want 1", res, err)
	}
}

func ownerDelegated(t *testing.T, srv *relaytest.Server, owner *peer, ttl time.Duration) *peer {
	t.Helper()
	st, _ := testutil.Store(t)
	id, err := node.Load(st.HomeDir())
	if err != nil {
		t.Fatal(err)
	}
	job := &peer{st: st, id: id, tier: node.TierService}
	pin(t, job, owner)
	d, err := owner.node.Issue(context.Background(), id.PublicKey(), []string{"page:*"}, nil, ttl)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	n, err := relay.Open(st.HomeDir(), st, id, relay.Config{URL: srv.URL, Mesh: relay.DefaultMesh})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Adopt(context.Background(), d); err != nil {
		t.Fatalf("adopt: %v", err)
	}
	if err := n.Join(context.Background()); err != nil {
		t.Fatalf("instance join: %v", err)
	}
	job.node = n
	return job
}

func TestAnOwnerDelegatedInstanceWritesAndReadsWithOnlyItsOwnKey(t *testing.T) {
	srv := relaytest.NewServer(t)
	a := newPeer(t, srv, node.TierOwner)
	join(t, srv, a)
	job := ownerDelegated(t, srv, a, time.Hour)
	if _, err := os.Stat(filepath.Join(job.st.HomeDir(), "relay", relay.DefaultMesh, "service.pem")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("service.pem on an owner-delegated instance: %v; want none", err)
	}
	if d := mustConfig(t, job).Delegation; d == nil || d.Service != a.id.ID() {
		t.Fatalf("instance delegation = %+v; want one by the owner %s", d, a.id.ID())
	}
	testutil.Post(t, job.st, store.Record{Drive: "page:1", Kind: kinds.Note, Text: "from the instance"})
	if res, err := job.node.Push(context.Background(), configs(t, job)); err != nil || res.Pushed != 1 {
		t.Fatalf("instance push = %+v, %v; want 1", res, err)
	}
	if got := pull(t, a); got.Inserted != 1 {
		t.Fatalf("owner inserted %d instance records, want 1", got.Inserted)
	}
	testutil.Post(t, a.st, store.Record{Drive: "page:1", Kind: kinds.Note, Text: secret})
	if _, err := a.node.Push(context.Background(), configs(t, a)); err != nil {
		t.Fatal(err)
	}
	if got := pull(t, job); got.Inserted != 1 {
		t.Fatalf("instance inserted %d owner records, want 1 opened with its own key", got.Inserted)
	}
}

func TestAnInstanceAdoptsItsOwnersRenewalWithoutLosingARecord(t *testing.T) {
	srv := relaytest.NewServer(t)
	a := newPeer(t, srv, node.TierOwner)
	join(t, srv, a)
	job := ownerDelegated(t, srv, a, time.Hour)
	first := mustConfig(t, job).Delegation
	renewed, err := a.node.Issue(context.Background(), job.id.PublicKey(), []string{"page:*"}, nil, 2*time.Hour)
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	testutil.Post(t, job.st, store.Record{Drive: "page:1", Kind: kinds.Note, Text: "written across the renewal"})
	if res, err := job.node.Push(context.Background(), configs(t, job)); err != nil || res.Pushed != 1 || len(res.Rejected) != 0 {
		t.Fatalf("push after the owner renewed = %+v, %v; want 1 pushed, none refused", res, err)
	}
	pull(t, job)
	held := mustConfig(t, job).Delegation
	if bytes.Equal(held.Sig, first.Sig) || !bytes.Equal(held.Sig, renewed.Sig) {
		t.Fatalf("instance holds %s after a pull, want the renewal expiring %s", held.Expires, renewed.Expires)
	}
	if _, err := a.node.Issue(context.Background(), job.id.PublicKey(), []string{"page:*"}, nil, 3*time.Hour); err != nil {
		t.Fatalf("second renewal: %v", err)
	}
	reopened, err := relay.Open(job.st.HomeDir(), job.st, job.id, mustConfig(t, job))
	if err != nil {
		t.Fatal(err)
	}
	testutil.Post(t, job.st, store.Record{Drive: "page:1", Kind: kinds.Note, Text: "under the adopted renewal"})
	if res, err := reopened.Push(context.Background(), configs(t, job)); err != nil || res.Pushed != 1 || len(res.Rejected) != 0 {
		t.Fatalf("push on the adopted renewal after a second one = %+v, %v; want 1 pushed", res, err)
	}
	if got := pull(t, a); got.Inserted != 2 {
		t.Fatalf("owner inserted %d records, want both written across the renewals", got.Inserted)
	}
}

func TestAnOwnerRevokesAnInstanceAndItsNextRotationLeavesItOut(t *testing.T) {
	srv := relaytest.NewServer(t)
	a := newPeer(t, srv, node.TierOwner)
	join(t, srv, a)
	job := ownerDelegated(t, srv, a, time.Hour)
	if err := a.node.Revoke(context.Background(), job.id.ID()); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if _, err := a.node.Rotate(context.Background()); err != nil {
		t.Fatalf("rotate after the revoke: %v", err)
	}
	if _, err := job.node.Pull(context.Background(), 0, nil); err == nil {
		t.Fatal("a revoked instance still pulls")
	}
}

func TestARotationWrapsTheNewKeyToOwnerDelegatedInstances(t *testing.T) {
	srv := relaytest.NewServer(t)
	a := newPeer(t, srv, node.TierOwner)
	join(t, srv, a)
	job := ownerDelegated(t, srv, a, time.Hour)
	if epoch, err := a.node.Rotate(context.Background()); err != nil || epoch != 2 {
		t.Fatalf("rotate = %d, %v; want 2", epoch, err)
	}
	pull(t, job)
	testutil.Post(t, a.st, store.Record{Drive: "page:1", Kind: kinds.Note, Text: secret})
	if _, err := a.node.Push(context.Background(), configs(t, a)); err != nil {
		t.Fatal(err)
	}
	if got := pull(t, job); got.Inserted != 1 {
		t.Fatalf("instance inserted %d records sealed after the rotation, want 1", got.Inserted)
	}
	testutil.Post(t, job.st, store.Record{Drive: "page:1", Kind: kinds.Note, Text: "sealed under epoch 2"})
	if res, err := job.node.Push(context.Background(), configs(t, job)); err != nil || res.Pushed != 1 {
		t.Fatalf("instance push after the rotation = %+v, %v; want 1", res, err)
	}
}

func TestAServiceDelegatedInstanceMovesToItsOwnersDelegationAndDropsTheServiceKey(t *testing.T) {
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
	st, _ := testutil.Store(t)
	id, err := node.Load(st.HomeDir())
	if err != nil {
		t.Fatal(err)
	}
	job := &peer{st: st, id: id, tier: node.TierService}
	pin(t, job, a)
	legacy, err := relay.Open(st.HomeDir(), st, id, relay.Config{URL: srv.URL, Mesh: relay.DefaultMesh, Tier: string(node.TierService)})
	if err != nil {
		t.Fatal(err)
	}
	if err := legacy.Delegate([]string{"page:*"}, nil, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := legacy.Join(context.Background()); err != nil {
		t.Fatalf("service-delegated join: %v", err)
	}
	t.Setenv(relay.ServiceKeyEnv, "")
	kept := filepath.Join(st.HomeDir(), "relay", relay.DefaultMesh, "service.pem")
	if _, err := os.Stat(kept); err != nil {
		t.Fatalf("service-delegated join kept no service key: %v", err)
	}
	d, err := a.node.Issue(context.Background(), id.PublicKey(), []string{"page:*"}, nil, time.Hour)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	moved, err := relay.Open(st.HomeDir(), st, id, relay.Config{URL: srv.URL, Mesh: relay.DefaultMesh, RelayPub: mustConfig(t, job).RelayPub})
	if err != nil {
		t.Fatal(err)
	}
	if err := moved.Adopt(context.Background(), d); err != nil {
		t.Fatalf("adopt: %v", err)
	}
	if err := moved.Join(context.Background()); err != nil {
		t.Fatalf("owner-delegated join: %v", err)
	}
	if _, err := os.Stat(kept); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("service key after the owner-delegated join: %v; want it gone", err)
	}
	reopened, err := relay.Open(st.HomeDir(), st, id, mustConfig(t, job))
	if err != nil {
		t.Fatalf("reopen with no service key: %v", err)
	}
	if reopened.Service != nil {
		t.Fatal("the reopened instance holds a service key")
	}
	testutil.Post(t, st, store.Record{Drive: "page:1", Kind: kinds.Note, Text: "after the move"})
	if res, err := reopened.Push(context.Background(), configs(t, job)); err != nil || res.Pushed != 1 {
		t.Fatalf("push after the move = %+v, %v; want 1", res, err)
	}
}

func TestAnInstanceRefusesADelegationNoPinnedOwnerSigned(t *testing.T) {
	srv := relaytest.NewServer(t)
	a := newPeer(t, srv, node.TierOwner)
	b := newPeer(t, srv, node.TierOwner)
	pin(t, a, b)
	pin(t, b, a)
	join(t, srv, a)
	join(t, srv, b)
	st, _ := testutil.Store(t)
	id, err := node.Load(st.HomeDir())
	if err != nil {
		t.Fatal(err)
	}
	pin(t, &peer{st: st}, a)
	d, err := b.node.Issue(context.Background(), id.PublicKey(), []string{"page:*"}, nil, time.Hour)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	n, err := relay.Open(st.HomeDir(), st, id, relay.Config{URL: srv.URL, Mesh: relay.DefaultMesh})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Adopt(context.Background(), d); err == nil {
		t.Fatal("adopted a delegation by an owner this node never pinned")
	}
	other, err := a.node.Issue(context.Background(), a.id.PublicKey(), []string{"page:*"}, nil, time.Hour)
	if err == nil {
		t.Fatalf("an owner delegated its own key: %+v", other)
	}
}
