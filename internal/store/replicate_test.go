package store_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/node"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func TestApplyBuffersAReplyUntilItsTargetArrives(t *testing.T) {
	ctx := context.Background()
	a, _ := testutil.Store(t)
	b, _ := testutil.Store(t)
	c, _ := testutil.Store(t)
	ask := testutil.Post(t, a, store.Record{Lane: "root", Kind: kinds.Ask, Text: "who owns the relay?"})
	asks, err := a.Export(ctx, a.Self(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Apply(ctx, asks, nil); err != nil {
		t.Fatal(err)
	}
	local, err := b.Query(ctx, store.Filter{Drive: "d", Origin: a.Self()})
	if err != nil || len(local) != 1 {
		t.Fatalf("b holds %+v, %v", local, err)
	}
	testutil.Post(t, b, store.Record{Lane: "p3", Kind: kinds.Answer, Text: "p3 does", Re: local[0].Seq, Resolves: local[0].Seq})
	answers, err := b.Export(ctx, b.Self(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(answers) != 1 || *answers[0].Re != (store.Pointer{Origin: a.Self(), Seq: ask.Seq}) {
		t.Fatalf("b exports %+v; want a pointer at a's #%d", answers, ask.Seq)
	}
	got, err := c.Apply(ctx, answers, nil)
	if err != nil || got != (store.Applied{Buffered: 1}) {
		t.Fatalf("Apply(answer before ask) = %+v, %v", got, err)
	}
	if rows, err := c.Query(ctx, store.Filter{Drive: "d"}); err != nil || len(rows) != 0 {
		t.Fatalf("c shows %+v, %v before the ask arrives", rows, err)
	}
	if again, err := c.Apply(ctx, answers, nil); err != nil || again != (store.Applied{Duplicate: 1}) {
		t.Fatalf("re-Apply(buffered) = %+v, %v", again, err)
	}
	receipt := store.Receipt{Channel: "mesh", Origin: a.Self(), ChangeID: "c1", Revision: "1", Digest: "abc"}
	got, err = c.Apply(ctx, asks, &receipt)
	if err != nil || got != (store.Applied{Inserted: 1, Released: 1}) {
		t.Fatalf("Apply(ask) = %+v, %v", got, err)
	}
	rows, err := c.Query(ctx, store.Filter{Drive: "d"})
	if err != nil || len(rows) != 2 || rows[1].Re != rows[0].Seq || rows[1].Resolves != rows[0].Seq || rows[1].Origin != b.Self() {
		t.Fatalf("c after release = %+v, %v", rows, err)
	}
	if held, ok, err := c.Receipt(ctx, "mesh", a.Self()); err != nil || !ok || held != receipt {
		t.Fatalf("Receipt() = %+v %v %v", held, ok, err)
	}
	if _, ok, err := c.Receipt(ctx, "relay", a.Self()); err != nil || ok {
		t.Fatalf("relay receipt = %v %v, want none", ok, err)
	}
	if echo, err := a.Apply(ctx, asks, nil); err != nil || echo != (store.Applied{Duplicate: 1}) {
		t.Fatalf("Apply(own rows) = %+v, %v", echo, err)
	}
}

func TestCompactionHoldsUnackedRows(t *testing.T) {
	ctx := context.Background()
	st, clock := testutil.Store(t)
	first := testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "acked by both"})
	testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "acked by forge only"})
	if err := st.Ack(ctx, "forge", first.Seq+1); err != nil {
		t.Fatal(err)
	}
	if err := st.Ack(ctx, "relay", first.Seq); err != nil {
		t.Fatal(err)
	}
	if err := st.Ack(ctx, "forge", first.Seq); err != nil {
		t.Fatal(err)
	}
	if marks, err := st.Watermarks(ctx); err != nil || marks["forge"] != first.Seq+1 || marks["relay"] != first.Seq {
		t.Fatalf("Watermarks() = %v, %v; an ack must never lower one", marks, err)
	}
	clock.Advance(72 * time.Hour)
	res, err := st.Compact(ctx, "d", 48*time.Hour)
	if err != nil || res != (store.CompactResult{Folded: 1, Digests: 1}) {
		t.Fatalf("Compact() = %+v, %v; want only the row every peer acked folded", res, err)
	}
	if err := st.ForgetPeer(ctx, "relay"); err != nil {
		t.Fatal(err)
	}
	if res, err = st.Compact(ctx, "d", 48*time.Hour); err != nil || res.Folded != 1 {
		t.Fatalf("Compact() after forgetting relay = %+v, %v", res, err)
	}
}

func TestPresenceReplicatesSessions(t *testing.T) {
	ctx := context.Background()
	a, _ := testutil.Store(t)
	b, _ := testutil.Store(t)
	if err := a.Bind(ctx, store.PeerKey{Session: "sess-a"}, "d", false); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Announce(ctx, store.Peer{Session: "sess-a", Cwd: "/src", PID: 42, Env: map[string]string{"K": "v"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Claim(ctx, store.Claim{Session: "sess-a", Rel: "opened", Kind: "pr", Key: "1"}); err != nil {
		t.Fatal(err)
	}
	paired, err := a.ExportPresence(ctx, node.TierPaired)
	if err != nil || len(paired.Peers) != 1 || paired.Peers[0].PID != 0 || paired.Peers[0].Cwd != "" || paired.Peers[0].Env != nil {
		t.Fatalf("paired presence = %+v, %v", paired, err)
	}
	owner, err := a.ExportPresence(ctx, node.TierOwner)
	if err != nil || owner.Peers[0].PID != 42 || len(owner.Claims) != 1 {
		t.Fatalf("owner presence = %+v, %v", owner, err)
	}
	if err := b.ApplyPresence(ctx, owner); err != nil {
		t.Fatal(err)
	}
	p, err := b.Peer(ctx, store.PeerKey{Session: "sess-a"})
	if err != nil || p.Origin != a.Self() || p.Drive != "d" {
		t.Fatalf("b sees %+v, %v", p, err)
	}
	if _, ok, err := b.Binding(ctx, store.PeerKey{Session: "sess-a"}); err != nil || ok {
		t.Fatalf("a remote session binds on b: %v %v", ok, err)
	}
	if claims, err := b.Claims(ctx, store.ClaimFilter{Kind: "pr", Key: "1"}); err != nil || len(claims) != 1 {
		t.Fatalf("b claims = %+v, %v", claims, err)
	}
	if err := b.ApplyPresence(ctx, owner); err != nil {
		t.Fatal(err)
	}
	if err := b.ApplyPresence(ctx, store.Presence{Origin: b.Self()}); err == nil {
		t.Fatal("ApplyPresence() accepted this node's own origin")
	}
	if err := b.ForgetPeer(ctx, a.Self()); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Peer(ctx, store.PeerKey{Session: "sess-a"}); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("forgotten peer's session = %v", err)
	}
}

func TestMembers(t *testing.T) {
	ctx := context.Background()
	st, _ := testutil.Store(t)
	peer, _ := testutil.Store(t)
	pub := peer.Node().PublicKey()
	if err := st.PutMember(ctx, store.Member{Node: "0123456789abcdef", PublicKey: pub, Tier: node.TierOwner, Via: "mesh"}); err == nil {
		t.Fatal("PutMember() accepted an id that is not the key's")
	}
	if err := st.PutMember(ctx, store.Member{Node: peer.Self(), PublicKey: pub, Tier: "admin", Via: "pair"}); err == nil {
		t.Fatal("PutMember() accepted an unknown tier")
	}
	if err := st.PutMember(ctx, store.Member{Node: peer.Self(), PublicKey: pub, Tier: node.TierPaired, Via: "carrier-pigeon"}); err == nil {
		t.Fatal("PutMember() accepted an unknown via")
	}
	if err := st.PutMember(ctx, store.Member{Node: peer.Self(), PublicKey: pub, Tier: node.TierOwner, Via: "mesh", Host: "yasyf@forge", By: "owner"}); err != nil {
		t.Fatal(err)
	}
	m, err := st.Member(ctx, peer.Self())
	if err != nil || m.Tier != node.TierOwner || m.Via != "mesh" || m.Host != "yasyf@forge" || string(m.PublicKey) != string(pub) {
		t.Fatalf("Member() = %+v, %v", m, err)
	}
	if dropped, err := st.DropMember(ctx, peer.Self()); err != nil || !dropped {
		t.Fatalf("DropMember() = %v, %v", dropped, err)
	}
	if _, err := st.Member(ctx, peer.Self()); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("dropped member = %v", err)
	}
}

func TestPeerAddressOnAnotherNode(t *testing.T) {
	ctx := context.Background()
	st, _ := testutil.Store(t)
	testutil.Post(t, st, store.Record{Drive: "other", Lane: "root", Kind: kinds.Go, Text: "remote go", To: []string{store.PeerAddressOn(st.Self(), "sess-api")}})
	testutil.Post(t, st, store.Record{Drive: "other", Lane: "root", Kind: kinds.Go, Text: "elsewhere", To: []string{store.PeerAddressOn("ffffffffffffffff", "sess-api")}})
	got, err := st.Query(ctx, store.Filter{Drive: "d", Peer: "sess-api", To: "api"})
	if err != nil || len(got) != 1 || got[0].Text != "remote go" {
		t.Fatalf("Query() = %+v, %v", got, err)
	}
}

func TestServiceRecordsTravelWithBodyAttrsAndID(t *testing.T) {
	ctx := context.Background()
	platy, _ := testutil.Store(t)
	mac, clock := testutil.Store(t)
	r := store.Record{
		Drive: "page:1", Lane: "platy", Kind: "oncall.fix.status", Text: "fix deployed", ID: "env-1",
		Fields: store.Fields{Attrs: map[string]string{"actor": "platy", "epoch": "3"}}, Body: []byte(`{"status":"live"}`),
	}
	first := testutil.Post(t, platy, r)
	if first.ExpiresAt == nil || first.ExpiresAt.Sub(first.At) != kinds.ServiceTTL {
		t.Fatalf("service record expires %v, want after %v", first.ExpiresAt, kinds.ServiceTTL)
	}
	r.Text = "a retried envelope"
	again, dup, err := platy.Post(ctx, r, 0)
	if err != nil || !dup || again.Seq != first.Seq {
		t.Fatalf("Post(same id) = #%d dup=%v %v; want #%d as a duplicate", again.Seq, dup, err, first.Seq)
	}
	if _, _, err := platy.Post(ctx, store.Record{Drive: "d", Lane: "x", Kind: kinds.Note, Text: "big", Body: []byte(`"` + strings.Repeat("x", store.MaxBody) + `"`)}, 0); err == nil {
		t.Fatal("Post() accepted a body over the limit")
	}
	rows, err := platy.Export(ctx, platy.Self(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := mac.Apply(ctx, rows, nil); err != nil || got.Inserted != 1 {
		t.Fatalf("Apply() = %+v, %v", got, err)
	}
	clock.Advance(time.Hour)
	held, err := mac.Query(ctx, store.Filter{Drive: "page:1"})
	if err != nil || len(held) != 1 || held[0].ID != "env-1" || string(held[0].Body) != `{"status":"live"}` || held[0].Fields.Attrs["epoch"] != "3" {
		t.Fatalf("mac holds %+v, %v", held, err)
	}
	other := "ffffffffffffffff"
	replay := store.Row{Origin: other, OriginSeq: 9, ID: "env-1", Drive: "page:1", Lane: "platy", Kind: "oncall.fix.status", Text: "replay", At: clock.Now()}
	reply := store.Row{Origin: other, OriginSeq: 10, Drive: "page:1", Lane: "ic", Kind: kinds.Note, Text: "ack", At: clock.Now(), Re: &store.Pointer{Origin: other, Seq: 9}}
	if got, err := mac.Apply(ctx, []store.Row{reply, replay}, nil); err != nil || got != (store.Applied{Duplicate: 1, Buffered: 1, Released: 1}) {
		t.Fatalf("Apply(reply, same id from another origin) = %+v, %v", got, err)
	}
	acked, err := mac.Query(ctx, store.Filter{Drive: "page:1", Lanes: []string{"ic"}})
	if err != nil || len(acked) != 1 || acked[0].Re != held[0].Seq {
		t.Fatalf("reply to the aliased row = %+v, %v; want re #%d", acked, err, held[0].Seq)
	}
}

func TestCompactedThroughAndSchemaFingerprint(t *testing.T) {
	ctx := context.Background()
	st, clock := testutil.Store(t)
	if through, err := st.CompactedThrough(ctx); err != nil || through != 0 {
		t.Fatalf("fresh CompactedThrough() = %d, %v", through, err)
	}
	old := testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "old"})
	clock.Advance(72 * time.Hour)
	if _, err := st.Compact(ctx, "d", 48*time.Hour); err != nil {
		t.Fatal(err)
	}
	if through, err := st.CompactedThrough(ctx); err != nil || through != old.Seq {
		t.Fatalf("CompactedThrough() = %d, %v; want #%d", through, err, old.Seq)
	}
	other, _ := testutil.Store(t)
	if fp := st.SchemaFingerprint(); len(fp) != 64 || fp != other.SchemaFingerprint() {
		t.Fatalf("SchemaFingerprint() = %q / %q", fp, other.SchemaFingerprint())
	}
}

func TestSplitPeer(t *testing.T) {
	for _, tt := range []struct{ in, node, rest string }{
		{"0123456789abcdef/3785cb11-d400", "0123456789abcdef", "3785cb11-d400"},
		{"3785cb11-d400-486b", "", "3785cb11-d400-486b"},
		{"3785cb11/agent-1", "", "3785cb11/agent-1"},
		{"0123456789ABCDEF/s", "", "0123456789ABCDEF/s"},
	} {
		if node, rest := store.SplitPeer(tt.in); node != tt.node || rest != tt.rest {
			t.Errorf("SplitPeer(%q) = %q, %q; want %q, %q", tt.in, node, rest, tt.node, tt.rest)
		}
	}
}

func TestCompactionKeepsTheResolverOfAHeldRow(t *testing.T) {
	ctx := context.Background()
	a, clock := testutil.Store(t)
	b, _ := testutil.Store(t)
	if err := a.Ack(ctx, "forge", 0); err != nil {
		t.Fatal(err)
	}
	ask := testutil.Post(t, a, store.Record{Lane: "root", Kind: kinds.Ask, Text: "ship it?"})
	rows, err := a.Export(ctx, a.Self(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Apply(ctx, rows, nil); err != nil {
		t.Fatal(err)
	}
	asked, err := b.Query(ctx, store.Filter{Drive: "d"})
	if err != nil || len(asked) != 1 {
		t.Fatalf("b holds %+v, %v", asked, err)
	}
	testutil.Post(t, b, store.Record{Lane: "api", Kind: kinds.Answer, Text: "yes", Resolves: asked[0].Seq})
	answers, err := b.Export(ctx, b.Self(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Apply(ctx, answers, nil); err != nil {
		t.Fatal(err)
	}
	clock.Advance(72 * time.Hour)
	if _, err := a.Compact(ctx, "d", 48*time.Hour); err != nil {
		t.Fatal(err)
	}
	if open, err := a.OpenItems(ctx, "d"); err != nil || len(open) != 0 {
		t.Fatalf("after compaction a reopens %+v (ask #%d), %v", open, ask.Seq, err)
	}
}

func TestLocalSessionDisplacesAReplicatedRow(t *testing.T) {
	ctx := context.Background()
	a, _ := testutil.Store(t)
	b, _ := testutil.Store(t)
	if err := b.ApplyPresence(ctx, store.Presence{Origin: a.Self(), Peers: []store.Peer{{Session: "sess-x", Drive: "remote", Root: true}}}); err != nil {
		t.Fatal(err)
	}
	if err := b.BindLane(ctx, store.PeerKey{Session: "sess-x"}, "local", "api", time.Time{}); err != nil {
		t.Fatal(err)
	}
	p, err := b.Peer(ctx, store.PeerKey{Session: "sess-x"})
	if err != nil || p.Origin != "" || p.Drive != "local" || p.Root {
		t.Fatalf("local bind over a replicated row = %+v, %v", p, err)
	}
}

func TestApplyRefusesAFoldedRow(t *testing.T) {
	ctx := context.Background()
	a, _ := testutil.Store(t)
	b, clock := testutil.Store(t)
	testutil.Post(t, a, store.Record{Kind: kinds.Note, Text: "old news"})
	rows, err := a.Export(ctx, a.Self(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Apply(ctx, rows, nil); err != nil {
		t.Fatal(err)
	}
	clock.Advance(72 * time.Hour)
	if res, err := b.Compact(ctx, "d", 48*time.Hour); err != nil || res.Folded != 1 {
		t.Fatalf("Compact() = %+v, %v", res, err)
	}
	if got, err := b.Apply(ctx, rows, nil); err != nil || got != (store.Applied{Duplicate: 1}) {
		t.Fatalf("replayed folded row = %+v, %v", got, err)
	}
}
