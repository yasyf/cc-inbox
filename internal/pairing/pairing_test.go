package pairing_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/e2e"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/node"
	"github.com/yasyf/cc-inbox/internal/pairing"
	"github.com/yasyf/cc-inbox/internal/relay"
	"github.com/yasyf/cc-inbox/internal/relay/relaytest"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

const secret = "the launch code is swordfish"

type peer struct {
	st *store.Store
	id node.Node
}

func (p *peer) home() string { return p.st.HomeDir() }

func newPeer(t *testing.T) *peer {
	t.Helper()
	st, _ := testutil.Store(t)
	id, err := node.Load(st.HomeDir())
	if err != nil {
		t.Fatal(err)
	}
	return &peer{st: st, id: id}
}

func newOwners(t *testing.T, srv *relaytest.Server, n int) []*peer {
	t.Helper()
	owners := make([]*peer, n)
	for i := range owners {
		owners[i] = newPeer(t)
		srv.AddMember(owners[i].id, e2e.TierOwner)
	}
	for _, o := range owners {
		for _, other := range owners {
			if other != o {
				put(t, o, store.Member{Node: other.id.ID(), PublicKey: other.id.PublicKey(), Tier: node.TierOwner, By: "synckit:test", At: time.Now()})
			}
		}
	}
	for _, o := range owners {
		n, err := relay.Open(o.home(), o.st, o.id, relay.Config{URL: srv.URL, Mesh: relay.DefaultMesh, Tier: e2e.TierOwner})
		if err != nil {
			t.Fatal(err)
		}
		if err := n.Join(context.Background()); err != nil {
			t.Fatalf("owner join: %v", err)
		}
	}
	return owners
}

func put(t *testing.T, p *peer, m store.Member) {
	t.Helper()
	if err := p.st.PutMember(context.Background(), m); err != nil {
		t.Fatal(err)
	}
}

func cfg(t *testing.T, p *peer) relay.Config {
	t.Helper()
	c, ok, err := relay.LoadConfig(p.home(), relay.DefaultMesh)
	if err != nil || !ok {
		t.Fatalf("LoadConfig: ok=%v err=%v", ok, err)
	}
	return c
}

func relayNode(t *testing.T, p *peer) *relay.Node {
	t.Helper()
	n, err := relay.Open(p.home(), p.st, p.id, cfg(t, p))
	if err != nil {
		t.Fatal(err)
	}
	return n
}

type consentKey struct {
	key  *ecdsa.PrivateKey
	mu   sync.Mutex
	asks []pairing.Ask
}

func newConsent(t *testing.T, srv *relaytest.Server) *consentKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pub, err := key.PublicKey.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	srv.TrustConsentKey(pub)
	return &consentKey{key: key}
}

func (c *consentKey) Approve(_ context.Context, a pairing.Ask) (pairing.Attestation, error) {
	c.mu.Lock()
	c.asks = append(c.asks, a)
	c.mu.Unlock()
	nonce, err := base64.StdEncoding.DecodeString(a.Nonce)
	if err != nil {
		return pairing.Attestation{}, err
	}
	digest := sha256.Sum256(append(nonce, relaytest.ConsentSubject(a.Argv, "")...))
	sig, err := ecdsa.SignASN1(rand.Reader, c.key, digest[:])
	if err != nil {
		return pairing.Attestation{}, err
	}
	pub, _ := c.key.PublicKey.Bytes()
	kid := sha256.Sum256(pub)
	return pairing.Attestation{KeyID: hex.EncodeToString(kid[:]), Sig: base64.StdEncoding.EncodeToString(sig), SignedBy: "yasyf@studio"}, nil
}

type denyConsent struct{}

func (denyConsent) Approve(context.Context, pairing.Ask) (pairing.Attestation, error) {
	return pairing.Attestation{}, pairing.ErrDenied
}

type lineWriter struct {
	mu   sync.Mutex
	buf  bytes.Buffer
	code chan string
}

var codeLine = regexp.MustCompile(`pairing code  ([0-9A-Z]{4}-[0-9A-Z]{4})`)

func (w *lineWriter) Write(b []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf.Write(b)
	if m := codeLine.FindSubmatch(b); m != nil {
		w.code <- string(m[1])
	}
	return len(b), nil
}

func (w *lineWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

type joining struct {
	out   *lineWriter
	code  string
	done  chan struct{}
	owner store.Member
	err   error
}

func startJoin(t *testing.T, srv *relaytest.Server, p *peer) *joining {
	t.Helper()
	j := &joining{out: &lineWriter{code: make(chan string, 1)}, done: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	go func() {
		defer close(j.done)
		j.owner, j.err = pairing.Join(ctx, p.home(), p.st, p.id, pairing.JoinOptions{URL: srv.URL, Mesh: relay.DefaultMesh, Poll: 5 * time.Millisecond}, j.out)
	}()
	select {
	case j.code = <-j.out.code:
	case <-j.done:
		t.Fatalf("join ended before printing a code: %v\n%s", j.err, j.out)
	}
	return j
}

func (j *joining) wait(t *testing.T) {
	t.Helper()
	<-j.done
}

func pair(t *testing.T, srv *relaytest.Server, owner *peer, consent pairing.Consent) *peer {
	t.Helper()
	x := newPeer(t)
	j := startJoin(t, srv, x)
	code, err := pairing.ParseCode(j.code)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pairing.Accept(context.Background(), owner.home(), owner.st, owner.id, cfg(t, owner), consent, pairing.AcceptOptions{Code: code, Tier: node.TierPaired}, io.Discard); err != nil {
		t.Fatalf("accept: %v", err)
	}
	j.wait(t)
	if j.err != nil {
		t.Fatalf("join: %v\n%s", j.err, j.out)
	}
	return x
}

func post(t *testing.T, p *peer, text string) {
	t.Helper()
	testutil.Post(t, p.st, store.Record{Kind: kinds.Note, Text: text})
	n := relayNode(t, p)
	cs, err := relay.Configs(p.home())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := n.Push(context.Background(), cs); err != nil {
		t.Fatalf("push: %v", err)
	}
}

func pull(t *testing.T, p *peer) {
	t.Helper()
	if _, err := relayNode(t, p).Pull(context.Background(), 0, pairing.Handler(p.home(), p.st, p.id)); err != nil {
		t.Fatalf("pull: %v", err)
	}
}

func texts(t *testing.T, p *peer) []string {
	t.Helper()
	recs, err := p.st.Query(context.Background(), store.Filter{Drive: "d"})
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, r := range recs {
		out = append(out, r.Text)
	}
	return out
}

func pinned(t *testing.T, p *peer, id string) (store.Member, bool) {
	t.Helper()
	m, err := p.st.Member(context.Background(), id)
	if err != nil {
		return store.Member{}, false
	}
	return m, true
}

func peerIDs(t *testing.T, p *peer, directory bool) []string {
	t.Helper()
	cs, err := relay.Configs(p.home())
	if err != nil {
		t.Fatal(err)
	}
	ps, err := pairing.Peers(context.Background(), p.st, p.id, cs, directory)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, x := range ps {
		out = append(out, x.Node)
	}
	return out
}

func TestPairAdmitsAReaderThatDecryptsOwnerRecords(t *testing.T) {
	srv := relaytest.NewServer(t)
	owners := newOwners(t, srv, 2)
	a, b := owners[0], owners[1]
	consent := newConsent(t, srv)
	x := pair(t, srv, a, consent)

	m, ok := pinned(t, a, x.id.ID())
	if !ok || m.Tier != node.TierPaired || m.By != "pair:"+a.id.ID() {
		t.Fatalf("owner pinned the reader as %+v (ok=%v); want tier paired by pair:<owner>", m, ok)
	}
	for _, o := range owners {
		if m, ok := pinned(t, x, o.id.ID()); !ok || m.Tier != node.TierOwner {
			t.Fatalf("reader pinned owner %s as %+v (ok=%v); want owner from the signed roster", o.id.ID(), m, ok)
		}
	}
	if got := cfg(t, x); got.Tier != e2e.TierPaired || !bytes.Equal(got.RelayPub, srv.Pub()) {
		t.Fatalf("reader config = %+v; want tier paired with the relay key pinned", got)
	}
	if len(consent.asks) != 1 || len(consent.asks[0].Argv) != 5 || !slices.Equal(consent.asks[0].Argv[:3], []string{"cci", "pair", "accept"}) || consent.asks[0].Argv[4] != x.id.Fingerprint() {
		t.Fatalf("Touch ID argv = %+v; want cci pair accept CODE <reader fingerprint>", consent.asks)
	}

	pull(t, b)
	if m, ok := pinned(t, b, x.id.ID()); !ok || m.Tier != node.TierPaired || m.By != "pair:"+a.id.ID() {
		t.Fatalf("second owner pinned the reader as %+v (ok=%v); want paired, granted by the accepting owner", m, ok)
	}

	post(t, a, secret)
	pull(t, x)
	if got := texts(t, x); !slices.Equal(got, []string{secret}) {
		t.Fatalf("reader tail = %q; want the owner's record", got)
	}
	for _, body := range srv.Pushes {
		if bytes.Contains(body, []byte(secret)) {
			t.Fatal("the relay saw the record in plaintext")
		}
	}
}

func TestUnpairRotatesSoTheReaderReadsNothingNew(t *testing.T) {
	srv := relaytest.NewServer(t)
	a := newOwners(t, srv, 1)[0]
	x := pair(t, srv, a, newConsent(t, srv))
	before := srv.Epoch()

	var out bytes.Buffer
	if _, err := pairing.Unpair(context.Background(), a.home(), a.st, a.id, cfg(t, a), x.id.ID()[:6], &out); err != nil {
		t.Fatalf("unpair: %v", err)
	}
	if _, ok := pinned(t, a, x.id.ID()); ok {
		t.Fatal("unpair left the reader pinned on the owner")
	}
	if srv.Epoch() != before+1 {
		t.Fatalf("epoch = %d after unpair; want %d", srv.Epoch(), before+1)
	}
	if slices.Contains(peerIDs(t, a, true), x.id.ID()) {
		t.Fatal("cci peers --mesh still lists the unpaired reader")
	}

	post(t, a, secret)
	var push struct{ Envelopes []e2e.Envelope }
	if err := json.Unmarshal(srv.Pushes[len(srv.Pushes)-1], &push); err != nil {
		t.Fatal(err)
	}
	last := push.Envelopes[0]
	xn := relayNode(t, x)
	members, err := xn.Members(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	keys, err := xn.Keyring.Keys(relay.DefaultMesh, x.id, members)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := relay.OpenRow(last, keys, members); !errors.Is(err, e2e.ErrNoGroupKey) {
		t.Fatalf("the unpaired reader opened a post-rotation record: err = %v", err)
	}
}

func TestPairedWriteUnpairsTheReaderOnEveryOwner(t *testing.T) {
	srv := relaytest.NewServer(t)
	owners := newOwners(t, srv, 2)
	a, b := owners[0], owners[1]
	x := pair(t, srv, a, newConsent(t, srv))
	pull(t, b)
	before := srv.Epoch()

	testutil.Post(t, x.st, store.Record{Kind: kinds.Note, Text: "a reader writing"})
	cs, err := relay.Configs(x.home())
	if err != nil {
		t.Fatal(err)
	}
	rogue := relayNode(t, x)
	rogue.Config.Tier = e2e.TierOwner
	if _, err := rogue.Push(context.Background(), cs); !relay.IsUnpaired(err) {
		t.Fatalf("paired push err = %v; want the relay's unpaired rejection", err)
	}
	for _, o := range owners {
		pull(t, o)
		if _, ok := pinned(t, o, x.id.ID()); ok {
			t.Fatalf("owner %s still pins the violating reader", o.id.ID())
		}
		if slices.Contains(peerIDs(t, o, true), x.id.ID()) {
			t.Fatalf("cci peers --mesh on owner %s still lists the violating reader", o.id.ID())
		}
	}
	if srv.Epoch() != before+1 {
		t.Fatalf("epoch = %d; want exactly one rotation to %d whichever owner won", srv.Epoch(), before+1)
	}
}

func TestUnsignedUnpairIsRefused(t *testing.T) {
	srv := relaytest.NewServer(t)
	a := newOwners(t, srv, 1)[0]
	x := pair(t, srv, a, newConsent(t, srv))
	srv.ForgeControl(e2e.Control{Type: e2e.ControlUnpaired, Mesh: relay.DefaultMesh, Node: x.id.ID(), Reason: "violation", By: "relay"})
	pull(t, a)
	if _, ok := pinned(t, a, x.id.ID()); !ok {
		t.Fatal("a peer.unpaired not signed by the pinned relay key dropped the reader")
	}
}

func TestJoinedWithoutAnOwnerGrantIsRefused(t *testing.T) {
	srv := relaytest.NewServer(t)
	a := newOwners(t, srv, 1)[0]
	intruder := newPeer(t)
	srv.Control(e2e.Control{Type: e2e.ControlJoined, Mesh: relay.DefaultMesh, Node: intruder.id.ID(), Pub: intruder.id.PublicKey(), Tier: e2e.TierPaired, By: "relay"})
	forged, err := intruder.id.Sign(e2e.GrantMessage(relay.DefaultMesh, intruder.id.ID(), intruder.id.PublicKey(), e2e.TierPaired, a.id.ID(), "now"))
	if err != nil {
		t.Fatal(err)
	}
	srv.Control(e2e.Control{Type: e2e.ControlJoined, Mesh: relay.DefaultMesh, Node: intruder.id.ID(), Pub: intruder.id.PublicKey(), Tier: e2e.TierPaired, By: a.id.ID(),
		Grant: &e2e.Grant{By: a.id.ID(), At: "now", Sig: forged}})
	pull(t, a)
	if _, ok := pinned(t, a, intruder.id.ID()); ok {
		t.Fatal("a relay-injected peer.joined without a valid owner grant pinned a key")
	}
}

func TestDeniedConsentLeavesTheCodePending(t *testing.T) {
	srv := relaytest.NewServer(t)
	a := newOwners(t, srv, 1)[0]
	x := newPeer(t)
	j := startJoin(t, srv, x)
	code, _ := pairing.ParseCode(j.code)
	_, err := pairing.Accept(context.Background(), a.home(), a.st, a.id, cfg(t, a), denyConsent{}, pairing.AcceptOptions{Code: code, Tier: node.TierPaired}, io.Discard)
	if !errors.Is(err, pairing.ErrDenied) {
		t.Fatalf("accept err = %v; want ErrDenied", err)
	}
	if _, ok := pinned(t, a, x.id.ID()); ok {
		t.Fatal("a denied accept pinned the joining node")
	}
	srv.ExpirePairing(code)
	j.wait(t)
	if !errors.Is(j.err, pairing.ErrExpired) {
		t.Fatalf("join err = %v; want ErrExpired", j.err)
	}
}

func TestServiceTierBindsItsScopeIntoTheAttestation(t *testing.T) {
	srv := relaytest.NewServer(t)
	a := newOwners(t, srv, 1)[0]
	consent := newConsent(t, srv)
	x := newPeer(t)
	j := startJoin(t, srv, x)
	code, _ := pairing.ParseCode(j.code)
	allow, err := pairing.ParseAllow("page:*|oncall.say", "ops@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pairing.Accept(context.Background(), a.home(), a.st, a.id, cfg(t, a), consent, pairing.AcceptOptions{Code: code, Tier: node.TierService, Allow: allow}, io.Discard); err != nil {
		t.Fatalf("accept: %v", err)
	}
	j.wait(t)
	if j.err != nil {
		t.Fatalf("join: %v", j.err)
	}
	want := append(pairing.AcceptArgv(code, x.id.Fingerprint()), "service", "ops@example.com|page:*|oncall.say", "ops@example.com")
	if !slices.Equal(consent.asks[0].Argv, want) {
		t.Fatalf("service argv = %q; want %q", consent.asks[0].Argv, want)
	}
	if m, ok := pinned(t, a, x.id.ID()); !ok || m.Tier != node.TierService {
		t.Fatalf("owner pinned the service writer as %+v (ok=%v)", m, ok)
	}
}

func TestReplayedGrantDoesNotReadmitAnUnpairedReader(t *testing.T) {
	srv := relaytest.NewServer(t)
	owners := newOwners(t, srv, 2)
	a, b := owners[0], owners[1]
	x := pair(t, srv, a, newConsent(t, srv))
	pull(t, b)
	old := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339Nano)
	sig, err := a.id.Sign(e2e.GrantMessage(relay.DefaultMesh, x.id.ID(), x.id.PublicKey(), e2e.TierPaired, a.id.ID(), old))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pairing.Unpair(context.Background(), a.home(), a.st, a.id, cfg(t, a), x.id.ID(), io.Discard); err != nil {
		t.Fatalf("unpair: %v", err)
	}
	pull(t, b)
	srv.Control(e2e.Control{Type: e2e.ControlJoined, Mesh: relay.DefaultMesh, Node: x.id.ID(), Pub: x.id.PublicKey(), Tier: e2e.TierPaired, By: a.id.ID(),
		Grant: &e2e.Grant{By: a.id.ID(), At: old, Sig: sig}})
	pull(t, b)
	if _, ok := pinned(t, b, x.id.ID()); ok {
		t.Fatal("a grant signed before the unpair readmitted the reader")
	}
}
