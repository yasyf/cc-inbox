package pairing_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/e2e"
	"github.com/yasyf/cc-inbox/internal/node"
	"github.com/yasyf/cc-inbox/internal/pairing"
	"github.com/yasyf/cc-inbox/internal/relay"
	"github.com/yasyf/cc-inbox/internal/relay/relaytest"
	"github.com/yasyf/cc-inbox/internal/store"
)

var proof = json.RawMessage(`{"url":"https://sts.us-west-2.amazonaws.com/?Action=GetCallerIdentity","method":"GET"}`)

func endorsedFounder(t *testing.T) (*peer, *consentKey) {
	t.Helper()
	p := newPeer(t)
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sePub, err := key.PublicKey.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	nonce := bytes.Repeat([]byte{7}, 32)
	digest := sha256.Sum256(append(slices.Clone(nonce), relaytest.ConsentSubject([]string{"cci", "node", "endorse", p.id.Fingerprint()}, "")...))
	sig, err := ecdsa.SignASN1(rand.Reader, key, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(node.Endorsement{
		KeyID: node.FingerprintOf(sePub), PublicKey: base64.StdEncoding.EncodeToString(sePub),
		Nonce: base64.StdEncoding.EncodeToString(nonce), Sig: base64.StdEncoding.EncodeToString(sig),
	})
	if err := os.WriteFile(filepath.Join(node.Dir(p.home()), "endorsement.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if p.id, err = node.Load(p.home()); err != nil {
		t.Fatal(err)
	}
	return p, &consentKey{key: key}
}

func found(t *testing.T, srv *relaytest.Server, p *peer) {
	t.Helper()
	if err := pairing.Found(context.Background(), p.home(), p.st, p.id, relay.Config{URL: srv.URL, Mesh: relay.DefaultMesh}, "studio", proof, &bytes.Buffer{}); err != nil {
		t.Fatalf("Found: %v", err)
	}
}

func TestFoundRefusesAnUnendorsedNode(t *testing.T) {
	srv := relaytest.NewServer(t)
	p := newPeer(t)
	err := pairing.Found(context.Background(), p.home(), p.st, p.id, relay.Config{URL: srv.URL, Mesh: relay.DefaultMesh}, "studio", proof, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "cci node endorse") {
		t.Fatalf("Found(unendorsed) = %v", err)
	}
	if len(srv.Proofs) != 0 {
		t.Fatalf("relay saw %d proofs from an unendorsed node", len(srv.Proofs))
	}
}

func TestFoundJoinsAsOwnerAndEnrollsAServiceThatReadsTheGroupKey(t *testing.T) {
	srv := relaytest.NewServer(t)
	founder, consent := endorsedFounder(t)
	found(t, srv, founder)
	c := cfg(t, founder)
	if c.Tier != e2e.TierOwner || srv.Epoch() != 1 || len(srv.Proofs) != 1 {
		t.Fatalf("after found: tier %q, epoch %d, proofs %d", c.Tier, srv.Epoch(), len(srv.Proofs))
	}

	svc := newPeer(t)
	allow, err := pairing.ParseAllow("page:*|oncall.say", "platy")
	if err != nil {
		t.Fatal(err)
	}
	opts := pairing.EnrollOptions{Pub: svc.id.PublicKey(), Tier: node.TierService, Allow: allow, Name: "platy"}
	if _, err := pairing.Enroll(context.Background(), founder.home(), founder.st, founder.id, c, consent, opts, &bytes.Buffer{}); err != nil {
		t.Fatalf("Enroll: %v", err)
	}
	want := []string{"cci", "mesh", "enroll", svc.id.Fingerprint(), "service", "platy|page:*|oncall.say"}
	if len(consent.asks) != 1 || !slices.Equal(consent.asks[0].Argv, want) {
		t.Fatalf("Touch ID asks = %+v, want argv %q", consent.asks, want)
	}
	pinned, err := founder.st.Members(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(pinned, func(m store.Member) bool { return m.Node == svc.id.ID() && m.Tier == node.TierService }) {
		t.Fatalf("founder pins %+v, want the service", pinned)
	}

	put(t, svc, store.Member{Node: founder.id.ID(), PublicKey: founder.id.PublicKey(), Tier: node.TierOwner, Via: "relay", By: "test", At: time.Now()})
	n, err := relay.Open(svc.home(), svc.st, svc.id, relay.Config{URL: srv.URL, Mesh: relay.DefaultMesh, Tier: e2e.TierService})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Join(context.Background()); err != nil {
		t.Fatalf("service join: %v", err)
	}
	if got := n.Keyring.Latest(relay.DefaultMesh); got != 1 {
		t.Fatalf("service holds group key epoch %d, want 1", got)
	}
}

func TestEnrollOwnerAdmitsOnlyAPinnedOwner(t *testing.T) {
	srv := relaytest.NewServer(t)
	founder, consent := endorsedFounder(t)
	found(t, srv, founder)
	stranger := newPeer(t)
	put(t, founder, store.Member{Node: stranger.id.ID(), PublicKey: stranger.id.PublicKey(), Tier: node.TierPaired, Via: "pair", By: "test", At: time.Now()})
	opts := pairing.EnrollOptions{Target: stranger.id.ID(), Tier: node.TierOwner}
	if _, err := pairing.Enroll(context.Background(), founder.home(), founder.st, founder.id, cfg(t, founder), consent, opts, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "not owner") {
		t.Fatalf("Enroll(paired as owner) = %v", err)
	}
	if len(consent.asks) != 0 {
		t.Fatalf("Touch ID asked %d times for a refused enrollment", len(consent.asks))
	}
}
