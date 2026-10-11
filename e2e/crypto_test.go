package e2e_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/e2e"
	"github.com/yasyf/cc-inbox/e2e/e2etest"
)

func member(id e2e.Identity, tier string) e2e.Member {
	return e2e.Member{Node: id.ID(), PublicKey: id.PublicKey(), Tier: tier}
}

func members(ms ...e2e.Member) map[string]e2e.Member {
	out := map[string]e2e.Member{}
	for _, m := range ms {
		out[m.Node] = m
	}
	return out
}

func TestWrapRoundTrip(t *testing.T) {
	owner, reader := e2etest.NewIdentity(t), e2etest.NewIdentity(t)
	k, err := e2e.NewGroupKey(1)
	if err != nil {
		t.Fatal(err)
	}
	w, err := e2e.WrapKey(k, "default", owner, member(reader, e2e.TierPaired))
	if err != nil {
		t.Fatal(err)
	}
	got, err := e2e.UnwrapKey(w, "default", reader, members(member(owner, e2e.TierOwner)))
	if err != nil {
		t.Fatal(err)
	}
	if got.Epoch != 1 || !bytes.Equal(got.Key, k.Key) {
		t.Fatalf("unwrapped %d/%x, want 1/%x", got.Epoch, got.Key, k.Key)
	}
}

func TestUnwrapRefusesUnpinnedSigner(t *testing.T) {
	owner, relayKey, reader := e2etest.NewIdentity(t), e2etest.NewIdentity(t), e2etest.NewIdentity(t)
	k, _ := e2e.NewGroupKey(2)
	forged, err := e2e.WrapKey(k, "default", relayKey, member(reader, e2e.TierPaired))
	if err != nil {
		t.Fatal(err)
	}
	pinned := members(member(owner, e2e.TierOwner), member(relayKey, e2e.TierService))
	if _, err := e2e.UnwrapKey(forged, "default", reader, pinned); !errors.Is(err, e2e.ErrNotOwner) {
		t.Fatalf("forged wrap: err = %v, want ErrNotOwner", err)
	}
	forged.From = owner.ID()
	if _, err := e2e.UnwrapKey(forged, "default", reader, pinned); !errors.Is(err, e2e.ErrBadSignature) {
		t.Fatalf("relabelled wrap: err = %v, want ErrBadSignature", err)
	}
}

func TestUnwrapByOtherNodeFails(t *testing.T) {
	owner, reader, other := e2etest.NewIdentity(t), e2etest.NewIdentity(t), e2etest.NewIdentity(t)
	k, _ := e2e.NewGroupKey(1)
	w, _ := e2e.WrapKey(k, "default", owner, member(reader, e2e.TierPaired))
	w.To = other.ID()
	if _, err := e2e.UnwrapKey(w, "default", other, members(member(owner, e2e.TierOwner))); err == nil {
		t.Fatal("a wrap for another node opened")
	}
}

func seal(t *testing.T, k e2e.GroupKey, id e2e.Identity, tier string, pt string) e2e.Envelope {
	t.Helper()
	e, err := e2e.Seal(k, id, e2e.Envelope{
		Mesh: "default", Drive: "d", OriginSeq: 7, Tier: tier, KindClass: "decision", TS: time.Unix(0, 0).UTC().Format(time.RFC3339Nano),
	}, []byte(pt))
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestSealOpen(t *testing.T) {
	a := e2etest.NewIdentity(t)
	k, _ := e2e.NewGroupKey(3)
	e := seal(t, k, a, e2e.TierOwner, "the secret text")
	if bytes.Contains(e.Body, []byte("secret")) {
		t.Fatal("ciphertext holds plaintext")
	}
	pt, err := e2e.Open(e, map[uint64]e2e.GroupKey{3: k}, members(member(a, e2e.TierOwner)))
	if err != nil {
		t.Fatal(err)
	}
	if string(pt) != "the secret text" {
		t.Fatalf("opened %q", pt)
	}
}

func TestOpenRefusals(t *testing.T) {
	a, b := e2etest.NewIdentity(t), e2etest.NewIdentity(t)
	k, _ := e2e.NewGroupKey(1)
	wrong, _ := e2e.NewGroupKey(1)
	keys := map[uint64]e2e.GroupKey{1: k}
	pinned := members(member(a, e2e.TierOwner), member(b, e2e.TierPaired))
	cases := []struct {
		name string
		env  func() e2e.Envelope
		keys map[uint64]e2e.GroupKey
		want error
	}{
		{"unknown origin", func() e2e.Envelope { return seal(t, k, e2etest.NewIdentity(t), e2e.TierOwner, "x") }, keys, e2e.ErrUnknownMember},
		{"tier escalation", func() e2e.Envelope { return seal(t, k, b, e2e.TierOwner, "x") }, keys, e2e.ErrTierMismatch},
		{"paired write", func() e2e.Envelope { return seal(t, k, b, e2e.TierPaired, "x") }, keys, e2e.ErrPairedWrite},
		{"header swap", func() e2e.Envelope { e := seal(t, k, a, e2e.TierOwner, "x"); e.Drive = "other"; return e }, keys, e2e.ErrBadSignature},
		{"missing epoch", func() e2e.Envelope { return seal(t, k, a, e2e.TierOwner, "x") }, map[uint64]e2e.GroupKey{}, e2e.ErrNoGroupKey},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := e2e.Open(c.env(), c.keys, pinned); !errors.Is(err, c.want) {
				t.Fatalf("err = %v, want %v", err, c.want)
			}
		})
	}
	if _, err := e2e.Open(seal(t, k, a, e2e.TierOwner, "x"), map[uint64]e2e.GroupKey{1: wrong}, pinned); err == nil {
		t.Fatal("wrong group key decrypted")
	}
}
