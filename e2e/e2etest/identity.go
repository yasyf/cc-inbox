package e2etest

import (
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

type Identity struct {
	key *ecdsa.PrivateKey
	ecd *ecdh.PrivateKey
}

func NewIdentity(t testing.TB) *Identity {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ecd, err := key.ECDH()
	if err != nil {
		t.Fatal(err)
	}
	return &Identity{key: key, ecd: ecd}
}

func (i *Identity) ID() string {
	sum := sha256.Sum256(i.PublicKey())
	return hex.EncodeToString(sum[:8])
}

func (i *Identity) PublicKey() []byte { return i.ecd.PublicKey().Bytes() }

func (i *Identity) Sign(msg []byte) ([]byte, error) {
	d := sha256.Sum256(msg)
	return ecdsa.SignASN1(rand.Reader, i.key, d[:])
}

func (i *Identity) KeyExchanger() ecdh.KeyExchanger { return i.ecd }
