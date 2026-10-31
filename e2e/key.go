package e2e

import (
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
)

// Key is a P-256 key held in memory: the Identity a node or a stateless
// service signs, and agrees group keys, as.
type Key struct {
	priv *ecdsa.PrivateKey
}

func NewKey(priv *ecdsa.PrivateKey) Key { return Key{priv: priv} }

// ParseKey reads a P-256 key from PKCS#8 PEM, the form node key files and
// service secrets carry.
func ParseKey(raw []byte) (Key, error) {
	block, _ := pem.Decode(raw)
	if block == nil || block.Type != "PRIVATE KEY" {
		return Key{}, errors.New("not a PKCS#8 PEM private key")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return Key{}, fmt.Errorf("parse node key: %w", err)
	}
	ec, ok := key.(*ecdsa.PrivateKey)
	if !ok || ec.Curve != elliptic.P256() {
		return Key{}, errors.New("node key is not a P-256 ECDSA key")
	}
	return Key{priv: ec}, nil
}

func (k Key) Private() *ecdsa.PrivateKey { return k.priv }

// ID is the key's name in origins: the first 16 hex characters of its
// fingerprint. Trust a full key, never an ID.
func (k Key) ID() string { return NodeID(k.PublicKey()) }

// PublicKey is the uncompressed X9.63 encoding of the key's P-256 point.
func (k Key) PublicKey() []byte {
	pub, err := k.priv.PublicKey.Bytes()
	if err != nil {
		panic("e2e: encode a valid P-256 public key: " + err.Error())
	}
	return pub
}

// Sign signs SHA-256(msg) with ECDSA, ASN.1 DER encoded.
func (k Key) Sign(msg []byte) ([]byte, error) {
	sum := sha256.Sum256(msg)
	sig, err := ecdsa.SignASN1(rand.Reader, k.priv, sum[:])
	if err != nil {
		return nil, fmt.Errorf("sign: %w", err)
	}
	return sig, nil
}

// KeyExchanger is the key as an ECDH private key, for HPKE and key wraps.
func (k Key) KeyExchanger() ecdh.KeyExchanger {
	priv, err := k.priv.ECDH()
	if err != nil {
		panic("e2e: convert a valid P-256 key to ECDH: " + err.Error())
	}
	return priv
}
