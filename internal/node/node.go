// Package node is this machine's identity in a cci mesh: a P-256 key whose
// fingerprint names the node, signs its requests, and agrees group keys.
package node

import (
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// KeyEnv carries a PKCS#8 PEM node key for hosts with no persistent home,
// such as CI agents holding a service-tier key in their secret environment.
const KeyEnv = "CCI_NODE_KEY"

// Tier is the trust a node holds in a mesh.
type Tier string

const (
	TierOwner   Tier = "owner"
	TierService Tier = "service"
	TierPaired  Tier = "paired"
)

// Node is one machine's key, plus the Secure Enclave endorsement of it when
// the owner has run `cci node endorse`.
type Node struct {
	key         *ecdsa.PrivateKey
	endorsement *Endorsement
}

// Load reads the node key from $CCI_NODE_KEY or <home>/node/key.pem,
// creating the file on first use.
func Load(home string) (Node, error) {
	dir := Dir(home)
	if env := os.Getenv(KeyEnv); env != "" {
		key, err := parse([]byte(env))
		if err != nil {
			return Node{}, fmt.Errorf("%s: %w", KeyEnv, err)
		}
		return Node{key: key}, nil
	}
	key, err := loadOrCreate(filepath.Join(dir, "key.pem"))
	if err != nil {
		return Node{}, err
	}
	n := Node{key: key}
	raw, err := os.ReadFile(filepath.Join(dir, endorsementFile)) //nolint:gosec // G304: dir is the node directory under the cci home.
	switch {
	case errors.Is(err, os.ErrNotExist):
		return n, nil
	case err != nil:
		return Node{}, fmt.Errorf("read node endorsement: %w", err)
	}
	var e Endorsement
	if err := json.Unmarshal(raw, &e); err != nil {
		return Node{}, fmt.Errorf("decode node endorsement: %w", err)
	}
	if err := VerifyEndorsement(e, n.PublicKey()); err != nil {
		return Node{}, fmt.Errorf("node endorsement: %w", err)
	}
	n.endorsement = &e
	return n, nil
}

// Dir is where the node key and its endorsement live.
func Dir(home string) string {
	return filepath.Join(home, "node")
}

func loadOrCreate(path string) (*ecdsa.PrivateKey, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // G304: path is the node key under the cci home.
	if err == nil {
		key, err := parse(raw)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		return key, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read node key: %w", err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate node key: %w", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("encode node key: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create node dir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) //nolint:gosec // G304: path is the node key under the cci home.
	if errors.Is(err, os.ErrExist) {
		return loadOrCreate(path)
	}
	if err != nil {
		return nil, fmt.Errorf("create node key: %w", err)
	}
	if err := pem.Encode(f, &pem.Block{Type: "PRIVATE KEY", Bytes: der}); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("write node key: %w", err)
	}
	if err := f.Close(); err != nil {
		return nil, fmt.Errorf("write node key: %w", err)
	}
	return key, nil
}

func parse(raw []byte) (*ecdsa.PrivateKey, error) {
	block, _ := pem.Decode(raw)
	if block == nil || block.Type != "PRIVATE KEY" {
		return nil, errors.New("not a PKCS#8 PEM private key")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse node key: %w", err)
	}
	ec, ok := key.(*ecdsa.PrivateKey)
	if !ok || ec.Curve != elliptic.P256() {
		return nil, errors.New("node key is not a P-256 ECDSA key")
	}
	return ec, nil
}

// ID is the node's name in origins and peer:<node>/<session> addresses: the
// first 16 hex characters of its fingerprint. Trust a full key, never an ID.
func (n Node) ID() string {
	return IDOf(n.PublicKey())
}

// Fingerprint is the lowercase hex SHA-256 of the X9.63 public key, the same
// form authkit uses for a key_id.
func (n Node) Fingerprint() string {
	return FingerprintOf(n.PublicKey())
}

// PublicKey is the uncompressed X9.63 encoding of the node's P-256 point.
func (n Node) PublicKey() []byte {
	pub, err := n.key.PublicKey.Bytes()
	if err != nil {
		panic("node: encode a valid P-256 public key: " + err.Error())
	}
	return pub
}

func (n Node) ECDSAPublic() *ecdsa.PublicKey {
	return &n.key.PublicKey
}

// Sign signs SHA-256(msg) with ECDSA, ASN.1 DER encoded.
func (n Node) Sign(msg []byte) ([]byte, error) {
	sum := sha256.Sum256(msg)
	sig, err := ecdsa.SignASN1(rand.Reader, n.key, sum[:])
	if err != nil {
		return nil, fmt.Errorf("sign: %w", err)
	}
	return sig, nil
}

// Verify reports whether sig is an ECDSA P-256 signature over SHA-256(msg) by
// the X9.63 public key pub.
func Verify(pub, msg, sig []byte) bool {
	key, err := ecdsa.ParseUncompressedPublicKey(elliptic.P256(), pub)
	if err != nil {
		return false
	}
	sum := sha256.Sum256(msg)
	return ecdsa.VerifyASN1(key, sum[:], sig)
}

// KeyExchanger is the node key as an ECDH private key, for HPKE and key wraps.
func (n Node) KeyExchanger() ecdh.KeyExchanger {
	priv, err := n.key.ECDH()
	if err != nil {
		panic("node: convert a valid P-256 key to ECDH: " + err.Error())
	}
	return priv
}

// Agree returns the raw P-256 ECDH shared secret with peerPub; derive keys
// from it with a KDF, never use it directly.
func (n Node) Agree(peerPub []byte) ([]byte, error) {
	peer, err := ecdh.P256().NewPublicKey(peerPub)
	if err != nil {
		return nil, fmt.Errorf("peer public key: %w", err)
	}
	priv, err := n.key.ECDH()
	if err != nil {
		return nil, fmt.Errorf("node ECDH key: %w", err)
	}
	secret, err := priv.ECDH(peer)
	if err != nil {
		return nil, fmt.Errorf("agree: %w", err)
	}
	return secret, nil
}

func FingerprintOf(pub []byte) string {
	sum := sha256.Sum256(pub)
	return hex.EncodeToString(sum[:])
}

func IDOf(pub []byte) string {
	return FingerprintOf(pub)[:16]
}
