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

	"github.com/yasyf/cc-inbox/e2e"
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
	e2e.Key
	endorsement *Endorsement
}

// Load reads the node key from $CCI_NODE_KEY or <home>/node/key.pem,
// creating the file on first use.
func Load(home string) (Node, error) {
	dir := Dir(home)
	if env := os.Getenv(KeyEnv); env != "" {
		key, err := e2e.ParseKey([]byte(env))
		if err != nil {
			return Node{}, fmt.Errorf("%s: %w", KeyEnv, err)
		}
		return Node{Key: key}, nil
	}
	key, err := loadOrCreate(filepath.Join(dir, "key.pem"))
	if err != nil {
		return Node{}, err
	}
	n := Node{Key: key}
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

// Parse reads a node key from PKCS#8 PEM, the form $CCI_NODE_KEY and
// service secrets carry.
func Parse(raw []byte) (Node, error) {
	key, err := e2e.ParseKey(raw)
	if err != nil {
		return Node{}, err
	}
	return Node{Key: key}, nil
}

// Dir is where the node key and its endorsement live.
func Dir(home string) string {
	return filepath.Join(home, "node")
}

func loadOrCreate(path string) (e2e.Key, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // G304: path is the node key under the cci home.
	if err == nil {
		key, err := e2e.ParseKey(raw)
		if err != nil {
			return e2e.Key{}, fmt.Errorf("%s: %w", path, err)
		}
		return key, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return e2e.Key{}, fmt.Errorf("read node key: %w", err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return e2e.Key{}, fmt.Errorf("generate node key: %w", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return e2e.Key{}, fmt.Errorf("encode node key: %w", err)
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return e2e.Key{}, fmt.Errorf("create node dir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".key-*.pem")
	if err != nil {
		return e2e.Key{}, fmt.Errorf("create node key: %w", err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if err := pem.Encode(tmp, &pem.Block{Type: "PRIVATE KEY", Bytes: der}); err != nil {
		_ = tmp.Close()
		return e2e.Key{}, fmt.Errorf("write node key: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return e2e.Key{}, fmt.Errorf("write node key: %w", err)
	}
	if err := os.Link(tmp.Name(), path); errors.Is(err, os.ErrExist) {
		return loadOrCreate(path)
	} else if err != nil {
		return e2e.Key{}, fmt.Errorf("install node key: %w", err)
	}
	return e2e.NewKey(key), nil
}

// Fingerprint is the lowercase hex SHA-256 of the X9.63 public key, the same
// form authkit uses for a key_id.
func (n Node) Fingerprint() string {
	return FingerprintOf(n.PublicKey())
}

func (n Node) ECDSAPublic() *ecdsa.PublicKey {
	return &n.Private().PublicKey
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

// Agree returns the raw P-256 ECDH shared secret with peerPub; derive keys
// from it with a KDF, never use it directly.
func (n Node) Agree(peerPub []byte) ([]byte, error) {
	peer, err := ecdh.P256().NewPublicKey(peerPub)
	if err != nil {
		return nil, fmt.Errorf("peer public key: %w", err)
	}
	priv, err := n.Private().ECDH()
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
	return e2e.NodeID(pub)
}
