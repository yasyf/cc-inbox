package node_test

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/cc-inbox/internal/node"
)

func TestFileKeyRoundTrip(t *testing.T) {
	t.Setenv(node.KeyEnv, "")
	home := t.TempDir()
	first, err := node.Load(home)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	info, err := os.Stat(filepath.Join(node.Dir(home), "key.pem"))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("key file = %v, %v; want mode 0600", info, err)
	}
	again, err := node.Load(home)
	if err != nil {
		t.Fatalf("second Load() error = %v", err)
	}
	if first.ID() != again.ID() || first.Fingerprint() != again.Fingerprint() {
		t.Fatalf("reloaded node %s/%s, want %s/%s", again.ID(), again.Fingerprint(), first.ID(), first.Fingerprint())
	}
	if len(first.ID()) != 16 || first.Fingerprint()[:16] != first.ID() || len(first.PublicKey()) != 65 {
		t.Fatalf("id %q fingerprint %q pub %d bytes", first.ID(), first.Fingerprint(), len(first.PublicKey()))
	}
	msg := []byte("cci-relay/req/v1")
	sig, err := first.Sign(msg)
	if err != nil {
		t.Fatalf("Sign() error = %v", err)
	}
	if !node.Verify(again.PublicKey(), msg, sig) || node.Verify(again.PublicKey(), []byte("tampered"), sig) {
		t.Fatal("Verify() accepted a tampered message or rejected a good one")
	}
	if _, ok := first.Endorsement(); ok {
		t.Fatal("fresh node reports an endorsement")
	}
}

func TestKeyEnvOverridesFile(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(node.KeyEnv, string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})))
	home := t.TempDir()
	n, err := node.Load(home)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	want, _ := key.PublicKey.Bytes()
	if !bytes.Equal(n.PublicKey(), want) {
		t.Fatal("Load() ignored the environment key")
	}
	if _, err := os.Stat(node.Dir(home)); !os.IsNotExist(err) {
		t.Fatalf("environment key wrote %s: %v", node.Dir(home), err)
	}
}

func TestAgreeIsSymmetric(t *testing.T) {
	t.Setenv(node.KeyEnv, "")
	a, err := node.Load(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b, err := node.Load(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ab, err := a.Agree(b.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	ba, err := b.Agree(a.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ab, ba) || len(ab) != 32 {
		t.Fatalf("Agree() = %x / %x, want one 32-byte secret", ab, ba)
	}
	if !bytes.Equal(a.KeyExchanger().PublicKey().Bytes(), a.PublicKey()) {
		t.Fatal("KeyExchanger() is not the node key")
	}
}

func TestVerifyEndorsement(t *testing.T) {
	t.Setenv(node.KeyEnv, "")
	n, err := node.Load(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	enclave, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sePub, _ := enclave.PublicKey.Bytes()
	nonce := bytes.Repeat([]byte{7}, 32)
	sum := sha256.Sum256(append(append([]byte{}, nonce...), authkitSubject("cci", "node", "endorse", n.Fingerprint())...))
	sig, err := ecdsa.SignASN1(rand.Reader, enclave, sum[:])
	if err != nil {
		t.Fatal(err)
	}
	e := node.Endorsement{KeyID: node.FingerprintOf(sePub), PublicKey: base64.StdEncoding.EncodeToString(sePub), Nonce: base64.StdEncoding.EncodeToString(nonce), Sig: base64.StdEncoding.EncodeToString(sig)}
	if err := node.VerifyEndorsement(e, n.PublicKey()); err != nil {
		t.Fatalf("VerifyEndorsement() error = %v", err)
	}
	other, err := node.Load(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := node.VerifyEndorsement(e, other.PublicKey()); err == nil {
		t.Fatal("VerifyEndorsement() accepted another node's key")
	}
	e.KeyID = other.Fingerprint()
	if err := node.VerifyEndorsement(e, n.PublicKey()); err == nil {
		t.Fatal("VerifyEndorsement() accepted a key_id naming another key")
	}
}

func authkitSubject(argv ...string) []byte {
	var b bytes.Buffer
	for _, arg := range argv {
		_ = binary.Write(&b, binary.BigEndian, uint64(len(arg)))
		b.WriteString(arg)
	}
	b.WriteByte(0)
	sum := sha256.Sum256(b.Bytes())
	return sum[:]
}
