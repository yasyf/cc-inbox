package node

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/yasyf/synckit/authkit"
)

const endorsementFile = "endorsement.json"

// Endorsement is a Secure Enclave key's signature, made through authkit
// consent-sign under Touch ID, over the argv `cci node endorse <fingerprint>`.
// It binds the node's file key to the owner's hardware key once, so the
// daemon can sign unattended with a key a verifier can trace to the Enclave.
type Endorsement struct {
	KeyID     string `json:"key_id"`
	PublicKey string `json:"public_key"`
	Nonce     string `json:"nonce"`
	Sig       string `json:"sig"`
}

func (n Node) Endorsement() (Endorsement, bool) {
	if n.endorsement == nil {
		return Endorsement{}, false
	}
	return *n.endorsement, true
}

func endorseArgv(fingerprint string) []string {
	return []string{"cci", "node", "endorse", fingerprint}
}

// Endorse asks the authkit helper to sign this node's fingerprint with the
// Secure Enclave key, one Touch ID tap, and saves the endorsement under home.
func (n Node) Endorse(ctx context.Context, home string) (Endorsement, error) {
	var bridge authkit.Bridge
	gen, err := bridge.Run(ctx, nil, nil, "keygen")
	if err != nil {
		return Endorsement{}, fmt.Errorf("authkit keygen: %w", err)
	}
	if gen.Code != authkit.CodeApproved {
		return Endorsement{}, fmt.Errorf("authkit keygen exited %d: %s", gen.Code, bytes.TrimSpace(gen.Stderr))
	}
	var key struct {
		KeyID     string `json:"key_id"`
		PublicKey string `json:"public_key"`
	}
	if err := json.Unmarshal(gen.Stdout, &key); err != nil {
		return Endorsement{}, fmt.Errorf("decode authkit keygen: %w", err)
	}
	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		return Endorsement{}, fmt.Errorf("endorsement nonce: %w", err)
	}
	req, err := json.Marshal(map[string]any{"nonce": base64.StdEncoding.EncodeToString(nonce), "argv": endorseArgv(n.Fingerprint())})
	if err != nil {
		return Endorsement{}, fmt.Errorf("encode consent-sign request: %w", err)
	}
	signed, err := bridge.Run(ctx, req, nil, "consent-sign")
	if err != nil {
		return Endorsement{}, fmt.Errorf("authkit consent-sign: %w", err)
	}
	if signed.Code != authkit.CodeApproved {
		return Endorsement{}, fmt.Errorf("authkit consent-sign exited %d: %s", signed.Code, bytes.TrimSpace(signed.Stderr))
	}
	var sig struct {
		KeyID string `json:"key_id"`
		Sig   string `json:"sig"`
	}
	if err := json.Unmarshal(signed.Stdout, &sig); err != nil {
		return Endorsement{}, fmt.Errorf("decode authkit consent-sign: %w", err)
	}
	e := Endorsement{KeyID: sig.KeyID, PublicKey: key.PublicKey, Nonce: base64.StdEncoding.EncodeToString(nonce), Sig: sig.Sig}
	if err := VerifyEndorsement(e, n.PublicKey()); err != nil {
		return Endorsement{}, err
	}
	raw, err := json.MarshalIndent(e, "", "  ")
	if err != nil {
		return Endorsement{}, fmt.Errorf("encode endorsement: %w", err)
	}
	if err := os.WriteFile(filepath.Join(Dir(home), endorsementFile), append(raw, '\n'), 0o600); err != nil {
		return Endorsement{}, fmt.Errorf("write endorsement: %w", err)
	}
	return e, nil
}

// VerifyEndorsement checks that e is an authkit consent-sign signature by the
// key it names over `cci node endorse <fingerprint of nodePub>`.
func VerifyEndorsement(e Endorsement, nodePub []byte) error {
	sePub, err := base64.StdEncoding.DecodeString(e.PublicKey)
	if err != nil {
		return fmt.Errorf("endorsement public key: %w", err)
	}
	if FingerprintOf(sePub) != e.KeyID {
		return errors.New("endorsement key_id does not match its public key")
	}
	nonce, err := base64.StdEncoding.DecodeString(e.Nonce)
	if err != nil {
		return fmt.Errorf("endorsement nonce: %w", err)
	}
	sig, err := base64.StdEncoding.DecodeString(e.Sig)
	if err != nil {
		return fmt.Errorf("endorsement signature: %w", err)
	}
	if !Verify(sePub, append(nonce, subject(endorseArgv(FingerprintOf(nodePub)))...), sig) {
		return errors.New("endorsement signature does not verify for this node key")
	}
	return nil
}

func subject(argv []string) []byte {
	var b bytes.Buffer
	for _, arg := range argv {
		_ = binary.Write(&b, binary.BigEndian, uint64(len(arg)))
		b.WriteString(arg)
	}
	b.WriteByte(0)
	sum := sha256.Sum256(b.Bytes())
	return sum[:]
}
