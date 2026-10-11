package e2e

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hpke"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	TierOwner   = "owner"
	TierService = "service"
	TierPaired  = "paired"

	wrapVersion   = 1
	recordVersion = 1
	keySize       = 32
)

var (
	ErrUnknownMember = errors.New("e2e: origin is not in the local member table")
	ErrTierMismatch  = errors.New("e2e: envelope tier differs from the member's pinned tier")
	ErrPairedWrite   = errors.New("e2e: paired members are read-only")
	ErrBadSignature  = errors.New("e2e: signature does not verify")
	ErrNoGroupKey    = errors.New("e2e: no group key for this epoch")
	ErrNotOwner      = errors.New("e2e: wrap is not from a pinned owner key")
)

type Identity interface {
	ID() string
	PublicKey() []byte
	Sign(msg []byte) ([]byte, error)
	KeyExchanger() ecdh.KeyExchanger
}

type Member struct {
	Node      string
	PublicKey []byte
	Tier      string
}

type GroupKey struct {
	Epoch uint64
	Key   []byte
}

func NewGroupKey(epoch uint64) (GroupKey, error) {
	k := make([]byte, keySize)
	if _, err := rand.Read(k); err != nil {
		return GroupKey{}, fmt.Errorf("generate group key: %w", err)
	}
	return GroupKey{Epoch: epoch, Key: k}, nil
}

type Wrap struct {
	V     int    `json:"v"`
	Mesh  string `json:"mesh"`
	Epoch uint64 `json:"epoch"`
	To    string `json:"to"`
	From  string `json:"from"`
	CT    []byte `json:"ct"`
	Sig   []byte `json:"sig"`
}

func (w Wrap) info() []byte {
	return []byte(joinLines("cci-mesh/wrap/v1", w.Mesh, u64(w.Epoch), w.To, w.From))
}

func (w Wrap) Message() []byte {
	ct := sha256.Sum256(w.CT)
	return append(append(w.info(), '\n'), hex.EncodeToString(ct[:])...)
}

func WrapKey(k GroupKey, mesh string, from Identity, to Member) (Wrap, error) {
	if err := clean(mesh, from.ID(), to.Node); err != nil {
		return Wrap{}, err
	}
	pub, err := ecdh.P256().NewPublicKey(to.PublicKey)
	if err != nil {
		return Wrap{}, fmt.Errorf("member %s key: %w", to.Node, err)
	}
	hpk, err := hpke.NewDHKEMPublicKey(pub)
	if err != nil {
		return Wrap{}, fmt.Errorf("member %s key: %w", to.Node, err)
	}
	w := Wrap{V: wrapVersion, Mesh: mesh, Epoch: k.Epoch, To: to.Node, From: from.ID()}
	if w.CT, err = hpke.Seal(hpk, hpke.HKDFSHA256(), hpke.AES256GCM(), w.info(), k.Key); err != nil {
		return Wrap{}, fmt.Errorf("seal group key for %s: %w", to.Node, err)
	}
	if w.Sig, err = from.Sign(w.Message()); err != nil {
		return Wrap{}, fmt.Errorf("sign wrap for %s: %w", to.Node, err)
	}
	return w, nil
}

func UnwrapKey(w Wrap, mesh string, id Identity, members map[string]Member) (GroupKey, error) {
	if w.V != wrapVersion || w.Mesh != mesh || w.To != id.ID() {
		return GroupKey{}, fmt.Errorf("e2e: wrap v%d for %s/%s is not addressed to %s/%s", w.V, w.Mesh, w.To, mesh, id.ID())
	}
	owner, ok := members[w.From]
	if !ok || owner.Tier != TierOwner {
		return GroupKey{}, fmt.Errorf("%w: %s", ErrNotOwner, w.From)
	}
	if !verify(owner.PublicKey, w.Message(), w.Sig) {
		return GroupKey{}, fmt.Errorf("wrap epoch %d from %s: %w", w.Epoch, w.From, ErrBadSignature)
	}
	priv, err := hpke.NewDHKEMPrivateKey(id.KeyExchanger())
	if err != nil {
		return GroupKey{}, fmt.Errorf("node key: %w", err)
	}
	key, err := hpke.Open(priv, hpke.HKDFSHA256(), hpke.AES256GCM(), w.info(), w.CT)
	if err != nil {
		return GroupKey{}, fmt.Errorf("open wrap epoch %d: %w", w.Epoch, err)
	}
	if len(key) != keySize {
		return GroupKey{}, fmt.Errorf("e2e: wrap epoch %d holds a %d-byte key", w.Epoch, len(key))
	}
	return GroupKey{Epoch: w.Epoch, Key: key}, nil
}

type Envelope struct {
	V         int    `json:"v"`
	Mesh      string `json:"mesh"`
	Drive     string `json:"drive"`
	Origin    string `json:"origin"`
	OriginSeq int64  `json:"origin_seq"`
	Tier      string `json:"tier"`
	KindClass string `json:"kind_class"`
	TS        string `json:"ts"`
	Epoch     uint64 `json:"epoch"`
	Nonce     []byte `json:"nonce"`
	Body      []byte `json:"body"`
	Sig       []byte `json:"sig"`
}

func (e Envelope) header() []byte {
	return []byte(joinLines("cci-mesh/record/v1", e.Mesh, e.Drive, e.Origin, strconv.FormatInt(e.OriginSeq, 10),
		e.Tier, e.KindClass, e.TS, u64(e.Epoch)))
}

func (e Envelope) Message() []byte {
	body := sha256.Sum256(e.Body)
	msg := append(e.header(), '\n')
	msg = append(msg, b64(e.Nonce)...)
	msg = append(msg, '\n')
	return append(msg, hex.EncodeToString(body[:])...)
}

func Seal(k GroupKey, id Identity, e Envelope, plaintext []byte) (Envelope, error) {
	if err := clean(e.Mesh, e.Drive, e.Tier, e.KindClass, e.TS); err != nil {
		return Envelope{}, err
	}
	e.V, e.Origin, e.Epoch = recordVersion, id.ID(), k.Epoch
	gcm, err := newGCM(k.Key)
	if err != nil {
		return Envelope{}, err
	}
	e.Nonce = make([]byte, gcm.NonceSize())
	if _, err := rand.Read(e.Nonce); err != nil {
		return Envelope{}, fmt.Errorf("record nonce: %w", err)
	}
	e.Body = gcm.Seal(nil, e.Nonce, plaintext, e.header())
	if e.Sig, err = id.Sign(e.Message()); err != nil {
		return Envelope{}, fmt.Errorf("sign record %s/%d: %w", e.Origin, e.OriginSeq, err)
	}
	return e, nil
}

func Open(e Envelope, keys map[uint64]GroupKey, members map[string]Member) ([]byte, error) {
	if e.V != recordVersion {
		return nil, fmt.Errorf("e2e: envelope version %d", e.V)
	}
	if err := clean(e.Mesh, e.Drive, e.Origin, e.Tier, e.KindClass, e.TS); err != nil {
		return nil, err
	}
	m, ok := members[e.Origin]
	switch {
	case !ok:
		return nil, fmt.Errorf("%w: %s", ErrUnknownMember, e.Origin)
	case m.Tier != e.Tier:
		return nil, fmt.Errorf("%w: %s pinned %s, sent %s", ErrTierMismatch, e.Origin, m.Tier, e.Tier)
	case m.Tier == TierPaired:
		return nil, fmt.Errorf("%w: %s", ErrPairedWrite, e.Origin)
	}
	if !verify(m.PublicKey, e.Message(), e.Sig) {
		return nil, fmt.Errorf("record %s/%d: %w", e.Origin, e.OriginSeq, ErrBadSignature)
	}
	k, ok := keys[e.Epoch]
	if !ok {
		return nil, fmt.Errorf("%w: %d", ErrNoGroupKey, e.Epoch)
	}
	gcm, err := newGCM(k.Key)
	if err != nil {
		return nil, err
	}
	if len(e.Nonce) != gcm.NonceSize() {
		return nil, fmt.Errorf("e2e: record %s/%d nonce is %d bytes", e.Origin, e.OriginSeq, len(e.Nonce))
	}
	pt, err := gcm.Open(nil, e.Nonce, e.Body, e.header())
	if err != nil {
		return nil, fmt.Errorf("decrypt record %s/%d epoch %d: %w", e.Origin, e.OriginSeq, e.Epoch, err)
	}
	return pt, nil
}

func verify(pub, msg, sig []byte) bool {
	key, err := ecdsa.ParseUncompressedPublicKey(elliptic.P256(), pub)
	if err != nil {
		return false
	}
	d := sha256.Sum256(msg)
	return ecdsa.VerifyASN1(key, d[:], sig)
}

func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("group key: %w", err)
	}
	return cipher.NewGCM(block)
}

func joinLines(fields ...string) string { return strings.Join(fields, "\n") }

func u64(n uint64) string { return strconv.FormatUint(n, 10) }

func b64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

func clean(fields ...string) error {
	for _, f := range fields {
		if f == "" || strings.ContainsAny(f, "\n\r") {
			return fmt.Errorf("e2e: header field %q is empty or holds a newline", f)
		}
	}
	return nil
}

func VerifyEnvelope(pub []byte, e Envelope) bool {
	return verify(pub, e.Message(), e.Sig)
}
