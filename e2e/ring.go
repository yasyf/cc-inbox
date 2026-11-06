package e2e

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

const ringVersion = 1

// RingMesh is the mesh name a ring key's wraps carry, so a ring wrap can
// never be taken for a group key wrap of the mesh itself.
func RingMesh(mesh string) string { return mesh + "#ring" }

// SealedEpoch is one epoch's group key sealed under a ring key, which an owner
// wraps once for each instance it delegates.
type SealedEpoch struct {
	V     int    `json:"v"`
	Mesh  string `json:"mesh"`
	Ring  uint64 `json:"ring"`
	Epoch uint64 `json:"epoch"`
	From  string `json:"from"`
	Nonce []byte `json:"nonce"`
	CT    []byte `json:"ct"`
	Sig   []byte `json:"sig"`
}

func (s SealedEpoch) header() []byte {
	return []byte(joinLines("cci-mesh/ring-epoch/v1", s.Mesh, u64(s.Ring), u64(s.Epoch), s.From))
}

func (s SealedEpoch) Message() []byte {
	ct := sha256.Sum256(s.CT)
	return []byte(joinLines(string(s.header()), b64(s.Nonce), hex.EncodeToString(ct[:])))
}

func SealEpoch(ring GroupKey, mesh string, k GroupKey, from Identity) (SealedEpoch, error) {
	if err := clean(mesh, from.ID()); err != nil {
		return SealedEpoch{}, err
	}
	s := SealedEpoch{V: ringVersion, Mesh: mesh, Ring: ring.Epoch, Epoch: k.Epoch, From: from.ID()}
	gcm, err := newGCM(ring.Key)
	if err != nil {
		return SealedEpoch{}, err
	}
	s.Nonce = make([]byte, gcm.NonceSize())
	if _, err := rand.Read(s.Nonce); err != nil {
		return SealedEpoch{}, fmt.Errorf("ring nonce: %w", err)
	}
	s.CT = gcm.Seal(nil, s.Nonce, k.Key, s.header())
	if s.Sig, err = from.Sign(s.Message()); err != nil {
		return SealedEpoch{}, fmt.Errorf("sign sealed epoch %d: %w", k.Epoch, err)
	}
	return s, nil
}

func OpenEpoch(ring GroupKey, mesh string, s SealedEpoch, members map[string]Member) (GroupKey, error) {
	if s.V != ringVersion || s.Mesh != mesh || s.Ring != ring.Epoch {
		return GroupKey{}, fmt.Errorf("e2e: sealed epoch v%d of %s ring %d is not for %s ring %d", s.V, s.Mesh, s.Ring, mesh, ring.Epoch)
	}
	owner, ok := members[s.From]
	if !ok || owner.Tier != TierOwner {
		return GroupKey{}, fmt.Errorf("%w: %s", ErrNotOwner, s.From)
	}
	if !verify(owner.PublicKey, s.Message(), s.Sig) {
		return GroupKey{}, fmt.Errorf("sealed epoch %d from %s: %w", s.Epoch, s.From, ErrBadSignature)
	}
	gcm, err := newGCM(ring.Key)
	if err != nil {
		return GroupKey{}, err
	}
	key, err := gcm.Open(nil, s.Nonce, s.CT, s.header())
	if err != nil {
		return GroupKey{}, fmt.Errorf("open sealed epoch %d: %w", s.Epoch, err)
	}
	if len(key) != keySize {
		return GroupKey{}, fmt.Errorf("e2e: sealed epoch %d holds a %d-byte key", s.Epoch, len(key))
	}
	return GroupKey{Epoch: s.Epoch, Key: key}, nil
}
