package e2e

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

var ErrOutOfScope = errors.New("e2e: envelope is outside its delegation")

type Delegation struct {
	V           int      `json:"v"`
	Mesh        string   `json:"mesh"`
	Service     string   `json:"service"`
	Instance    string   `json:"instance"`
	InstancePub []byte   `json:"instance_pub"`
	Drives      []string `json:"drives"`
	Kinds       []string `json:"kinds"`
	Expires     string   `json:"expires"`
	Sig         []byte   `json:"sig"`
}

func NodeID(pub []byte) string {
	sum := sha256.Sum256(pub)
	return hex.EncodeToString(sum[:])[:16]
}

func (d Delegation) Message() []byte {
	return []byte(joinLines("cci-mesh/delegation/v1", d.Mesh, d.Service, d.Instance, b64(d.InstancePub),
		list(d.Drives), list(d.Kinds), d.Expires))
}

func list(globs []string) string {
	b, err := json.Marshal(globs)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func validGlobs(globs []string) error {
	for _, g := range globs {
		if g == "" || g == "*" || strings.ContainsAny(strings.TrimSuffix(g, "*"), "*\n\r") {
			return fmt.Errorf("e2e: scope %q must be non-empty, not bare *, with * only as a suffix", g)
		}
	}
	return nil
}

func Delegate(service Identity, mesh string, instancePub []byte, drives, kinds []string, expires time.Time) (Delegation, error) {
	if len(drives) == 0 {
		return Delegation{}, errors.New("e2e: a delegation names at least one drive")
	}
	d := Delegation{
		V: 1, Mesh: mesh, Service: service.ID(), Instance: NodeID(instancePub), InstancePub: instancePub,
		Drives: drives, Kinds: kinds, Expires: expires.UTC().Format(time.RFC3339Nano),
	}
	if err := clean(d.Mesh, d.Expires); err != nil {
		return Delegation{}, err
	}
	if err := validGlobs(slices.Concat(drives, kinds)); err != nil {
		return Delegation{}, err
	}
	sig, err := service.Sign(d.Message())
	if err != nil {
		return Delegation{}, fmt.Errorf("sign delegation: %w", err)
	}
	d.Sig = sig
	return d, nil
}

func VerifyDelegation(servicePub []byte, d Delegation) bool {
	return d.V == 1 && verify(servicePub, d.Message(), d.Sig)
}

func (d Delegation) check(members map[string]Member, e Envelope) error {
	svc, ok := members[d.Service]
	switch {
	case !ok:
		return fmt.Errorf("%w: delegating service %s", ErrUnknownMember, d.Service)
	case svc.Tier != TierService || e.Tier != TierService:
		return fmt.Errorf("%w: delegation from %s tier %s carried tier %s", ErrTierMismatch, d.Service, svc.Tier, e.Tier)
	case !VerifyDelegation(svc.PublicKey, d):
		return fmt.Errorf("delegation of %s by %s: %w", d.Instance, d.Service, ErrBadSignature)
	}
	expires, err := time.Parse(time.RFC3339Nano, d.Expires)
	if err != nil {
		return fmt.Errorf("%w: expiry %q", ErrOutOfScope, d.Expires)
	}
	ts, err := time.Parse(time.RFC3339Nano, e.TS)
	switch {
	case err != nil || !ts.Before(expires):
		return fmt.Errorf("%w: written %s, delegation expires %s", ErrOutOfScope, e.TS, d.Expires)
	case d.Instance != NodeID(d.InstancePub) || e.Origin != d.Instance || e.Mesh != d.Mesh:
		return fmt.Errorf("%w: origin %s/%s, delegated %s/%s", ErrOutOfScope, e.Mesh, e.Origin, d.Mesh, d.Instance)
	case !Matches(d.Drives, e.Drive):
		return fmt.Errorf("%w: drive %s", ErrOutOfScope, e.Drive)
	case len(d.Kinds) > 0 && !Matches(d.Kinds, e.KindClass):
		return fmt.Errorf("%w: kind %s", ErrOutOfScope, e.KindClass)
	}
	return nil
}

func Matches(globs []string, s string) bool {
	for _, g := range globs {
		if prefix, ok := strings.CutSuffix(g, "*"); ok && strings.HasPrefix(s, prefix) || g == s {
			return true
		}
	}
	return false
}
