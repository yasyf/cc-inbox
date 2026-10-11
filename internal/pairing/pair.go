package pairing

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"time"

	"github.com/yasyf/cc-inbox/e2e"
	"github.com/yasyf/cc-inbox/internal/node"
	"github.com/yasyf/cc-inbox/internal/relay"
	"github.com/yasyf/cc-inbox/internal/store"
)

const viaPair = "pair"

var ErrExpired = errors.New("pairing: the code expired before an owner accepted it")

type JoinOptions struct {
	URL  string
	Mesh string
	Poll time.Duration
}

func Join(ctx context.Context, home string, st *store.Store, self node.Node, opts JoinOptions, out io.Writer) (store.Member, error) {
	if opts.Poll <= 0 {
		return store.Member{}, fmt.Errorf("pairing: poll interval %s must be positive", opts.Poll)
	}
	code, err := NewCode()
	if err != nil {
		return store.Member{}, err
	}
	cfg := relay.Config{URL: opts.URL, Mesh: opts.Mesh, Tier: e2e.TierPaired}
	prior, joined, err := relay.LoadConfig(home, opts.Mesh)
	if err != nil {
		return store.Member{}, err
	}
	presented, err := fetchRelayKey(ctx, relay.Client{Config: cfg, Identity: self})
	if err != nil {
		return store.Member{}, fmt.Errorf("pin the relay key: %w", err)
	}
	cfg.RelayPub = presented
	if joined && !bytes.Equal(prior.RelayPub, presented) {
		return store.Member{}, fmt.Errorf("pairing: %s presents a different relay key than the one mesh %s pinned; refusing", opts.URL, opts.Mesh)
	}
	c := relay.Client{Config: cfg, Identity: self}
	var jr joinReply
	if err := c.Call(ctx, http.MethodPost, "/v1/pair/join", joinRequest{Mesh: cfg.Mesh, Code: code, Pub: self.PublicKey(), Fingerprint: self.Fingerprint()}, &jr); err != nil {
		return store.Member{}, fmt.Errorf("start pairing: %w", err)
	}
	if _, err := fmt.Fprintf(out, "pairing code  %s\nfingerprint   %s\nOn an owner machine run: cci pair accept %s --mesh %s\nThe code expires at %s.\n",
		FormatCode(code), FormatFingerprint(self.Fingerprint()), FormatCode(code), cfg.Mesh, jr.ExpiresAt.Local().Format(time.Kitchen)); err != nil {
		return store.Member{}, err
	}
	sr, err := awaitAccept(ctx, c, code, self.PublicKey(), opts.Poll)
	if err != nil {
		return store.Member{}, err
	}
	if !bytes.Equal(sr.RelayPub, cfg.RelayPub) {
		return store.Member{}, errors.New("pairing: the relay key changed between join and accept; refusing")
	}
	if cfg.Tier, err = verifyAcceptance(cfg.Mesh, self, sr); err != nil {
		return store.Member{}, err
	}
	owner, err := pinRoster(ctx, st, sr)
	if err != nil {
		return store.Member{}, err
	}
	if err := relay.SaveConfig(home, cfg); err != nil {
		return store.Member{}, err
	}
	n, err := relay.Open(home, st, self, cfg)
	if err != nil {
		return store.Member{}, err
	}
	n.Keyring.Add(*sr.Wrap)
	if err := n.Keyring.Save(home, cfg.Mesh); err != nil {
		return store.Member{}, err
	}
	if _, err := fmt.Fprintf(out, "paired as %s, accepted by owner %s\nowner fingerprint %s\nCompare it with `cci node` on that machine.\n",
		cfg.Tier, owner.Node, FormatFingerprint(node.FingerprintOf(owner.PublicKey))); err != nil {
		return store.Member{}, err
	}
	return owner, nil
}

func verifyAcceptance(mesh string, self node.Node, sr statusReply) (string, error) {
	if node.IDOf(sr.Owner.Pub) != sr.Owner.ID {
		return "", fmt.Errorf("pairing: owner id %s does not match its key", sr.Owner.ID)
	}
	if !node.Verify(sr.Owner.Pub, rosterStatement(mesh, sr.Roster), sr.RosterSig) {
		return "", errors.New("pairing: the owner roster is not signed by the accepting owner")
	}
	if !slices.ContainsFunc(sr.Roster, func(k keyRef) bool { return k.ID == sr.Owner.ID && bytes.Equal(k.Pub, sr.Owner.Pub) }) {
		return "", errors.New("pairing: the owner roster omits the accepting owner")
	}
	for _, k := range sr.Roster {
		if node.IDOf(k.Pub) != k.ID {
			return "", fmt.Errorf("pairing: roster id %s does not match its key", k.ID)
		}
	}
	if sr.Grant == nil || sr.Grant.By != sr.Owner.ID || (sr.Tier != e2e.TierPaired && sr.Tier != e2e.TierService) ||
		!sr.Grant.Verify(sr.Owner.Pub, mesh, self.ID(), self.PublicKey(), sr.Tier) {
		return "", fmt.Errorf("pairing: no grant from the accepting owner admits this node as %q", sr.Tier)
	}
	owners := map[string]e2e.Member{sr.Owner.ID: {Node: sr.Owner.ID, PublicKey: sr.Owner.Pub, Tier: e2e.TierOwner}}
	if _, err := e2e.UnwrapKey(*sr.Wrap, mesh, self, owners); err != nil {
		return "", fmt.Errorf("pairing: the accepting owner's group key does not unwrap: %w", err)
	}
	return sr.Tier, nil
}

func awaitAccept(ctx context.Context, c relay.Client, code string, pub []byte, poll time.Duration) (statusReply, error) {
	tick := time.NewTicker(poll)
	defer tick.Stop()
	for {
		var sr statusReply
		if err := c.Call(ctx, http.MethodGet, statusPath(c.Config.Mesh, code, pub), nil, &sr); err != nil {
			return statusReply{}, fmt.Errorf("poll pairing status: %w", err)
		}
		switch sr.Status {
		case "accepted":
			if sr.Owner == nil || sr.Wrap == nil {
				return statusReply{}, errors.New("pairing: accepted without the owner key or a group key wrap")
			}
			return sr, nil
		case "expired":
			return statusReply{}, ErrExpired
		case "pending":
		default:
			return statusReply{}, fmt.Errorf("pairing: unknown status %q", sr.Status)
		}
		select {
		case <-ctx.Done():
			return statusReply{}, ctx.Err()
		case <-tick.C:
		}
	}
}

func pinRoster(ctx context.Context, st *store.Store, sr statusReply) (store.Member, error) {
	now := st.Now()
	for _, k := range sr.Roster {
		if err := st.PutMember(ctx, store.Member{Node: k.ID, PublicKey: k.Pub, Tier: node.TierOwner, Via: viaPair, By: sr.Owner.ID, At: now}); err != nil {
			return store.Member{}, err
		}
	}
	return st.Member(ctx, sr.Owner.ID)
}

type AcceptOptions struct {
	Code  string
	Tier  node.Tier
	Allow Allow
}

func Accept(ctx context.Context, home string, st *store.Store, self node.Node, cfg relay.Config, consent Consent, opts AcceptOptions, out io.Writer) (store.Member, error) {
	if cfg.Tier != e2e.TierOwner {
		return store.Member{}, fmt.Errorf("pairing: only an owner node accepts a pairing; this node joined mesh %s as %s", cfg.Mesh, cfg.Tier)
	}
	n, err := relay.Open(home, st, self, cfg)
	if err != nil {
		return store.Member{}, err
	}
	c := relay.Client{Config: cfg, Identity: self}
	var p pending
	if err := c.Call(ctx, http.MethodGet, lookupPath(cfg.Mesh, opts.Code), nil, &p); err != nil {
		return store.Member{}, fmt.Errorf("look up pairing code %s: %w", FormatCode(opts.Code), err)
	}
	if node.FingerprintOf(p.Pub) != p.Fingerprint {
		return store.Member{}, fmt.Errorf("pairing: the relay's fingerprint %s does not match the joining key; refusing", p.Fingerprint)
	}
	id := node.IDOf(p.Pub)
	if _, err := fmt.Fprintf(out, "joining node  %s\nfingerprint   %s\nConfirm it matches the fingerprint `cci pair` printed on the joining machine; the Touch ID sheet shows it too.\n",
		id, FormatFingerprint(p.Fingerprint)); err != nil {
		return store.Member{}, err
	}
	members, err := n.Members(ctx)
	if err != nil {
		return store.Member{}, err
	}
	if _, err := n.Keyring.Current(self, cfg.Mesh, members); err != nil {
		return store.Member{}, fmt.Errorf("pairing: this owner holds no group key for mesh %s: %w", cfg.Mesh, err)
	}
	member := store.Member{Node: id, PublicKey: p.Pub, Tier: opts.Tier, Via: viaPair, By: self.ID(), At: st.Now()}
	req := acceptRequest{Mesh: cfg.Mesh, Code: opts.Code, Fingerprint: p.Fingerprint, Nonce: p.Nonce, Tier: opts.Tier, Argv: AcceptArgv(opts.Code, p.Fingerprint)}
	if opts.Tier == node.TierService {
		req.Argv = append(req.Argv, string(node.TierService), CanonicalAllow(opts.Allow), opts.Allow.Label)
		req.Allow, req.Name = &opts.Allow, opts.Allow.Label
	}
	if req.Attestation, err = consent.Approve(ctx, Ask{Reason: "pair " + id + " as " + string(opts.Tier), Subject: "cci-pair:" + p.Fingerprint, Nonce: p.Nonce, Argv: req.Argv}); err != nil {
		return store.Member{}, err
	}
	if err := n.RefreshKeys(ctx); err != nil {
		return store.Member{}, err
	}
	key, err := n.Keyring.Current(self, cfg.Mesh, members)
	if err != nil {
		return store.Member{}, err
	}
	if req.Wrap, err = e2e.WrapKey(key, cfg.Mesh, self, e2e.Member{Node: id, PublicKey: p.Pub, Tier: string(opts.Tier)}); err != nil {
		return store.Member{}, err
	}
	req.Grant = e2e.Grant{By: self.ID(), At: member.At.UTC().Format(time.RFC3339Nano)}
	if req.Grant.Sig, err = self.Sign(e2e.GrantMessage(cfg.Mesh, id, p.Pub, string(opts.Tier), req.Grant.By, req.Grant.At)); err != nil {
		return store.Member{}, err
	}
	for _, m := range members {
		if m.Tier == e2e.TierOwner {
			req.Roster = append(req.Roster, keyRef{ID: m.Node, Pub: m.PublicKey})
		}
	}
	if req.RosterSig, err = self.Sign(rosterStatement(cfg.Mesh, req.Roster)); err != nil {
		return store.Member{}, err
	}
	var a accepted
	if err := c.Call(ctx, http.MethodPost, "/v1/pair/accept", req, &a); err != nil {
		return store.Member{}, fmt.Errorf("accept pairing %s: %w", FormatCode(opts.Code), err)
	}
	if err := st.PutMember(ctx, member); err != nil {
		return store.Member{}, err
	}
	if _, err := fmt.Fprintf(out, "paired %s as %s in mesh %s\n", id, opts.Tier, cfg.Mesh); err != nil {
		return store.Member{}, err
	}
	return member, nil
}
