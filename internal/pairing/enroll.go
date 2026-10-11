package pairing

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/yasyf/cc-inbox/e2e"
	"github.com/yasyf/cc-inbox/internal/node"
	"github.com/yasyf/cc-inbox/internal/relay"
	"github.com/yasyf/cc-inbox/internal/store"
)

const viaRelay = "relay"

type foundRequest struct {
	Mesh        string           `json:"mesh"`
	Pub         []byte           `json:"pub"`
	Name        string           `json:"name"`
	Endorsement node.Endorsement `json:"endorsement"`
	Proof       json.RawMessage  `json:"proof"`
}

type nonceReply struct {
	Nonce []byte `json:"nonce"`
}

type enrollRequest struct {
	Mesh        string            `json:"mesh"`
	Pub         []byte            `json:"pub"`
	Name        string            `json:"name"`
	Tier        node.Tier         `json:"tier"`
	Allow       *Allow            `json:"allow,omitempty"`
	Endorsement *node.Endorsement `json:"endorsement,omitempty"`
	Nonce       []byte            `json:"nonce"`
	Attestation Attestation       `json:"attestation"`
	Grant       e2e.Grant         `json:"grant"`
}

func Found(ctx context.Context, home string, st *store.Store, self node.Node, cfg relay.Config, name string, proof json.RawMessage, out io.Writer) error {
	endorsement, ok := self.Endorsement()
	if !ok {
		return errors.New("pairing: founding a mesh needs this node's Secure Enclave endorsement; run cci node endorse first")
	}
	if !json.Valid(proof) {
		return errors.New("pairing: the caller proof is not JSON; pass the output of ci cci-relay prove")
	}
	cfg.Tier = e2e.TierOwner
	c := relay.Client{Config: cfg, Identity: self}
	req := foundRequest{Mesh: cfg.Mesh, Pub: self.PublicKey(), Name: name, Endorsement: endorsement, Proof: proof}
	if err := c.Call(ctx, http.MethodPost, "/v1/found", req, nil); err != nil {
		return fmt.Errorf("found mesh %s: %w", cfg.Mesh, err)
	}
	n, err := relay.Open(home, st, self, cfg)
	if err != nil {
		return err
	}
	if err := n.Join(ctx); err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "founded mesh %s at %s as owner %s, group key epoch %d\n", cfg.Mesh, cfg.URL, self.ID(), n.Keyring.Latest(cfg.Mesh))
	return err
}

type EnrollOptions struct {
	Target      string
	Pub         []byte
	Tier        node.Tier
	Allow       Allow
	Name        string
	Endorsement *node.Endorsement
}

func EnrollArgv(pub []byte, tier node.Tier, allow *Allow) []string {
	scope := ""
	if allow != nil {
		scope = CanonicalAllow(*allow)
	}
	return []string{"cci", "mesh", "enroll", node.FingerprintOf(pub), string(tier), scope}
}

func Enroll(ctx context.Context, home string, st *store.Store, self node.Node, cfg relay.Config, consent Consent, opts EnrollOptions, out io.Writer) (store.Member, error) {
	if cfg.Tier != e2e.TierOwner {
		return store.Member{}, fmt.Errorf("pairing: only an owner node enrolls members; this node joined mesh %s as %s", cfg.Mesh, cfg.Tier)
	}
	req := enrollRequest{Mesh: cfg.Mesh, Tier: opts.Tier, Name: opts.Name}
	var member store.Member
	switch opts.Tier {
	case node.TierOwner:
		m, err := resolveMember(ctx, st, opts.Target)
		if err != nil {
			return store.Member{}, err
		}
		if m.Tier != node.TierOwner {
			return store.Member{}, fmt.Errorf("pairing: %s is pinned as %s, not owner; owners enroll only nodes this one already trusts as owners", m.Node, m.Tier)
		}
		if opts.Endorsement != nil {
			if err := node.VerifyEndorsement(*opts.Endorsement, m.PublicKey); err != nil {
				return store.Member{}, fmt.Errorf("pairing: endorsement for %s: %w", m.Node, err)
			}
		}
		member, req.Pub, req.Endorsement = m, m.PublicKey, opts.Endorsement
	case node.TierService:
		member = store.Member{Node: node.IDOf(opts.Pub), PublicKey: opts.Pub, Tier: node.TierService, Via: viaRelay, By: self.ID(), At: st.Now()}
		req.Pub, req.Allow = opts.Pub, &opts.Allow
	default:
		return store.Member{}, fmt.Errorf("pairing: enroll admits owner or service, not %q", opts.Tier)
	}
	if req.Name == "" {
		req.Name = member.Node
	}
	req.Grant = e2e.Grant{By: self.ID(), At: st.Now().UTC().Format(time.RFC3339Nano)}
	var err error
	if req.Grant.Sig, err = self.Sign(e2e.GrantMessage(cfg.Mesh, member.Node, req.Pub, string(req.Tier), req.Grant.By, req.Grant.At)); err != nil {
		return store.Member{}, err
	}
	c := relay.Client{Config: cfg, Identity: self}
	var nr nonceReply
	if err := c.Call(ctx, http.MethodPost, "/v1/nonce", map[string]string{"mesh": cfg.Mesh}, &nr); err != nil {
		return store.Member{}, fmt.Errorf("enroll nonce: %w", err)
	}
	req.Nonce = nr.Nonce
	argv := EnrollArgv(req.Pub, req.Tier, req.Allow)
	if req.Attestation, err = consent.Approve(ctx, Ask{Reason: "enroll " + member.Node + " as " + string(opts.Tier), Subject: "cci-enroll:" + node.FingerprintOf(req.Pub), Nonce: base64.StdEncoding.EncodeToString(nr.Nonce), Argv: argv}); err != nil {
		return store.Member{}, err
	}
	if err := c.Call(ctx, http.MethodPost, "/v1/enroll", req, nil); err != nil {
		return store.Member{}, fmt.Errorf("enroll %s: %w", member.Node, err)
	}
	if opts.Tier == node.TierService {
		if err := st.PutMember(ctx, member); err != nil {
			return store.Member{}, err
		}
	}
	n, err := relay.Open(home, st, self, cfg)
	if err != nil {
		return store.Member{}, err
	}
	if err := n.RefreshKeys(ctx); err != nil {
		return store.Member{}, err
	}
	if err := n.Share(ctx); err != nil {
		return store.Member{}, fmt.Errorf("share the group key with %s: %w", member.Node, err)
	}
	if _, err := fmt.Fprintf(out, "enrolled %s as %s in mesh %s\n", member.Node, opts.Tier, cfg.Mesh); err != nil {
		return store.Member{}, err
	}
	return member, nil
}
