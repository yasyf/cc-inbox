package pairing

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/yasyf/cc-inbox/e2e"
	"github.com/yasyf/cc-inbox/internal/node"
	"github.com/yasyf/cc-inbox/internal/relay"
)

type issuerRequest struct {
	Mesh        string      `json:"mesh"`
	Expires     string      `json:"expires"`
	Nonce       []byte      `json:"nonce"`
	Attestation Attestation `json:"attestation"`
}

func IssueArgv(issuerPub []byte, expires string) []string {
	return []string{"cci", "relay", "grant", node.FingerprintOf(issuerPub), expires}
}

func GrantIssuer(ctx context.Context, home string, self node.Node, cfg relay.Config, consent Consent, life time.Duration, out io.Writer) error {
	if cfg.Tier != e2e.TierOwner {
		return fmt.Errorf("pairing: only an owner node issues delegations; this node joined mesh %s as %s", cfg.Mesh, cfg.Tier)
	}
	c := relay.Client{Config: cfg, Identity: self}
	var nr nonceReply
	if err := c.Call(ctx, http.MethodPost, "/v1/nonce", map[string]string{"mesh": cfg.Mesh}, &nr); err != nil {
		return fmt.Errorf("issuer grant nonce: %w", err)
	}
	expires := time.Now().Add(life).UTC()
	req := issuerRequest{Mesh: cfg.Mesh, Expires: expires.Format(time.RFC3339Nano), Nonce: nr.Nonce}
	var err error
	ask := Ask{Reason: "let this node issue relay delegations until " + req.Expires, Subject: "cci-issuer:" + self.Fingerprint(), Nonce: base64.StdEncoding.EncodeToString(nr.Nonce), Argv: IssueArgv(self.PublicKey(), req.Expires)}
	if req.Attestation, err = consent.Approve(ctx, ask); err != nil {
		return err
	}
	if err := c.Call(ctx, http.MethodPost, "/v1/issuer", req, nil); err != nil {
		return fmt.Errorf("issuer grant: %w", err)
	}
	if err := relay.WriteIssuerGrant(home, cfg.Mesh, expires); err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "%s issues relay delegations in mesh %s until %s\n", self.ID(), cfg.Mesh, req.Expires)
	return err
}
