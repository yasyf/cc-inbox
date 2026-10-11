package pairing

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
)

var (
	ErrDenied      = errors.New("pairing: the owner denied the Touch ID prompt")
	ErrUnavailable = errors.New("pairing: no attended owner host could show the Touch ID prompt")
)

type Ask struct {
	Reason  string
	Subject string
	Nonce   string
	Argv    []string
}

type Attestation struct {
	KeyID         string `json:"key_id"`
	Sig           string `json:"sig"`
	SignedBy      string `json:"signed_by"`
	RequestedFrom string `json:"requested_from"`
}

type Consent interface {
	Approve(ctx context.Context, a Ask) (Attestation, error)
}

type Synckitd struct {
	Path string
}

func (s Synckitd) Approve(ctx context.Context, a Ask) (Attestation, error) {
	args := []string{"consent", "request", "--client", "cci", "--reason", a.Reason, "--subject", a.Subject, "--nonce", a.Nonce}
	for _, v := range a.Argv {
		args = append(args, "--argv", v)
	}
	var reply struct {
		Verdict     string       `json:"verdict"`
		Routed      bool         `json:"routed"`
		Attestation *Attestation `json:"attestation"`
	}
	if err := s.run(ctx, &reply, args...); err != nil {
		return Attestation{}, err
	}
	switch reply.Verdict {
	case "approved":
	case "denied":
		return Attestation{}, ErrDenied
	case "unavailable":
		return Attestation{}, ErrUnavailable
	default:
		return Attestation{}, fmt.Errorf("synckitd consent request: unknown verdict %q", reply.Verdict)
	}
	if reply.Attestation == nil {
		return Attestation{}, errors.New("synckitd consent request: approved without an attestation")
	}
	att := *reply.Attestation
	if reply.Routed {
		var hosts struct {
			Self string `json:"self"`
		}
		if err := s.run(ctx, &hosts, "host", "ls", "--json"); err != nil {
			return Attestation{}, err
		}
		att.RequestedFrom = hosts.Self
	}
	return att, nil
}

func (s Synckitd) run(ctx context.Context, out any, args ...string) error {
	path := s.Path
	if path == "" {
		path = "synckitd"
	}
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Stderr = &stderr
	raw, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("synckitd %s %s: %w: %s", args[0], args[1], err, bytes.TrimSpace(stderr.Bytes()))
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("synckitd %s %s: parse %q: %w", args[0], args[1], bytes.TrimSpace(raw), err)
	}
	return nil
}

func AcceptArgv(code, fingerprint string) []string {
	return []string{"cci", "pair", "accept", code, fingerprint}
}
