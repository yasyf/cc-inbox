package pairing

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func fakeSynckitd(t *testing.T, consentReply string) (Synckitd, string) {
	t.Helper()
	dir := t.TempDir()
	argsFile := filepath.Join(dir, "args")
	script := "#!/bin/sh\nif [ \"$1\" = host ]; then echo '{\"self\":\"yasyf@studio\",\"hosts\":[]}'; exit 0; fi\n" +
		"printf '%s\\n' \"$@\" > " + argsFile + "\necho '" + consentReply + "'\n"
	bin := filepath.Join(dir, "synckitd")
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return Synckitd{Path: bin}, argsFile
}

func TestSynckitdApproveSendsTheArgvAndNonce(t *testing.T) {
	s, argsFile := fakeSynckitd(t, `{"verdict":"approved","approved_by":"yasyf@studio","routed":false,"attestation":{"key_id":"kid","sig":"c2ln","signed_by":"yasyf@studio"}}`)
	argv := AcceptArgv("ABCDEFGH", "ff00")
	att, err := s.Approve(context.Background(), Ask{Reason: "pair x", Subject: "cci-pair:ff00", Nonce: "bm9uY2U=", Argv: argv})
	if err != nil {
		t.Fatal(err)
	}
	if att.KeyID != "kid" || att.Sig != "c2ln" || att.RequestedFrom != "" {
		t.Fatalf("local approval = %+v; want key kid, sig c2ln, empty requested_from", att)
	}
	raw, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Split(strings.TrimSpace(string(raw)), "\n")
	want := []string{
		"consent", "request", "--client", "cci", "--reason", "pair x", "--subject", "cci-pair:ff00", "--nonce", "bm9uY2U=",
		"--argv", "cci", "--argv", "pair", "--argv", "accept", "--argv", "ABCDEFGH", "--argv", "ff00",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("synckitd argv = %q\nwant %q", got, want)
	}
}

func TestSynckitdRoutedApprovalCarriesTheRequestingHost(t *testing.T) {
	s, _ := fakeSynckitd(t, `{"verdict":"approved","approved_by":"yasyf@forge","routed":true,"attestation":{"key_id":"kid","sig":"c2ln","signed_by":"yasyf@forge"}}`)
	att, err := s.Approve(context.Background(), Ask{Nonce: "bm9uY2U=", Argv: []string{"cci"}})
	if err != nil {
		t.Fatal(err)
	}
	if att.RequestedFrom != "yasyf@studio" || att.SignedBy != "yasyf@forge" {
		t.Fatalf("routed approval = %+v; want requested_from yasyf@studio, signed_by yasyf@forge", att)
	}
}

func TestSynckitdVerdicts(t *testing.T) {
	for reply, want := range map[string]error{
		`{"verdict":"denied"}`:      ErrDenied,
		`{"verdict":"unavailable"}`: ErrUnavailable,
	} {
		s, _ := fakeSynckitd(t, reply)
		if _, err := s.Approve(context.Background(), Ask{Argv: []string{"cci"}}); !errors.Is(err, want) {
			t.Fatalf("%s: err = %v; want %v", reply, err, want)
		}
	}
	s, _ := fakeSynckitd(t, `{"verdict":"approved"}`)
	if _, err := s.Approve(context.Background(), Ask{Argv: []string{"cci"}}); err == nil {
		t.Fatal("an approval without an attestation was accepted")
	}
}
