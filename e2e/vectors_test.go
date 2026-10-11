package e2e_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/yasyf/cc-inbox/e2e"
)

type signed struct {
	Message string `json:"message"`
	Sig     []byte `json:"sig_b64"`
}

type vectors struct {
	Key struct {
		Pub []byte `json:"pub_b64"`
		ID  string `json:"id"`
	} `json:"key"`
	Envelope struct {
		Envelope e2e.Envelope `json:"envelope"`
		Signed   signed       `json:"signed"`
	} `json:"envelope"`
	Control struct {
		Control e2e.Control `json:"control"`
		Signed  signed      `json:"signed"`
	} `json:"control"`
	Request struct {
		Method string `json:"method"`
		Target string `json:"target"`
		At     string `json:"at"`
		Nonce  string `json:"nonce"`
		Body   string `json:"body"`
		Signed signed `json:"signed"`
	} `json:"request"`
	Delegations map[string]struct {
		Delegation e2e.Delegation `json:"delegation"`
		Signed     signed         `json:"signed"`
	} `json:"-"`
}

func TestRelayVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/relay-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var v vectors
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &v.Delegations); err != nil {
		t.Fatal(err)
	}
	if got := e2e.NodeID(v.Key.Pub); got != v.Key.ID {
		t.Fatalf("NodeID = %s, relay says %s", got, v.Key.ID)
	}
	check := func(name string, msg []byte, s signed) {
		t.Helper()
		if string(msg) != s.Message {
			t.Errorf("%s message differs from the relay's:\n e2e:   %q\n relay: %q", name, msg, s.Message)
		}
	}
	e := v.Envelope.Envelope
	check("envelope", e.Message(), v.Envelope.Signed)
	e.Sig = v.Envelope.Signed.Sig
	if !e2e.VerifyEnvelope(v.Key.Pub, e) {
		t.Error("relay envelope signature does not verify")
	}
	c := v.Control.Control
	check("control", c.Message(), v.Control.Signed)
	c.Sig = v.Control.Signed.Sig
	if !c.Verify(v.Key.Pub) {
		t.Error("relay control signature does not verify")
	}
	r := v.Request
	if !e2e.VerifyRequest(v.Key.Pub, r.Method, r.Target, r.At, r.Nonce, []byte(r.Body), r.Signed.Sig) {
		t.Error("relay request signature does not verify")
	}
	for _, name := range []string{"delegation_kinds_nil", "delegation_kinds_empty", "delegation_kinds_set"} {
		d, ok := v.Delegations[name]
		if !ok {
			t.Fatalf("vector %s missing", name)
		}
		check(name, d.Delegation.Message(), d.Signed)
		if !e2e.VerifyDelegation(v.Key.Pub, withSig(d.Delegation, d.Signed.Sig)) {
			t.Errorf("%s signature does not verify", name)
		}
	}
}

func withSig(d e2e.Delegation, sig []byte) e2e.Delegation {
	d.Sig = sig
	return d
}

