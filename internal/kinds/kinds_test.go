package kinds_test

import (
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

func TestUnknownKindDiagnosticIsBounded(t *testing.T) {
	_, err := kinds.Parse("unknown")
	if err == nil {
		t.Fatal("Parse accepted an unknown kind")
	}
	message := err.Error()
	if len(message) > 180 || !strings.HasPrefix(message, `unknown kind "unknown"`) || !strings.Contains(message, "cci post --help") || !strings.Contains(message, "dotted service kind") {
		t.Errorf("unknown kind diagnostic = %q; want a bounded diagnostic with help and service kind guidance", message)
	}
	for _, name := range kinds.Names() {
		if got, err := kinds.Parse(name); err != nil || string(got) != name {
			t.Errorf("Parse(%q) = %q, %v", name, got, err)
		}
	}
}

func TestParseAcceptsDottedServiceKinds(t *testing.T) {
	for _, s := range []string{"oncall.fix.status", "oncall.control.takeover", "a.b"} {
		k, err := kinds.Parse(s)
		if err != nil || !k.Service() || k.Spec().TTL != kinds.ServiceTTL || k.Opens() {
			t.Errorf("Parse(%q) = %q, %v; want a service kind with the service TTL", s, k, err)
		}
	}
	for _, s := range []string{"oncall", "oncall.", ".fix", "Oncall.fix", "oncall..fix", "oncall.fix status"} {
		if _, err := kinds.Parse(s); err == nil {
			t.Errorf("Parse(%q) accepted a malformed kind", s)
		}
	}
	if k, err := kinds.Parse("ask"); err != nil || k.Service() {
		t.Errorf("Parse(ask) = %q, %v", k, err)
	}
}
