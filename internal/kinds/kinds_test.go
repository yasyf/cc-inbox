package kinds_test

import (
	"testing"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

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
	for s, want := range map[string]kinds.Kind{"ask": kinds.Ask, "STATE": kinds.State, "Fix-Live": kinds.FixLive} {
		if k, err := kinds.Parse(s); err != nil || k != want {
			t.Errorf("Parse(%q) = %q, %v; want %q", s, k, err, want)
		}
	}
}
