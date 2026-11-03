package cli_test

import (
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/peers"
)

func TestAPostToALaneNoLiveLaneReadsTellsThePoster(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())
	for _, name := range peers.IdentityEnv {
		t.Setenv(name, "")
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "aaaa1111-root")
	t.Setenv("CCI_AGENT_ID", "")
	if _, err := run(t, "drive", "use", "d", "--root"); err != nil {
		t.Fatal(err)
	}
	for _, lane := range []string{"sv-2", "sv-3"} {
		if _, err := run(t, "post", "--lane", lane, "--kind", "state", "--text", lane+" up"); err != nil {
			t.Fatal(err)
		}
	}
	out, err := run(t, "post", "--lane", "root", "--kind", "note", "--to", "sv", "--text", "pool word")
	if err != nil || !strings.HasPrefix(out, "cci: --to sv: sv-3, sv-2 run under sv at once; this reaches sv-3\n") {
		t.Fatalf("post to a stem two running lanes share = %q, %v; want the pool named", out, err)
	}
	if _, err := run(t, "post", "--lane", "root", "--kind", "stand-down", "--to", "sv-2", "--text", "sv-2 stands down"); err != nil {
		t.Fatal(err)
	}
	out, err = run(t, "post", "--lane", "root", "--kind", "note", "--to", "sv-2", "--text", "late word")
	if err != nil || !strings.HasPrefix(out, "cci: --to sv-2: sv-2 stood down, so this reaches sv-3; --to sv reaches whichever lane holds it\n") {
		t.Fatalf("post to a retired sv-2 = %q, %v; want the hint naming sv", out, err)
	}
	out, err = run(t, "tail", "--drive", "d", "--since", "0", "--reader", "sv-3")
	if err != nil || !strings.Contains(out, "NOTE root -> sv-2 late word") || !strings.Contains(out, "NOTE root -> sv pool word") {
		t.Fatalf("tail --reader sv-3 = %q, %v; want the stem's and retired sv-2's mail", out, err)
	}
	if _, err := run(t, "post", "--lane", "root", "--kind", "stand-down", "--to", "sv-3", "--text", "sv-3 stands down"); err != nil {
		t.Fatal(err)
	}
	out, err = run(t, "post", "--lane", "root", "--kind", "note", "--to", "sv", "--text", "anyone?")
	if err != nil || !strings.HasPrefix(out, "cci: --to sv: no lane holds sv; the first lane to register as sv or sv-<n> reads this\n") {
		t.Fatalf("post to a stem no lane holds = %q, %v; want the poster told", out, err)
	}
	if _, err := run(t, "post", "--lane", "sv-4", "--kind", "state", "--text", "sv-4 up"); err != nil {
		t.Fatal(err)
	}
	out, err = run(t, "post", "--lane", "root", "--kind", "note", "--to", "sv", "--text", "welcome")
	if err != nil || strings.HasPrefix(out, "cci:") {
		t.Fatalf("post to a stem one lane holds = %q, %v; want no notice", out, err)
	}
	out, err = run(t, "tail", "--drive", "d", "--since", "0", "--reader", "sv-4")
	if err != nil || !strings.Contains(out, "NOTE root -> sv anyone?") || !strings.Contains(out, "NOTE root -> sv welcome") {
		t.Fatalf("tail --reader sv-4 = %q, %v; want what waited on the stem and what followed", out, err)
	}
}
