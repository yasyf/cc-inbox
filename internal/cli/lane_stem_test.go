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
	if err != nil || !strings.Contains(out, "note root>sv late word") || !strings.Contains(out, "note root>sv pool word") {
		t.Fatalf("tail --reader sv-3 = %q, %v; want the stem's mail, retired sv-2's stored for the stem", out, err)
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
	if err != nil || !strings.Contains(out, "note root>sv anyone?") || !strings.Contains(out, "note root>sv welcome") {
		t.Fatalf("tail --reader sv-4 = %q, %v; want what waited on the stem and what followed", out, err)
	}
}

func TestAPostToANumberedLaneIsStoredForItsStemWhileTheStemReachesThatLane(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())
	for _, name := range peers.IdentityEnv {
		t.Setenv(name, "")
	}
	if _, err := run(t, "post", "--drive", "d", "--lane", "sv-2", "--kind", "state", "--text", "sv-2 up"); err != nil {
		t.Fatal(err)
	}
	to := func(name, text string) string {
		t.Helper()
		out, err := run(t, "post", "--drive", "d", "--lane", "root", "--kind", "note", "--to", name, "--text", text, "--json")
		if err != nil {
			t.Fatal(err)
		}
		_, stored, _ := strings.Cut(out, `"to":`)
		stored, _, _ = strings.Cut(stored, "]")
		return stored + "]"
	}

	alone := to("sv-2", "first")
	if _, err := run(t, "post", "--drive", "d", "--lane", "sv-3", "--kind", "state", "--text", "sv-3 up"); err != nil {
		t.Fatal(err)
	}
	older, newer, unregistered := to("sv-2", "second"), to("sv-3", "third"), to("other-1", "fourth")

	if alone != `["sv"]` || older != `["sv-2"]` || newer != `["sv"]` || unregistered != `["other-1"]` {
		t.Fatalf("stored recipients alone %s, older %s, newer %s, unregistered %s; want sv while the stem reaches the named lane, the name otherwise", alone, older, newer, unregistered)
	}
	owned, err := run(t, "post", "--drive", "d", "--lane", "root", "--kind", "decide", "--owned-by", "sv-desk", "--to", "sv-3", "--text", "sv-desk owns sv-3", "--json")
	if err != nil || !strings.Contains(owned, `"to":["sv-3"]`) {
		t.Fatalf("an owned-by decide = %q, %v; want the owned lane kept by name", owned, err)
	}
	out, err := run(t, "tail", "--drive", "d", "--since", "0", "--reader", "sv-3")
	if err != nil || !strings.Contains(out, "note root>sv first") || !strings.Contains(out, "note root>sv third") || strings.Contains(out, "second") {
		t.Fatalf("tail --reader sv-3 = %q, %v; want the stem's mail and none sent to the running sv-2", out, err)
	}
}

func TestAReaderNamesAWriterByTheStemItHoldsAndAnOlderLaneByItsNumber(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())
	for _, name := range peers.IdentityEnv {
		t.Setenv(name, "")
	}
	if _, err := run(t, "post", "--drive", "d", "--lane", "sv-2", "--kind", "state", "--text", "first word"); err != nil {
		t.Fatal(err)
	}
	alone, err := run(t, "tail", "--drive", "d", "--since", "0")
	if err != nil || !strings.Contains(alone, "state sv first word") {
		t.Fatalf("tail with sv-2 alone = %q, %v; want sv-2 named by its stem", alone, err)
	}

	if _, err := run(t, "post", "--drive", "d", "--lane", "sv-3", "--kind", "state", "--text", "second word"); err != nil {
		t.Fatal(err)
	}
	both, err := run(t, "tail", "--drive", "d", "--since", "0")
	if err != nil || !strings.Contains(both, "state sv-2 first word") || !strings.Contains(both, "state sv second word") {
		t.Fatalf("tail with sv-3 newer = %q, %v; want sv-2 by its number and sv-3 by the stem", both, err)
	}
	asJSON, err := run(t, "tail", "--drive", "d", "--since", "0", "--json")
	if err != nil || strings.Count(asJSON, `"stem":"sv"`) != 1 || !strings.Contains(asJSON, `"lane":"sv-3"`) {
		t.Fatalf("tail --json = %q, %v; want the stem on sv-3's record alone and every lane exact", asJSON, err)
	}
}
