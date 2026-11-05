package cli_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/node"
	"github.com/yasyf/cc-inbox/internal/relay"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func TestABareNumberANonownerNodePrintedResolvesOnThatNode(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	t.Setenv("CODEX_THREAD_ID", "")
	sprite, err := store.Open(t.Context(), home)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sprite.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := relay.SaveConfig(home, relay.Config{Mesh: "machine", URL: "https://relay.invalid", Tier: "service"}); err != nil {
		t.Fatal(err)
	}
	mac, _ := testutil.Store(t)
	if err := sprite.PutMember(t.Context(), store.Member{Node: mac.Self(), PublicKey: mac.Node().PublicKey(), Tier: node.TierOwner, Via: "relay", By: "test", At: time.Now()}); err != nil {
		t.Fatal(err)
	}
	testutil.Post(t, mac, store.Record{Lane: "root", Kind: kinds.Note, Text: "the mac's first"})
	rows, err := mac.Export(t.Context(), mac.Self(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sprite.Apply(t.Context(), rows, nil); err != nil {
		t.Fatal(err)
	}

	out, err := run(t, "post", "--drive", "d", "--lane", "finder-desk", "--kind", "note", "--text", "sprite pass")
	if err != nil {
		t.Fatal(err)
	}
	printed := strings.TrimSpace(out)
	digits, tag, _ := strings.Cut(strings.TrimPrefix(printed, "#"), "@")
	if tag != sprite.Self()[:8] {
		t.Fatalf("post on the sprite printed %q; want its own number, tagged @%s", printed, sprite.Self()[:8])
	}
	for _, ref := range []string{digits, "#" + digits} {
		if out, err := run(t, "show", ref); err != nil || !strings.Contains(out, "sprite pass") {
			t.Errorf("show %s after the sprite printed %s = %q, %v; want the sprite's note", ref, printed, out, err)
		}
	}
	if out, err := run(t, "show", "1"); err != nil || !strings.Contains(out, "the mac's first") {
		t.Errorf("show 1 on the sprite = %q, %v; want the mac's #1, which an owner node wrote", out, err)
	}

	out, err = run(t, "post", "--drive", "d", "--lane", "meta-desk", "--kind", "answer", "--re", digits, "--text", "seen", "--json")
	if err != nil {
		t.Fatalf("post --re %s after the sprite printed %s: %v", digits, printed, err)
	}
	var reply store.Record
	if err := json.Unmarshal([]byte(out), &reply); err != nil {
		t.Fatal(err)
	}
	want, err := sprite.Resolve(t.Context(), printed)
	if err != nil {
		t.Fatal(err)
	}
	if reply.Re != want {
		t.Errorf("post --re %s answers seq %d; want the sprite's own record %s, seq %d", digits, reply.Re, printed, want)
	}
}
