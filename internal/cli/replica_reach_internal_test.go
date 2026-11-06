package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/peers"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func TestOnlyTheOwnerNodeSaysNoLaneHoldsAName(t *testing.T) {
	t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())
	for _, name := range peers.IdentityEnv {
		t.Setenv(name, "")
	}
	st, _ := testutil.Store(t)
	for _, owner := range []bool{true, false} {
		st.SetOwner(owner)
		var notices bytes.Buffer
		to, err := resolveRecipients(context.Background(), st, "d", []string{"obs-desk"}, kinds.Note, false, &notices)
		if err != nil || len(to) != 1 || to[0] != "obs-desk" {
			t.Fatalf("owner=%t: recipients = %v, %v; want obs-desk as typed", owner, to, err)
		}
		if said := strings.Contains(notices.String(), "no lane holds obs-desk"); said != owner {
			t.Errorf("owner=%t: notices = %q; only the owner node's registry is whole enough to say no lane holds obs-desk", owner, notices.String())
		}
	}
}
