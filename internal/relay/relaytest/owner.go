package relaytest

import (
	"context"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/e2e"
	"github.com/yasyf/cc-inbox/internal/node"
	"github.com/yasyf/cc-inbox/internal/relay"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

// JoinOwner joins a fresh owner to s's default mesh after pinning each
// service, so the group key its join mints is wrapped to them.
func JoinOwner(t *testing.T, s *Server, services ...e2e.Identity) (*store.Store, *relay.Node) {
	t.Helper()
	st, _ := testutil.Store(t)
	id, err := node.Load(st.HomeDir())
	if err != nil {
		t.Fatal(err)
	}
	s.AddMember(id, e2e.TierOwner)
	for _, svc := range services {
		s.AddMember(svc, e2e.TierService)
		if err := st.PutMember(context.Background(), store.Member{Node: svc.ID(), PublicKey: svc.PublicKey(), Tier: node.TierService, Via: "relay", By: "test", At: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	n, err := relay.Open(st.HomeDir(), st, id, relay.Config{URL: s.URL, Mesh: relay.DefaultMesh, Tier: e2e.TierOwner})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Join(context.Background()); err != nil {
		t.Fatal(err)
	}
	return st, n
}
