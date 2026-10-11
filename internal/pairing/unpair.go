package pairing

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/yasyf/cc-inbox/e2e"
	"github.com/yasyf/cc-inbox/internal/node"
	"github.com/yasyf/cc-inbox/internal/relay"
	"github.com/yasyf/cc-inbox/internal/store"
)

func Unpair(ctx context.Context, home string, st *store.Store, self node.Node, cfg relay.Config, target string, out io.Writer) (store.Member, error) {
	if cfg.Tier != e2e.TierOwner {
		return store.Member{}, fmt.Errorf("pairing: only an owner node unpairs; this node joined mesh %s as %s", cfg.Mesh, cfg.Tier)
	}
	m, err := resolveMember(ctx, st, target)
	if err != nil {
		return store.Member{}, err
	}
	if m.Node == self.ID() {
		return store.Member{}, errors.New("pairing: refusing to unpair this node; run cci unpair from another owner")
	}
	var r unpairReply
	if err := (relay.Client{Config: cfg, Identity: self}).Call(ctx, http.MethodPost, "/v1/unpair", unpairRequest{Mesh: cfg.Mesh, Node: m.Node}, &r); err != nil {
		return store.Member{}, fmt.Errorf("unpair %s: %w", m.Node, err)
	}
	if err := forget(ctx, home, st, self, cfg, m.Node, r.At, (*relay.Node).Rotate); err != nil {
		return store.Member{}, err
	}
	_, err = fmt.Fprintf(out, "unpaired %s (%s) from mesh %s; group key rotated\n", m.Node, m.Tier, cfg.Mesh)
	return m, err
}

func resolveMember(ctx context.Context, st *store.Store, target string) (store.Member, error) {
	members, err := st.Members(ctx)
	if err != nil {
		return store.Member{}, err
	}
	target = strings.ToLower(strings.ReplaceAll(target, " ", ""))
	var hits []store.Member
	for _, m := range members {
		if strings.HasPrefix(m.Node, target) || strings.HasPrefix(node.FingerprintOf(m.PublicKey), target) {
			hits = append(hits, m)
		}
	}
	switch len(hits) {
	case 1:
		return hits[0], nil
	case 0:
		return store.Member{}, fmt.Errorf("no pinned node matches %q; cci peers lists them", target)
	default:
		return store.Member{}, fmt.Errorf("%q matches %d pinned nodes; pass more of the id", target, len(hits))
	}
}

func forget(ctx context.Context, home string, st *store.Store, self e2e.Identity, cfg relay.Config, id string, at time.Time, rotate func(*relay.Node, context.Context) (uint64, error)) error {
	if err := revoke(home, cfg.Mesh, id, at); err != nil {
		return err
	}
	if _, err := st.DropMember(ctx, id); err != nil {
		return err
	}
	n, err := relay.Open(home, st, self, cfg)
	if err != nil {
		return err
	}
	n.Keyring.DropFrom(id)
	if err := n.Keyring.Save(home, cfg.Mesh); err != nil {
		return err
	}
	if cfg.Tier != e2e.TierOwner {
		return nil
	}
	if _, err := rotate(n, ctx); err != nil && !errors.Is(err, relay.ErrEpochTaken) {
		return fmt.Errorf("rotate the group key after unpairing %s: %w", id, err)
	}
	return nil
}

func Handler(home string, st *store.Store, self e2e.Identity) relay.ControlHandler {
	return func(ctx context.Context, c e2e.Control) error {
		if c.Type != e2e.ControlUnpaired && c.Type != e2e.ControlJoined {
			return nil
		}
		cfg, ok, err := relay.LoadConfig(home, c.Mesh)
		if err != nil || !ok {
			return err
		}
		if c.Type == e2e.ControlJoined {
			return admit(ctx, home, st, self, c)
		}
		if c.Node == self.ID() {
			slog.Warn("this node was unpaired by the relay; forgetting the mesh", "mesh", c.Mesh, "reason", c.Reason, "by", c.By)
			return relay.Forget(home, c.Mesh)
		}
		at, err := time.Parse(time.RFC3339Nano, c.At)
		if err != nil {
			slog.Warn("peer.unpaired with an unparseable time; using now", "node", c.Node, "at", c.At)
			at = time.Now()
		}
		return forget(ctx, home, st, self, cfg, c.Node, at, func(n *relay.Node, ctx context.Context) (uint64, error) {
			return n.RotateAfter(ctx, c.Epoch)
		})
	}
}

func admit(ctx context.Context, home string, st *store.Store, self e2e.Identity, c e2e.Control) error {
	if c.Node == self.ID() {
		return nil
	}
	refuse := func(why string) error {
		slog.Warn("peer.joined refused", "node", c.Node, "mesh", c.Mesh, "why", why)
		return nil
	}
	if c.Grant == nil {
		return refuse("no owner grant")
	}
	if node.IDOf(c.Pub) != c.Node {
		return refuse("key does not match the node id")
	}
	granted, err := time.Parse(time.RFC3339Nano, c.Grant.At)
	if err != nil {
		return refuse("grant time " + c.Grant.At + " is not RFC 3339")
	}
	revoked, err := loadRevoked(home, c.Mesh)
	if err != nil {
		return err
	}
	if at, ok := revoked[c.Node]; ok && !granted.After(at) {
		return refuse("grant predates the node's unpairing at " + at.Format(time.RFC3339Nano))
	}
	by := store.Member{Node: self.ID(), PublicKey: self.PublicKey(), Tier: node.TierOwner}
	if c.Grant.By != self.ID() {
		by, err = st.Member(ctx, c.Grant.By)
		if errors.Is(err, sql.ErrNoRows) {
			return refuse("granted by " + c.Grant.By + ", which is not pinned here")
		}
		if err != nil {
			return err
		}
	}
	if by.Tier != node.TierOwner {
		return refuse("granted by " + c.Grant.By + ", which is " + string(by.Tier) + ", not owner")
	}
	if !c.Grant.Verify(by.PublicKey, c.Mesh, c.Node, c.Pub, c.Tier) {
		return refuse("grant signature does not verify against owner " + c.Grant.By)
	}
	return st.PutMember(ctx, store.Member{Node: c.Node, PublicKey: c.Pub, Tier: node.Tier(c.Tier), Via: viaPair, By: c.Grant.By, At: st.Now()})
}
