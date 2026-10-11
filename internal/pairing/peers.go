package pairing

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/yasyf/cc-inbox/internal/node"
	"github.com/yasyf/cc-inbox/internal/relay"
	"github.com/yasyf/cc-inbox/internal/store"
)

type Peer struct {
	Node        string    `json:"node"`
	Fingerprint string    `json:"fingerprint"`
	Tier        node.Tier `json:"tier"`
	RelayTier   node.Tier `json:"relay_tier,omitempty"`
	Via         []string  `json:"via"`
	Meshes      []string  `json:"meshes,omitempty"`
	Host        string    `json:"host,omitempty"`
	Name        string    `json:"name,omitempty"`
	By          string    `json:"by,omitempty"`
	At          time.Time `json:"at,omitzero"`
}

func (p Peer) Line(now time.Time) string {
	parts := []string{p.Node, string(p.Tier), strings.Join(p.Via, "+")}
	if len(p.Meshes) > 0 {
		parts = append(parts, "mesh:"+strings.Join(p.Meshes, ","))
	}
	for _, kv := range [][2]string{{"relay-tier", string(p.RelayTier)}, {"host", p.Host}, {"name", p.Name}, {"by", p.By}} {
		if kv[1] != "" {
			parts = append(parts, kv[0]+":"+kv[1])
		}
	}
	if !p.At.IsZero() {
		parts = append(parts, "since:"+now.Sub(p.At).Round(time.Minute).String())
	}
	return strings.Join(parts, "  ")
}

func Peers(ctx context.Context, st *store.Store, self node.Node, cfgs []relay.Config, withDirectory bool) ([]Peer, error) {
	selfTier := node.TierOwner
	selfMeshes := make([]string, 0, len(cfgs))
	for _, c := range cfgs {
		selfMeshes = append(selfMeshes, c.Mesh)
		if c.Tier != string(node.TierOwner) {
			selfTier = node.Tier(c.Tier)
		}
	}
	out := []Peer{{Node: self.ID(), Fingerprint: self.Fingerprint(), Tier: selfTier, Via: []string{"self"}, Meshes: selfMeshes}}
	idx := map[string]int{self.ID(): 0}
	members, err := st.Members(ctx)
	if err != nil {
		return nil, err
	}
	for _, m := range members {
		if _, ok := idx[m.Node]; ok {
			continue
		}
		idx[m.Node] = len(out)
		p := Peer{Node: m.Node, Fingerprint: node.FingerprintOf(m.PublicKey), Tier: m.Tier, Via: []string{m.Via}, Host: m.Host, By: m.By, At: m.At}
		out = append(out, p)
	}
	if !withDirectory {
		return out, nil
	}
	for _, cfg := range cfgs {
		dir, err := FetchDirectory(ctx, relay.Client{Config: cfg, Identity: self})
		if err != nil {
			return nil, fmt.Errorf("read the relay directory for mesh %s: %w", cfg.Mesh, err)
		}
		for _, d := range dir {
			i, ok := idx[d.ID]
			if !ok {
				i = len(out)
				idx[d.ID] = i
				out = append(out, Peer{Node: d.ID, Fingerprint: node.FingerprintOf(d.Pub), Tier: d.Tier, By: d.By, At: d.At, Via: []string{"unpinned"}})
			}
			p := &out[i]
			if d.ID != self.ID() {
				p.Meshes = append(p.Meshes, cfg.Mesh)
			}
			if !slices.Contains(p.Via, "relay") && d.ID != self.ID() {
				p.Via = append(p.Via, "relay")
			}
			if p.Name == "" {
				p.Name = d.Name
			}
			if d.Tier != p.Tier {
				p.RelayTier = d.Tier
			}
		}
	}
	return out, nil
}
