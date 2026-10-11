package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"time"

	"github.com/yasyf/cc-inbox/e2e"
	"github.com/yasyf/cc-inbox/internal/node"
	"github.com/yasyf/cc-inbox/internal/store"
)

const (
	pushBatch = 25
	pullMax   = 10
)

var ErrEpochTaken = errors.New("relay: another owner rotated to this epoch first")

type ControlHandler func(ctx context.Context, c e2e.Control) error

type Node struct {
	Home     string
	Store    *store.Store
	Identity e2e.Identity
	Service  e2e.Identity
	Config   Config
	HTTP     *http.Client
	Keyring  *Keyring
}

func Open(home string, st *store.Store, id e2e.Identity, cfg Config) (*Node, error) {
	ring, err := LoadKeyring(home, cfg.Mesh)
	if err != nil {
		return nil, err
	}
	n := &Node{Home: home, Store: st, Identity: id, Config: cfg, Keyring: ring}
	if cfg.Delegation != nil {
		if n.Service, err = ServiceKey(); err != nil {
			return nil, err
		}
		if n.Service.ID() != cfg.Delegation.Service {
			return nil, fmt.Errorf("relay: $%s is node %s, the delegation names %s", ServiceKeyEnv, n.Service.ID(), cfg.Delegation.Service)
		}
	}
	return n, nil
}

const ServiceKeyEnv = "CCI_SERVICE_KEY"

func ServiceKey() (e2e.Identity, error) {
	raw := os.Getenv(ServiceKeyEnv)
	if raw == "" {
		return nil, fmt.Errorf("relay: a delegated instance needs the service key in $%s", ServiceKeyEnv)
	}
	n, err := node.Parse([]byte(raw))
	if err != nil {
		return nil, fmt.Errorf("$%s: %w", ServiceKeyEnv, err)
	}
	return n, nil
}

func (n *Node) keyHolder() e2e.Identity {
	if n.Service != nil {
		return n.Service
	}
	return n.Identity
}

func (n *Node) Delegate(drives, kinds []string, ttl time.Duration) error {
	svc, err := ServiceKey()
	if err != nil {
		return err
	}
	if svc.ID() == n.Identity.ID() {
		return fmt.Errorf("relay: the instance key is the service key; run the instance with a fresh CCI_HOME and without $%s", node.KeyEnv)
	}
	d, err := e2e.Delegate(svc, n.Config.Mesh, n.Identity.PublicKey(), drives, kinds, time.Now().Add(ttl))
	if err != nil {
		return err
	}
	n.Service, n.Config.Delegation = svc, &d
	return nil
}

func (n *Node) client() Client {
	return Client{Config: n.Config, Identity: n.Identity, HTTP: n.HTTP}
}

func Peer(mesh string) string { return "relay:" + mesh }

func (n *Node) Join(ctx context.Context) error {
	var rk relayKey
	if err := n.client().Call(ctx, http.MethodGet, "/v1/relay", nil, &rk); err != nil {
		return err
	}
	switch {
	case len(n.Config.RelayPub) == 0:
		n.Config.RelayPub = rk.Pub
	case !slices.Equal(n.Config.RelayPub, rk.Pub):
		return fmt.Errorf("relay: %s presents a different relay key than the one pinned at first join", n.Config.URL)
	}
	var jr joinResponse
	join := joinRequest{Mesh: n.Config.Mesh, Pub: n.Identity.PublicKey(), Delegation: n.Config.Delegation}
	if err := n.client().Call(ctx, http.MethodPost, "/v1/join", join, &jr); err != nil {
		return err
	}
	if jr.Tier != n.Config.Tier {
		return fmt.Errorf("relay: %s says this node is %q, its local tier is %q", n.Config.URL, jr.Tier, n.Config.Tier)
	}
	if err := SaveConfig(n.Home, n.Config); err != nil {
		return err
	}
	if err := n.RefreshKeys(ctx); err != nil {
		return err
	}
	if jr.Tier == e2e.TierOwner && jr.Epoch == 0 {
		if _, err := n.Rotate(ctx); err != nil && !errors.Is(err, ErrEpochTaken) {
			return err
		}
	}
	return nil
}

func (n *Node) Members(ctx context.Context) (map[string]e2e.Member, error) {
	ms, err := n.Store.Members(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]e2e.Member, len(ms)+1)
	for _, m := range ms {
		out[m.Node] = e2e.Member{Node: m.Node, PublicKey: m.PublicKey, Tier: string(m.Tier)}
	}
	out[n.Identity.ID()] = e2e.Member{Node: n.Identity.ID(), PublicKey: n.Identity.PublicKey(), Tier: n.Config.Tier}
	if n.Service != nil {
		out[n.Service.ID()] = e2e.Member{Node: n.Service.ID(), PublicKey: n.Service.PublicKey(), Tier: e2e.TierService}
	}
	return out, nil
}

func (n *Node) RefreshKeys(ctx context.Context) error {
	var kr keysResponse
	q := url.Values{"mesh": {n.Config.Mesh}, "since": {"0"}}
	if err := n.client().Call(ctx, http.MethodGet, "/v1/keys?"+q.Encode(), nil, &kr); err != nil {
		return err
	}
	members, err := n.Members(ctx)
	if err != nil {
		return err
	}
	added := false
	for _, w := range kr.Wraps {
		if _, err := e2e.UnwrapKey(w, n.Config.Mesh, n.keyHolder(), members); err != nil {
			slog.Warn("relay wrap refused", "mesh", n.Config.Mesh, "epoch", w.Epoch, "from", w.From, "err", err)
			continue
		}
		if n.Keyring.Add(w) {
			added = true
		}
	}
	if !added {
		return nil
	}
	return n.Keyring.Save(n.Home, n.Config.Mesh)
}

func (n *Node) Rotate(ctx context.Context) (uint64, error) {
	var kr keysResponse
	q := url.Values{"mesh": {n.Config.Mesh}, "since": {strconv.FormatUint(n.Keyring.Latest(n.Config.Mesh)+1, 10)}}
	if err := n.client().Call(ctx, http.MethodGet, "/v1/keys?"+q.Encode(), nil, &kr); err != nil {
		return 0, err
	}
	return n.RotateAfter(ctx, kr.Epoch)
}

func (n *Node) RotateAfter(ctx context.Context, base uint64) (uint64, error) {
	if n.Config.Tier != e2e.TierOwner {
		return 0, fmt.Errorf("relay: only an owner node rotates the group key (this node is %s)", n.Config.Tier)
	}
	if local := n.Keyring.Latest(n.Config.Mesh); base < local {
		return 0, fmt.Errorf("%w: rotating after epoch %d, this node already holds %d", ErrEpochTaken, base, local)
	}
	k, err := e2e.NewGroupKey(base + 1)
	if err != nil {
		return 0, err
	}
	wraps, err := n.wrapAll(ctx, k)
	if err != nil {
		return 0, err
	}
	req := keysRequest{Mesh: n.Config.Mesh, Wraps: wraps, Rotate: true}
	if err := n.client().Call(ctx, http.MethodPut, "/v1/keys/"+strconv.FormatUint(k.Epoch, 10), req, nil); err != nil {
		var se *StatusError
		if errors.As(err, &se) && se.Code == http.StatusConflict {
			return 0, fmt.Errorf("%w: %d", ErrEpochTaken, k.Epoch)
		}
		return 0, err
	}
	n.Keyring.Add(wraps[0])
	return k.Epoch, n.Keyring.Save(n.Home, n.Config.Mesh)
}

func (n *Node) Share(ctx context.Context) error {
	if n.Config.Tier != e2e.TierOwner {
		return nil
	}
	members, err := n.Members(ctx)
	if err != nil {
		return err
	}
	k, err := n.Keyring.Current(n.Identity, n.Config.Mesh, members)
	if err != nil {
		return err
	}
	wraps, err := n.wrapAll(ctx, k)
	if err != nil {
		return err
	}
	return n.client().Call(ctx, http.MethodPut, "/v1/keys/"+strconv.FormatUint(k.Epoch, 10), keysRequest{Mesh: n.Config.Mesh, Wraps: wraps}, nil)
}

func (n *Node) wrapAll(ctx context.Context, k e2e.GroupKey) ([]e2e.Wrap, error) {
	local, err := n.Members(ctx)
	if err != nil {
		return nil, err
	}
	var mr membersResponse
	if err := n.client().Call(ctx, http.MethodGet, "/v1/members?"+url.Values{"mesh": {n.Config.Mesh}}.Encode(), nil, &mr); err != nil {
		return nil, err
	}
	self, err := e2e.WrapKey(k, n.Config.Mesh, n.Identity, local[n.Identity.ID()])
	if err != nil {
		return nil, err
	}
	wraps := []e2e.Wrap{self}
	for _, rm := range mr.Members {
		m, ok := local[rm.ID]
		if !ok || rm.ID == n.Identity.ID() || !slices.Equal(m.PublicKey, rm.Pub) {
			continue
		}
		w, err := e2e.WrapKey(k, n.Config.Mesh, n.Identity, m)
		if err != nil {
			return nil, err
		}
		wraps = append(wraps, w)
	}
	return wraps, nil
}

func Route(configs []Config, drive string) string {
	catchAll := ""
	for _, c := range configs {
		if c.Carries(drive) {
			return c.Mesh
		}
		if len(c.Routes) == 0 && catchAll == "" {
			catchAll = c.Mesh
		}
	}
	return catchAll
}

type Rejection struct {
	ID        string `json:"id,omitempty"`
	Drive     string `json:"drive"`
	OriginSeq int64  `json:"origin_seq"`
	Reason    string `json:"reason"`
}

type PushResult struct {
	Pushed   int         `json:"pushed"`
	Rejected []Rejection `json:"rejected"`
}

func (n *Node) Push(ctx context.Context, configs []Config) (PushResult, error) {
	res := PushResult{Rejected: []Rejection{}}
	if n.Config.Tier == e2e.TierPaired {
		return res, nil
	}
	marks, err := n.Store.Watermarks(ctx)
	if err != nil {
		return res, err
	}
	rows, err := n.Store.Export(ctx, n.Store.Self(), marks[Peer(n.Config.Mesh)], 0)
	if err != nil || len(rows) == 0 {
		return res, err
	}
	members, err := n.Members(ctx)
	if err != nil {
		return res, err
	}
	k, err := n.Keyring.Current(n.keyHolder(), n.Config.Mesh, members)
	if err != nil {
		return res, err
	}
	for batch := range slices.Chunk(rows, pushBatch) {
		var envs []e2e.Envelope
		sent := map[int64]store.Row{}
		for _, r := range batch {
			if Route(configs, r.Drive) != n.Config.Mesh {
				continue
			}
			e, err := SealRow(k, n.Identity, n.Config, r)
			if err != nil {
				return res, err
			}
			envs = append(envs, e)
			sent[r.OriginSeq] = r
		}
		if len(envs) > 0 {
			var pr pushResponse
			if err := n.client().Call(ctx, http.MethodPost, "/v1/push", pushRequest{Mesh: n.Config.Mesh, Envelopes: envs}, &pr); err != nil {
				return res, err
			}
			if err := accounted(envs, pr); err != nil {
				return res, err
			}
			for _, rj := range pr.Rejected {
				r := sent[rj.OriginSeq]
				slog.Warn("relay refused record", "mesh", n.Config.Mesh, "origin_seq", rj.OriginSeq, "reason", rj.Reason)
				res.Rejected = append(res.Rejected, Rejection{ID: r.ID, Drive: r.Drive, OriginSeq: rj.OriginSeq, Reason: rj.Reason})
			}
			res.Pushed += len(pr.Accepted)
		}
		if err := n.Store.Ack(ctx, Peer(n.Config.Mesh), batch[len(batch)-1].OriginSeq); err != nil {
			return res, err
		}
	}
	return res, nil
}

func (n *Node) Sync(ctx context.Context, drive string) (store.Applied, error) {
	var total store.Applied
	var oo struct {
		Origins []struct {
			Origin string `json:"origin"`
		} `json:"origins"`
	}
	q := url.Values{"mesh": {n.Config.Mesh}, "drive": {drive}}
	if err := n.client().Call(ctx, http.MethodGet, "/v1/log/origins?"+q.Encode(), nil, &oo); err != nil {
		return total, err
	}
	if err := n.RefreshKeys(ctx); err != nil {
		return total, err
	}
	members, err := n.Members(ctx)
	if err != nil {
		return total, err
	}
	keys, err := n.Keyring.Keys(n.Config.Mesh, n.keyHolder(), members)
	if err != nil {
		slog.Warn("relay keyring holds an unusable wrap", "mesh", n.Config.Mesh, "err", err)
	}
	for _, o := range oo.Origins {
		var after int64
		for {
			var lr struct {
				Envelopes []e2e.Envelope `json:"envelopes"`
				More      bool           `json:"more"`
			}
			lq := url.Values{
				"mesh": {n.Config.Mesh}, "drive": {drive}, "origin": {o.Origin},
				"after": {strconv.FormatInt(after, 10)}, "limit": {"500"},
			}
			if err := n.client().Call(ctx, http.MethodGet, "/v1/log?"+lq.Encode(), nil, &lr); err != nil {
				return total, err
			}
			var rows []store.Row
			for _, e := range lr.Envelopes {
				after = max(after, e.OriginSeq)
				r, err := n.open(e, keys, members)
				if err != nil {
					slog.Warn("relay log record skipped", "mesh", n.Config.Mesh, "drive", drive, "err", err)
					continue
				}
				rows = append(rows, r)
			}
			a, err := n.Store.Apply(ctx, rows, nil)
			if err != nil {
				return total, err
			}
			total.Inserted += a.Inserted
			total.Duplicate += a.Duplicate
			total.Buffered += a.Buffered
			total.Released += a.Released
			if !lr.More || len(lr.Envelopes) == 0 {
				break
			}
		}
	}
	return total, nil
}

func accounted(envs []e2e.Envelope, pr pushResponse) error {
	answered := map[int64]bool{}
	for _, r := range slices.Concat(pr.Accepted, pr.Rejected) {
		answered[r.OriginSeq] = true
	}
	for _, e := range envs {
		if !answered[e.OriginSeq] {
			return fmt.Errorf("relay: push response omits origin_seq %d; not advancing past it", e.OriginSeq)
		}
	}
	return nil
}

func SealRow(k e2e.GroupKey, id e2e.Identity, cfg Config, r store.Row) (e2e.Envelope, error) {
	pt, err := json.Marshal(r)
	if err != nil {
		return e2e.Envelope{}, fmt.Errorf("encode row %s/%d: %w", r.Origin, r.OriginSeq, err)
	}
	return e2e.Seal(k, id, e2e.Envelope{
		Mesh: cfg.Mesh, Drive: r.Drive, OriginSeq: r.OriginSeq, Tier: cfg.Tier,
		KindClass: string(r.Kind), TS: r.At.UTC().Format(time.RFC3339Nano), Delegation: cfg.Delegation,
	}, pt)
}

func OpenRow(e e2e.Envelope, keys map[uint64]e2e.GroupKey, members map[string]e2e.Member) (store.Row, error) {
	pt, err := e2e.Open(e, keys, members)
	if err != nil {
		return store.Row{}, err
	}
	var r store.Row
	if err := json.Unmarshal(pt, &r); err != nil {
		return store.Row{}, fmt.Errorf("decode record %s/%d: %w", e.Origin, e.OriginSeq, err)
	}
	if r.Origin != e.Origin || r.OriginSeq != e.OriginSeq || r.Drive != e.Drive || string(r.Kind) != e.KindClass {
		return store.Row{}, fmt.Errorf("relay: record %s/%d body disagrees with its signed header", e.Origin, e.OriginSeq)
	}
	return r, nil
}

func (n *Node) Pull(ctx context.Context, wait time.Duration, handle ControlHandler) (store.Applied, error) {
	q := url.Values{"mesh": {n.Config.Mesh}, "wait": {strconv.Itoa(int(wait / time.Second))}, "max": {strconv.Itoa(pullMax)}}
	if n.Config.Tap != "" {
		q.Set("tap", n.Config.Tap)
	}
	var pr pullResponse
	if err := n.client().Call(ctx, http.MethodGet, "/v1/pull?"+q.Encode(), nil, &pr); err != nil {
		return store.Applied{}, err
	}
	if len(pr.Messages) == 0 {
		return store.Applied{}, nil
	}
	members, err := n.Members(ctx)
	if err != nil {
		return store.Applied{}, err
	}
	keys, err := n.Keyring.Keys(n.Config.Mesh, n.keyHolder(), members)
	if err != nil {
		slog.Warn("relay keyring holds an unusable wrap", "mesh", n.Config.Mesh, "err", err)
	}
	var rows []store.Row
	var acks, rowAcks []string
	refreshed := false
	for _, m := range pr.Messages {
		switch {
		case m.Control != nil:
			ok, err := n.control(ctx, *m.Control, handle)
			if err != nil {
				return store.Applied{}, err
			}
			if ok {
				acks = append(acks, m.Receipt)
			}
		case m.Envelope != nil:
			r, err := n.open(*m.Envelope, keys, members)
			if errors.Is(err, e2e.ErrNoGroupKey) && !refreshed {
				refreshed = true
				if err := n.RefreshKeys(ctx); err != nil {
					return store.Applied{}, err
				}
				keys, _ = n.Keyring.Keys(n.Config.Mesh, n.keyHolder(), members)
				r, err = n.open(*m.Envelope, keys, members)
			}
			switch {
			case err == nil:
				rows = append(rows, r)
				rowAcks = append(rowAcks, m.Receipt)
			case errors.Is(err, e2e.ErrNoGroupKey), errors.Is(err, e2e.ErrUnknownMember):
				slog.Info("relay record held for redelivery", "mesh", n.Config.Mesh, "err", err)
			default:
				slog.Warn("relay record dropped", "mesh", n.Config.Mesh, "err", err)
				acks = append(acks, m.Receipt)
			}
		default:
			acks = append(acks, m.Receipt)
		}
	}
	applied, err := n.Store.Apply(ctx, rows, nil)
	if err != nil {
		return applied, err
	}
	acks = append(acks, rowAcks...)
	if len(acks) == 0 {
		return applied, nil
	}
	return applied, n.client().Call(ctx, http.MethodPost, "/v1/ack", ackRequest{Mesh: n.Config.Mesh, Receipts: acks, Tap: n.Config.Tap}, nil)
}

func (n *Node) open(e e2e.Envelope, keys map[uint64]e2e.GroupKey, members map[string]e2e.Member) (store.Row, error) {
	if e.Mesh != n.Config.Mesh {
		return store.Row{}, fmt.Errorf("relay: envelope for mesh %s arrived on %s", e.Mesh, n.Config.Mesh)
	}
	return OpenRow(e, keys, members)
}

func (n *Node) control(ctx context.Context, c e2e.Control, handle ControlHandler) (bool, error) {
	if c.Mesh != n.Config.Mesh || !c.Verify(n.Config.RelayPub) {
		slog.Warn("relay control dropped: signature or mesh does not match", "type", c.Type, "mesh", c.Mesh)
		return true, nil
	}
	if handle != nil {
		if err := handle(ctx, c); err != nil {
			slog.Warn("relay control handler failed; left for redelivery", "type", c.Type, "err", err)
			return false, nil
		}
	}
	switch c.Type {
	case e2e.ControlRekey:
		if err := n.RefreshKeys(ctx); err != nil {
			return false, err
		}
	case e2e.ControlJoined:
		if err := n.Share(ctx); err != nil {
			slog.Warn("relay share after join failed", "mesh", n.Config.Mesh, "node", c.Node, "err", err)
		}
	}
	return true, nil
}

type StatusReport struct {
	Mesh      string        `json:"mesh"`
	URL       string        `json:"url"`
	Node      string        `json:"node"`
	Tier      string        `json:"tier"`
	Epoch     uint64        `json:"epoch"`
	Routes    []string      `json:"routes,omitempty"`
	Pushed    int64         `json:"pushed_through"`
	Unpushed  int           `json:"unpushed"`
	Remote    *RemoteStatus `json:"remote,omitempty"`
	RemoteErr string        `json:"remote_error,omitempty"`
}

func (n *Node) Status(ctx context.Context) (StatusReport, error) {
	marks, err := n.Store.Watermarks(ctx)
	if err != nil {
		return StatusReport{}, err
	}
	mark := marks[Peer(n.Config.Mesh)]
	pending, err := n.Store.Export(ctx, n.Store.Self(), mark, 0)
	if err != nil {
		return StatusReport{}, err
	}
	rep := StatusReport{
		Mesh: n.Config.Mesh, URL: n.Config.URL, Node: n.Identity.ID(), Tier: n.Config.Tier,
		Epoch: n.Keyring.Latest(n.Config.Mesh), Routes: n.Config.Routes, Pushed: mark,
	}
	if n.Config.Tier != e2e.TierPaired {
		rep.Unpushed = len(pending)
	}
	var rs RemoteStatus
	if err := n.client().Call(ctx, http.MethodGet, "/v1/status?"+url.Values{"mesh": {n.Config.Mesh}}.Encode(), nil, &rs); err != nil {
		rep.RemoteErr = err.Error()
	} else {
		rep.Remote = &rs
	}
	return rep, nil
}
