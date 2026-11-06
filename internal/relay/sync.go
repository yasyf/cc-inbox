package relay

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/yasyf/cc-inbox/e2e"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/node"
	"github.com/yasyf/cc-inbox/internal/store"
)

const (
	pushBatch    = 25
	seedProgress = 2000
	pullMax      = 10
	logPage      = 100
	logReaders   = 16
	instanceLife = 24 * time.Hour
	instanceSkew = 5 * time.Minute
	renewWithin  = 2 * time.Hour
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
	Progress io.Writer

	servicePEM []byte
}

func Open(home string, st *store.Store, id e2e.Identity, cfg Config) (*Node, error) {
	ring, err := LoadKeyring(home, cfg.Mesh)
	if err != nil {
		return nil, err
	}
	n := &Node{Home: home, Store: st, Identity: id, Config: cfg, Keyring: ring}
	if cfg.Delegation == nil {
		return n, nil
	}
	if n.Service, err = ServiceKey(home, cfg.Mesh); err != nil || n.Service == nil {
		return n, err
	}
	if n.Service.ID() != cfg.Delegation.Service {
		return nil, fmt.Errorf("relay: the service key is node %s, the delegation names %s", n.Service.ID(), cfg.Delegation.Service)
	}
	return n, nil
}

const ServiceKeyEnv = "CCI_SERVICE_KEY"

// ServiceKey reads the service key a service-delegated instance holds from
// $CCI_SERVICE_KEY, or else from the mesh's service.pem, which Delegate
// writes so that later processes, the daemon among them, need no secret in
// their environment. It is nil for an instance an owner delegated, which
// holds no key but its own.
func ServiceKey(home, mesh string) (e2e.Identity, error) {
	if raw := os.Getenv(ServiceKeyEnv); raw != "" {
		return parseServiceKey([]byte(raw), "$"+ServiceKeyEnv)
	}
	path := serviceKeyPath(home, mesh)
	raw, err := os.ReadFile(path) //nolint:gosec // G304: path is the mesh's service key under the cci home.
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read service key: %w", err)
	}
	return parseServiceKey(raw, path)
}

func parseServiceKey(raw []byte, from string) (e2e.Identity, error) {
	n, err := node.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", from, err)
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
	raw := os.Getenv(ServiceKeyEnv)
	if raw == "" {
		return fmt.Errorf("relay: delegating an instance needs the service key in $%s", ServiceKeyEnv)
	}
	svc, err := parseServiceKey([]byte(raw), "$"+ServiceKeyEnv)
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
	n.Service, n.Config.Delegation, n.servicePEM = svc, &d, []byte(raw)
	return nil
}

// Issue signs the instance key pub a delegation over drives and kinds for
// ttl, wraps it every group key this owner holds, and registers both with the
// relay, which takes a renewal of a live instance the same way. The instance
// joins with the delegation through Adopt and holds no key but its own.
func (n *Node) Issue(ctx context.Context, pub []byte, drives, kinds []string, ttl time.Duration) (e2e.Delegation, error) {
	if n.Config.Tier != e2e.TierOwner {
		return e2e.Delegation{}, fmt.Errorf("relay: only an owner node issues delegations (this node is %s)", n.Config.Tier)
	}
	if ttl <= 0 || ttl > instanceLife {
		return e2e.Delegation{}, fmt.Errorf("relay: a delegation lasts at most %s, not %s", instanceLife, ttl)
	}
	var d e2e.Delegation
	err := withIssued(n.Home, n.Config.Mesh, func(issued map[string]issuedGrant) error {
		var err error
		if d, err = n.issue(ctx, pub, drives, kinds, ttl, issued); errors.Is(err, errRotatedMeanwhile) {
			if err := n.RefreshKeys(ctx); err != nil {
				return err
			}
			d, err = n.issue(ctx, pub, drives, kinds, ttl, issued)
		}
		return err
	})
	return d, err
}

var errRotatedMeanwhile = errors.New("relay: the mesh rotated during the delegation")

func expiry(d e2e.Delegation) time.Time {
	at, _ := time.Parse(time.RFC3339Nano, d.Expires)
	return at
}

func (n *Node) issue(ctx context.Context, pub []byte, drives, kinds []string, ttl time.Duration, issued map[string]issuedGrant) (e2e.Delegation, error) {
	d, err := e2e.Delegate(n.Identity, n.Config.Mesh, pub, drives, kinds, time.Now().Add(ttl))
	if err != nil {
		return e2e.Delegation{}, err
	}
	if d.Instance == n.Identity.ID() {
		return e2e.Delegation{}, errors.New("relay: an owner never delegates its own key")
	}
	held, ok := issued[d.Instance]
	switch {
	case ok && held.Revoked:
		return e2e.Delegation{}, fmt.Errorf("relay: %s was revoked; this owner issues it nothing before its last delegation expires at %s", d.Instance, held.Expires)
	case ok && !expiry(d).After(expiry(held.Delegation)):
		return e2e.Delegation{}, fmt.Errorf("relay: %s already holds a delegation expiring %s; a renewal must outlast it", d.Instance, held.Expires)
	}
	members, err := n.Members(ctx)
	if err != nil {
		return e2e.Delegation{}, err
	}
	if _, err := n.Keyring.Current(n.Identity, n.Config.Mesh, members); err != nil {
		return e2e.Delegation{}, err
	}
	keys, _ := n.Keyring.Keys(n.Config.Mesh, n.Identity, members)
	to := e2e.Member{Node: d.Instance, PublicKey: pub, Tier: e2e.TierService}
	wraps := make([]e2e.Wrap, 0, len(keys))
	for _, k := range keys {
		w, err := e2e.WrapKey(k, n.Config.Mesh, n.Identity, to)
		if err != nil {
			return e2e.Delegation{}, err
		}
		wraps = append(wraps, w)
	}
	issued[d.Instance] = issuedGrant{Delegation: d}
	req := delegateRequest{Mesh: n.Config.Mesh, Delegation: d, Wraps: wraps}
	if err := n.client().Call(ctx, http.MethodPost, "/v1/delegate", req, nil); err != nil {
		var se *StatusError
		if errors.As(err, &se) && se.Code == http.StatusConflict && strings.Contains(se.Message, "rotated") {
			return e2e.Delegation{}, fmt.Errorf("%w: %w", errRotatedMeanwhile, err)
		}
		return e2e.Delegation{}, err
	}
	return d, nil
}

// Revoke ends an instance this owner delegated: the relay drops its queue and
// wraps, and the group key rotates so the instance reads nothing written after.
func (n *Node) Revoke(ctx context.Context, instance string) error {
	err := withIssued(n.Home, n.Config.Mesh, func(issued map[string]issuedGrant) error {
		held, ok := issued[instance]
		if !ok {
			return fmt.Errorf("relay: this owner holds no live delegation of %s", instance)
		}
		held.Revoked = true
		issued[instance] = held
		return n.client().Call(ctx, http.MethodPost, "/v1/unpair", unpairRequest{Mesh: n.Config.Mesh, Node: instance}, nil)
	})
	if err != nil {
		return err
	}
	_, err = n.Rotate(ctx)
	return err
}

// Adopt makes this node the instance d delegates, holding no key but its own.
// It refuses a delegation that names another key or mesh or that no pinned
// owner signed; the Join that follows drops a service key an earlier join kept.
func (n *Node) Adopt(ctx context.Context, d e2e.Delegation) error {
	members, err := n.Members(ctx)
	if err != nil {
		return err
	}
	if err := n.ownDelegation(d, members); err != nil {
		return err
	}
	n.Service, n.servicePEM, n.Config.Delegation, n.Config.Tier = nil, nil, &d, e2e.TierService
	return nil
}

func (n *Node) ownDelegation(d e2e.Delegation, members map[string]e2e.Member) error {
	if !slices.Equal(d.InstancePub, n.Identity.PublicKey()) || d.Service == n.Identity.ID() {
		return fmt.Errorf("relay: the delegation is for %s, not this node %s", d.Instance, n.Identity.ID())
	}
	return ownerSigned(d, n.Config.Mesh, members)
}

func ownerSigned(d e2e.Delegation, mesh string, members map[string]e2e.Member) error {
	owner, ok := members[d.Service]
	switch {
	case d.Mesh != mesh || d.Instance != e2e.NodeID(d.InstancePub):
		return fmt.Errorf("relay: the delegation of %s in mesh %s is not one of mesh %s", d.Instance, d.Mesh, mesh)
	case !ok || owner.Tier != e2e.TierOwner:
		return fmt.Errorf("relay: the delegation of %s is by %s, no pinned owner", d.Instance, d.Service)
	case !e2e.VerifyDelegation(owner.PublicKey, d):
		return fmt.Errorf("relay: the delegation of %s by %s: %w", d.Instance, d.Service, e2e.ErrBadSignature)
	}
	return nil
}

func (n *Node) adoptRegistered(ctx context.Context, d *e2e.Delegation) error {
	held := n.Config.Delegation
	if d == nil || held == nil || n.Service != nil || slices.Equal(d.Sig, held.Sig) {
		return nil
	}
	expires, err := time.Parse(time.RFC3339Nano, d.Expires)
	if err != nil {
		return fmt.Errorf("relay: delegation expiry %q: %w", d.Expires, err)
	}
	if was, err := time.Parse(time.RFC3339Nano, held.Expires); err == nil && !expires.After(was) || !time.Now().Before(expires) {
		return fmt.Errorf("relay: the relay offers a delegation expiring %s, not a renewal of the one expiring %s", d.Expires, held.Expires)
	}
	members, err := n.Members(ctx)
	if err != nil {
		return err
	}
	if err := n.ownDelegation(*d, members); err != nil {
		return err
	}
	n.Config.Delegation = d
	return SaveConfig(n.Home, n.Config)
}

// Renew signs this instance a fresh delegation with the service key it kept
// and joins with it, once the one it holds is within renewWithin of expiring
// or past it, so a long-lived instance never loses the relay at its expiry. An
// instance an owner delegated holds no key to renew with: its owner renews it
// on the relay, and the instance adopts that delegation when it refreshes keys.
func (n *Node) Renew(ctx context.Context) error {
	held := n.Config.Delegation
	if held == nil || n.Service == nil {
		return nil
	}
	expires, err := time.Parse(time.RFC3339Nano, held.Expires)
	if err != nil {
		return fmt.Errorf("relay: delegation expiry %q: %w", held.Expires, err)
	}
	if time.Until(expires) > renewWithin {
		return nil
	}
	d, err := e2e.Delegate(n.Service, n.Config.Mesh, n.Identity.PublicKey(), held.Drives, held.Kinds, time.Now().Add(instanceLife))
	if err != nil {
		return err
	}
	n.Config.Delegation = &d
	return n.Join(ctx)
}

func (n *Node) client() Client {
	return Client{Config: n.Config, Identity: n.Identity, HTTP: n.HTTP}
}

func Peer(mesh string) string { return "relay:" + mesh }

const syncCursor = "relay-sync:"

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
	switch {
	case n.servicePEM != nil:
		if err := writeFile(serviceKeyPath(n.Home, n.Config.Mesh), n.servicePEM); err != nil {
			return fmt.Errorf("keep service key: %w", err)
		}
	case n.Config.Delegation != nil && n.Service == nil:
		if err := os.Remove(serviceKeyPath(n.Home, n.Config.Mesh)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("drop the kept service key: %w", err)
		}
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
	if err := n.adoptRegistered(ctx, kr.Delegation); err != nil {
		slog.Warn("relay delegation refused", "mesh", n.Config.Mesh, "err", err)
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

func (n *Node) shareWithIssued(ctx context.Context) error {
	if n.Config.Tier != e2e.TierOwner {
		return nil
	}
	issued, err := loadIssued(n.Home, n.Config.Mesh)
	if err != nil || len(issued) == 0 {
		return err
	}
	return n.Share(ctx)
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
	to := make([]e2e.Member, 0, len(mr.Members))
	for _, rm := range mr.Members {
		m, ok := local[rm.ID]
		if ok && rm.ID != n.Identity.ID() && slices.Equal(m.PublicKey, rm.Pub) {
			to = append(to, m)
		}
	}
	issued, err := loadIssued(n.Home, n.Config.Mesh)
	if err != nil {
		return nil, err
	}
	live := time.Now().Add(instanceSkew)
	for _, g := range issued {
		if !g.Revoked && live.Before(expiry(g.Delegation)) {
			to = append(to, e2e.Member{Node: g.Instance, PublicKey: g.InstancePub, Tier: e2e.TierService})
		}
	}
	for _, m := range to {
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
	if err := os.MkdirAll(dir(n.Home, n.Config.Mesh), 0o700); err != nil {
		return res, err
	}
	lock, err := os.OpenFile(filepath.Join(dir(n.Home, n.Config.Mesh), "push.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return res, err
	}
	defer func() { _ = lock.Close() }()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return res, fmt.Errorf("lock push: %w", err)
	}
	marks, err := n.Store.Watermarks(ctx)
	if err != nil {
		return res, err
	}
	rows, err := n.Store.Export(ctx, n.Store.Self(), marks[Peer(n.Config.Mesh)], 0)
	if err != nil || len(rows) == 0 {
		return res, err
	}
	for batch := range slices.Chunk(rows, pushBatch) {
		pr, sent, err := n.pushBatch(ctx, configs, batch)
		if errors.Is(err, errStaleEpoch) {
			if err := n.RefreshKeys(ctx); err != nil {
				return res, err
			}
			pr, sent, err = n.pushBatch(ctx, configs, batch)
		}
		if err != nil {
			return res, err
		}
		for _, rj := range pr.Rejected {
			r := sent[rj.OriginSeq]
			slog.Warn("relay refused record", "mesh", n.Config.Mesh, "origin_seq", rj.OriginSeq, "reason", rj.Reason)
			res.Rejected = append(res.Rejected, Rejection{ID: r.ID, Drive: r.Drive, OriginSeq: rj.OriginSeq, Reason: rj.Reason})
		}
		res.Pushed += len(pr.Accepted)
		if err := n.Store.Ack(ctx, Peer(n.Config.Mesh), batch[len(batch)-1].OriginSeq); err != nil {
			return res, err
		}
	}
	return res, nil
}

var errStaleEpoch = errors.New("relay: sealed under a superseded group key")

const ownerKeepsLocal = "every dotted kind, such as lr.note, lr.item, lr.escalation and board.card"

// relayRefuses names what the relay never takes from an owner: a dotted kind is
// a service's namespace there, so cci's typed lr.* records stay on this machine.
func (n *Node) relayRefuses(kind kinds.Kind) bool {
	return n.Config.Tier == e2e.TierOwner && strings.Contains(string(kind), ".")
}

func (n *Node) pushBatch(ctx context.Context, configs []Config, batch []store.Row) (pushResponse, map[int64]store.Row, error) {
	ring, err := LoadKeyring(n.Home, n.Config.Mesh)
	if err != nil {
		return pushResponse{}, nil, err
	}
	n.Keyring = ring
	members, err := n.Members(ctx)
	if err != nil {
		return pushResponse{}, nil, err
	}
	k, err := n.Keyring.Current(n.keyHolder(), n.Config.Mesh, members)
	if err != nil {
		return pushResponse{}, nil, err
	}
	var envs []e2e.Envelope
	sent := map[int64]store.Row{}
	for _, r := range batch {
		if Route(configs, r.Drive) != n.Config.Mesh || n.relayRefuses(r.Kind) {
			continue
		}
		e, err := SealRow(k, n.Identity, n.Config, r)
		if err != nil {
			return pushResponse{}, nil, err
		}
		envs = append(envs, e)
		sent[r.OriginSeq] = r
	}
	var pr pushResponse
	if len(envs) == 0 {
		return pr, sent, nil
	}
	if err := n.client().Call(ctx, http.MethodPost, "/v1/push", pushRequest{Mesh: n.Config.Mesh, Envelopes: envs}, &pr); err != nil {
		var se *StatusError
		if errors.As(err, &se) && se.Kind == "stale_epoch" {
			return pr, sent, fmt.Errorf("%w: %w", errStaleEpoch, err)
		}
		return pr, sent, err
	}
	return pr, sent, accounted(envs, pr)
}

func (n *Node) Sync(ctx context.Context, drive string) (store.Applied, error) {
	var total store.Applied
	origins, err := n.origins(ctx, drive)
	if err != nil {
		return total, err
	}
	l, err := n.logReader(ctx)
	if err != nil {
		return total, err
	}
	for i, origin := range origins {
		cursor := syncCursor + n.Config.Mesh + ":" + origin
		after, _, err := n.Store.Cursor(ctx, cursor, drive)
		if err != nil {
			return total, err
		}
		before := total
		readable := true
		for width, stride, more := 1, int64(logPage), true; more; width = logReaders {
			var through int64
			start := after
			used := stride
			for i, read := range l.pages(ctx, drive, origin, start, width, used) {
				if read.err != nil {
					return total, read.err
				}
				a, err := n.Store.Apply(ctx, read.rows, nil)
				if err != nil {
					return total, err
				}
				total.Inserted += a.Inserted
				total.Duplicate += a.Duplicate
				total.Buffered += a.Buffered
				total.Released += a.Released
				after = read.after
				if readable = readable && read.readable; readable {
					through = after
				}
				if i == 0 {
					stride = max(logPage, after-start)
				}
				if more = read.more; !more || start+int64(i+1)*used > after {
					break
				}
			}
			if through == 0 {
				continue
			}
			if err := n.Store.SetCursor(ctx, cursor, drive, through); err != nil && !errors.Is(err, store.ErrUnconfirmed) {
				return total, err
			}
		}
		if n.Progress != nil {
			_, _ = fmt.Fprintf(n.Progress, "origin %d of %d, %s: %d new, %d duplicate; %d new so far\n", i+1, len(origins), origin, total.Inserted-before.Inserted, total.Duplicate-before.Duplicate, total.Inserted)
		}
	}
	return total, nil
}

// Seed applies a drive's rows from the gzipped JSON lines ExportDrive wrote, and
// starts Sync where the copy stops, in one transaction. A copy cut off partway
// keeps every row it read, and Sync resumes from those; a cancelled seed keeps
// none. The caller trusts the node the copy came from: rows are applied unverified.
func (n *Node) Seed(ctx context.Context, drive string, from io.Reader) (store.Applied, error) {
	unzipped, err := gzip.NewReader(from)
	if err != nil {
		return store.Applied{}, fmt.Errorf("seed: %w", err)
	}
	through := map[string]int64{}
	decoder := json.NewDecoder(unzipped)
	var broken error
	rows := func(yield func(store.Row) bool) {
		for read := 1; ; read++ {
			var row store.Row
			if err := decoder.Decode(&row); err != nil {
				if !errors.Is(err, io.EOF) {
					broken = fmt.Errorf("seed: %w", err)
				}
				return
			}
			if row.Drive != drive {
				broken = fmt.Errorf("seed: %s/%d is on drive %s, not %s", row.Origin, row.OriginSeq, row.Drive, drive)
				return
			}
			through[row.Origin] = max(through[row.Origin], row.OriginSeq)
			if n.Progress != nil && read%seedProgress == 0 {
				_, _ = fmt.Fprintf(n.Progress, "read %d rows\n", read)
			}
			if !yield(row) {
				return
			}
		}
	}
	a, err := n.Store.ApplyAll(ctx, rows, func() []store.CursorMove {
		moves := make([]store.CursorMove, 0, len(through))
		for origin, seq := range through {
			moves = append(moves, store.CursorMove{Name: syncCursor + n.Config.Mesh + ":" + origin, Drive: drive, Seq: seq})
		}
		return moves
	})
	if err != nil {
		return store.Applied{}, errors.Join(err, broken)
	}
	return a, broken
}

// ExportDrive writes a drive's rows as gzipped JSON lines for Seed on another
// node, from the first record st took that was written at or after since; the
// zero since writes them all.
func ExportDrive(ctx context.Context, st *store.Store, drive string, since time.Time, to io.Writer) error {
	zipped := gzip.NewWriter(to)
	encoder := json.NewEncoder(zipped)
	if err := st.VisitDriveExports(ctx, drive, since, func(row store.Row) error { return encoder.Encode(row) }); err != nil {
		return err
	}
	return zipped.Close()
}

// Find reads the record num names on drive from the relay's log: an owner's
// record for a bare number, else the record of the origin num's tag starts.
func (n *Node) Find(ctx context.Context, drive string, num store.Number) (store.Row, bool, error) {
	l, err := n.logReader(ctx)
	if err != nil {
		return store.Row{}, false, err
	}
	return l.find(ctx, drive, num)
}

func (l logReader) find(ctx context.Context, drive string, num store.Number) (store.Row, bool, error) {
	origins, err := l.n.origins(ctx, drive)
	if err != nil {
		return store.Row{}, false, err
	}
	for _, origin := range origins {
		if num.Tag == "" && l.members[origin].Tier != e2e.TierOwner || num.Tag != "" && !strings.HasPrefix(origin, num.Tag) {
			continue
		}
		rows, _, _, _, err := l.page(ctx, drive, origin, num.Seq-1, 1)
		if err != nil {
			return store.Row{}, false, err
		}
		if len(rows) == 1 && rows[0].OriginSeq == num.Seq {
			return rows[0], true, nil
		}
	}
	return store.Row{}, false, nil
}

// Range reads origin's records on drive from the relay's log, oldest first,
// from origin_seq after up to before, or to the log's end when before is 0.
func (n *Node) Range(ctx context.Context, drive, origin string, after, before int64) ([]store.Row, error) {
	l, err := n.logReader(ctx)
	if err != nil {
		return nil, err
	}
	var out []store.Row
	for more := true; more && (before == 0 || after < before-1); {
		limit := logPage
		if before > 0 {
			limit = int(min(int64(limit), before-after-1))
		}
		var rows []store.Row
		if rows, after, more, _, err = l.page(ctx, drive, origin, after, limit); err != nil {
			return nil, err
		}
		for _, r := range rows {
			if before == 0 || r.OriginSeq < before {
				out = append(out, r)
			}
		}
	}
	return out, nil
}

func (n *Node) origins(ctx context.Context, drive string) ([]string, error) {
	var oo struct {
		Origins []struct {
			Origin string `json:"origin"`
		} `json:"origins"`
	}
	q := url.Values{"mesh": {n.Config.Mesh}, "drive": {drive}}
	if err := n.client().Call(ctx, http.MethodGet, "/v1/log/origins?"+q.Encode(), nil, &oo); err != nil {
		return nil, err
	}
	out := make([]string, len(oo.Origins))
	for i, o := range oo.Origins {
		out[i] = o.Origin
	}
	return out, nil
}

type logReader struct {
	n       *Node
	keys    map[uint64]e2e.GroupKey
	members map[string]e2e.Member
}

func (n *Node) logReader(ctx context.Context) (logReader, error) {
	if err := n.RefreshKeys(ctx); err != nil {
		return logReader{}, err
	}
	members, err := n.Members(ctx)
	if err != nil {
		return logReader{}, err
	}
	keys, err := n.Keyring.Keys(n.Config.Mesh, n.keyHolder(), members)
	if err != nil {
		slog.Warn("relay keyring holds an unusable wrap", "mesh", n.Config.Mesh, "err", err)
	}
	return logReader{n: n, keys: keys, members: members}, nil
}

type logRead struct {
	rows     []store.Row
	after    int64
	more     bool
	readable bool
	err      error
}

// pages reads width pages of origin's log at once and returns them oldest
// first. A page starts stride sequence numbers past the one before. Sync sets
// stride to the numbers its last page spanned, so an origin sparse on this
// drive reads no page twice, and stops at the first page that starts past where
// the one before ended, so a denser stretch leaves no hole.
func (l logReader) pages(ctx context.Context, drive, origin string, after int64, width int, stride int64) []logRead {
	reads := make([]logRead, width)
	var wg sync.WaitGroup
	for i := range reads {
		wg.Go(func() {
			r := &reads[i]
			r.rows, r.after, r.more, r.readable, r.err = l.page(ctx, drive, origin, after+int64(i)*stride, logPage)
		})
	}
	wg.Wait()
	return reads
}

func (l logReader) page(ctx context.Context, drive, origin string, after int64, limit int) ([]store.Row, int64, bool, bool, error) {
	var lr struct {
		Envelopes []e2e.Envelope `json:"envelopes"`
		More      bool           `json:"more"`
	}
	q := url.Values{
		"mesh": {l.n.Config.Mesh}, "drive": {drive}, "origin": {origin},
		"after": {strconv.FormatInt(after, 10)}, "limit": {strconv.Itoa(limit)},
	}
	if err := l.n.client().Call(ctx, http.MethodGet, "/v1/log?"+q.Encode(), nil, &lr); err != nil {
		return nil, after, false, false, err
	}
	var rows []store.Row
	for _, e := range lr.Envelopes {
		after = max(after, e.OriginSeq)
		r, err := l.n.open(e, l.keys, l.members)
		if err != nil {
			slog.Warn("relay log record skipped", "mesh", l.n.Config.Mesh, "drive", drive, "err", err)
			continue
		}
		rows = append(rows, r)
	}
	return rows, after, lr.More && len(lr.Envelopes) > 0, len(rows) == len(lr.Envelopes), nil
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
	if err := n.adoptRegistered(ctx, pr.Delegation); err != nil {
		slog.Warn("relay delegation refused", "mesh", n.Config.Mesh, "err", err)
	}
	if len(pr.Messages) == 0 {
		return store.Applied{}, n.fillAwaited(ctx)
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
	if err := n.fillAwaited(ctx); err != nil {
		return applied, err
	}
	acks = append(acks, rowAcks...)
	if len(acks) == 0 {
		return applied, nil
	}
	return applied, n.client().Call(ctx, http.MethodPost, "/v1/ack", ackRequest{Mesh: n.Config.Mesh, Receipts: acks, Tap: n.Config.Tap}, nil)
}

func (n *Node) fillAwaited(ctx context.Context) error {
	configs, err := Configs(n.Home)
	if err != nil {
		return err
	}
	awaited := func() ([]store.Wait, error) {
		waits, err := n.Store.Awaiting(ctx)
		return slices.DeleteFunc(waits, func(w store.Wait) bool { return Route(configs, w.Drive) != n.Config.Mesh }), err
	}
	waits, err := awaited()
	if err != nil || len(waits) == 0 {
		return err
	}
	l, err := n.logReader(ctx)
	if err != nil {
		return err
	}
	tried := map[store.Pointer]bool{}
	for {
		var found []store.Row
		for _, w := range waits {
			if tried[w.Pointer] || w.Seq < 1 {
				continue
			}
			tried[w.Pointer] = true
			row, ok, err := l.find(ctx, w.Drive, store.Number{Seq: w.Seq, Tag: w.Origin})
			if err != nil {
				return err
			}
			if ok {
				found = append(found, row)
			}
		}
		if len(found) == 0 {
			break
		}
		if _, err := n.Store.Apply(ctx, found, nil); err != nil {
			return err
		}
		if waits, err = awaited(); err != nil {
			return err
		}
	}
	absent := make([]store.Pointer, len(waits))
	for i, w := range waits {
		absent[i] = w.Pointer
	}
	_, err = n.Store.Settle(ctx, absent)
	return err
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
		if err := n.shareWithIssued(ctx); err != nil {
			slog.Warn("relay share with issued instances after a rekey failed", "mesh", n.Config.Mesh, "err", err)
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
	KeptLocal string        `json:"kept_local,omitempty"`
	Remote    *RemoteStatus `json:"remote,omitempty"`
	RemoteErr string        `json:"remote_error,omitempty"`
}

type ScopeReport struct {
	Mesh    string   `json:"mesh"`
	Node    string   `json:"node"`
	Tier    string   `json:"tier"`
	Drives  []string `json:"drives"`
	Kinds   []string `json:"kinds"`
	Expires string   `json:"expires,omitempty"`
}

func (n *Node) Scope(ctx context.Context) (ScopeReport, error) {
	rep := ScopeReport{Mesh: n.Config.Mesh, Node: n.Identity.ID(), Tier: n.Config.Tier}
	switch n.Config.Tier {
	case e2e.TierOwner:
		rep.Drives, rep.Kinds = []string{"*"}, []string{"*"}
		return rep, nil
	case e2e.TierPaired:
		rep.Drives, rep.Kinds = []string{"*"}, []string{}
		return rep, nil
	}
	enrolled, d := n.Identity.ID(), n.Config.Delegation
	if d != nil {
		enrolled, rep.Drives, rep.Kinds, rep.Expires = d.Service, d.Drives, d.Kinds, d.Expires
		if len(d.Kinds) > 0 {
			return rep, nil
		}
		if n.Service == nil {
			rep.Kinds = []string{"*"}
			return rep, nil
		}
	}
	var mr membersResponse
	if err := n.client().Call(ctx, http.MethodGet, "/v1/members?"+url.Values{"mesh": {n.Config.Mesh}}.Encode(), nil, &mr); err != nil {
		return ScopeReport{}, err
	}
	at := slices.IndexFunc(mr.Members, func(m RemoteMember) bool { return m.ID == enrolled })
	if at < 0 || mr.Members[at].Allow == nil {
		return ScopeReport{}, fmt.Errorf("relay: the relay lists no scope for service member %s", enrolled)
	}
	rep.Kinds = mr.Members[at].Allow.Kinds
	if d == nil {
		rep.Drives = mr.Members[at].Allow.Drives
	}
	return rep, nil
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
	if n.Config.Tier == e2e.TierOwner {
		rep.KeptLocal = ownerKeepsLocal
	}
	var rs RemoteStatus
	if err := n.client().Call(ctx, http.MethodGet, "/v1/status?"+url.Values{"mesh": {n.Config.Mesh}}.Encode(), nil, &rs); err != nil {
		rep.RemoteErr = err.Error()
	} else {
		rep.Remote = &rs
	}
	return rep, nil
}
