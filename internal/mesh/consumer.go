package mesh

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/yasyf/synckit/delivery"
	"github.com/yasyf/synckit/syncservice"

	"github.com/yasyf/cc-inbox/internal/node"
	"github.com/yasyf/cc-inbox/internal/store"
)

const (
	Service    = "cci"
	Channel    = "mesh"
	itemID     = "records"
	via        = "mesh"
	enrolledBy = "synckitd host add"
	signDomain = "cci.mesh.v1"
	rowBudget  = 6 << 20
)

type body struct {
	Node     string         `json:"node"`
	Rows     []store.Row    `json:"rows"`
	Presence store.Presence `json:"presence"`
}

type payload struct {
	Body json.RawMessage `json:"body"`
	Sig  []byte          `json:"sig"`
}

type identity struct {
	Node        string `json:"node"`
	Fingerprint string `json:"fingerprint"`
	PublicKey   []byte `json:"public_key"`
}

type Consumer struct {
	st     *store.Store
	home   string
	status func(ctx context.Context, serviceID string) ([]delivery.PeerStatus, error)
}

func NewConsumer(st *store.Store, home string) *Consumer {
	return &Consumer{st: st, home: home, status: delivery.Status}
}

var _ syncservice.Enroller = (*Consumer)(nil)

func (c *Consumer) Capabilities(context.Context) (syncservice.Capabilities, error) {
	caps := syncservice.DefaultCapabilities(Service)
	caps.Methods = append(caps.Methods, syncservice.EnrollMethods...)
	return caps, nil
}

func (c *Consumer) List(ctx context.Context) ([]syncservice.WatchItem, error) {
	seq, err := c.st.MaxSeq(ctx)
	if err != nil {
		return nil, err
	}
	return []syncservice.WatchItem{{ID: itemID, WatchDirs: []string{c.home}, Fingerprint: strconv.FormatInt(seq, 10)}}, nil
}

func (c *Consumer) Reconcile(context.Context, string) (syncservice.ReconcileResult, error) {
	return syncservice.ReconcileResult{}, nil
}

func (c *Consumer) Export(ctx context.Context, request syncservice.ExportRequest) (syncservice.ChangeEnvelope, error) {
	if err := c.checkService(request.ServiceID, request.SchemaFingerprint); err != nil {
		return syncservice.ChangeEnvelope{}, err
	}
	since, err := request.SinceRevision.Uint64()
	if err != nil {
		return syncservice.ChangeEnvelope{}, err
	}
	if err := c.ackPeers(ctx); err != nil {
		return syncservice.ChangeEnvelope{}, err
	}
	rows, err := c.st.Export(ctx, c.st.Self(), int64(since), 0)
	if err != nil {
		return syncservice.ChangeEnvelope{}, err
	}
	source := since
	if len(rows) > 0 {
		source = uint64(rows[len(rows)-1].OriginSeq)
	}
	if source == 0 {
		return syncservice.ChangeEnvelope{}, errors.New("mesh: this node has written no records to export")
	}
	kind, base := syncservice.ChangeDelta, since
	window, err := newestWithin(rows, rowBudget)
	if err != nil {
		return syncservice.ChangeEnvelope{}, err
	}
	floor, err := c.st.CompactedThrough(ctx)
	if err != nil {
		return syncservice.ChangeEnvelope{}, err
	}
	if since == 0 || int64(since) < floor || len(window) < len(rows) {
		kind, base, rows = syncservice.ChangeSnapshot, 0, window
	}
	presence, err := c.st.ExportPresence(ctx, node.TierOwner)
	if err != nil {
		return syncservice.ChangeEnvelope{}, err
	}
	raw, err := json.Marshal(body{Node: c.st.Self(), Rows: nonNil(rows), Presence: presence})
	if err != nil {
		return syncservice.ChangeEnvelope{}, fmt.Errorf("mesh: encode change: %w", err)
	}
	baseRev, sourceRev := syncservice.NewRevision(base), syncservice.NewRevision(source)
	sig, err := c.st.Node().Sign(signed(kind, baseRev, sourceRev, raw))
	if err != nil {
		return syncservice.ChangeEnvelope{}, err
	}
	encoded, err := json.Marshal(payload{Body: raw, Sig: sig})
	if err != nil {
		return syncservice.ChangeEnvelope{}, fmt.Errorf("mesh: encode change: %w", err)
	}
	return syncservice.NewExportedChange(Service, c.st.SchemaFingerprint(), kind, baseRev, sourceRev, encoded)
}

func (c *Consumer) ackPeers(ctx context.Context) error {
	statuses, err := c.status(ctx, Service)
	if err != nil {
		return fmt.Errorf("mesh: read delivery status: %w", err)
	}
	members, err := c.st.Members(ctx)
	if err != nil {
		return err
	}
	hosts := map[string]string{}
	for _, m := range members {
		if m.Via == via {
			hosts[m.Host] = m.Node
		}
	}
	for _, s := range statuses {
		peer, ok := hosts[s.Peer]
		if !ok {
			continue
		}
		acked, err := s.Acked.Uint64()
		if err != nil {
			return err
		}
		if err := c.st.Ack(ctx, peer, int64(acked)); err != nil {
			return err
		}
	}
	return nil
}

func newestWithin(rows []store.Row, budget int) ([]store.Row, error) {
	used := 0
	for i := len(rows) - 1; i >= 0; i-- {
		raw, err := json.Marshal(rows[i])
		if err != nil {
			return nil, fmt.Errorf("mesh: encode row: %w", err)
		}
		if used += len(raw) + 1; used > budget {
			return rows[i+1:], nil
		}
	}
	return rows, nil
}

func (c *Consumer) Apply(ctx context.Context, change syncservice.ChangeEnvelope) (syncservice.ApplyResult, error) {
	if err := c.checkService(change.ServiceID, change.SchemaFingerprint); err != nil {
		return syncservice.ApplyResult{}, err
	}
	b, err := c.verify(ctx, change)
	if err != nil {
		return syncservice.ApplyResult{}, err
	}
	held, ok, err := c.st.Receipt(ctx, Channel, b.Node)
	if err != nil {
		return syncservice.ApplyResult{}, err
	}
	var fence *syncservice.Receipt
	if ok {
		fence = &syncservice.Receipt{Origin: change.Origin, ChangeID: held.ChangeID, Revision: syncservice.Revision(held.Revision), PayloadDigest: held.Digest}
	}
	decision, ack, err := syncservice.Fence(fence, change)
	if err != nil || decision != syncservice.FenceApply {
		return ack, err
	}
	receipt := change.Receipt()
	if _, err := c.st.Apply(ctx, b.Rows, &store.Receipt{
		Channel: Channel, Origin: b.Node, ChangeID: receipt.ChangeID, Revision: string(receipt.Revision), Digest: receipt.PayloadDigest,
	}); err != nil {
		return syncservice.ApplyResult{}, err
	}
	if err := c.st.ApplyPresence(ctx, b.Presence); err != nil {
		return syncservice.ApplyResult{}, err
	}
	return syncservice.ApplyResult{AckedRevision: change.SourceRevision}, nil
}

func (c *Consumer) verify(ctx context.Context, change syncservice.ChangeEnvelope) (body, error) {
	var p payload
	if err := json.Unmarshal(change.Payload, &p); err != nil {
		return body{}, fmt.Errorf("mesh: decode change from %s: %w", change.Origin, err)
	}
	var b body
	if err := json.Unmarshal(p.Body, &b); err != nil {
		return body{}, fmt.Errorf("mesh: decode change from %s: %w", change.Origin, err)
	}
	m, err := c.st.Member(ctx, b.Node)
	if errors.Is(err, sql.ErrNoRows) {
		return body{}, fmt.Errorf("mesh: node %s from %s is not enrolled; run `synckitd host enroll %s`", b.Node, change.Origin, change.Origin)
	}
	if err != nil {
		return body{}, err
	}
	if m.Tier != node.TierOwner || m.Via != via || m.Host != change.Origin {
		return body{}, fmt.Errorf("mesh: node %s is enrolled as %s via %s for %q, not as the owner host %s", b.Node, m.Tier, m.Via, m.Host, change.Origin)
	}
	if !node.Verify(m.PublicKey, signed(change.Kind, change.BaseRevision, change.SourceRevision, p.Body), p.Sig) {
		return body{}, fmt.Errorf("mesh: change from %s does not carry node %s's signature", change.Origin, b.Node)
	}
	if b.Presence.Origin != b.Node {
		return body{}, fmt.Errorf("mesh: node %s sent presence for %s", b.Node, b.Presence.Origin)
	}
	for _, r := range b.Rows {
		if r.Origin != b.Node {
			return body{}, fmt.Errorf("mesh: node %s sent row %s/%d written by another node", b.Node, r.Origin, r.OriginSeq)
		}
	}
	return b, nil
}

func (c *Consumer) Identity(context.Context) ([]byte, error) {
	n := c.st.Node()
	return json.Marshal(identity{Node: n.ID(), Fingerprint: n.Fingerprint(), PublicKey: n.PublicKey()})
}

func (c *Consumer) Enroll(ctx context.Context, request syncservice.EnrollRequest) error {
	var id identity
	if err := json.Unmarshal(request.Identity, &id); err != nil {
		return fmt.Errorf("mesh: decode identity of %s: %w", request.Host, err)
	}
	if id.Node != node.IDOf(id.PublicKey) || id.Fingerprint != node.FingerprintOf(id.PublicKey) {
		return fmt.Errorf("mesh: identity of %s does not match its public key", request.Host)
	}
	if id.Node == c.st.Self() {
		return fmt.Errorf("mesh: %s reports this node's own identity", request.Host)
	}
	members, err := c.st.Members(ctx)
	if err != nil {
		return err
	}
	for _, m := range members {
		if m.Via != via || m.Host != request.Host || m.Node == id.Node {
			continue
		}
		if _, err := c.st.DropMember(ctx, m.Node); err != nil {
			return err
		}
	}
	if err := c.st.PutMember(ctx, store.Member{Node: id.Node, PublicKey: id.PublicKey, Tier: node.TierOwner, Via: via, Host: request.Host, By: enrolledBy}); err != nil {
		return err
	}
	return c.st.Ack(ctx, id.Node, 0)
}

func (c *Consumer) checkService(serviceID, fingerprint string) error {
	if serviceID != Service || fingerprint != c.st.SchemaFingerprint() {
		return fmt.Errorf("mesh: change for %s at schema %s; this cci serves %s at schema %s", serviceID, fingerprint, Service, c.st.SchemaFingerprint())
	}
	return nil
}

func signed(kind syncservice.ChangeKind, base, source syncservice.Revision, raw []byte) []byte {
	return append([]byte(strings.Join([]string{signDomain, string(kind), string(base), string(source)}, "\n")+"\n"), raw...)
}

func nonNil(rows []store.Row) []store.Row {
	if rows == nil {
		return []store.Row{}
	}
	return rows
}
