package mesh

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
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
	Service       = "cci"
	Channel       = "mesh"
	itemID        = "records"
	via           = "mesh"
	enrolledBy    = "synckitd host add"
	signDomain    = "cci.mesh.v1"
	payloadBudget = syncservice.MaxTransferPayload - 1<<20
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
	Node        string            `json:"node"`
	Fingerprint string            `json:"fingerprint"`
	PublicKey   []byte            `json:"public_key"`
	Endorsement *node.Endorsement `json:"endorsement,omitempty"`
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
	_, digest, err := c.presence(ctx)
	if err != nil {
		return nil, err
	}
	return []syncservice.WatchItem{{ID: itemID, WatchDirs: []string{c.home}, Fingerprint: strconv.FormatInt(seq, 10) + ":" + digest}}, nil
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
	presence, digest, err := c.presence(ctx)
	if err != nil {
		return syncservice.ChangeEnvelope{}, err
	}
	log, err := openRevisions(c.home)
	if err != nil {
		return syncservice.ChangeEnvelope{}, err
	}
	head, _ := log.head()
	newer, err := c.st.Export(ctx, c.st.Self(), head.Seq, 0)
	if err != nil {
		return syncservice.ChangeEnvelope{}, err
	}
	seq := head.Seq
	if len(newer) > 0 {
		seq = newer[len(newer)-1].OriginSeq
	}
	if head, err = log.advance(seq, digest, c.st.Now()); err != nil {
		return syncservice.ChangeEnvelope{}, err
	}
	floor, err := c.st.CompactedThrough(ctx)
	if err != nil {
		return syncservice.ChangeEnvelope{}, err
	}
	kind, base, after := syncservice.ChangeDelta, since, int64(0)
	if prior, ok := log.at(since); ok && since != 0 && prior.Seq >= floor {
		after = prior.Seq
	} else {
		kind, base = syncservice.ChangeSnapshot, 0
	}
	rows, err := c.st.Export(ctx, c.st.Self(), after, 0)
	if err != nil {
		return syncservice.ChangeEnvelope{}, err
	}
	encodedPresence, err := json.Marshal(presence)
	if err != nil {
		return syncservice.ChangeEnvelope{}, fmt.Errorf("mesh: encode presence: %w", err)
	}
	window, err := newestWithin(rows, payloadBudget-len(encodedPresence))
	if err != nil {
		return syncservice.ChangeEnvelope{}, err
	}
	if len(window) < len(rows) {
		kind, base = syncservice.ChangeSnapshot, 0
	}
	raw, err := json.Marshal(body{Node: c.st.Self(), Rows: nonNil(window), Presence: presence})
	if err != nil {
		return syncservice.ChangeEnvelope{}, fmt.Errorf("mesh: encode change: %w", err)
	}
	baseRev, sourceRev := syncservice.NewRevision(base), syncservice.NewRevision(head.Rev)
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

func (c *Consumer) presence(ctx context.Context) (store.Presence, string, error) {
	p, err := c.st.ExportPresence(ctx, node.TierOwner)
	if err != nil {
		return store.Presence{}, "", err
	}
	raw, err := json.Marshal([]any{p.Peers, p.Claims})
	if err != nil {
		return store.Presence{}, "", fmt.Errorf("mesh: encode presence: %w", err)
	}
	sum := sha256.Sum256(raw)
	return p, hex.EncodeToString(sum[:]), nil
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
	log, err := openRevisions(c.home)
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
		acked, err := log.acked(s.Acked)
		if err != nil {
			return err
		}
		if err := c.st.Ack(ctx, peer, acked); err != nil {
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
	if err := c.st.ApplyPresence(ctx, b.Presence); err != nil {
		return syncservice.ApplyResult{}, err
	}
	receipt := change.Receipt()
	if _, err := c.st.Apply(ctx, b.Rows, &store.Receipt{
		Channel: Channel, Origin: b.Node, ChangeID: receipt.ChangeID, Revision: string(receipt.Revision), Digest: receipt.PayloadDigest,
	}); err != nil {
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
	n, err := node.Load(c.home)
	if err != nil {
		return nil, err
	}
	id := identity{Node: n.ID(), Fingerprint: n.Fingerprint(), PublicKey: n.PublicKey()}
	if e, ok := n.Endorsement(); ok {
		id.Endorsement = &e
	}
	return json.Marshal(id)
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
	if id.Endorsement == nil {
		return fmt.Errorf("mesh: node %s on %s has no Secure Enclave endorsement; run `cci node endorse` there, then `synckitd host enroll %s` here", id.Node, request.Host, request.Host)
	}
	if err := node.VerifyEndorsement(*id.Endorsement, id.PublicKey); err != nil {
		return fmt.Errorf("mesh: endorsement of node %s on %s: %w", id.Node, request.Host, err)
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
