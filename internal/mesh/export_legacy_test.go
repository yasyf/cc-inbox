package mesh

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/yasyf/synckit/syncservice"
)

func (c *Consumer) legacyExport(ctx context.Context, request syncservice.ExportRequest) (syncservice.ChangeEnvelope, error) {
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
