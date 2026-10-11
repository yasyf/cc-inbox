package mesh

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/synckit/delivery"
	"github.com/yasyf/synckit/syncservice"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/node"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

const (
	hostA = "yasyf@studio"
	hostB = "yasyf@forge"
)

type meshNode struct {
	st       *store.Store
	consumer *Consumer
	statuses []delivery.PeerStatus
}

func newMeshNode(t *testing.T) *meshNode {
	t.Helper()
	n := newUnendorsedNode(t)
	endorse(t, n.st)
	return n
}

func newUnendorsedNode(t *testing.T) *meshNode {
	t.Helper()
	t.Setenv(node.KeyEnv, "")
	st, _ := testutil.Store(t)
	n := &meshNode{st: st}
	n.consumer = &Consumer{st: st, home: st.HomeDir(), status: func(context.Context, string) ([]delivery.PeerStatus, error) {
		return n.statuses, nil
	}}
	return n
}

func endorse(t *testing.T, st *store.Store) {
	t.Helper()
	enclave, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sePub, err := enclave.PublicKey.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	var subject bytes.Buffer
	for _, arg := range []string{"cci", "node", "endorse", st.Node().Fingerprint()} {
		_ = binary.Write(&subject, binary.BigEndian, uint64(len(arg)))
		subject.WriteString(arg)
	}
	subject.WriteByte(0)
	argv := sha256.Sum256(subject.Bytes())
	nonce := bytes.Repeat([]byte{7}, 32)
	digest := sha256.Sum256(append(append([]byte{}, nonce...), argv[:]...))
	sig, err := ecdsa.SignASN1(rand.Reader, enclave, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(node.Endorsement{
		KeyID: node.FingerprintOf(sePub), PublicKey: base64.StdEncoding.EncodeToString(sePub),
		Nonce: base64.StdEncoding.EncodeToString(nonce), Sig: base64.StdEncoding.EncodeToString(sig),
	})
	if err := os.WriteFile(filepath.Join(node.Dir(st.HomeDir()), "endorsement.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func enroll(t *testing.T, at *meshNode, host string, peer *meshNode) {
	t.Helper()
	identity, err := peer.consumer.Identity(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := at.consumer.Enroll(t.Context(), syncservice.EnrollRequest{Host: host, Identity: identity}); err != nil {
		t.Fatal(err)
	}
}

func export(t *testing.T, from *meshNode, host string, since uint64) syncservice.ChangeEnvelope {
	t.Helper()
	change, err := from.consumer.Export(t.Context(), syncservice.ExportRequest{
		ServiceID: Service, SchemaFingerprint: from.st.SchemaFingerprint(), SinceRevision: syncservice.NewRevision(since),
	})
	if err != nil {
		t.Fatal(err)
	}
	bound, err := syncservice.BindDelivery(change, host)
	if err != nil {
		t.Fatal(err)
	}
	return bound
}

func remoteRows(t *testing.T, st *store.Store, origin string) []store.Record {
	t.Helper()
	rows, err := st.Query(t.Context(), store.Filter{Drive: "d", Origin: origin})
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestMeshReplicatesBothWaysWithResolvedReplies(t *testing.T) {
	a, b := newMeshNode(t), newMeshNode(t)
	enroll(t, a, hostB, b)
	enroll(t, b, hostA, a)
	ask := testutil.Post(t, a.st, store.Record{Lane: "root", Kind: kinds.Ask, Text: "who owns the relay?"})
	change := export(t, a, hostA, 0)
	if change.Kind != syncservice.ChangeSnapshot {
		t.Fatalf("first export = %s", change.Kind)
	}
	if ack, err := b.consumer.Apply(t.Context(), change); err != nil || ack.AckedRevision != change.SourceRevision {
		t.Fatalf("Apply = %+v, %v", ack, err)
	}
	if ack, err := b.consumer.Apply(t.Context(), change); err != nil || ack.AckedRevision != change.SourceRevision {
		t.Fatalf("replayed Apply = %+v, %v", ack, err)
	}
	onB := remoteRows(t, b.st, a.st.Self())
	if len(onB) != 1 || onB[0].Text != ask.Text {
		t.Fatalf("b holds %+v", onB)
	}
	testutil.Post(t, b.st, store.Record{Lane: "p3", Kind: kinds.Answer, Text: "p3 does", Re: onB[0].Seq, Resolves: onB[0].Seq})
	if _, err := a.consumer.Apply(t.Context(), export(t, b, hostB, 0)); err != nil {
		t.Fatal(err)
	}
	answers := remoteRows(t, a.st, b.st.Self())
	if len(answers) != 1 || answers[0].Resolves != ask.Seq {
		t.Fatalf("a holds %+v; want a reply resolving #%d", answers, ask.Seq)
	}
	follow := testutil.Post(t, a.st, store.Record{Lane: "root", Kind: kinds.Note, Text: "thanks"})
	since, _ := change.SourceRevision.Uint64()
	delta := export(t, a, hostA, since)
	if delta.Kind != syncservice.ChangeDelta || delta.BaseRevision != change.SourceRevision {
		t.Fatalf("second export = %s from %s", delta.Kind, delta.BaseRevision)
	}
	if _, err := b.consumer.Apply(t.Context(), delta); err != nil {
		t.Fatal(err)
	}
	if got := remoteRows(t, b.st, a.st.Self()); len(got) != 2 || got[1].Text != follow.Text {
		t.Fatalf("b after delta holds %+v", got)
	}
}

func TestMeshAsksForSnapshotWhenTheReceiptIsBehind(t *testing.T) {
	a, b := newMeshNode(t), newMeshNode(t)
	enroll(t, b, hostA, a)
	testutil.Post(t, a.st, store.Record{Kind: kinds.Note, Text: "one"})
	first, _ := export(t, a, hostA, 0).SourceRevision.Uint64()
	testutil.Post(t, a.st, store.Record{Kind: kinds.Note, Text: "two"})
	ack, err := b.consumer.Apply(t.Context(), export(t, a, hostA, first))
	if err != nil || !ack.NeedSnapshot {
		t.Fatalf("delta without a receipt = %+v, %v; want NeedSnapshot", ack, err)
	}
	if rows := remoteRows(t, b.st, a.st.Self()); len(rows) != 0 {
		t.Fatalf("a refused delta stored %+v", rows)
	}
}

func TestMeshRefusesUntrustedChanges(t *testing.T) {
	a, b, stranger := newMeshNode(t), newMeshNode(t), newMeshNode(t)
	testutil.Post(t, a.st, store.Record{Kind: kinds.Note, Text: "hello"})
	testutil.Post(t, stranger.st, store.Record{Kind: kinds.Note, Text: "forged"})
	tamper := func(t *testing.T, change syncservice.ChangeEnvelope, edit func(*body)) syncservice.ChangeEnvelope {
		t.Helper()
		var p payload
		if err := json.Unmarshal(change.Payload, &p); err != nil {
			t.Fatal(err)
		}
		var bd body
		if err := json.Unmarshal(p.Body, &bd); err != nil {
			t.Fatal(err)
		}
		edit(&bd)
		raw, _ := json.Marshal(bd)
		p.Body = raw
		encoded, _ := json.Marshal(p)
		out, err := syncservice.NewExportedChange(Service, change.SchemaFingerprint, change.Kind, change.BaseRevision, change.SourceRevision, encoded)
		if err != nil {
			t.Fatal(err)
		}
		bound, err := syncservice.BindDelivery(out, change.Origin)
		if err != nil {
			t.Fatal(err)
		}
		return bound
	}
	tests := []struct {
		name   string
		setup  func(t *testing.T)
		change func(t *testing.T) syncservice.ChangeEnvelope
		want   string
	}{
		{"unenrolled node", func(*testing.T) {}, func(t *testing.T) syncservice.ChangeEnvelope { return export(t, a, hostA, 0) }, "is not enrolled"},
		{"another host's origin", func(t *testing.T) { enroll(t, b, hostA, a) }, func(t *testing.T) syncservice.ChangeEnvelope { return export(t, a, hostB, 0) }, "not as the owner host"},
		{"paired tier", func(t *testing.T) {
			enroll(t, b, hostA, a)
			m, err := b.st.Member(t.Context(), a.st.Self())
			if err != nil {
				t.Fatal(err)
			}
			m.Tier = node.TierPaired
			if err := b.st.PutMember(t.Context(), m); err != nil {
				t.Fatal(err)
			}
		}, func(t *testing.T) syncservice.ChangeEnvelope { return export(t, a, hostA, 0) }, "not as the owner host"},
		{"edited body", func(t *testing.T) { enroll(t, b, hostA, a) }, func(t *testing.T) syncservice.ChangeEnvelope {
			return tamper(t, export(t, a, hostA, 0), func(bd *body) { bd.Rows[0].Text = "edited" })
		}, "signature"},
		{"stranger signs as an enrolled node", func(t *testing.T) { enroll(t, b, hostA, a) }, func(t *testing.T) syncservice.ChangeEnvelope {
			return tamper(t, export(t, stranger, hostA, 0), func(bd *body) { bd.Node = a.st.Self() })
		}, "signature"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b = newMeshNode(t)
			tt.setup(t)
			if _, err := b.consumer.Apply(t.Context(), tt.change(t)); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Apply = %v, want an error containing %q", err, tt.want)
			}
			if rows, err := b.st.Query(t.Context(), store.Filter{Drive: "d"}); err != nil || len(rows) != 0 {
				t.Fatalf("refused change stored %+v, %v", rows, err)
			}
		})
	}
}

func TestEnrollReplacesARekeyedHost(t *testing.T) {
	a, old, fresh := newMeshNode(t), newMeshNode(t), newMeshNode(t)
	enroll(t, a, hostB, old)
	enroll(t, a, hostB, fresh)
	members, err := a.st.Members(t.Context())
	if err != nil || len(members) != 1 || members[0].Node != fresh.st.Self() || members[0].Via != "mesh" || members[0].Host != hostB || members[0].Tier != node.TierOwner {
		t.Fatalf("members = %+v, %v", members, err)
	}
	self, err := a.consumer.Identity(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := a.consumer.Enroll(t.Context(), syncservice.EnrollRequest{Host: hostA, Identity: self}); err == nil {
		t.Fatal("Enroll accepted this node's own identity")
	}
	bare, err := newUnendorsedNode(t).consumer.Identity(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := a.consumer.Enroll(t.Context(), syncservice.EnrollRequest{Host: "yasyf@home", Identity: bare}); err == nil || !strings.Contains(err.Error(), "endorsement") {
		t.Fatalf("Enroll(unendorsed) = %v", err)
	}
}

func TestExportAcksEnrolledPeersAndBoundsTheSnapshot(t *testing.T) {
	a, b := newMeshNode(t), newMeshNode(t)
	enroll(t, a, hostB, b)
	var last store.Record
	for i := range 3 {
		last = testutil.Post(t, a.st, store.Record{Kind: kinds.Note, Text: strings.Repeat(string(rune('x'+i)), 300)})
	}
	a.statuses = []delivery.PeerStatus{{Peer: hostB, Acked: syncservice.NewRevision(2)}, {Peer: "yasyf@home", Acked: syncservice.NewRevision(3)}}
	export(t, a, hostA, 0)
	marks, err := a.st.Watermarks(t.Context())
	if err != nil || len(marks) != 1 || marks[b.st.Self()] != 2 {
		t.Fatalf("watermarks = %v, %v", marks, err)
	}
	rows, err := a.st.Export(t.Context(), a.st.Self(), 0, 0)
	if err != nil || len(rows) != 3 {
		t.Fatalf("export = %d rows, %v", len(rows), err)
	}
	one, _ := json.Marshal(rows[0])
	window, err := newestWithin(rows, 2*len(one)+2)
	if err != nil || len(window) != 2 || window[1].OriginSeq != last.Seq {
		t.Fatalf("newestWithin = %d rows ending %+v, %v", len(window), window, err)
	}
}

func TestPresenceAloneAdvancesTheRevision(t *testing.T) {
	a, b := newMeshNode(t), newMeshNode(t)
	enroll(t, b, hostA, a)
	testutil.Post(t, a.st, store.Record{Kind: kinds.Note, Text: "hello"})
	first := export(t, a, hostA, 0)
	if _, err := b.consumer.Apply(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	since, _ := first.SourceRevision.Uint64()
	if same := export(t, a, hostA, since); same.SourceRevision != first.SourceRevision {
		t.Fatalf("unchanged node moved its revision from %s to %s", first.SourceRevision, same.SourceRevision)
	}
	if _, err := a.st.Announce(t.Context(), store.Peer{Session: "s-1", Role: "root"}); err != nil {
		t.Fatal(err)
	}
	next := export(t, a, hostA, since)
	if next.Kind != syncservice.ChangeDelta || next.SourceRevision == first.SourceRevision {
		t.Fatalf("presence change exported %s at %s, want a newer delta", next.Kind, next.SourceRevision)
	}
	if _, err := b.consumer.Apply(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	peers, err := b.st.Peers(t.Context())
	if err != nil || len(peers) != 1 || peers[0].Session != "s-1" || peers[0].Origin != a.st.Self() {
		t.Fatalf("b sees %+v, %v", peers, err)
	}
}

func TestUnknownRevisionGetsAFullSnapshot(t *testing.T) {
	a := newMeshNode(t)
	testutil.Post(t, a.st, store.Record{Kind: kinds.Note, Text: "one"})
	testutil.Post(t, a.st, store.Record{Kind: kinds.Note, Text: "two"})
	change := export(t, a, hostA, 12345)
	var p payload
	var bd body
	if err := json.Unmarshal(change.Payload, &p); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(p.Body, &bd); err != nil {
		t.Fatal(err)
	}
	if change.Kind != syncservice.ChangeSnapshot || len(bd.Rows) != 2 {
		t.Fatalf("export from an unknown revision = %s with %d rows", change.Kind, len(bd.Rows))
	}
}
