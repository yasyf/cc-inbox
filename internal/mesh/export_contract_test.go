package mesh

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/node"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
	"github.com/yasyf/synckit/syncservice"
)

func compareMeshExportWithLegacy(t *testing.T, n *meshNode, since uint64) syncservice.ChangeEnvelope {
	t.Helper()
	path := filepath.Join(n.st.HomeDir(), "mesh", "revisions.json")
	prior, readErr := os.ReadFile(path)
	if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
		t.Fatal(readErr)
	}
	request := syncservice.ExportRequest{ServiceID: Service, SchemaFingerprint: n.st.SchemaFingerprint(), SinceRevision: syncservice.NewRevision(since)}
	old, oldErr := n.consumer.legacyExport(t.Context(), request)
	oldLog, oldLogErr := os.ReadFile(path)
	if readErr == nil {
		if err := os.WriteFile(path, prior, 0o600); err != nil {
			t.Fatal(err)
		}
	} else if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	current, currentErr := n.consumer.Export(t.Context(), request)
	currentLog, currentLogErr := os.ReadFile(path)
	if fmt.Sprint(oldErr) != fmt.Sprint(currentErr) {
		t.Fatalf("legacy error=%v current=%v", oldErr, currentErr)
	}
	if !bytes.Equal(oldLog, currentLog) || errors.Is(oldLogErr, os.ErrNotExist) != errors.Is(currentLogErr, os.ErrNotExist) {
		t.Fatalf("revision publication frontier changed: old=%s/%v current=%s/%v", oldLog, oldLogErr, currentLog, currentLogErr)
	}
	if oldErr != nil {
		return current
	}
	if old.Kind != current.Kind || old.BaseRevision != current.BaseRevision || old.SourceRevision != current.SourceRevision || old.SchemaFingerprint != current.SchemaFingerprint {
		t.Fatalf("wire revisions/kind changed: old=%+v current=%+v", old, current)
	}
	var oldPayload, currentPayload payload
	if err := json.Unmarshal(old.Payload, &oldPayload); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(current.Payload, &currentPayload); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(oldPayload.Body, currentPayload.Body) {
		t.Fatalf("unsigned canonical payload bytes differ: old=%s current=%s", oldPayload.Body, currentPayload.Body)
	}
	for _, change := range []syncservice.ChangeEnvelope{old, current} {
		var signedPayload payload
		if err := json.Unmarshal(change.Payload, &signedPayload); err != nil {
			t.Fatal(err)
		}
		if !node.Verify(n.st.Node().PublicKey(), signed(change.Kind, change.BaseRevision, change.SourceRevision, signedPayload.Body), signedPayload.Sig) {
			t.Fatal("selected payload signature is invalid")
		}
	}
	return current
}

func TestIndexedMeshExportPreservesLegacyContract(t *testing.T) {
	for _, edit := range []string{"clean", "refs", "fields", "recipients", "also_origins", "body", "historical_refs", "digest_and_foreign"} {
		t.Run(edit, func(t *testing.T) {
			n := newMeshNode(t)
			first := testutil.Post(t, n.st, store.Record{Kind: kinds.Ask, Text: "question"})
			testutil.Post(t, n.st, store.Record{Kind: kinds.Answer, Text: "answer", Re: first.Seq, Resolves: first.Seq, To: []string{"peer:reader"}})
			last := testutil.Post(t, n.st, store.Record{Kind: kinds.Note, Text: "latest", Refs: store.Refs{Targets: []string{"api"}}})
			initial := compareMeshExportWithLegacy(t, n, 0)
			since, err := initial.SourceRevision.Uint64()
			if err != nil {
				t.Fatal(err)
			}
			compareMeshExportWithLegacy(t, n, since)
			if _, err := n.st.Announce(t.Context(), store.Peer{Session: "presence", Role: "root"}); err != nil {
				t.Fatal(err)
			}
			compareMeshExportWithLegacy(t, n, since)
			if edit == "digest_and_foreign" {
				testutil.Post(t, n.st, store.Record{Kind: kinds.Digest, Text: "excluded digest"})
				if _, err := n.st.Apply(t.Context(), []store.Row{{Origin: "foreign", OriginSeq: 900000, Drive: "d", Lane: "foreign", Kind: kinds.Note, At: n.st.Now(), Text: "foreign", Source: "post"}}, nil); err != nil {
					t.Fatal(err)
				}
				compareMeshExportWithLegacy(t, n, since)
				return
			}
			setters := map[string]string{"refs": "refs='{'", "fields": "fields='{'", "recipients": "recipients='[1]'", "also_origins": "also_resolves_origin='{'", "body": "body='{'", "historical_refs": "refs='{'"}
			if set := setters[edit]; set != "" {
				if edit != "historical_refs" {
					if err := os.Remove(filepath.Join(n.st.HomeDir(), "mesh", "revisions.json")); err != nil {
						t.Fatal(err)
					}
				}
				db := openMeshFixtureDB(t, n.st)
				if _, err := db.ExecContext(t.Context(), "UPDATE records SET "+set+" WHERE seq=?", last.Seq); err != nil {
					t.Fatal(err)
				}
				compareMeshExportWithLegacy(t, n, 0)
			} else {
				testutil.Post(t, n.st, store.Record{Kind: kinds.Note, Text: "next"})
				compareMeshExportWithLegacy(t, n, since)
				compareMeshExportWithLegacy(t, n, ^uint64(0))
			}
		})
	}
}

func TestIndexedMeshExportStopsAtSelectedBoundary(t *testing.T) {
	n := newMeshNode(t)
	for i := range 9 {
		testutil.Post(t, n.st, store.Record{Kind: kinds.Note, Text: fmt.Sprint(i)})
	}
	for _, budget := range []int{-1, 0, 1, 200, 400, 800, 10000} {
		all, err := n.st.Export(t.Context(), n.st.Self(), 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		want, err := newestWithin(all, budget)
		if err != nil {
			t.Fatal(err)
		}
		got, trimmed, err := n.consumer.newestExport(t.Context(), 0, budget)
		a, _ := json.Marshal(nonNil(want))
		b, _ := json.Marshal(nonNil(got))
		if err != nil || !bytes.Equal(a, b) || trimmed != (len(want) < len(all)) {
			t.Fatalf("budget=%d old=%s new=%s trimmed=%v error=%v", budget, a, b, trimmed, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := n.consumer.newestExport(ctx, 0, 1000); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled prefix=%v", err)
	}
}

func TestIndexedMeshExportDoesNotDecodeUnselectedHistory(t *testing.T) {
	n := newMeshNode(t)
	old := testutil.Post(t, n.st, store.Record{Kind: kinds.Note, Text: "private old record"})
	testutil.Post(t, n.st, store.Record{Kind: kinds.Note, Text: strings.Repeat("overflow", 100)})
	latest := testutil.Post(t, n.st, store.Record{Kind: kinds.Note, Text: ""})
	rows, err := n.st.Export(t.Context(), n.st.Self(), latest.Seq-1, 1)
	if err != nil || len(rows) != 1 {
		t.Fatalf("latest fixture=%v/%v", rows, err)
	}
	raw, err := json.Marshal(rows[0])
	if err != nil {
		t.Fatal(err)
	}
	presence, _, err := n.consumer.presence(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(presence)
	if err != nil {
		t.Fatal(err)
	}
	db := openMeshFixtureDB(t, n.st)
	if _, err := db.ExecContext(t.Context(), "UPDATE records SET text=? WHERE seq=?", strings.Repeat("s", payloadBudget-len(encoded)-len(raw)-32), latest.Seq); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), "UPDATE records SET refs='{' WHERE seq=?", old.Seq); err != nil {
		t.Fatal(err)
	}
	request := syncservice.ExportRequest{ServiceID: Service, SchemaFingerprint: n.st.SchemaFingerprint(), SinceRevision: syncservice.NewRevision(0)}
	if _, err := n.consumer.legacyExport(t.Context(), request); err == nil || !strings.Contains(err.Error(), "decode refs") {
		t.Fatalf("immutable global export error=%v", err)
	}
	change, err := n.consumer.Export(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	var signedPayload payload
	if err := json.Unmarshal(change.Payload, &signedPayload); err != nil {
		t.Fatal(err)
	}
	var exported body
	if err := json.Unmarshal(signedPayload.Body, &exported); err != nil {
		t.Fatal(err)
	}
	if len(exported.Rows) != 1 || exported.Rows[0].OriginSeq != latest.Seq || change.Kind != syncservice.ChangeSnapshot {
		t.Fatalf("selected prefix changed: %+v", exported.Rows)
	}
	if !node.Verify(n.st.Node().PublicKey(), signed(change.Kind, change.BaseRevision, change.SourceRevision, signedPayload.Body), signedPayload.Sig) {
		t.Fatal("selected payload signature invalid")
	}
	if _, err := db.ExecContext(t.Context(), "UPDATE records SET refs='{' WHERE seq=?", latest.Seq); err != nil {
		t.Fatal(err)
	}
	if _, err := n.consumer.Export(t.Context(), request); err == nil || !strings.Contains(err.Error(), "decode refs") {
		t.Fatalf("selected corruption lost: %v", err)
	}
}

func openMeshFixtureDB(t *testing.T, st *store.Store) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(st.HomeDir(), store.DBFile))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}
