package mesh

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"modernc.org/sqlite"

	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/synckit/syncservice"
)

var exportCounterID atomic.Uint64

func TestMeshExportHistoryBaseline(t *testing.T) {
	for _, population := range []int{32, 10000} {
		t.Run(fmt.Sprint(population), func(t *testing.T) {
			var reads atomic.Int64
			function := fmt.Sprintf("cci_mesh_export_text_%d", exportCounterID.Add(1))
			if err := sqlite.RegisterDeterministicScalarFunction(function, 1, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
				reads.Add(1)
				return args[0], nil
			}); err != nil {
				t.Fatal(err)
			}
			n := newMeshNode(t)
			ctx := t.Context()
			db, err := sql.Open("sqlite", filepath.Join(n.st.HomeDir(), store.DBFile))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			if _, err := db.ExecContext(ctx, `WITH RECURSIVE n(x) AS(VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<?)
INSERT INTO records(seq,origin,origin_seq,drive,lane,kind,at,text,source,hash)
SELECT x,?,x,'d','history','note',x,?,'post','mesh-history-'||x FROM n`, population, n.st.Self(), strings.Repeat("old", 80)); err != nil {
				t.Fatal(err)
			}
			last := int64(population + 1)
			if _, err := db.ExecContext(ctx, `INSERT INTO records(seq,origin,origin_seq,drive,lane,kind,at,text,source,hash) VALUES(?,?,?,'d','selected','note',1,'','post','mesh-selected')`, last, n.st.Self(), last); err != nil {
				t.Fatal(err)
			}
			selected, err := n.st.Export(ctx, n.st.Self(), int64(population), 1)
			if err != nil || len(selected) != 1 {
				t.Fatalf("selected fixture=%v/%v", selected, err)
			}
			empty, err := json.Marshal(selected[0])
			if err != nil {
				t.Fatal(err)
			}
			presence, _, err := n.consumer.presence(ctx)
			if err != nil {
				t.Fatal(err)
			}
			encodedPresence, err := json.Marshal(presence)
			if err != nil {
				t.Fatal(err)
			}
			textBytes := payloadBudget - len(encodedPresence) - len(empty) - 32
			if _, err := db.ExecContext(ctx, "UPDATE records SET text=? WHERE seq=?", strings.Repeat("s", textBytes), last); err != nil {
				t.Fatal(err)
			}
			for _, statement := range []string{"ALTER TABLE records RENAME COLUMN text TO counted_text", "ALTER TABLE records ADD COLUMN text TEXT GENERATED ALWAYS AS (" + function + "(counted_text)) VIRTUAL"} {
				if _, err := db.ExecContext(ctx, statement); err != nil {
					t.Fatal(err)
				}
			}
			invoke := func(since syncservice.Revision, wantKind syncservice.ChangeKind, wantRows int, wantReads int64) syncservice.ChangeEnvelope {
				t.Helper()
				reads.Store(0)
				change, err := n.consumer.Export(ctx, syncservice.ExportRequest{ServiceID: Service, SchemaFingerprint: n.st.SchemaFingerprint(), SinceRevision: since})
				if err != nil {
					t.Fatal(err)
				}
				count := reads.Load()
				var signedPayload payload
				if err := json.Unmarshal(change.Payload, &signedPayload); err != nil {
					t.Fatal(err)
				}
				var exported body
				if err := json.Unmarshal(signedPayload.Body, &exported); err != nil {
					t.Fatal(err)
				}
				if change.Kind != wantKind || len(exported.Rows) != wantRows || count != wantReads {
					t.Fatalf("actualConsumerExport population=%d since=%s kind=%s rows=%d text_reads=%d want=%s/%d/%d", population, since, change.Kind, len(exported.Rows), count, wantKind, wantRows, wantReads)
				}
				if wantRows == 1 && (exported.Rows[0].OriginSeq != last || len(exported.Rows[0].Text) != textBytes) {
					t.Fatalf("selected suffix changed: seq=%d textbytes=%d", exported.Rows[0].OriginSeq, len(exported.Rows[0].Text))
				}
				if len(change.Payload) > syncservice.MaxTransferPayload {
					t.Fatal("fixture exceeded actual payload bound")
				}
				t.Logf("MESH_EXPORT_BASELINE history=%d requested_since=%s selected_rows=%d actual_decoded_text_reads=%d payload_bytes=%d source_revision=%s", population, since, len(exported.Rows), count, len(change.Payload), change.SourceRevision)
				return change
			}
			coldReads, warmReads := int64(4), int64(2)
			if os.Getenv("CCI_MESH_EXPECT_GLOBAL") == "1" {
				coldReads, warmReads = int64(2*(population+1)), int64(population+1)
			}
			first := invoke(syncservice.NewRevision(0), syncservice.ChangeSnapshot, 1, coldReads)
			again := invoke(syncservice.NewRevision(0), syncservice.ChangeSnapshot, 1, warmReads)
			if again.SourceRevision != first.SourceRevision {
				t.Fatal("unchanged snapshot advanced revision")
			}
			invoke(first.SourceRevision, syncservice.ChangeDelta, 0, 0)
		})
	}
}
