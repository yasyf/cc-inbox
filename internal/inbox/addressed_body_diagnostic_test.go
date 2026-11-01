package inbox

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"modernc.org/sqlite"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
)

var addressedBodyCounterID atomic.Uint64

func TestAddressedDoneBodyTransferDiagnostic(t *testing.T) {
	for _, population := range []int{32, 10000} {
		for _, asJSON := range []bool{false, true} {
			t.Run(fmt.Sprintf("rows=%d/json=%t", population, asJSON), func(t *testing.T) {
				ctx := context.Background()
				home := t.TempDir()
				var reads, transferred atomic.Int64
				function := fmt.Sprintf("cci_addressed_body_%d", addressedBodyCounterID.Add(1))
				if err := sqlite.RegisterDeterministicScalarFunction(function, 1, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
					reads.Add(1)
					transferred.Add(int64(len(args[0].(string))))
					return args[0], nil
				}); err != nil {
					t.Fatal(err)
				}
				st, err := store.Open(ctx, home)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = st.Close() }()
				db, err := sql.Open("sqlite", filepath.Join(home, store.DBFile))
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = db.Close() }()
				large, err := json.Marshal(map[string]string{"payload": strings.Repeat("done-body-only ", 2048)})
				if err != nil {
					t.Fatal(err)
				}
				const tiny = `{"payload":"tiny"}`
				const pendingBody = `{"pending":true}`
				if _, err := db.ExecContext(ctx, `WITH RECURSIVE history(n) AS (
VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n<?
)
INSERT INTO records(drive,lane,kind,at,text,recipients,body,source,hash)
SELECT 'd','worker','note',1,'done-'||n,'["desk"]',
CASE WHEN n=1 THEN ? ELSE ? END,'post','done-'||n FROM history`, population, string(large), tiny); err != nil {
					t.Fatal(err)
				}
				if _, err := db.ExecContext(ctx, `INSERT INTO records(drive,lane,kind,at,text,recipients,body,source,hash)
VALUES('foreign','worker','note',1,'pending','["desk@d"]',?,'post','pending')`, pendingBody); err != nil {
					t.Fatal(err)
				}
				if err := st.SetCursor(ctx, "reader#addressed", "d", int64(population)); err != nil {
					t.Fatal(err)
				}
				for _, statement := range []string{
					"ALTER TABLE records RENAME COLUMN body TO counted_body",
					"ALTER TABLE records ADD COLUMN body TEXT GENERATED ALWAYS AS (" + function + "(counted_body)) VIRTUAL",
				} {
					if _, err := db.ExecContext(ctx, statement); err != nil {
						t.Fatal(err)
					}
				}
				f := store.Filter{Drive: "d", For: "desk", Peer: "other", PeerAfter: 100000, Kinds: []kinds.Kind{kinds.State}, Lanes: []string{"ignored"}, Topics: []string{"ignored"}}
				ahead, err := addressedAhead(ctx, st, f, TailOptions{Cursor: "reader", JSON: asJSON})
				if err != nil {
					t.Fatal(err)
				}
				if ahead.sent != int64(population) || len(ahead.delivered) != population || len(ahead.pending) != 1 || ahead.pending[0].Seq != int64(population+1) || string(ahead.pending[0].Body) != pendingBody {
					t.Fatalf("addressed done/pending contract changed: %+v", ahead)
				}
				for seq := int64(1); seq <= int64(population); seq++ {
					if !ahead.delivered[seq] {
						t.Fatalf("done record #%d omitted", seq)
					}
				}
				wantBytes := len(pendingBody)
				if reads.Load() != 1 || transferred.Load() != int64(wantBytes) {
					t.Fatalf("ADDRESSED_DONE_BODY_BOUND_EXCEEDED rows=%d json=%t reads=%d want=1 bytes=%d want_bytes=%d", population, asJSON, reads.Load(), transferred.Load(), wantBytes)
				}
				t.Logf("ADDRESSED_DONE_BODY_BOUND rows=%d json=%t actual_body_reads=%d done_body_reads=0 pending_body_reads=1 actual_body_bytes=%d CPU_gain=UNKNOWN", population, asJSON, reads.Load(), transferred.Load())
			})
		}
	}
}
