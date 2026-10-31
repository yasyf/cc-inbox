package mesh

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
	"github.com/yasyf/synckit/syncservice"
	"modernc.org/sqlite"
)

type meshExportFrontierContext struct {
	context.Context
	afterValidation func() error
}

func (c meshExportFrontierContext) Err() error {
	if err := c.Context.Err(); err != nil {
		return err
	}
	return c.afterValidation()
}

func TestMeshExportRevisionNeverIncludesUnvalidatedConcurrentAppend(t *testing.T) {
	var armed, inserted atomic.Bool
	function := fmt.Sprintf("cci_mesh_frontier_%d", exportCounterID.Add(1))
	var overflow int64
	if err := sqlite.RegisterDeterministicScalarFunction(function, 2, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
		if seq, ok := args[1].(int64); ok && seq == overflow {
			armed.Store(true)
		}
		return args[0], nil
	}); err != nil {
		t.Fatal(err)
	}
	n := newMeshNode(t)
	old := testutil.Post(t, n.st, store.Record{Kind: kinds.Note, Text: strings.Repeat("overflow", 100)})
	latest := testutil.Post(t, n.st, store.Record{Kind: kinds.Note, Text: ""})
	overflow = old.Seq
	rows, err := n.st.Export(t.Context(), n.st.Self(), old.Seq, 1)
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
	for _, q := range []string{"ALTER TABLE records RENAME COLUMN text TO counted_text", "ALTER TABLE records ADD COLUMN text TEXT GENERATED ALWAYS AS (" + function + "(counted_text,seq)) VIRTUAL"} {
		if _, err := db.ExecContext(t.Context(), q); err != nil {
			t.Fatal(err)
		}
	}
	ctx := meshExportFrontierContext{Context: t.Context(), afterValidation: func() error {
		if armed.CompareAndSwap(true, false) && inserted.CompareAndSwap(false, true) {
			_, err := db.ExecContext(t.Context(), `INSERT INTO records(seq,origin,origin_seq,drive,lane,kind,at,counted_text,refs,source,hash) VALUES(?,?,?,'d','concurrent','note',1,'unvalidated','{','post','unvalidated-append')`, latest.Seq+1, n.st.Self(), latest.Seq+1)
			return err
		}
		return nil
	}}
	_, err = n.consumer.Export(ctx, syncservice.ExportRequest{ServiceID: Service, SchemaFingerprint: n.st.SchemaFingerprint(), SinceRevision: syncservice.NewRevision(0)})
	if !inserted.Load() || err == nil || !strings.Contains(err.Error(), "decode refs") {
		t.Fatalf("concurrent append fixture inserted=%t error=%v", inserted.Load(), err)
	}
	log, err := openRevisions(n.st.HomeDir())
	if err != nil {
		t.Fatal(err)
	}
	head, _ := log.head()
	if head.Seq != latest.Seq {
		t.Fatalf("mesh revision included unvalidated concurrent append: seq=%d want=%d", head.Seq, latest.Seq)
	}
}
