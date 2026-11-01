package inbox

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"modernc.org/sqlite"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
)

var addressedBodyCounterID atomic.Uint64

func TestAddressedDoneBodyTransferDiagnostic(t *testing.T) {
	for _, population := range []int{32, 10000} {
		for _, asJSON := range []bool{false, true} {
			for _, mode := range []string{"readerFor", "laneMailBacklog", "laneMailPage"} {
				t.Run(fmt.Sprintf("rows=%d/json=%t/mode=%s", population, asJSON, mode), func(t *testing.T) {
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
INSERT INTO records(drive,lane,kind,at,text,topic,recipients,body,source,hash)
SELECT 'd',CASE WHEN n%2=0 THEN 'main' ELSE 'root' END,'note',1,'done-'||n,'kept','["desk"]',
CASE WHEN n=1 THEN ? ELSE ? END,'post','done-'||n FROM history`, population, string(large), tiny); err != nil {
						t.Fatal(err)
					}
					if _, err := db.ExecContext(ctx, `INSERT INTO records(drive,lane,kind,at,text,topic,recipients,body,source,hash)
VALUES('foreign','root','note',1,'pending','kept','["desk@d"]',?,'post','pending')`, pendingBody); err != nil {
						t.Fatal(err)
					}
					if mode != "readerFor" {
						if _, err := db.ExecContext(ctx, `INSERT INTO records(drive,lane,kind,at,text,topic,recipients,body,source,hash)
VALUES('d','worker','note',1,'page-worker','kept','["desk"]',?,'post','page-worker'),
('d','root','note',1,'other-topic','other','["desk"]',?,'post','other-topic'),
('d','main','state',1,'other-kind','kept','["desk"]',?,'post','other-kind')`, tiny, tiny, tiny); err != nil {
							t.Fatal(err)
						}
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
					f := store.Filter{Drive: "d", For: "desk", Peer: "other", PeerAfter: 100000, Kinds: []kinds.Kind{kinds.State}, Lanes: []string{"ignored"}, Topics: []string{"ignored"}, Limit: pageRows}
					wantFilter := f
					wantFilter.For, wantFilter.Addressed = "", "desk"
					wantFilter.Kinds, wantFilter.Lanes, wantFilter.Topics = nil, nil, nil
					if mode != "readerFor" {
						f.For, f.To = "", "desk"
						f.Kinds, f.Lanes, f.Topics = []kinds.Kind{kinds.Note}, []string{"root", "main", "worker"}, []string{"kept"}
						wantFilter = f
						wantFilter.Lanes = []string{"root", "main"}
					}
					wantFilter.Peer, wantFilter.PeerAfter, wantFilter.Limit = "", 0, 0
					var recordQueries atomic.Int64
					now := st.Now()
					st.Now = func() time.Time {
						recordQueries.Add(1)
						return now
					}
					var page []store.Record
					if mode == "laneMailPage" {
						query := st.QueryText
						if asJSON {
							query = st.Query
						}
						pageFilter := f
						for {
							part, err := query(ctx, pageFilter)
							if err != nil {
								t.Fatal(err)
							}
							page = append(page, part...)
							if len(part) < pageRows {
								break
							}
							pageFilter.After = part[len(part)-1].Seq
						}
						wantReads, wantBytes := int64(0), int64(0)
						if asJSON {
							wantReads = int64(population + 2)
							wantBytes = int64(len(large) + population*len(tiny) + len(pendingBody))
						}
						if len(page) != population+2 || recordQueries.Load() != int64((population+2)/pageRows+1) || reads.Load() != wantReads || transferred.Load() != wantBytes {
							t.Fatalf("page work changed: records=%d queries=%d body_reads=%d body_bytes=%d", len(page), recordQueries.Load(), reads.Load(), transferred.Load())
						}
					}
					pageQueries, pageReads, pageBytes := recordQueries.Swap(0), reads.Swap(0), transferred.Swap(0)
					before := slices.Clone(page)
					ahead, err := addressedAhead(ctx, st, f, TailOptions{Cursor: "reader", JSON: asJSON}, page, mode == "laneMailBacklog")
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(ahead.filter, wantFilter) || !reflect.DeepEqual(page, before) {
						t.Fatalf("addressed filter or supplied page changed: filter=%+v want=%+v", ahead.filter, wantFilter)
					}
					if ahead.sent != int64(population) || len(ahead.delivered) != population || len(ahead.pending) != 1 || ahead.pending[0].Seq != int64(population+1) {
						t.Fatalf("addressed done/pending contract changed: %+v", ahead)
					}
					for seq := int64(1); seq <= int64(population); seq++ {
						if !ahead.delivered[seq] {
							t.Fatalf("done record #%d omitted", seq)
						}
					}
					if mode == "laneMailPage" {
						if !reflect.DeepEqual(ahead.pending, []store.Record{page[population]}) || recordQueries.Load() != 0 || reads.Load() != 0 || transferred.Load() != 0 {
							t.Fatalf("page-derived route changed: pending=%+v queries=%d body_reads=%d body_bytes=%d", ahead.pending, recordQueries.Load(), reads.Load(), transferred.Load())
						}
						t.Logf("ADDRESSED_PAGE_DERIVED_BOUND rows=%d json=%t additional_record_queries=0 additional_body_reads=0 additional_body_bytes=0 page_record_queries=%d page_body_reads=%d page_body_bytes=%d CPU_gain=UNKNOWN", population, asJSON, pageQueries, pageReads, pageBytes)
						return
					}
					if string(ahead.pending[0].Body) != pendingBody || recordQueries.Load() != 2 {
						t.Fatalf("pending Body or query count changed: pending=%+v record_queries=%d", ahead.pending, recordQueries.Load())
					}
					wantBytes := len(pendingBody)
					if reads.Load() != 1 || transferred.Load() != int64(wantBytes) {
						t.Fatalf("ADDRESSED_DONE_BODY_BOUND_EXCEEDED rows=%d json=%t mode=%s reads=%d want=1 bytes=%d want_bytes=%d", population, asJSON, mode, reads.Load(), transferred.Load(), wantBytes)
					}
					t.Logf("ADDRESSED_DONE_BODY_BOUND rows=%d json=%t mode=%s record_queries=%d actual_body_reads=%d done_body_reads=0 pending_body_reads=1 actual_body_bytes=%d CPU_gain=UNKNOWN", population, asJSON, mode, recordQueries.Load(), reads.Load(), transferred.Load())
				})
			}
		}
	}
}
