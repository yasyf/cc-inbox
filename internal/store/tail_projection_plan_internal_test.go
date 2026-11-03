package store

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

func TestTailProjectionPrivateFixturePlan(t *testing.T) {
	for _, population := range []int{32, 10000} {
		t.Run(fmt.Sprint(population), func(t *testing.T) {
			ctx := context.Background()
			st, err := Open(ctx, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = st.Close() }()
			st.Now = func() time.Time { return time.UnixMilli(1793564000000) }
			var version, sourceID string
			if err := st.db.QueryRowContext(ctx, "SELECT sqlite_version(), sqlite_source_id()").Scan(&version, &sourceID); err != nil {
				t.Fatal(err)
			}
			body, err := json.Marshal(map[string]string{"payload": strings.Repeat("body-only-token ", 2048)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := st.db.ExecContext(ctx, `INSERT INTO records(drive,lane,kind,at,text,source,hash,origin)
VALUES('durable-agents-rebuild','worker','note',1,'anchor','post','anchor',?)`, st.Self()); err != nil {
				t.Fatal(err)
			}
			if _, err := st.db.ExecContext(ctx, `WITH RECURSIVE history(n) AS (
VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n<?
)
INSERT INTO records(drive,lane,kind,at,text,recipients,body,source,hash,origin,origin_seq,id)
SELECT CASE WHEN n=? THEN 'foreign' ELSE 'durable-agents-rebuild' END,
CASE WHEN n=? THEN 'other-lane' ELSE 'desk-runner' END,
CASE WHEN n=? THEN 'state' WHEN n%3=0 THEN 'blocker' WHEN n%3=1 THEN 'report' ELSE 'note' END,
1,'selected-'||n,CASE WHEN n=? THEN '["peer:tail-native-private-fixture"]' ELSE '[]' END,
CASE WHEN n=1 THEN ? ELSE '{"payload":"tiny"}' END,
'post','baseline-'||n,?,0,'baseline-'||n FROM history`, population, population, population, population, population, string(body), st.Self()); err != nil {
				t.Fatal(err)
			}
			f := Filter{Drive: "durable-agents-rebuild", Peer: "tail-native-private-fixture", Kinds: []kinds.Kind{kinds.Blocker, kinds.Report, kinds.Note}, Lanes: []string{"desk-runner"}, After: 1, Limit: 500}
			pages, records, bodyBytes := 0, 0, 0
			for {
				fullSQL, fullArgs := st.selectionColumns(f, columns)
				textSQL, textArgs := st.selectionColumns(f, textColumns)
				if strings.Replace(fullSQL, columns, textColumns, 1) != textSQL || !reflect.DeepEqual(fullArgs, textArgs) {
					t.Fatalf("projection changed SQL predicates or parameters: full=%s %v text=%s %v", fullSQL, fullArgs, textSQL, textArgs)
				}
				fullPlan := queryPlan(ctx, t, st, fullSQL, fullArgs...)
				textPlan := queryPlan(ctx, t, st, textSQL, textArgs...)
				if !reflect.DeepEqual(fullPlan, textPlan) {
					t.Fatalf("Body projection changed query plan: full=%v text=%v", fullPlan, textPlan)
				}
				plan := strings.Join(textPlan, "\n")
				for _, index := range []string{"records_drive_lane", "record_recipients_address", "record_json_errors_field"} {
					if !strings.Contains(plan, index) {
						t.Fatalf("Tail plan lacks %s: %s", index, plan)
					}
				}
				if strings.Contains(plan, "TEMP B-TREE") || strings.Contains(plan, "SCAN records") {
					t.Fatalf("Tail plan sorts or scans record history: %s", plan)
				}
				full, err := st.Query(ctx, f)
				if err != nil {
					t.Fatal(err)
				}
				text, err := st.QueryText(ctx, f)
				if err != nil {
					t.Fatal(err)
				}
				if len(full) != len(text) {
					t.Fatalf("projection row counts differ: full=%d text=%d", len(full), len(text))
				}
				for i := range full {
					if full[i].Seq != int64(records+i+2) || len(text[i].Body) != 0 {
						t.Fatalf("projection changed sequence or retained Body: full=%+v text=%+v", full[i], text[i])
					}
					bodyBytes += len(full[i].Body)
					full[i].Body = nil
					if !reflect.DeepEqual(full[i], text[i]) {
						t.Fatalf("projection changed non-Body record: full=%+v text=%+v", full[i], text[i])
					}
				}
				if pages == 0 {
					t.Logf("TAIL_PRIVATE_PLAN rows=%d sqlite=%s source_id=%s full_static_opcodes=%d text_static_opcodes=%d sql=%q args=%v plan=%q", population, version, sourceID, tailProjectionOpcodeCount(ctx, t, st, fullSQL, fullArgs...), tailProjectionOpcodeCount(ctx, t, st, textSQL, textArgs...), textSQL, textArgs, textPlan)
				}
				pages++
				records += len(text)
				if len(text) < f.Limit {
					break
				}
				f.After = text[len(text)-1].Seq
			}
			wantBytes := len(body) + (population-1)*len(`{"payload":"tiny"}`)
			if records != population || pages != population/500+1 || bodyBytes != wantBytes {
				t.Fatalf("projection work rows/pages/body_bytes=%d/%d/%d want=%d/%d/%d", records, pages, bodyBytes, population, population/500+1, wantBytes)
			}
			t.Logf("TAIL_PRIVATE_PROJECTION rows=%d pages=%d scans_per_projection=%d full_body_bytes=%d text_body_bytes=0 runtime_VM_steps=UNKNOWN CPU_gain=UNKNOWN", population, pages, records, bodyBytes)
		})
	}
}

func tailProjectionOpcodeCount(ctx context.Context, t *testing.T, st *Store, query string, args ...any) int {
	t.Helper()
	rows, err := st.db.QueryContext(ctx, "EXPLAIN "+query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	count := 0
	for rows.Next() {
		var address, p1, p2, p3, p5 int
		var opcode string
		var p4, annotation any
		if err := rows.Scan(&address, &opcode, &p1, &p2, &p3, &p4, &p5, &annotation); err != nil {
			t.Fatal(err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return count
}
