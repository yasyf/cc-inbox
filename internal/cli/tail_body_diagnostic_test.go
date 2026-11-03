package cli_test

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"modernc.org/sqlite"

	"github.com/yasyf/cc-inbox/internal/store"
)

func TestTailTextBodyTransferDiagnostic(t *testing.T) {
	for _, population := range []int{32, 10000} {
		t.Run(fmt.Sprint(population), func(t *testing.T) {
			db := tailCostStore(t)
			t.Setenv("CLAUDE_CODE_SESSION_ID", "tail-body-session")
			body, err := json.Marshal(map[string]string{"payload": strings.Repeat("body-only-token ", 2048)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(t.Context(), `WITH RECURSIVE history(n) AS (
VALUES(0) UNION ALL SELECT n+1 FROM history WHERE n<?
)
INSERT INTO records(drive,lane,kind,at,text,recipients,body,source,hash)
SELECT CASE WHEN n=? THEN 'foreign' ELSE 'durable-agents-rebuild' END,
CASE WHEN n=? THEN 'other-lane' ELSE 'desk-runner' END,
CASE n%3 WHEN 0 THEN 'blocker' WHEN 1 THEN 'report' ELSE 'note' END,
1,'selected-'||n,
CASE WHEN n=? THEN '["peer:tail-body-session"]' ELSE '[]' END,
CASE WHEN n=1 THEN ? ELSE '{"payload":"tiny"}' END,
'post','tail-body-'||n FROM history`, population, population, population, population, string(body)); err != nil {
				t.Fatal(err)
			}
			args := []string{"tail", "--drive", "durable-agents-rebuild", "--lane", "desk-runner", "--kind", "blocker", "--kind", "report", "--kind", "note", "--since-seq", "1"}
			textArgs := append(slices.Clone(args), "--budget", "4000000", "--width", "700")
			want, err := run(t, textArgs...)
			if err != nil {
				t.Fatal(err)
			}
			var reads, transferred atomic.Int64
			function := fmt.Sprintf("cci_tail_body_%d", tailCounterID.Add(1))
			if err := sqlite.RegisterDeterministicScalarFunction(function, 1, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
				reads.Add(1)
				transferred.Add(int64(len(args[0].(string))))
				return args[0], nil
			}); err != nil {
				t.Fatal(err)
			}
			for _, statement := range []string{
				"ALTER TABLE records RENAME COLUMN body TO counted_body",
				"ALTER TABLE records ADD COLUMN body TEXT GENERATED ALWAYS AS (" + function + "(counted_body)) VIRTUAL",
			} {
				if _, err := db.ExecContext(t.Context(), statement); err != nil {
					t.Fatal(err)
				}
			}
			got, err := run(t, textArgs...)
			if err != nil {
				t.Fatal(err)
			}
			if got != want || strings.Count(got, "\n")-strings.Count("\n"+got, "\n@") != population || strings.Contains(got, "body-only-token") {
				t.Fatalf("text Tail output changed or omitted records: got bytes=%d want bytes=%d", len(got), len(want))
			}
			if reads.Load() != 0 {
				t.Fatalf("TAIL_BODY_TRANSFER_BOUND_EXCEEDED rows=%d json=false reads=%d want=0", population, reads.Load())
			}
			t.Logf("TAIL_BODY_DIAGNOSTIC rows=%d json=false actual_body_reads=%d actual_body_bytes=%d output_bytes=%d", population, reads.Load(), transferred.Load(), len(got))
			reads.Store(0)
			transferred.Store(0)
			out, err := run(t, append(args, "--json", "--budget", "300000000")...)
			if err != nil {
				t.Fatal(err)
			}
			lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
			if len(lines) != population || reads.Load() < int64(population) {
				t.Fatalf("JSON Tail rows/body reads=%d/%d, want %d/at least %d", len(lines), reads.Load(), population, population)
			}
			for i, line := range lines {
				var record store.Record
				if err := json.Unmarshal([]byte(line), &record); err != nil {
					t.Fatal(err)
				}
				expected := []byte(`{"payload":"tiny"}`)
				if i == 0 {
					expected = body
				}
				if record.Seq != int64(i+2) || !bytes.Equal(record.Body, expected) {
					t.Fatalf("JSON Tail row %d changed seq/body", i)
				}
			}
			var cursors int
			if err := db.QueryRowContext(t.Context(), "SELECT count(*) FROM cursors").Scan(&cursors); err != nil {
				t.Fatal(err)
			}
			if cursors != 0 {
				t.Fatalf("explicit since Tail changed private cursor population to %d", cursors)
			}
			t.Logf("TAIL_BODY_DIAGNOSTIC rows=%d json=true actual_body_reads=%d actual_body_bytes=%d output_bytes=%d", population, reads.Load(), transferred.Load(), len(out))
		})
	}
}
