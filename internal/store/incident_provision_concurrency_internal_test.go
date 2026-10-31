package store

import (
	"bufio"
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"modernc.org/sqlite"
)

func TestIncidentProvisioningSerializesPreopenedOldWriter(t *testing.T) {
	binary := os.Getenv("CCI_OLD_STORE_PROBE")
	if binary == "" {
		t.Skip("CI supplies immutable old store probe")
	}
	for _, population := range []int{32, 10000} {
		t.Run(fmt.Sprint(population), func(t *testing.T) {
			ctx := t.Context()
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			var writes atomic.Int64
			name := fmt.Sprintf("cci_incident_backfill_%d", incidentCostID.Add(1))
			if err := sqlite.RegisterScalarFunction(name, 0, func(_ *sqlite.FunctionContext, _ []driver.Value) (driver.Value, error) {
				if writes.Add(1) == 1 {
					close(entered)
					<-release
				}
				return int64(1), nil
			}); err != nil {
				t.Fatal(err)
			}
			st, err := Open(ctx, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			if _, err := st.db.ExecContext(ctx, `WITH RECURSIVE n(x) AS(VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<?)
INSERT INTO records(drive,lane,kind,at,text,source,hash)
SELECT 'concurrent','seed','incident',x,'unrelated #'||(900000+x),'post','old-seed-'||x FROM n`, population); err != nil {
				t.Fatal(err)
			}
			if _, err := st.db.ExecContext(ctx, "CREATE TRIGGER incident_backfill_barrier AFTER INSERT ON incident_records BEGIN SELECT "+name+"(); END"); err != nil {
				t.Fatal(err)
			}
			command := exec.CommandContext(ctx, binary, "wait-incident", st.HomeDir())
			stdout, err := command.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			stdin, err := command.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			var stderr bytes.Buffer
			command.Stderr = &stderr
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			events, done := make(chan string, 3), make(chan struct{})
			var processErr error
			go func() {
				scanner := bufio.NewScanner(stdout)
				for scanner.Scan() {
					events <- scanner.Text()
				}
				processErr = errors.Join(scanner.Err(), command.Wait())
				close(events)
				close(done)
			}()
			defer func() {
				unblock()
				_ = stdin.Close()
				<-done
				if processErr != nil {
					t.Errorf("oldwriter: %v/%s", processErr, stderr.String())
				}
			}()
			if event := <-events; event != "ready" {
				t.Fatalf("oldwriter was not preopened: %q", event)
			}
			type provisionResult struct {
				report IncidentProvisionReport
				err    error
			}
			completed := make(chan provisionResult, 1)
			go func() {
				report, err := st.ProvisionIncidentScope(ctx, "concurrent")
				completed <- provisionResult{report, err}
			}()
			<-entered
			started := time.Now()
			if _, err := fmt.Fprintln(stdin, "write"); err != nil {
				t.Fatal(err)
			}
			if event := <-events; event != "writing" {
				t.Fatalf("oldwriter did not arrive during backfill: %q", event)
			}
			select {
			case <-done:
				t.Fatalf("oldwriter bypassed real backfill transaction: %v/%s", processErr, stderr.String())
			case <-time.After(100 * time.Millisecond):
			}
			unblock()
			provision := <-completed
			if provision.err != nil || !provision.report.Ready {
				t.Fatalf("actualprovision: %+v/%v", provision.report, provision.err)
			}
			var posted struct {
				Seq         int64  `json:"seq"`
				Fingerprint string `json:"fingerprint"`
			}
			if err := json.Unmarshal([]byte(<-events), &posted); err != nil {
				t.Fatal(err)
			}
			if posted.Seq != int64(population+1) || posted.Fingerprint != schema14Wire3Fingerprint {
				t.Fatalf("oldwriter metadata: %+v", posted)
			}
			index, err := OpenIncidentIndex(ctx, st)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = index.Close() }()
			got, err := index.Holds(ctx, st, "concurrent", []string{"31415"})
			if err != nil || got["31415"].Seq != posted.Seq {
				t.Fatalf("oldwriter trigger delta missing: %v/%v", got, err)
			}
			if writes.Load() < int64(population) || provision.report.MaxWritePhase < 100*time.Millisecond {
				t.Fatalf("fixture did not hold actual metadata backfill: writes=%d report=%+v", writes.Load(), provision.report)
			}
			if os.Getenv("CCI_INCIDENT_NATIVE_BUDGET") == "1" && provision.report.MaxWritePhase >= 5*time.Second {
				t.Fatalf("incident writer budget exceeded: %+v", provision.report)
			}
			t.Logf("incident_records=%d oldwriter_preopened=true arrived_during_real_backfill=true max_writer_phase=%s oldwriter_wait=%s fingerprint=%s", population, provision.report.MaxWritePhase, time.Since(started), posted.Fingerprint)
		})
	}
}
