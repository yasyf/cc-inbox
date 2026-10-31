package store

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestLaneScoreIndexProvisioningSerializesOldWriter(t *testing.T) {
	binary := os.Getenv("CCI_OLD_STORE_PROBE")
	if binary == "" {
		t.Skip("CCI_OLD_STORE_PROBE identifies the CI-built immutable old-store probe")
	}
	for _, population := range []int{32, 10000} {
		t.Run(fmt.Sprintf("rows=%d", population), func(t *testing.T) {
			ctx := t.Context()
			home := t.TempDir()
			st := openMembershipStore(ctx, t, home)
			if _, err := st.db.ExecContext(ctx, "DROP INDEX records_drive_lane_score"); err != nil {
				t.Fatal(err)
			}
			if _, err := st.db.ExecContext(ctx, `WITH RECURSIVE history(n) AS (
VALUES(1) UNION ALL SELECT n + 1 FROM history WHERE n < ?
)
INSERT INTO records (drive,lane,kind,at,text,recipients,source,hash)
SELECT 'concurrent','seed','note',n,'seed','["peer:before"]','post','before-' || n FROM history`, population); err != nil {
				t.Fatal(err)
			}
			cmd := exec.CommandContext(ctx, binary, "wait-write", home)
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			stdin, err := cmd.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			events, done := make(chan string, 3), make(chan struct{})
			var processErr error
			go func() {
				scanner := bufio.NewScanner(stdout)
				for scanner.Scan() {
					events <- scanner.Text()
				}
				processErr = errors.Join(scanner.Err(), cmd.Wait())
				close(events)
				close(done)
			}()
			defer func() {
				_ = stdin.Close()
				<-done
				if processErr != nil {
					t.Errorf("old writer process: %v: %s", processErr, stderr.String())
				}
			}()
			if event := <-events; event != "ready" {
				t.Fatalf("old writer readiness = %q", event)
			}
			lock, err := os.OpenFile(filepath.Join(home, "init.lock"), os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = lock.Close() }()
			if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) }()
			missing, err := missingPhysicalIndexes(ctx, st.db)
			if err != nil {
				t.Fatal(err)
			}
			tx, err := st.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()
			started := time.Now()
			if _, err := fmt.Fprintln(stdin, "write"); err != nil {
				t.Fatal(err)
			}
			if event := <-events; event != "writing" {
				t.Fatalf("old writer started = %q", event)
			}
			select {
			case <-done:
				t.Fatalf("old writer completed while provisioning held the writer lock: %v: %s", processErr, stderr.String())
			case <-time.After(100 * time.Millisecond):
			}
			if err := createPhysicalIndexes(ctx, tx, missing); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			elapsed := time.Since(started)
			var result struct {
				Seq         int64  `json:"seq"`
				Fingerprint string `json:"fingerprint"`
			}
			if err := json.Unmarshal([]byte(<-events), &result); err != nil {
				t.Fatal(err)
			}
			if result.Seq != int64(population+1) || result.Fingerprint != schema14Wire3Fingerprint || elapsed >= 5*time.Second {
				t.Fatalf("old writer result = %+v after %s", result, elapsed)
			}
			rows, err := st.LaneTips(ctx, "concurrent", 500)
			if err != nil || len(rows) != 2 || rows[0].Seq != result.Seq {
				t.Fatalf("old writer missing from indexed lane winners: %+v err=%v", rows, err)
			}
			assertLaneIndexCount(t, st, len(physicalIndexes))
			assertMembershipsInstalled(ctx, t, st, true)
			assertMembership(ctx, t, st, "record_recipients", "'peer:concurrent-old'", result.Seq, 1)
			assertMembershipsDerived(ctx, t, st)
			t.Logf("old_writer_preopened=true rows=%d waited_across_atomic_backfill=true posted_seq=%d held_for=%s fingerprint=%s", population, result.Seq, elapsed, result.Fingerprint)
		})
	}
}
