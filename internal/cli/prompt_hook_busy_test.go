package cli_test

import (
	"context"
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
)

func TestPromptHookAnswersWithinItsBudgetWhileTheStoreIsHeld(t *testing.T) {
	for _, tt := range []struct {
		name string
		hold func(t *testing.T, home string) func()
	}{
		{"writer", holdWriter},
		{"migration lock", holdMigrationLock},
	} {
		t.Run(tt.name, func(t *testing.T) {
			home := t.TempDir()
			st, err := store.Open(t.Context(), home)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			if err := st.Bind(t.Context(), store.PeerKey{Session: "root"}, "busy-drive", true); err != nil {
				t.Fatal(err)
			}
			release := tt.hold(t, home)
			t.Cleanup(release)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			hook := exec.CommandContext(ctx, os.Args[0], "hook", "prompt")
			hook.Env = append(os.Environ(), "CCI_HOME="+home)
			hook.Stdin = strings.NewReader(`{"session_id":"root","hook_event_name":"UserPromptSubmit","prompt":"ship the fix while the store is busy"}`)
			start := time.Now()
			out, err := hook.CombinedOutput()
			elapsed := time.Since(start)
			if err != nil {
				t.Fatalf("prompt hook after %v = %v; output %q", elapsed, err, out)
			}
			if elapsed > 5*time.Second {
				t.Fatalf("prompt hook took %v behind the held store", elapsed)
			}
			t.Logf("prompt hook answered in %v while the store was held", elapsed)
			release()
			deadline := time.Now().Add(30 * time.Second)
			for {
				records, err := st.Query(t.Context(), store.Filter{Drive: "busy-drive", Kinds: []kinds.Kind{kinds.Owner}})
				if err != nil {
					t.Fatal(err)
				}
				if len(records) == 1 && records[0].Text == "ship the fix while the store is busy" {
					return
				}
				if len(records) > 1 || time.Now().After(deadline) {
					t.Fatalf("owner records after the store freed = %+v, want the deferred prompt once", records)
				}
				time.Sleep(50 * time.Millisecond)
			}
		})
	}
}

func holdWriter(t *testing.T, home string) func() {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(home, store.DBFile))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(t.Context(), "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	return func() {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
	}
}

func holdMigrationLock(t *testing.T, home string) func() {
	t.Helper()
	lock, err := os.OpenFile(filepath.Join(home, "init.lock"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lock.Close() })
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	return func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) }
}
