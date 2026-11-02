package cli_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
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
			prompt := func(text string) (time.Duration, error) {
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				hook := exec.CommandContext(ctx, os.Args[0], "hook", "prompt")
				hook.Env = append(os.Environ(), "CCI_HOME="+home)
				hook.Stdin = strings.NewReader(`{"session_id":"root","hook_event_name":"UserPromptSubmit","prompt":"` + text + `"}`)
				start := time.Now()
				out, err := hook.CombinedOutput()
				if err != nil {
					return time.Since(start), fmt.Errorf("%w; output %q", err, out)
				}
				return time.Since(start), nil
			}
			elapsed, err := prompt("ship the fix while the store is busy")
			if err != nil {
				t.Fatalf("prompt hook after %v = %v", elapsed, err)
			}
			if elapsed > 5*time.Second {
				t.Fatalf("prompt hook took %v behind the held store", elapsed)
			}
			if kept, _ := filepath.Glob(filepath.Join(home, "prompts", "*.json")); len(kept) != 1 {
				t.Fatalf("kept prompts = %v, want the one the held store could not take", kept)
			}
			release()
			if _, err := prompt("and the next one"); err != nil {
				t.Fatalf("prompt hook after the store freed = %v", err)
			}
			records, err := st.Query(t.Context(), store.Filter{Drive: "busy-drive", Kinds: []kinds.Kind{kinds.Owner}})
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, r := range records {
				got = append(got, r.Text)
			}
			slices.Sort(got)
			if !slices.Equal(got, []string{"and the next one", "ship the fix while the store is busy"}) {
				t.Fatalf("owner records after the store freed = %q, want the kept prompt and the next one once each", got)
			}
			if kept, _ := filepath.Glob(filepath.Join(home, "prompts", "*")); len(kept) != 0 {
				t.Fatalf("kept prompts after the next prompt = %v, want none", kept)
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
