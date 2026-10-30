package store

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestOpenCurrentSchemaDoesNotWaitForLocks(t *testing.T) {
	for _, lock := range []string{"migration", "writer"} {
		t.Run(lock, func(t *testing.T) {
			home := t.TempDir()
			st, err := Open(context.Background(), home)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			var release func()
			switch lock {
			case "migration":
				file, err := os.OpenFile(filepath.Join(home, "init.lock"), os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = file.Close() })
				if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
					t.Fatal(err)
				}
				release = func() { _ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN) }
			case "writer":
				tx, err := st.db.BeginTx(context.Background(), nil)
				if err != nil {
					t.Fatal(err)
				}
				release = func() { _ = tx.Rollback() }
			}
			t.Cleanup(release)
			opened := make(chan error, 1)
			go func() {
				other, err := Open(context.Background(), home)
				if err == nil {
					err = other.Close()
				}
				opened <- err
			}()
			select {
			case err := <-opened:
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("current-schema open completed while %s lock stayed held", lock)
			case <-time.After(2 * time.Second):
				release()
				err := <-opened
				t.Fatalf("current-schema open waited for %s lock: %v", lock, err)
			}
		})
	}
}
