package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/node"
)

func TestOpenCurrentSchemaDoesNotWaitForLocks(t *testing.T) {
	for _, lock := range []string{"reader", "writer"} {
		t.Run(lock, func(t *testing.T) {
			home := t.TempDir()
			st, err := Open(context.Background(), home)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			var release func()
			switch lock {
			case "reader":
				file, err := os.OpenFile(filepath.Join(home, "init.lock"), os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = file.Close() })
				if err := syscall.Flock(int(file.Fd()), syscall.LOCK_SH); err != nil {
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

func TestOpenWaitsForConcurrentSchemaUpgrade(t *testing.T) {
	home := t.TempDir()
	st, err := Open(context.Background(), home)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	file, err := os.OpenFile(filepath.Join(home, "init.lock"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN) })
	tx, err := st.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	if _, err := tx.ExecContext(context.Background(), "PRAGMA user_version = "+fmt.Sprint(len(migrations)+1)); err != nil {
		t.Fatal(err)
	}
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
		t.Fatalf("open crossed the active upgrade barrier: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	if err := <-opened; err == nil || !strings.Contains(err.Error(), "is newer than this cci") {
		t.Fatalf("open after newer schema commit = %v", err)
	}
}

func TestConcurrentColdOpenKeepsMigrationSerialized(t *testing.T) {
	home := t.TempDir()
	if _, err := node.Load(home); err != nil {
		t.Fatal(err)
	}
	const readers = 8
	start := make(chan struct{})
	opened := make(chan error, readers)
	for range readers {
		go func() {
			<-start
			st, err := Open(context.Background(), home)
			if err == nil {
				err = st.Close()
			}
			opened <- err
		}()
	}
	close(start)
	for range readers {
		if err := <-opened; err != nil {
			t.Fatal(err)
		}
	}
}
