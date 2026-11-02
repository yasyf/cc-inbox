package store

import (
	"context"
	"database/sql"
	"errors"
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

func TestConnectWaitsForConcurrentSchemaUpgrade(t *testing.T) {
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
		other, err := Connect(context.Background(), home)
		if err == nil {
			err = other.Close()
		}
		opened <- err
	}()
	select {
	case err := <-opened:
		t.Fatalf("connect crossed the active upgrade barrier: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	if err := <-opened; err == nil || !strings.Contains(err.Error(), "is newer than this cci") {
		t.Fatalf("connect after newer schema commit = %v", err)
	}
}

func TestConnectReturnsBusyWithinItsDeadlineWhileInitLockIsHeld(t *testing.T) {
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
	const deadline = 300 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	started := time.Now()
	other, err := Connect(ctx, home)
	elapsed := time.Since(started)
	if err == nil {
		_ = other.Close()
	}
	if !errors.Is(err, ErrBusy) || !Busy(err) || errors.Is(err, ErrNeedsInit) {
		t.Fatalf("connect behind a held init lock = %v, want ErrBusy", err)
	}
	if elapsed < deadline || elapsed > deadline+time.Second {
		t.Fatalf("connect behind a held init lock returned after %v, want about %v", elapsed, deadline)
	}
}

func TestConnectNeverMigrates(t *testing.T) {
	for _, tt := range []struct {
		name  string
		setup func(t *testing.T, home string)
	}{
		{"cold", func(t *testing.T, home string) {
			if _, err := node.Load(home); err != nil {
				t.Fatal(err)
			}
		}},
		{"behind", func(t *testing.T, home string) {
			st, err := Open(context.Background(), home)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = st.Close() }()
			if _, err := st.db.ExecContext(context.Background(), fmt.Sprintf("PRAGMA user_version = %d", len(migrations)-1)); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			home := t.TempDir()
			tt.setup(t, home)
			var before int
			if err := rawSchema(t, home, &before); err != nil {
				t.Fatal(err)
			}
			st, err := Connect(context.Background(), home)
			if err == nil {
				_ = st.Close()
			}
			if !errors.Is(err, ErrNeedsInit) || Busy(err) {
				t.Fatalf("connect to a %s store = %v, want ErrNeedsInit", tt.name, err)
			}
			var after int
			if err := rawSchema(t, home, &after); err != nil || after != before {
				t.Fatalf("connect moved schema %d to %d, %v", before, after, err)
			}
		})
	}
}

func TestConnectDoesNotWaitForIndexMaintenance(t *testing.T) {
	ctx := context.Background()
	home := t.TempDir()
	st, err := Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	lock, err := lockMaintenance(home)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lock.Close() })
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	if _, err := MaintainIndexes(ctx, home); err == nil {
		t.Fatal("a second maintainer ran beside the first")
	}
	connectCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	other, err := Connect(connectCtx, home)
	if err != nil {
		t.Fatalf("connect while maintenance held its lock and the writer = %v", err)
	}
	_ = other.Close()
}

func rawSchema(t *testing.T, home string, schema *int) error {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(home, DBFile))
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	return db.QueryRowContext(context.Background(), "PRAGMA user_version").Scan(schema)
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
