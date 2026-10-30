package cli_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/store"
)

func TestMaintainIndexesCommandReportsCost(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	st, err := store.Open(context.Background(), home)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	out, err := run(t, "maintain-indexes")
	if err != nil {
		t.Fatal(err)
	}
	var report store.IndexMaintenanceReport
	if err := json.Unmarshal([]byte(out), &report); err != nil {
		t.Fatal(err)
	}
	if len(report.Created) != 0 || report.Before.PageSize <= 0 || report.After.PageCount <= 0 {
		t.Fatalf("new cold store maintenance report = %+v", report)
	}
}

func TestMaintainIndexesCommandFailsBusyBeforeOpeningStore(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	lock, err := os.OpenFile(filepath.Join(home, "init.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lock.Close() })
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	release := func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) }
	t.Cleanup(release)
	done := make(chan error, 1)
	go func() {
		_, err := run(t, "maintain-indexes")
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("maintenance command accepted busy initialization lock")
		}
	case <-time.After(2 * time.Second):
		release()
		<-done
		t.Fatal("maintenance command waited on initialization lock")
	}
	if _, err := os.Stat(filepath.Join(home, store.DBFile)); !os.IsNotExist(err) {
		t.Fatalf("busy maintenance created database: %v", err)
	}
}

func TestMaintainIndexesCommandDoesNotCreateMissingDatabase(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	if err := os.WriteFile(filepath.Join(home, "init.lock"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := run(t, "maintain-indexes"); err == nil {
		t.Fatal("maintenance command accepted missing database")
	}
	if _, err := os.Stat(filepath.Join(home, store.DBFile)); !os.IsNotExist(err) {
		t.Fatalf("maintenance created missing database: %v", err)
	}
}

func TestMaintainIndexesRejectsArgumentsBeforeOpeningStore(t *testing.T) {
	home := filepath.Join(t.TempDir(), "unopened")
	t.Setenv("CCI_HOME", home)
	if _, err := run(t, "maintain-indexes", "extra"); err == nil {
		t.Fatal("maintenance accepted unexpected argument")
	}
	if _, err := os.Stat(home); !os.IsNotExist(err) {
		t.Fatalf("invalid maintenance initialized store: %v", err)
	}
}
