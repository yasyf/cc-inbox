package cli_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

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
	if len(report.Created) != 0 || len(report.Memberships) != 0 || report.Before.PageSize <= 0 || report.After.PageCount <= 0 {
		t.Fatalf("new cold store maintenance report = %+v", report)
	}
}

func TestMaintainIndexesCommandDoesNotCreateMissingDatabase(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
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
