package store

import (
	"os"
	"slices"
	"testing"
)

func TestCountTimestampIndexLifecycle(t *testing.T) {
	if os.Getenv("CCI_COUNT_TIMESTAMP_GATES") != "1" {
		t.Skip("the Count timestamp workflow supplies the index lifecycle gate")
	}
	home := t.TempDir()
	st := openMembershipStore(t.Context(), t, home)
	var present bool
	if err := st.db.QueryRowContext(t.Context(), "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='records_drive_at_seq')").Scan(&present); err != nil || !present {
		t.Fatalf("fresh timestamp index=%t, error=%v", present, err)
	}
	if _, err := st.db.ExecContext(t.Context(), "DROP INDEX records_drive_at_seq"); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openMembershipStore(t.Context(), t, home)
	if err := reopened.db.QueryRowContext(t.Context(), "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='records_drive_at_seq')").Scan(&present); err != nil || present {
		t.Fatalf("existing Open timestamp index=%t, error=%v", present, err)
	}
	if count, err := reopened.Count(t.Context(), Filter{Drive: "d", Peer: "reader"}); err != nil || count != 0 {
		t.Fatalf("indexless Count=%d, error=%v", count, err)
	}
	report, err := MaintainIndexes(t.Context(), home)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(report.Created, "records_drive_at_seq") {
		t.Fatalf("manual index report=%+v", report)
	}
}
