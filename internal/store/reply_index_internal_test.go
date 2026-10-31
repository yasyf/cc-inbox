package store

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestGrepReplyIndexRequiresExplicitMaintenance(t *testing.T) {
	ctx := t.Context()
	home := t.TempDir()
	st := openMembershipStore(ctx, t, home)
	fingerprint := st.SchemaFingerprint()
	if _, err := st.db.ExecContext(ctx, "DROP INDEX records_re"); err != nil {
		t.Fatal(err)
	}
	other := openMembershipStore(ctx, t, home)
	if _, err := other.ReplyPages(ctx, [][]int64{{1}}); err == nil || !strings.Contains(err.Error(), "records_re") {
		t.Fatalf("current Open created or bypassed missing index: %v", err)
	}
	report, err := MaintainIndexes(ctx, home)
	if err != nil || len(report.Created) != 1 || report.Created[0] != "records_re" {
		t.Fatalf("provision=%+v, %v", report, err)
	}
	if _, err := other.ReplyPages(ctx, [][]int64{{1}}); err != nil {
		t.Fatal(err)
	}
	if st.SchemaFingerprint() != fingerprint || len(migrations) != 14 || WireFormat != 4 {
		t.Fatal("reply index changed schema/wire/fingerprint")
	}
}

func TestGrepReplyIndexProvisioningWithinWriterBudget(t *testing.T) {
	population := 32
	if value := os.Getenv("CCI_GREP_REPLY_INDEX_ROWS"); value != "" {
		var err error
		population, err = strconv.Atoi(value)
		if err != nil {
			t.Fatal(err)
		}
	}
	ctx := t.Context()
	st := openMembershipStore(ctx, t, t.TempDir())
	if _, err := st.db.ExecContext(ctx, "DROP INDEX records_re"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, `WITH RECURSIVE n(x) AS(VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<?)
INSERT INTO records(drive,lane,kind,at,text,source,hash,re)
SELECT 'd','history','note',x,'history','post','reply-index-'||x,x%1000 FROM n`, population); err != nil {
		t.Fatal(err)
	}
	fingerprint := st.SchemaFingerprint()
	report, err := MaintainIndexes(ctx, st.home)
	if err != nil || len(report.Created) != 1 || report.Created[0] != "records_re" || report.Elapsed >= 5*time.Second {
		t.Fatalf("rows=%d provision=%+v err=%v", population, report, err)
	}
	var rows int
	if err := st.db.QueryRowContext(ctx, "SELECT count(*) FROM records").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != population || st.SchemaFingerprint() != fingerprint {
		t.Fatal("reply index changed logical state")
	}
	t.Logf("reply_index_rows=%d elapsed=%s writer_budget=%s created=%v before=%+v after=%+v", population, report.Elapsed, 5*time.Second, report.Created, report.Before, report.After)
}
