package store

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"
)

func TestUrgentRepeatIndexRequiresExplicitMaintenance(t *testing.T) {
	ctx := t.Context()
	home := t.TempDir()
	st := openMembershipStore(ctx, t, home)
	fingerprint := st.SchemaFingerprint()
	if _, err := st.db.ExecContext(ctx, "DROP INDEX records_urgent_repeat"); err != nil {
		t.Fatal(err)
	}
	other := openMembershipStore(ctx, t, home)
	var indexes int
	if err := other.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_schema WHERE name='records_urgent_repeat'").Scan(&indexes); err != nil || indexes != 0 {
		t.Fatalf("existing Open created physical index: count=%d error=%v", indexes, err)
	}
	report, err := MaintainIndexes(ctx, home)
	if err != nil || !slices.Equal(report.Created, []string{"records_urgent_repeat"}) {
		t.Fatalf("explicit repeat index maintenance = %+v, %v", report, err)
	}
	if other.SchemaFingerprint() != fingerprint || len(migrations) != 14 || WireFormat != 4 {
		t.Fatal("repeat index changed schema/wire/fingerprint")
	}
}

func TestUrgentRepeatIndexProvisioningWithinWriterBudget(t *testing.T) {
	population := 32
	if value := os.Getenv("CCI_URGENT_INDEX_ROWS"); value != "" {
		var err error
		population, err = strconv.Atoi(value)
		if err != nil {
			t.Fatal(err)
		}
	}
	ctx := t.Context()
	home := t.TempDir()
	st := openMembershipStore(ctx, t, home)
	if _, err := st.db.ExecContext(ctx, "DROP INDEX records_urgent_repeat"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, `WITH RECURSIVE history(n) AS (
VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n<?
)
INSERT INTO records(drive,lane,kind,topic,at,text,source,hash)
SELECT printf('d%0383d',n%64),printf('l%063d',n%512),printf('svc.k%059d',n%16),
printf('t%0383d',n),1793490000000+n,'history','post','urgent-build-'||n FROM history`, population); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("CCI_URGENT_INDEX_ROWS") != "" {
		command := exec.CommandContext(ctx, "python3", "-I", "-c", `import json,sqlite3,sys,time
from pathlib import Path
p=Path(sys.argv[1])
db=sqlite3.connect(p.as_uri()+"?mode=rw",uri=True,timeout=0)
started=time.monotonic()
db.execute("BEGIN IMMEDIATE")
db.execute("CREATE INDEX records_urgent_repeat ON records(drive, lane, kind, topic, seq, at)")
db.commit()
elapsed=time.monotonic()-started
assert db.total_changes==0
assert elapsed<5, elapsed
print(json.dumps({"python_sqlite":sqlite3.sqlite_version,"rows":int(sys.argv[2]),"seconds":elapsed,"writer_budget_seconds":5}))
db.close()
`, filepath.Join(home, "inbox.db"), strconv.Itoa(population))
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("Python sole-index builder: %v\n%s", err, output)
		}
		t.Logf("Python sole-index builder: %s", output)
		if _, err := st.db.ExecContext(ctx, "DROP INDEX records_urgent_repeat"); err != nil {
			t.Fatal(err)
		}
	}
	fingerprint := st.SchemaFingerprint()
	report, err := MaintainIndexes(ctx, home)
	if err != nil || !slices.Equal(report.Created, []string{"records_urgent_repeat"}) || report.Elapsed >= 5*time.Second {
		t.Fatalf("repeat index provisioning exceeds writer budget: rows=%d report=%+v error=%v", population, report, err)
	}
	var records, textKeyBytes, maxDrive, maxLane, maxKind, maxTopic int64
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*),
SUM(length(CAST(drive AS BLOB))+length(CAST(lane AS BLOB))+length(CAST(kind AS BLOB))+length(CAST(topic AS BLOB))),
MAX(length(CAST(drive AS BLOB))),MAX(length(CAST(lane AS BLOB))),MAX(length(CAST(kind AS BLOB))),MAX(length(CAST(topic AS BLOB))) FROM records`).Scan(&records, &textKeyBytes, &maxDrive, &maxLane, &maxKind, &maxTopic); err != nil {
		t.Fatal(err)
	}
	if st.SchemaFingerprint() != fingerprint || records != int64(population) || textKeyBytes != int64(population)*896 || maxDrive != 384 || maxLane != 64 || maxKind != 64 || maxTopic != 384 {
		t.Fatalf("repeat index changed logical state or seed widths: rows=%d text_key_bytes=%d max=%v", records, textKeyBytes, []int64{maxDrive, maxLane, maxKind, maxTopic})
	}
	t.Logf("urgent_repeat_index_rows=%d text_key_bytes=%d max_text_key_bytes=%v elapsed=%s writer_budget=%s created=%v", records, textKeyBytes, []int64{maxDrive, maxLane, maxKind, maxTopic}, report.Elapsed, 5*time.Second, report.Created)
}
