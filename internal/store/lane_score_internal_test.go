package store

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestLaneScoreIndexRequiresExplicitMaintenanceOnCurrentStore(t *testing.T) {
	home := t.TempDir()
	st := openMembershipStore(t.Context(), t, home)
	fingerprint := st.SchemaFingerprint()
	if _, err := st.db.ExecContext(t.Context(), "DROP INDEX records_drive_lane_score"); err != nil {
		t.Fatal(err)
	}
	other := openMembershipStore(t.Context(), t, home)
	if _, err := other.LaneTips(t.Context(), "d", 500); err == nil || !strings.Contains(err.Error(), "records_drive_lane_score") {
		t.Fatalf("current Open silently created or bypassed the required index: %v", err)
	}
	report, err := MaintainIndexes(t.Context(), home)
	if err != nil || len(report.Created) != 1 || report.Created[0] != "records_drive_lane_score" {
		t.Fatalf("explicit index provisioning = %+v, %v", report, err)
	}
	if _, err := other.LaneTips(t.Context(), "d", 500); err != nil {
		t.Fatal(err)
	}
	if other.SchemaFingerprint() != fingerprint || len(migrations) != 14 || WireFormat != 3 {
		t.Fatal("lane index changed schema/wire/fingerprint")
	}
}

func TestLaneScoreIndexProvisioningWithinWriterBudget(t *testing.T) {
	population := 32
	if value := os.Getenv("CCI_LANE_INDEX_ROWS"); value != "" {
		var err error
		population, err = strconv.Atoi(value)
		if err != nil {
			t.Fatal(err)
		}
	}
	home := t.TempDir()
	st := openMembershipStore(t.Context(), t, home)
	if _, err := st.db.ExecContext(t.Context(), "DROP INDEX records_drive_lane_score"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(t.Context(), `WITH RECURSIVE history(n) AS (
VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n < ?
)
INSERT INTO records(drive,lane,kind,at,text,source,hash)
SELECT 'drive-' || printf('%0128d',1),'lane-' || printf('%0128d',n%512),'note',4102444800000+n,'history','post','row-' || n FROM history`, population); err != nil {
		t.Fatal(err)
	}
	fingerprint := st.SchemaFingerprint()
	report, err := MaintainIndexes(t.Context(), home)
	if err != nil || len(report.Created) != 1 || report.Elapsed >= 5*time.Second {
		t.Fatalf("lane score provisioning exceeds writer budget: rows=%d report=%+v error=%v", population, report, err)
	}
	var records, driveBytes, laneBytes int
	if err := st.db.QueryRowContext(t.Context(), "SELECT COUNT(*),SUM(length(CAST(drive AS BLOB))),SUM(length(CAST(lane AS BLOB))) FROM records INDEXED BY records_drive_lane").Scan(&records, &driveBytes, &laneBytes); err != nil {
		t.Fatal(err)
	}
	if st.SchemaFingerprint() != fingerprint || records != population || driveBytes != 134*population || laneBytes != 133*population {
		t.Fatal("lane score provisioning changed logical state")
	}
	t.Logf("lane_score_index_rows=%d drive_bytes=%d lane_bytes=%d elapsed=%s writer_budget=%s created=%v before=%+v after=%+v", population, driveBytes, laneBytes, report.Elapsed, 5*time.Second, report.Created, report.Before, report.After)
}

func TestLaneScorePlanSeeksLaneAndOriginalScore(t *testing.T) {
	st := openMembershipStore(t.Context(), t, t.TempDir())
	rows, err := st.db.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+laneTipsSQL, "d", "d", "d", 500)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var plans []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plans = append(plans, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(plans, "\n")
	if !strings.Contains(joined, "records_drive_lane_score") || strings.Contains(joined, "GROUP BY") {
		t.Fatalf("lane plan lost indexed lane/score selection: %s", joined)
	}
	t.Logf("actual_lane_score_query_plan=%s", joined)
}
