package store

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

func TestWatchChangedGenerationQueryPlan(t *testing.T) {
	if os.Getenv("CCI_WATCH_CHANGED_GENERATION_DIAGNOSTIC") != "1" {
		t.Skip("set CCI_WATCH_CHANGED_GENERATION_DIAGNOSTIC=1 to run changed-generation Watch diagnostics")
	}
	for _, population := range []int{32, 10000} {
		for _, placement := range []string{"same-drive", "foreign-drive"} {
			t.Run(fmt.Sprintf("%d/%s", population, placement), func(t *testing.T) {
				st, err := Open(t.Context(), t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = st.Close() }()
				for _, query := range []string{"DROP TRIGGER IF EXISTS record_root_audience_insert", "DROP TRIGGER IF EXISTS record_root_audience_update", "DROP TRIGGER IF EXISTS record_root_audience_delete", "DROP INDEX IF EXISTS record_root_audience_stream", "DROP INDEX IF EXISTS record_root_audience_kind_stream", "DROP TABLE IF EXISTS record_root_audience"} {
					if _, err := st.db.ExecContext(t.Context(), query); err != nil {
						t.Fatal(err)
					}
				}
				var rootObjects int
				if err := st.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM sqlite_schema WHERE name IN ('record_root_audience','record_root_audience_stream','record_root_audience_kind_stream','record_root_audience_insert','record_root_audience_update','record_root_audience_delete')").Scan(&rootObjects); err != nil || rootObjects != 0 {
					t.Fatalf("unenrolled root audience objects=%d, error=%v", rootObjects, err)
				}
				t.Log("WATCH_CHANGED_GENERATION_UNENROLLED_PLAN root_audience_objects=0 route=general unbounded_reference=true")
				st.Now = func() time.Time { return time.Date(2026, 10, 4, 22, 0, 0, 0, time.UTC) }
				if _, err := st.db.ExecContext(t.Context(), `INSERT INTO records(seq,drive,lane,kind,at,text,source,hash,recipients,origin,origin_seq,id)
VALUES(1,'d','noise','ask',?,'control-0','post','watch-control','["other"]',?,1,'watch-control')`, st.Now().UnixMilli(), st.Self()); err != nil {
					t.Fatal(err)
				}
				drive := "d"
				if placement == "foreign-drive" {
					drive = "foreign"
				}
				if _, err := st.db.ExecContext(t.Context(), `WITH RECURSIVE history(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n<?)
INSERT INTO records(seq,drive,lane,kind,at,text,source,hash,recipients,origin,origin_seq,id)
SELECT n+1,?,'noise','ask',?,'ignored','post','changed-watch-'||n,'["other"]',?,n+1,'changed-watch-'||n FROM history`, population, drive, st.Now().UnixMilli(), st.Self()); err != nil {
					t.Fatal(err)
				}
				if err := st.SetCursor(t.Context(), "changed-root-channel", "d", 1); err != nil {
					t.Fatal(err)
				}
				filter := (Subscription{Drive: "d", Session: "missing", Reader: "root", Kinds: []kinds.Kind{kinds.Incident, kinds.Decide, kinds.Ask, kinds.Defect, kinds.Blocker}, ToKinds: DefaultToKinds("root")}).Filter()
				filter.After, filter.Limit = 1, 500
				query, params := st.selectionColumns(filter, columns)
				rows, err := st.db.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+query, params...)
				if err != nil {
					t.Fatal(err)
				}
				var plan []string
				for rows.Next() {
					var id, parent, unused int
					var detail string
					if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
						_ = rows.Close()
						t.Fatal(err)
					}
					plan = append(plan, detail)
				}
				if err := rows.Err(); err != nil {
					_ = rows.Close()
					t.Fatal(err)
				}
				if err := rows.Close(); err != nil {
					t.Fatal(err)
				}
				if len(plan) == 0 {
					t.Fatal("query plan is empty")
				}
				records, err := st.Query(t.Context(), filter)
				if err != nil || len(records) != 0 {
					t.Fatalf("quiet query records=%v error=%v", records, err)
				}
				cursor, ok, err := st.Cursor(t.Context(), "changed-root-channel", "d")
				if err != nil || !ok || cursor != 1 {
					t.Fatalf("quiet query cursor=%d found=%t error=%v", cursor, ok, err)
				}
				t.Logf("changed_watch_query=%s parameters=%v plan=%q", query, params, plan)
				t.Logf("WATCH_CHANGED_GENERATION_PLAN population=%d placement=%s rows=%d cursor=%d plan_nodes=%d", population, placement, len(records), cursor, len(plan))
			})
		}
	}
}
