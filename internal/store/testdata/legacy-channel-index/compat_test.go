package store

import (
	"crypto/sha256"
	"database/sql/driver"
	"fmt"
	"math/bits"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"modernc.org/sqlite"
)

func TestLegacyChannelIndexCompatibility(t *testing.T) {
	source, err := os.ReadFile("query.go")
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprintf("%x", sha256.Sum256(source)); got != "b7a4026f4ca3c644ae291729ff8e594daf9107371dc98a52d5b27fdb2825b39e" {
		t.Fatalf("legacy query source = %s", got)
	}
	var reads atomic.Int64
	if err := sqlite.RegisterScalarFunction("cci_legacy_recipient_probe", 1, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
		reads.Add(1)
		return args[0], nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, population := range []int{32, 10000} {
		for _, placement := range []string{"same-drive", "foreign-drive"} {
			for _, expiredHistory := range []bool{false, true} {
				for _, index := range []struct{ name, sql string }{
					{"predicate-cover", "CREATE INDEX legacy_trial_index ON legacy_index_history(seq,drive,recipients,lane,kind,source,expires_at)"},
					{"expiry", "CREATE INDEX legacy_trial_index ON legacy_index_history(expires_at,seq)"},
				} {
			t.Run(fmt.Sprintf("%d/%s/expired=%t/%s", population, placement, expiredHistory, index.name), func(t *testing.T) {
				ctx := t.Context()
				st, err := Open(ctx, t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = st.Close() })
				now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
				st.Now = func() time.Time { return now }
				exec := func(query string, args ...any) {
					t.Helper()
					if _, err := st.db.ExecContext(ctx, query, args...); err != nil {
						t.Fatal(err)
					}
				}
				exec(`INSERT INTO records (drive,lane,kind,at,text,recipients,source,hash)
VALUES ('foreign','worker','state',?,'delivered anchor','["peer:legacy-reader"]','post','anchor')`, now.UnixMilli())
				drive := "legacy-d"
				if placement == "foreign-drive" {
					drive = "foreign"
				}
				var expires any
				if expiredHistory {
					expires = now.Add(-time.Hour).UnixMilli()
				}
				exec(`WITH RECURSIVE history(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n<?)
INSERT INTO records (drive,lane,kind,at,text,recipients,expires_at,source,hash)
SELECT ?,'worker','state',?,'unmatched','["worker"]',?,'post','noise-'||n FROM history`, population, drive, now.UnixMilli(), expires)
				filter := Filter{Drive: "legacy-d", Peer: "legacy-reader", For: "root", Kinds: []kinds.Kind{kinds.Note}, After: 1, Limit: 500}
				physicalPlan := legacyIndexPlan(t, st, filter)
				exec("ALTER TABLE records RENAME TO legacy_index_history")
				columns, err := st.db.QueryContext(ctx, "SELECT name FROM pragma_table_info('legacy_index_history') ORDER BY cid")
				if err != nil {
					t.Fatal(err)
				}
				var identity, counted []string
				for columns.Next() {
					var name string
					if err := columns.Scan(&name); err != nil {
						t.Fatal(err)
					}
					identity = append(identity, name)
					if name == "recipients" {
						counted = append(counted, "cci_legacy_recipient_probe(recipients) AS recipients")
					} else {
						counted = append(counted, name)
					}
				}
				if err := columns.Err(); err != nil {
					t.Fatal(err)
				}
				if err := columns.Close(); err != nil {
					t.Fatal(err)
				}
				exec("CREATE VIEW records AS SELECT " + strings.Join(identity, ",") + " FROM legacy_index_history")
				view := func(projection []string) {
					t.Helper()
					exec("DROP VIEW records")
					exec("CREATE VIEW records AS SELECT " + strings.Join(projection, ",") + " FROM legacy_index_history")
				}
				for _, candidate := range []string{"existing", index.name} {
					if candidate == index.name {
						exec(index.sql)
						exec("ANALYZE")
					}
					view(identity)
					plan := legacyIndexPlan(t, st, filter)
					if candidate == "existing" && !slices.Equal(plan, physicalPlan) {
						t.Fatalf("identity view changes plan: %v != %v", plan, physicalPlan)
					}
					started := time.Now()
					for range 3 {
						rows, err := st.Query(ctx, filter)
						if err != nil || len(rows) != 0 {
							t.Fatalf("unchanged idle query rows=%v error=%v", rows, err)
						}
					}
					t.Logf("LEGACY_UNINSTRUMENTED_PRIVATE_TIMING history=%d placement=%s expired_history=%t candidate=%s polls=3 elapsed=%s plan=%v", population, placement, expiredHistory, candidate, time.Since(started), plan)
					view(counted)
					if got := legacyIndexPlan(t, st, filter); !slices.Equal(got, plan) {
						t.Fatalf("recipient counter changes plan: %v != %v", got, plan)
					}
					reads.Store(0)
					for range 3 {
						rows, err := st.Query(ctx, filter)
						if err != nil || len(rows) != 0 {
							t.Fatalf("counted idle query rows=%v error=%v", rows, err)
						}
					}
					work := reads.Load()
					bound := int64(3 * (2*bits.Len(uint(population)) + 8))
					t.Logf("LEGACY_RECIPIENT_WORK history=%d placement=%s expired_history=%t candidate=%s polls=3 materializations=%d bound=%d", population, placement, expiredHistory, candidate, work, bound)
					if !expiredHistory && work < int64(3*population) {
						t.Fatalf("legacy unmatched suffix did not expose linear recipient work: %d", work)
					}
					if !expiredHistory && candidate == index.name && os.Getenv("CCI_LEGACY_REQUIRE_BOUNDED_INDEX") == "1" && work > bound {
						t.Errorf("LEGACY_INDEX_NOT_BOUNDED history=%d placement=%s candidate=%s materializations=%d bound=%d", population, placement, candidate, work, bound)
					}
				}
				frontier := filter
				frontier.After = int64(population + 1)
				reads.Store(0)
				if rows, err := st.Query(ctx, frontier); err != nil || len(rows) != 0 || reads.Load() != 0 {
					t.Fatalf("known empty suffix control rows=%v error=%v reads=%d", rows, err, reads.Load())
				}
				for i, record := range []struct{ drive, lane, kind, to, body string; re int64 }{
					{"foreign", "worker", "state", `["peer:legacy-reader"]`, `{"kept":true}`, 0},
					{"legacy-d", "worker", "state", `["root"]`, "", 0},
					{"legacy-d", "worker", "note", `[]`, "", 0},
					{"foreign", "worker", "state", `["root@legacy-d"]`, "", 0},
					{"legacy-d", "worker", "note", `["all"]`, "", 0},
					{"legacy-d", "root", "note", `["worker"]`, "", 0},
					{"legacy-d", "worker", "note", `["worker"]`, "", int64(population + 7)},
				} {
					exec(`INSERT INTO legacy_index_history (drive,lane,kind,at,text,recipients,re,body,source,hash)
VALUES (?,?,?,?,?,?,?,?, 'post',?)`, record.drive, record.lane, record.kind, now.UnixMilli(), "late", record.to, record.re, record.body, fmt.Sprint("late-", i))
				}
				withIndex, err := st.Query(ctx, filter)
				if err != nil {
					t.Fatal(err)
				}
				var got []int64
				for _, row := range withIndex {
					got = append(got, row.Seq)
				}
				want := []int64{int64(population + 2), int64(population + 3), int64(population + 4), int64(population + 5)}
				if !slices.Equal(got, want) || string(withIndex[0].Body) != `{"kept":true}` {
					t.Fatalf("legacy scope/order/body got=%v want=%v", got, want)
				}
				view(identity)
				uninstrumented, err := st.Query(ctx, filter)
				if err != nil || !reflect.DeepEqual(withIndex, uninstrumented) {
					t.Fatalf("recipient counter changes records: error=%v", err)
				}
				exec("DROP INDEX legacy_trial_index")
				exec("ANALYZE")
				withoutIndex, err := st.Query(ctx, filter)
				if err != nil || !reflect.DeepEqual(withIndex, withoutIndex) {
					t.Fatalf("candidate changes records: error=%v", err)
				}
				exec(`INSERT INTO legacy_index_history (drive,lane,kind,at,text,recipients,source,hash) VALUES ('foreign','worker','state',?,'corrupt','not-json','post','corrupt')`, now.UnixMilli())
				for _, expired := range []bool{false, true} {
					if expired {
						exec("UPDATE legacy_index_history SET expires_at=1 WHERE hash='corrupt'")
					}
					_, oldErr := st.Query(ctx, filter)
					if !expired && (oldErr == nil || !strings.Contains(oldErr.Error(), "malformed JSON")) {
						t.Fatalf("malformed global-recipient control error=%v", oldErr)
					}
					exec(index.sql)
					exec("ANALYZE")
					_, newErr := st.Query(ctx, filter)
					if fmt.Sprint(oldErr) != fmt.Sprint(newErr) {
						t.Fatalf("candidate changes expired=%t error: %v != %v", expired, oldErr, newErr)
					}
					exec("DROP INDEX legacy_trial_index")
					exec("ANALYZE")
				}
					exec("DELETE FROM legacy_index_history WHERE hash='corrupt'")
					exec(`WITH RECURSIVE page(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM page WHERE n<501)
INSERT INTO legacy_index_history (drive,lane,kind,at,text,recipients,source,hash)
SELECT 'foreign','worker','state',?,'page','["peer:legacy-reader"]','post','page-'||n FROM page`, now.UnixMilli())
					exec(`INSERT INTO legacy_index_history (drive,lane,kind,at,text,recipients,source,hash) VALUES ('foreign','worker','state',?,'unread corrupt','not-json','post','unread-corrupt')`, now.UnixMilli())
					page, oldErr := st.Query(ctx, filter)
					if oldErr != nil || len(page) != 500 {
						t.Fatalf("old page frontier rows=%d error=%v", len(page), oldErr)
					}
					exec(index.sql)
					exec("ANALYZE")
					indexedPage, newErr := st.Query(ctx, filter)
					if newErr != nil || !reflect.DeepEqual(page, indexedPage) {
						t.Fatalf("candidate changes unread-corruption frontier rows=%d error=%v", len(indexedPage), newErr)
					}
			})
				}
			}
		}
	}
}

func legacyIndexPlan(t *testing.T, st *Store, filter Filter) []string {
	t.Helper()
	where, args := filter.where(st.Self(), st.Now())
	query := "EXPLAIN QUERY PLAN SELECT " + columns + " FROM records WHERE " + where + " ORDER BY seq ASC LIMIT 500"
	rows, err := st.db.QueryContext(t.Context(), query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, strings.ReplaceAll(detail, "legacy_index_history", "records"))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return plan
}
