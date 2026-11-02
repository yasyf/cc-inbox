package store

import (
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

func openRootParentFixture(t *testing.T, manual bool) *Store {
	t.Helper()
	ctx := t.Context()
	st := openMembershipStore(ctx, t, t.TempDir())
	if _, err := st.db.ExecContext(ctx, "DROP INDEX records_re"); err != nil {
		t.Fatal(err)
	}
	if manual {
		dropRootAudience(ctx, t, st)
		report, err := MaintainIndexes(ctx, st.HomeDir())
		if err != nil || !slices.Contains(report.Memberships, "record_root_audience_parent") {
			t.Fatalf("owned parent maintenance = %+v, %v", report, err)
		}
		if _, err := st.db.ExecContext(ctx, "DROP INDEX records_re"); err != nil {
			t.Fatal(err)
		}
	}
	assertRootAudienceInstalled(ctx, t, st, true)
	var optional bool
	if err := st.db.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM sqlite_schema WHERE name='records_re')").Scan(&optional); err != nil || optional {
		t.Fatalf("optional parent index = %t, %v", optional, err)
	}
	for _, query := range []struct {
		where string
		args  []any
	}{
		{"r.re = ? AND r.seq != ?", []any{int64(1000000), int64(1000000)}},
		{"r.re IN (?, ?) AND r.seq != ? AND (? IS NOT ? OR ? IS NOT ?)", []any{int64(1000000), int64(1000001), int64(1000001), int64(1000000), int64(1000001), "root", "main"}},
		{"r.re = ?", []any{int64(1000000)}},
	} {
		plan := strings.Join(queryPlan(ctx, t, st, rootAudienceRows(query.where), query.args...), "\n")
		if !strings.Contains(plan, "SEARCH r USING INDEX record_root_audience_parent") || strings.Contains(plan, "SCAN r") {
			t.Fatalf("ROOT_PARENT_DEPENDENCY_REQUIRED where=%q plan=%q", query.where, plan)
		}
		t.Logf("ROOT_PARENT_NATIVE_PLAN optional_records_re=false where=%q plan=%q", query.where, plan)
	}
	return st
}

func seedRootParentHistory(t *testing.T, st *Store, population int) time.Duration {
	t.Helper()
	ctx := t.Context()
	started := time.Now()
	_, err := st.db.ExecContext(ctx, `WITH RECURSIVE history(n) AS (
 VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n<?
 )
 INSERT INTO records(drive,lane,kind,at,text,recipients,source,hash)
 SELECT 'concurrent','seed','note',n,'seed','["peer:before"]','post','before-'||n FROM history`, population)
	elapsed := time.Since(started)
	if err != nil {
		t.Fatal(err)
	}
	var records, projected int
	if err := st.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&records); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM record_root_audience").Scan(&projected); err != nil || records != population || projected != population {
		t.Fatalf("root seed records=%d projected=%d want=%d, %v", records, projected, population, err)
	}
	t.Logf("ROOT_PARENT_SEED rows=%d optional_records_re=false elapsed=%s", population, elapsed)
	return elapsed
}

func TestRootAudienceOwnedParentDependency(t *testing.T) {
	for _, manual := range []bool{false, true} {
		t.Run(fmt.Sprint(manual), func(t *testing.T) {
			ctx := t.Context()
			st := openRootParentFixture(t, manual)
			seedRootParentHistory(t, st, 32)
			exec := func(query string, args ...any) {
				t.Helper()
				if _, err := st.db.ExecContext(ctx, query, args...); err != nil {
					t.Fatal(err)
				}
				assertRootAudienceDerived(ctx, t, st)
				for _, reader := range []string{"root", "main"} {
					f := Filter{Drive: "d", For: reader}
					got, gotErr := st.Query(ctx, f)
					want, wantErr := unboundedQuery(ctx, st, f)
					assertSameRead(t, reader, got, gotErr, want, wantErr)
					n, nErr := st.Count(ctx, f)
					m, mErr := unboundedCount(ctx, st, f)
					assertSameRead(t, reader+" count", n, nErr, m, mErr)
				}
			}
			exec(`INSERT INTO records(seq,drive,lane,kind,at,text,recipients,re,source,hash) VALUES(1001,'d','worker','note',1,'child','["other"]',1000,'post','child')`)
			exec(`INSERT INTO records(seq,drive,lane,kind,at,text,recipients,re,source,hash) VALUES(1000,'foreign','root','note',1,'parent','["other"]',0,'post','parent')`)
			for _, query := range []string{
				`UPDATE records SET lane='main' WHERE seq=1000`,
				`UPDATE records SET lane='worker' WHERE seq=1000`,
				`UPDATE records SET lane='root' WHERE seq=1000`,
				`UPDATE records SET seq=1002 WHERE seq=1000`,
				`UPDATE records SET re=1002 WHERE seq=1001`,
				`UPDATE records SET drive='other',kind='svc.raw',source='hook:main',recipients='["root"]' WHERE seq=1001`,
				`DELETE FROM records WHERE seq=1002`,
				`INSERT INTO records(seq,drive,lane,kind,at,text,recipients,re,source,hash) VALUES(1002,'foreign','main','note',1,'parent','["other"]',0,'post','parent-again')`,
			} {
				exec(query)
			}
			tx, err := st.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.ExecContext(ctx, "UPDATE records SET lane='worker' WHERE seq=1002"); err != nil {
				t.Fatal(err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			assertRootAudienceDerived(ctx, t, st)
			for _, damage := range []string{"DROP INDEX record_root_audience_parent", "CREATE INDEX record_root_audience_parent ON records(re, seq)"} {
				if _, err := st.db.ExecContext(ctx, damage); err != nil {
					t.Fatal(err)
				}
				before := schemaObjects(ctx, t, st)
				f := Filter{Drive: "d", For: "root"}
				if _, err := st.Query(ctx, f); err == nil {
					t.Fatal("missing owned dependency admitted")
				}
				if _, err := st.Count(ctx, f); err == nil {
					t.Fatal("missing owned dependency counted")
				}
				if _, err := MaintainIndexes(ctx, st.HomeDir()); err == nil {
					t.Fatal("partial owned dependency repaired")
				}
				if after := schemaObjects(ctx, t, st); !slices.Equal(before, after) {
					t.Fatal("owned dependency refusal changed schema")
				}
			}
		})
	}
}

func TestRootAudienceOwnedParentWriterCost(t *testing.T) {
	if os.Getenv("CCI_ROOT_PARENT_COST") == "" {
		t.Skip("CI runs the native writer budget without race instrumentation")
	}
	for _, population := range []int{32, 10000} {
		for _, manual := range []bool{false, true} {
			t.Run(fmt.Sprintf("rows=%d/manual=%t", population, manual), func(t *testing.T) {
				st := openRootParentFixture(t, manual)
				if elapsed := seedRootParentHistory(t, st, population); elapsed >= 5*time.Second {
					t.Fatalf("ROOT_PARENT_SEED_BOUND_EXCEEDED rows=%d elapsed=%s", population, elapsed)
				}
				assertRootAudienceDerived(t.Context(), t, st)
			})
		}
	}
}
