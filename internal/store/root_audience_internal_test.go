package store

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

func dropRootAudience(ctx context.Context, t *testing.T, st *Store) {
	t.Helper()
	for i := len(rootAudienceObjects) - 1; i >= 0; i-- {
		object := rootAudienceObjects[i]
		if _, err := st.db.ExecContext(ctx, "DROP "+object.kind+" "+object.name); err != nil {
			t.Fatal(err)
		}
	}
}

func assertRootAudienceInstalled(ctx context.Context, t *testing.T, st *Store, want bool) {
	t.Helper()
	installed, err := projectionInstalled(ctx, st.db, rootAudienceObjects[:], "root audience")
	if err != nil || installed != want {
		t.Fatalf("root installed=%t, %v; want %t", installed, err, want)
	}
}

func assertRootAudienceDerived(ctx context.Context, t *testing.T, st *Store) {
	t.Helper()
	installed, err := projectionInstalled(ctx, st.db, rootAudienceObjects[:], "root audience")
	if err != nil {
		t.Fatal(err)
	}
	if !installed {
		return
	}
	var differences int
	query := "WITH expected(source_seq, drive, kind, audience) AS (" + rootAudienceRows("1") + ") SELECT COUNT(*) FROM (SELECT * FROM (SELECT * FROM record_root_audience EXCEPT SELECT * FROM expected) UNION ALL SELECT * FROM (SELECT * FROM expected EXCEPT SELECT * FROM record_root_audience))"
	if err := st.db.QueryRowContext(ctx, query).Scan(&differences); err != nil || differences != 0 {
		t.Fatalf("root routing differences = %d, %v", differences, err)
	}
}

func TestRootAudienceParentAndWriterTransitions(t *testing.T) {
	for _, recreateRootLast := range []bool{false, true} {
		t.Run(fmt.Sprint(recreateRootLast), func(t *testing.T) {
			ctx := t.Context()
			st := openMembershipStore(ctx, t, t.TempDir())
			if recreateRootLast {
				for _, object := range rootAudienceObjects {
					if object.kind == "trigger" {
						if _, err := st.db.ExecContext(ctx, "DROP TRIGGER "+object.name); err != nil {
							t.Fatal(err)
						}
						if _, err := st.db.ExecContext(ctx, object.sql); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			exec := func(query string, args ...any) {
				t.Helper()
				if _, err := st.db.ExecContext(ctx, query, args...); err != nil {
					t.Fatal(query, err)
				}
				assertRootAudienceDerived(ctx, t, st)
			}
			for i, row := range []struct {
				seq, re                 int64
				drive, lane, recipients string
			}{
				{200, 100, "d", "worker", `["other"]`},
				{201, 101, "d", "worker", `["all"]`},
				{100, 0, "foreign", "root", `["other"]`},
				{0, 0, "d", "main", `[]`},
				{-2, -1, "d", "worker", `["root", "all"]`},
				{-1, -1, "foreign", "main", `[]`},
			} {
				exec(`INSERT INTO records(seq,drive,lane,kind,at,text,recipients,re,source,hash,origin,origin_seq) VALUES(?,?,?,'note',1,'fixture',?,?,'post',?,'root-audience-fixture',?)`, row.seq, row.drive, row.lane, row.recipients, row.re, fmt.Sprint(row.seq), i+1)
			}
			for _, query := range []string{
				`UPDATE records SET seq=101 WHERE seq=100`,
				`UPDATE records SET lane='worker' WHERE seq=101`,
				`UPDATE records SET lane='main' WHERE seq=101`,
				`UPDATE records SET re=101, recipients='["root", "all"]' WHERE seq=200`,
				`UPDATE records SET drive='other',kind='svc.raw',lane='root',source='hook:root' WHERE seq=200`,
				`UPDATE records SET source='post',lane='worker' WHERE seq=200`,
				`UPDATE records SET recipients='[' WHERE seq=200`,
				`UPDATE records SET recipients=jsonb('["main", "all"]') WHERE seq=200`,
				`UPDATE records SET recipients='{ "x": "root" }' WHERE seq=200`,
				`UPDATE records SET recipients='"main"' WHERE seq=200`,
				`UPDATE records SET recipients='[]', text='fresh', body='{"fresh":true}', at=2,topic='fresh',refs='{}' WHERE seq=200`,
				`DELETE FROM records WHERE seq=101`,
				`INSERT OR IGNORE INTO records(seq,drive,lane,kind,at,text,recipients,re,source,hash) VALUES(101,'foreign','root','note',1,'parent','[]',0,'post','new-parent')`,
				`UPDATE records SET lane='worker' WHERE seq=0`,
				`UPDATE records SET seq=7 WHERE seq=0`,
				`DELETE FROM records WHERE seq=-1`,
			} {
				exec(query)
			}
			tx, err := st.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.ExecContext(ctx, `UPDATE records SET lane='main',recipients='["root"]' WHERE seq=200`); err != nil {
				t.Fatal(err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			assertRootAudienceDerived(ctx, t, st)
		})
	}
}

func TestRootAudienceMatchesCanonicalPredicates(t *testing.T) {
	for _, variant := range membershipVariants {
		t.Run(variant.name, func(t *testing.T) {
			ctx := t.Context()
			st := openMembershipStore(ctx, t, t.TempDir())
			st.Now = func() time.Time { return time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC) }
			seedMembershipFixture(ctx, t, st, st.Now())
			if _, err := st.db.ExecContext(ctx, `INSERT INTO records(drive,lane,kind,at,text,topic,recipients,re,source,hash) VALUES
('d','worker','decide',1,'overlap','','["root","all"]',0,'post','root-overlap'),
('foreign','main','note',1,'parent','','["other"]',0,'post','root-parent'),
('d','worker','svc.raw',1,'reply','','["other"]',17,'post','root-reply'),
('d','worker','urgent',1,'prior','same','["other"]',0,'post','root-prior'),
('d','worker','urgent',2,'repeat','same','["root"]',0,'post','root-repeat')`); err != nil {
				t.Fatal(err)
			}
			for _, set := range variant.sets {
				if _, err := st.db.ExecContext(ctx, "UPDATE records SET "+set.set+" WHERE seq=?", set.seq); err != nil {
					t.Fatal(err)
				}
			}
			for _, reader := range []string{"root", "main"} {
				filters := []Filter{
					{Drive: "d", For: reader},
					{Drive: "d", For: reader, Kinds: []kinds.Kind{kinds.Decide}, ToKinds: []kinds.Kind{kinds.Note}},
					{Drive: "d", For: reader, Kinds: []kinds.Kind{kinds.Note}, ToKinds: []kinds.Kind{kinds.Decide}},
					{Drive: "d", For: reader, Kinds: []kinds.Kind{kinds.Decide}, ToKinds: []kinds.Kind{kinds.Decide}, Mute: []kinds.Kind{kinds.Decide}},
					{Drive: "d", For: reader, Kinds: []kinds.Kind{kinds.Kind("svc.raw")}},
					{Drive: "d", For: reader, Peer: "reader", After: 1, Limit: 3, Descending: true},
					{Drive: "d", For: reader, Peer: "reader", Before: 14, IncludeExpired: true, Limit: 500},
					(Subscription{Drive: "d", Reader: reader, Session: "reader", ToKinds: DefaultToKinds(reader)}).Filter(),
				}
				for i, f := range filters {
					got, gotErr := st.Query(ctx, f)
					want, wantErr := unboundedQuery(ctx, st, f)
					assertSameRead(t, fmt.Sprint(reader, i), got, gotErr, want, wantErr)
					n, nErr := st.Count(ctx, f)
					m, mErr := unboundedCount(ctx, st, f)
					assertSameRead(t, fmt.Sprint("count", reader, i), n, nErr, m, mErr)
				}
			}
		})
	}
}

func TestRootAudienceAdmissionAndManualProvision(t *testing.T) {
	ctx := t.Context()
	st := openMembershipStore(ctx, t, t.TempDir())
	f := (Subscription{Drive: "d", Reader: "root", Session: "reader", ToKinds: DefaultToKinds("root")}).Filter()
	connection := func(want bool) {
		t.Helper()
		conn, err := st.rootAudienceConnection(ctx, f)
		if err != nil || (conn != nil) != want {
			t.Fatalf("root capability = %v, %v; want %t", conn != nil, err, want)
		}
		if conn != nil {
			_ = conn.Close()
		}
	}
	connection(true)
	dropRootAudience(ctx, t, st)
	before := schemaObjects(ctx, t, st)
	connection(false)
	if _, err := st.Query(ctx, f); err != nil {
		t.Fatal(err)
	}
	if after := schemaObjects(ctx, t, st); !slices.Equal(before, after) {
		t.Fatal("unadmitted read changed schema")
	}
	home := st.home
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st = openMembershipStore(ctx, t, home)
	if after := schemaObjects(ctx, t, st); !slices.Equal(before, after) {
		t.Fatal("current Open provisioned root group")
	}
	connection(false)
	report, err := MaintainIndexes(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	for _, object := range rootAudienceObjects {
		if !slices.Contains(report.Memberships, object.name) {
			t.Fatal("missing root report", object.name)
		}
	}
	connection(true)
	repeated, err := MaintainIndexes(ctx, home)
	if err != nil || len(repeated.Memberships) != 0 {
		t.Fatalf("repeat=%+v, %v", repeated, err)
	}
	for _, damage := range []string{"DROP TRIGGER record_root_audience_delete", "DROP INDEX record_root_audience_stream; CREATE INDEX record_root_audience_stream ON record_root_audience(drive)"} {
		t.Run(damage, func(t *testing.T) {
			other := openMembershipStore(ctx, t, t.TempDir())
			if _, err := other.db.ExecContext(ctx, damage); err != nil {
				t.Fatal(err)
			}
			before := schemaObjects(ctx, t, other)
			if _, err := other.Query(ctx, f); err == nil {
				t.Fatal("foreign group admitted")
			}
			if _, err := other.Count(ctx, f); err == nil {
				t.Fatal("foreign group counted")
			}
			if _, err := MaintainIndexes(ctx, other.home); err == nil {
				t.Fatal("foreign group repaired")
			}
			if after := schemaObjects(ctx, t, other); !slices.Equal(before, after) {
				t.Fatal("refusal changed schema")
			}
			outside := f
			outside.Follow = []Follow{{Drive: "foreign", Kinds: []kinds.Kind{kinds.Note}}}
			if _, err := other.Query(ctx, outside); err != nil {
				t.Fatal("outside scope touched root group", err)
			}
		})
	}
}

func TestRootAudienceActualConnectionLimits(t *testing.T) {
	ctx := t.Context()
	st := openMembershipStore(ctx, t, t.TempDir())
	st.db.SetMaxOpenConns(1)
	conn, err := st.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sqlite.Limit(conn, sqlite3.SQLITE_LIMIT_COMPOUND_SELECT, 12); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{1, 2} {
		f := Filter{Drive: "d", For: "root", Peer: "reader"}
		for i := range n {
			f.Kinds = append(f.Kinds, kinds.Kind(fmt.Sprintf("svc.k%d", i)))
			f.ToKinds = append(f.ToKinds, kinds.Kind(fmt.Sprintf("svc.k%d", i)))
		}
		conn, err := st.rootAudienceConnection(ctx, f)
		if err != nil {
			t.Fatal(err)
		}
		if (conn != nil) != (n == 1) {
			t.Fatalf("kind count=%d admitted=%t", n, conn != nil)
		}
		if conn != nil {
			_ = conn.Close()
		}
		got, gotErr := st.Query(ctx, f)
		want, wantErr := unboundedQuery(ctx, st, f)
		assertSameRead(t, "compound boundary", got, gotErr, want, wantErr)
	}
	conn, err = st.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sqlite.Limit(conn, sqlite3.SQLITE_LIMIT_COMPOUND_SELECT, 500); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	f := Filter{Drive: "d", For: "root", Peer: "reader", Kinds: []kinds.Kind{kinds.Ask}, ToKinds: []kinds.Kind{kinds.Ask}}
	_, general := st.selectionColumns(f, columns)
	_, optimized := st.selectionColumnsAudience(f, columns, true)
	if len(optimized) <= len(general) {
		t.Fatal("bind boundary fixture does not distinguish routes")
	}
	for _, limit := range []int{len(general), len(optimized)} {
		conn, err = st.db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sqlite.Limit(conn, sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, limit); err != nil {
			t.Fatal(err)
		}
		_ = conn.Close()
		admitted, err := st.rootAudienceConnection(ctx, f)
		if err != nil {
			t.Fatal(err)
		}
		if (admitted != nil) != (limit == len(optimized)) {
			t.Fatalf("bind limit=%d admitted=%t", limit, admitted != nil)
		}
		if admitted != nil {
			_ = admitted.Close()
		}
		got, gotErr := st.Query(ctx, f)
		want, wantErr := unboundedQuery(ctx, st, f)
		assertSameRead(t, "bind boundary", got, gotErr, want, wantErr)
	}
}

func TestRootAudienceNativePlan(t *testing.T) {
	ctx := t.Context()
	st := openMembershipStore(ctx, t, t.TempDir())
	for _, f := range []Filter{{Drive: "d", For: "root"}, {Drive: "d", For: "main", Kinds: []kinds.Kind{kinds.Note}, ToKinds: DefaultToKinds("main")}} {
		conn, err := st.rootAudienceConnection(ctx, f)
		if err != nil || conn == nil {
			t.Fatal("not admitted", err)
		}
		_ = conn.Close()
		query, args := st.selectionColumnsAudience(f, columns, true)
		plan := queryPlan(ctx, t, st, query, args...)
		joined := strings.Join(plan, "\n")
		if !strings.Contains(joined, "SEARCH record_root_audience USING COVERING INDEX record_root_audience_") || strings.Contains(joined, "SCAN record_root_audience") || strings.Contains(joined, "SCAN records") {
			t.Fatalf("root plan: %s", joined)
		}
		t.Logf("ROOT_AUDIENCE_NATIVE_PLAN nodes=%d plan=%q", len(plan), plan)
	}
}

func TestRootAudienceCapabilityAcrossAtomicProvision(t *testing.T) {
	ctx := t.Context()
	st := openMembershipStore(ctx, t, t.TempDir())
	dropRootAudience(ctx, t, st)
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := installProjection(ctx, tx, rootAudienceObjects[:], "root audience"); err != nil {
		t.Fatal(err)
	}
	f := Filter{Drive: "d", For: "root"}
	before, err := st.rootAudienceConnection(ctx, f)
	if before != nil {
		_ = before.Close()
	}
	if err != nil || before != nil {
		t.Fatalf("uncommitted capability=%v,%v", before, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	after, err := st.rootAudienceConnection(ctx, f)
	if err != nil || after == nil {
		t.Fatal("committed group not admitted", err)
	}
	_ = after.Close()
}
