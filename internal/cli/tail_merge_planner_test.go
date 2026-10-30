package cli_test

import (
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"modernc.org/sqlite"
)

type tailPlannerResult struct {
	seqs  []int64
	phase string
	err   string
}

func TestTailOrderedCandidatesPlannerPrototype(t *testing.T) {
	const (
		predicate = `((drive = ?) OR EXISTS (SELECT 1 FROM json_each(recipients) WHERE value IN (?,?))) AND at >= ? AND (expires_at IS NULL OR expires_at > ?)`
		candidate = `SELECT seq AS candidate_seq FROM records WHERE drive = ?
UNION SELECT source_seq FROM record_recipients WHERE address = ?
UNION SELECT source_seq FROM record_recipients WHERE address = ?
UNION SELECT source_seq FROM record_json_errors WHERE field = 'recipients'
ORDER BY candidate_seq DESC`
	)
	variants := []struct {
		name, query string
		outerOrder  bool
	}{
		{"subquery-outer-order", "SELECT r.seq FROM (" + candidate + ") c CROSS JOIN records r ON r.seq = c.candidate_seq WHERE " + predicate + " ORDER BY c.candidate_seq DESC LIMIT 1", true},
		{"cte-outer-order", "WITH candidates AS (" + candidate + ") SELECT r.seq FROM candidates c CROSS JOIN records r ON r.seq = c.candidate_seq WHERE " + predicate + " ORDER BY c.candidate_seq DESC LIMIT 1", true},
		{"cte-not-materialized-outer-order", "WITH candidates AS NOT MATERIALIZED (" + candidate + ") SELECT r.seq FROM candidates c CROSS JOIN records r ON r.seq = c.candidate_seq WHERE " + predicate + " ORDER BY c.candidate_seq DESC LIMIT 1", true},
		{"subquery-no-flatten-outer-order", "SELECT r.seq FROM (" + candidate + " LIMIT -1) c CROSS JOIN records r ON r.seq = c.candidate_seq WHERE " + predicate + " ORDER BY c.candidate_seq DESC LIMIT 1", true},
		{"subquery-native-order", "SELECT r.seq FROM (" + candidate + ") c CROSS JOIN records r ON r.seq = c.candidate_seq WHERE " + predicate + " LIMIT 1", false},
		{"subquery-no-flatten-native-order", "SELECT r.seq FROM (" + candidate + " LIMIT -1) c CROSS JOIN records r ON r.seq = c.candidate_seq WHERE " + predicate + " LIMIT 1", false},
	}
	passing := make([]bool, len(variants))
	for i := range passing {
		passing[i] = true
	}
	for _, population := range []int{32, 10000} {
		for _, corruption := range []string{"none", "older", "newer"} {
			t.Run(fmt.Sprintf("rows=%d/corruption=%s", population, corruption), func(t *testing.T) {
				var comparisons atomic.Int64
				collation := fmt.Sprintf("cci_tail_merge_%d", tailCounterID.Add(1))
				if err := sqlite.RegisterCollationUtf8(collation, func(a, b string) int {
					comparisons.Add(1)
					return strings.Compare(a, b)
				}); err != nil {
					t.Fatal(err)
				}
				db := tailLimitComparedStore(t, collation)
				at := time.Now().UnixMilli()
				bad := func() {
					if _, err := db.Exec("INSERT INTO records (drive,lane,kind,at,text,recipients,source,hash) VALUES ('foreign','lane','note',?,'bad','{broken','post','bad')", at); err != nil {
						t.Fatal(err)
					}
				}
				if corruption == "older" {
					bad()
				}
				if _, err := db.Exec(`WITH RECURSIVE history(n) AS (
VALUES(1) UNION ALL SELECT n + 1 FROM history WHERE n < ?
)
INSERT INTO records (drive,lane,kind,at,text,source,hash)
SELECT 'durable-agents-rebuild','lane','note',?,'selected','post','selected-' || n FROM history`, population, at); err != nil {
					t.Fatal(err)
				}
				if corruption == "newer" {
					bad()
				}
				filterArgs := []any{"durable-agents-rebuild", "peer:tail-limit-session", "peer:0123456789abcdef/tail-limit-session", at - 36*3600000, at}
				candidateArgs := []any{"durable-agents-rebuild", "peer:tail-limit-session", "peer:0123456789abcdef/tail-limit-session"}
				args := append(slices.Clone(candidateArgs), filterArgs...)
				comparisons.Store(0)
				old := tailPlannerRows(db, "SELECT seq FROM records WHERE "+predicate+" ORDER BY seq DESC LIMIT 1", filterArgs)
				t.Logf("reference same_drive_rows=%d corruption=%s native_drive_comparisons=%d result=%+v", population, corruption, comparisons.Load(), old)
				for i, v := range variants {
					plan := tailPlannerPlan(t, db, v.query, args)
					comparisons.Store(0)
					got := tailPlannerRows(db, v.query, args)
					count := comparisons.Load()
					parity := slices.Equal(got.seqs, old.seqs) && got.phase == old.phase && got.err == old.err
					bounded := count <= 64
					passing[i] = passing[i] && parity && bounded
					t.Logf("variant=%s outer_order=%t same_drive_rows=%d corruption=%s native_drive_comparisons=%d parity=%t bounded=%t result=%+v plan=%s", v.name, v.outerOrder, population, corruption, count, parity, bounded, got, strings.Join(plan, " | "))
				}
			})
		}
	}
	var winners []string
	for i, v := range variants {
		if passing[i] {
			winners = append(winners, v.name)
		}
	}
	t.Logf("prototype variants preserving all observed native order/error frontiers and bounded comparisons: %v", winners)
	if len(winners) == 0 {
		t.Fatal("no SQL candidate stream passed the baseline frontier and top-one cost probes")
	}
}

func tailPlannerRows(db *sql.DB, query string, args []any) tailPlannerResult {
	rows, err := db.Query(query, args...)
	if err != nil {
		return tailPlannerResult{phase: "query", err: err.Error()}
	}
	defer func() { _ = rows.Close() }()
	var result tailPlannerResult
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			result.phase, result.err = "scan", err.Error()
			return result
		}
		result.seqs = append(result.seqs, seq)
	}
	if err := rows.Err(); err != nil {
		result.phase, result.err = "rows", err.Error()
	}
	return result
}

func TestCountPeerAfterPlannerErrorPrototype(t *testing.T) {
	db := tailCostStore(t)
	for _, row := range []struct{ drive, recipients string }{{"d", "[]"}, {"d", "["}} {
		if _, err := db.Exec("INSERT INTO records (drive,lane,kind,at,text,recipients,source,hash) VALUES (?,'lane','note',1,'selected',?,'post',?)", row.drive, row.recipients, row.recipients); err != nil {
			t.Fatal(err)
		}
	}
	predicate := `((drive = ?) OR (EXISTS (SELECT 1 FROM json_each(recipients) WHERE value IN (?,?)) AND seq > ?)) AND (expires_at IS NULL OR expires_at > ?)`
	args := []any{"d", "peer:reader", "peer:0123456789abcdef/reader", 1, time.Now().UnixMilli()}
	base := "SELECT COUNT(*) FROM records WHERE " + predicate
	want := tailPlannerRows(db, base, args)
	oldPlan := tailPlannerPlan(t, db, base, args)
	if want.err == "" || !strings.Contains(strings.Join(oldPlan, " | "), "MULTI-INDEX OR") {
		t.Fatalf("reference did not reproduce the peer-after native error: %+v plan=%v", want, oldPlan)
	}
	candidates := `(SELECT seq FROM records WHERE drive = ? UNION ALL SELECT source_seq FROM record_recipients WHERE address IN (?,?) AND source_seq > ? UNION ALL SELECT source_seq FROM record_json_errors WHERE field = 'recipients')`
	allArgs := append(slices.Clone(args), "d", "peer:reader", "peer:0123456789abcdef/reader", 1)
	for _, column := range []string{"seq", "+seq"} {
		q := base + " AND " + column + " IN " + candidates
		got := tailPlannerRows(db, q, allArgs)
		plan := tailPlannerPlan(t, db, q, allArgs)
		parity := slices.Equal(got.seqs, want.seqs) && got.phase == want.phase && got.err == want.err
		t.Logf("peer_after_count candidate=%s parity=%t result=%+v reference=%+v plan=%v original_plan=%v", column, parity, got, want, plan, oldPlan)
		if column == "+seq" && !parity {
			t.Fatal("unary-plus candidate did not preserve the original count error")
		}
	}
}

func tailPlannerPlan(t *testing.T, db *sql.DB, query string, args []any) []string {
	t.Helper()
	rows, err := db.Query("EXPLAIN QUERY PLAN "+query, args...)
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
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return plan
}
