package store

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"modernc.org/sqlite"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

func TestBoardFullPopulationSQLPredicateWork(t *testing.T) {
	if os.Getenv("CCI_BOARD_FULL_SQL_WORK") != "1" {
		t.Skip("set CCI_BOARD_FULL_SQL_WORK=1 for full board SQL populations")
	}
	var first int64
	for _, population := range []int{32, 10000, 100000} {
		native := boardUnionSourceWork(t, population, "native")
		identity := boardUnionSourceWork(t, population, "identity")
		counted := boardUnionSourceWork(t, population, "counted")
		if native.output != identity.output || native.output != counted.output {
			t.Fatal("native/identity/counted outputs differ")
		}
		if counted.predicates <= 0 {
			t.Fatal("actual SQLite predicate did not execute")
		}
		if population == 32 {
			first = counted.predicates
		} else if counted.predicates != first {
			t.Fatalf("BOARD_FULL_SQL_WORK_UNBOUNDED population=%d predicates=%d first=%d", population, counted.predicates, first)
		}
		t.Logf("BOARD_FULL_POPULATION_SQL_WORK population=%d record_predicate_evaluations=%d", population, counted.predicates)
	}
}

func TestBoardColdBackfillSQLPredicateWork(t *testing.T) {
	if os.Getenv("CCI_BOARD_FULL_SQL_WORK") != "1" {
		t.Skip("set CCI_BOARD_FULL_SQL_WORK=1 for full board SQL populations")
	}
	counts := map[int]int64{}
	for _, population := range []int{32, 10000, 100000} {
		native := boardColdSourceWork(t, population, "native")
		identity := boardColdSourceWork(t, population, "identity")
		counted := boardColdSourceWork(t, population, "counted")
		if native.output != identity.output || native.output != counted.output {
			t.Fatal("cold native/identity/counted outputs differ")
		}
		if counted.predicates <= int64(population) {
			t.Fatalf("cold probe did not execute complete backfill: population=%d predicates=%d", population, counted.predicates)
		}
		counts[population] = counted.predicates
		t.Logf("BOARD_COLD_BACKFILL_SQL_WORK population=%d record_predicate_evaluations=%d", population, counted.predicates)
	}
	difference := counts[10000] - counts[32]
	if difference <= 0 || difference%9968 != 0 {
		t.Fatalf("cold work is not an integral affine progression: %v", counts)
	}
	perRecord := difference / 9968
	if counts[100000] != counts[32]+perRecord*99968 {
		t.Fatalf("BOARD_COLD_SQL_WORK_NOT_AFFINE counts=%v", counts)
	}
}

func boardColdSourceWork(t *testing.T, population int, mode string) boardSourceWorkResult {
	t.Helper()
	home := t.TempDir()
	st, err := Open(t.Context(), home)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)
	st.Now = func() time.Time { return now }
	if err := st.InstallBoardProjection(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(t.Context(), `INSERT INTO records(seq,drive,lane,kind,at,text,source,hash,recipients,origin,origin_seq,id) VALUES(1,'d','root','go',?,'go #123','post','board-selected','[]',?,1,'board-selected')`, now.UnixMilli(), st.Self()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(t.Context(), `WITH RECURSIVE history(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n<?)
INSERT INTO records(seq,drive,lane,kind,at,text,source,hash,recipients,origin,origin_seq,id)
SELECT n+1,'d','root','decide',?,'ignored #999','post','board-unselected-'||n,'[]',?,n+1,'board-unselected-'||n FROM history`, population, now.UnixMilli(), st.Self()); err != nil {
		t.Fatal(err)
	}
	spec := BoardSpec{Drive: "d", Repo: "owner/repo", CWD: t.TempDir(), Filters: [3]Filter{
		{Drive: "d", Peer: "board-source-work", Kinds: []kinds.Kind{kinds.Go, kinds.Decide, kinds.Decision, kinds.Answer}},
		{Drive: "d", Peer: "board-source-work", Kinds: []kinds.Kind{kinds.Head, kinds.Review}},
		{Drive: "d", Peer: "board-source-work", Kinds: []kinds.Kind{kinds.Withdraw}, Lanes: []string{"root"}},
	}}
	index := NewBoardIndex(st)
	id, encoded, err := boardScopeID(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Close(); err != nil {
		t.Fatal(err)
	}
	saved := *st
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reads := &atomic.Int64{}
	function := fmt.Sprintf("cci_board_work_%d", boardSourceWorkID.Add(1))
	if mode != "native" {
		if err := sqlite.RegisterScalarFunction(function, 1, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
			if mode == "counted" {
				reads.Add(1)
			}
			return args[0], nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	db, err := sql.Open("sqlite", "file:"+filepath.Join(home, DBFile))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if mode != "native" {
		for _, query := range []string{"ALTER TABLE records RENAME TO board_cost_history", "CREATE VIEW records AS SELECT * FROM board_cost_history WHERE " + function + "(seq)>=0"} {
			if _, err := db.ExecContext(t.Context(), query); err != nil {
				t.Fatal(err)
			}
		}
	}
	probe, err := sql.Open("sqlite", storeDSN(home, int(busyTimeout.Milliseconds())))
	if err != nil {
		t.Fatal(err)
	}
	saved.db, saved.rw = probe, probe
	st = &saved
	defer func() { _ = st.Close() }()
	st.Now = func() time.Time { return now }
	index = NewBoardIndex(st)
	watch, err := newIncidentFileWatch()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = watch.Close() }()
	scope := &boardScope{id: id, spec: spec, watch: watch, pending: map[string]bool{}}
	reads.Store(0)
	if err := index.reconcile(t.Context(), scope, encoded, true, now); err != nil {
		t.Fatal(err)
	}
	tx, err := st.db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	snapshot := &BoardSnapshot{store: st, tx: tx, now: now}
	result := &BoardResult{}
	if err := index.selectResult(t.Context(), scope, snapshot, BoardSelection{PRs: []string{"123"}}, result); err != nil {
		t.Fatal(err)
	}
	evaluated := reads.Load()
	if result.Rulings.Error != nil || result.Reviews.Error != nil || result.Withdrawals.Error != nil {
		t.Fatalf("unexpected category errors: %+v", result)
	}
	if len(result.Rulings.Records) != 1 || result.Rulings.Records[0].Seq != 1 {
		t.Fatalf("selected records=%+v", result.Rulings.Records)
	}
	output, err := json.Marshal(struct {
		Rulings     []Record
		Withdrawals []Record
		Reviews     []Record
		Lanes       []string
	}{result.Rulings.Records, result.Withdrawals.Records, result.Reviews.Records, result.Reviews.Lanes})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := tx.QueryContext(t.Context(), "EXPLAIN QUERY PLAN SELECT "+columns+" FROM records WHERE seq=?", int64(2))
	if err != nil {
		t.Fatal(err)
	}
	var details []string
	for rows.Next() {
		var node, parent, unused int
		var detail string
		if err := rows.Scan(&node, &parent, &unused, &detail); err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		details = append(details, detail)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	plan := strings.Join(details, ";")
	if !strings.Contains(plan, "SEARCH") || !strings.Contains(plan, "PRIMARY KEY") {
		t.Fatalf("SQL probe lost primary-key candidate access: %s", plan)
	}
	return boardSourceWorkResult{output: string(output), predicates: evaluated, plan: plan}
}

func boardUnionSourceWork(t *testing.T, population int, mode string) boardSourceWorkResult {
	t.Helper()
	home := t.TempDir()
	st, err := Open(t.Context(), home)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)
	st.Now = func() time.Time { return now }
	if err := st.InstallBoardProjection(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(t.Context(), `INSERT INTO records(seq,drive,lane,kind,at,text,source,hash,recipients,origin,origin_seq,id) VALUES(1,'d','root','go',?,'go #123','post','board-selected','[]',?,1,'board-selected')`, now.UnixMilli(), st.Self()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(t.Context(), `WITH RECURSIVE history(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n<?)
INSERT INTO records(seq,drive,lane,kind,at,text,source,hash,recipients,origin,origin_seq,id)
SELECT n+1,'d','root','decide',?,'ignored #999','post','board-unselected-'||n,'[]',?,n+1,'board-unselected-'||n FROM history`, population, now.UnixMilli(), st.Self()); err != nil {
		t.Fatal(err)
	}
	spec := BoardSpec{Drive: "d", Repo: "owner/repo", CWD: t.TempDir(), Filters: [3]Filter{
		{Drive: "d", Peer: "board-source-work", Kinds: []kinds.Kind{kinds.Go, kinds.Decide, kinds.Decision, kinds.Answer}},
		{Drive: "d", Peer: "board-source-work", Kinds: []kinds.Kind{kinds.Head, kinds.Review}},
		{Drive: "d", Peer: "board-source-work", Kinds: []kinds.Kind{kinds.Withdraw}, Lanes: []string{"root"}},
	}}
	index := NewBoardIndex(st)
	id, err := index.Provision(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Close(); err != nil {
		t.Fatal(err)
	}
	saved := *st
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reads := &atomic.Int64{}
	function := fmt.Sprintf("cci_board_work_%d", boardSourceWorkID.Add(1))
	if mode != "native" {
		if err := sqlite.RegisterScalarFunction(function, 1, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
			if mode == "counted" {
				reads.Add(1)
			}
			return args[0], nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	db, err := sql.Open("sqlite", "file:"+filepath.Join(home, DBFile))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	table := "records"
	if mode != "native" {
		for _, query := range []string{"ALTER TABLE records RENAME TO board_cost_history", "CREATE VIEW records AS SELECT * FROM board_cost_history WHERE " + function + "(seq)>=0"} {
			if _, err := db.ExecContext(t.Context(), query); err != nil {
				t.Fatal(err)
			}
		}
		table = "board_cost_history"
	}
	probe, err := sql.Open("sqlite", storeDSN(home, int(busyTimeout.Milliseconds())))
	if err != nil {
		t.Fatal(err)
	}
	saved.db, saved.rw = probe, probe
	st = &saved
	defer func() { _ = st.Close() }()
	st.Now = func() time.Time { return now }
	index = NewBoardIndex(st)
	watch, err := newIncidentFileWatch()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = watch.Close() }()
	scope := &boardScope{id: id, spec: spec, watch: watch, pending: map[string]bool{}}
	updated, err := db.ExecContext(t.Context(), "UPDATE "+table+" SET text='changed ignored #999' WHERE seq=2")
	if err != nil {
		t.Fatal(err)
	}
	if count, err := updated.RowsAffected(); err != nil || count != 1 {
		t.Fatalf("old writer affected=%d error=%v", count, err)
	}
	reads.Store(0)
	if err := index.reconcile(t.Context(), scope, "", false, now); err != nil {
		t.Fatal(err)
	}
	tx, err := st.db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	snapshot := &BoardSnapshot{store: st, tx: tx, now: now}
	result := &BoardResult{}
	if err := index.selectResult(t.Context(), scope, snapshot, BoardSelection{PRs: []string{"123"}}, result); err != nil {
		t.Fatal(err)
	}
	evaluated := reads.Load()
	if result.Rulings.Error != nil || result.Reviews.Error != nil || result.Withdrawals.Error != nil {
		t.Fatalf("unexpected category errors: %+v", result)
	}
	if len(result.Rulings.Records) != 1 || result.Rulings.Records[0].Seq != 1 {
		t.Fatalf("selected records=%+v", result.Rulings.Records)
	}
	output, err := json.Marshal(struct {
		Rulings     []Record
		Withdrawals []Record
		Reviews     []Record
		Lanes       []string
	}{result.Rulings.Records, result.Withdrawals.Records, result.Reviews.Records, result.Reviews.Lanes})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := tx.QueryContext(t.Context(), "EXPLAIN QUERY PLAN SELECT "+columns+" FROM records WHERE seq=?", int64(2))
	if err != nil {
		t.Fatal(err)
	}
	var details []string
	for rows.Next() {
		var node, parent, unused int
		var detail string
		if err := rows.Scan(&node, &parent, &unused, &detail); err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		details = append(details, detail)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	plan := strings.Join(details, ";")
	if !strings.Contains(plan, "SEARCH") || !strings.Contains(plan, "PRIMARY KEY") {
		t.Fatalf("SQL probe lost primary-key candidate access: %s", plan)
	}
	return boardSourceWorkResult{output: string(output), predicates: evaluated, plan: plan}
}
