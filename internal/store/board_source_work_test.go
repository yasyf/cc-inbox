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

var boardSourceWorkID atomic.Uint64

type boardSourceWorkResult struct {
	output     string
	predicates int64
	plan       string
}

func TestBoardSameKindSQLPredicateWork(t *testing.T) {
	if os.Getenv("CCI_BOARD_SQL_WORK") != "1" {
		t.Skip("set CCI_BOARD_SQL_WORK=1 for private board SQL predicate controls")
	}
	var previous int64
	for _, population := range []int{32, 10000} {
		native := boardSourceWork(t, population, "native")
		identity := boardSourceWork(t, population, "identity")
		counted := boardSourceWork(t, population, "counted")
		if native.output != identity.output || native.output != counted.output {
			t.Fatalf("board SQL probe changed output: native=%q identity=%q counted=%q", native.output, identity.output, counted.output)
		}
		if counted.predicates <= 0 {
			t.Fatal("SQL predicate counter never executed")
		}
		if population == 32 {
			previous = counted.predicates
		} else if counted.predicates != previous {
			t.Fatalf("BOARD_SAME_KIND_SQL_WORK_UNBOUNDED population=%d predicates=%d previous=%d", population, counted.predicates, previous)
		}
		t.Logf("BOARD_SAME_KIND_SQL_WORK population=%d record_predicate_evaluations=%d native_plan=%q identity_plan=%q counted_plan=%q", population, counted.predicates, native.plan, identity.plan, counted.plan)
	}
}

func boardSourceWork(t *testing.T, population int, mode string) boardSourceWorkResult {
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
	st, err = Open(t.Context(), home)
	if err != nil {
		t.Fatal(err)
	}
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
