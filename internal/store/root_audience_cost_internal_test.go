package store

import (
	"context"
	"database/sql/driver"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"modernc.org/sqlite"
)

var rootCandidateEmissions atomic.Int64

func init() {
	if err := sqlite.RegisterScalarFunction("cci_root_candidate", 1, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
		rootCandidateEmissions.Add(1)
		return args[0], nil
	}); err != nil {
		panic(err)
	}
}

func rootCostSnapshot(ctx context.Context, t *testing.T, st *Store) []string {
	t.Helper()
	var out []string
	for _, query := range []string{"SELECT * FROM records ORDER BY seq", "SELECT * FROM cursors ORDER BY name, drive"} {
		rows, err := st.db.QueryContext(ctx, query)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			values := make([]any, len(columns))
			targets := make([]any, len(columns))
			for i := range targets {
				targets[i] = &values[i]
			}
			if err := rows.Scan(targets...); err != nil {
				t.Fatal(err)
			}
			var encoded []string
			for _, value := range values {
				if bytes, ok := value.([]byte); ok {
					encoded = append(encoded, fmt.Sprintf("[]byte:%x", bytes))
				} else {
					encoded = append(encoded, fmt.Sprintf("%T:%v", value, value))
				}
			}
			out = append(out, strings.Join(encoded, "|"))
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

func TestRootAudienceCandidateEmissions(t *testing.T) {
	if os.Getenv("CCI_ROOT_AUDIENCE_COST") != "1" {
		t.Skip("set CCI_ROOT_AUDIENCE_COST=1 with the paired candidate-expression instrumentation")
	}
	for _, population := range []int{32, 10000} {
		for _, placement := range []string{"same-drive", "foreign-drive"} {
			t.Run(fmt.Sprintf("%d/%s", population, placement), func(t *testing.T) {
				ctx := t.Context()
				st, err := Open(ctx, t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = st.Close() }()
				drive := "d"
				if placement == "foreign-drive" {
					drive = "foreign"
				}
				if _, err := st.db.ExecContext(ctx, `INSERT INTO records(seq,drive,lane,kind,at,text,recipients,source,hash) VALUES(1,'d','worker','note',1,'frontier','["other"]','post','frontier')`); err != nil {
					t.Fatal(err)
				}
				if _, err := st.db.ExecContext(ctx, `WITH RECURSIVE history(n) AS (VALUES(2) UNION ALL SELECT n+1 FROM history WHERE n < ?) INSERT INTO records(seq,drive,lane,kind,at,text,recipients,source,hash) SELECT n,?,'worker','ask',1,'noise','["other"]','post','root-noise-'||n FROM history`, population+1, drive); err != nil {
					t.Fatal(err)
				}
				if err := st.SetCursor(ctx, "root-cost", "d", 1); err != nil {
					t.Fatal(err)
				}
				var objects int
				if err := st.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_schema WHERE name IN ('record_root_audience','record_root_audience_stream','record_root_audience_kind_stream','record_root_audience_insert','record_root_audience_update','record_root_audience_delete')").Scan(&objects); err != nil {
					t.Fatal(err)
				}
				optimized := os.Getenv("CCI_ROOT_AUDIENCE_OPTIMIZED") == "1"
				if optimized && objects != 6 || !optimized && objects != 0 {
					t.Fatalf("actual root group objects=%d optimized=%t", objects, optimized)
				}
				f := (Subscription{Drive: "d", Reader: "root", Session: "reader", ToKinds: DefaultToKinds("root")}).Filter()
				f.After = 1
				f.Limit = 500
				var total int64
				for epoch := range 3 {
					if _, err := st.db.ExecContext(ctx, "UPDATE records SET text=? WHERE seq=1", fmt.Sprint("commit-", epoch)); err != nil {
						t.Fatal(err)
					}
					before := rootCostSnapshot(ctx, t, st)
					rootCandidateEmissions.Store(0)
					got, err := st.Query(ctx, f)
					if err != nil || len(got) != 0 {
						t.Fatalf("quiet root query=%v, %v", got, err)
					}
					count := rootCandidateEmissions.Load()
					total += count
					if after := rootCostSnapshot(ctx, t, st); !slices.Equal(before, after) {
						t.Fatal("query changed records/cursors")
					}
				}
				positive, _, err := st.Post(ctx, Record{Drive: "d", Lane: "worker", Kind: "ask", Text: "selected", To: []string{"root"}}, 0)
				if err != nil {
					t.Fatal(err)
				}
				rootCandidateEmissions.Store(0)
				got, err := st.Query(ctx, f)
				calls := rootCandidateEmissions.Load()
				if err != nil || len(got) != 1 || got[0].Seq != positive.Seq || calls == 0 {
					t.Fatalf("positive=%v, %v emissions=%d", got, err, calls)
				}
				cursor, ok, err := st.Cursor(ctx, "root-cost", "d")
				if err != nil || !ok || cursor != 1 {
					t.Fatalf("cursor=%d,%t,%v", cursor, ok, err)
				}
				t.Logf("ROOT_AUDIENCE_CANDIDATE_WORK population=%d placement=%s query_epochs=3 emitted=0 cursor=1 actual_candidate_emissions=%d selected_positive_emissions=%d optimized=%t", population, placement, total, calls, optimized)
				if total > 3 {
					t.Fatalf("ROOT_AUDIENCE_CANDIDATE_BOUND_EXCEEDED population=%d placement=%s actual=%d bound=3", population, placement, total)
				}
			})
		}
	}
}
