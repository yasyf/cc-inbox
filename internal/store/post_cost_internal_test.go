package store

import (
	"context"
	"database/sql/driver"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"modernc.org/sqlite"
)

var historyCounterID atomic.Uint64

func TestBindLaneExistingCursorDoesNotReadRecordHistory(t *testing.T) {
	for _, history := range []int{64, 4096} {
		t.Run(fmt.Sprint(history), func(t *testing.T) {
			var reads atomic.Int64
			function := fmt.Sprintf("cci_history_at_%d", historyCounterID.Add(1))
			if err := sqlite.RegisterScalarFunction(function, 1, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
				reads.Add(1)
				return args[0], nil
			}); err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			st, err := Open(ctx, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			seedLaneHistory(t, st, history)
			key := PeerKey{Session: "session", Agent: "agent"}
			started := time.UnixMilli(1_000_000)
			if err := st.BindLane(ctx, key, "drive", "lane", started); err != nil {
				t.Fatal(err)
			}
			for _, query := range []string{
				"ALTER TABLE records RENAME TO counted_history",
				fmt.Sprintf("CREATE VIEW records AS SELECT seq, %s(at) AS at FROM counted_history", function),
			} {
				if _, err := st.db.ExecContext(ctx, query); err != nil {
					t.Fatal(err)
				}
			}
			var found any
			if err := st.db.QueryRowContext(ctx, "SELECT MIN(seq) FROM records WHERE at >= ?", started.UnixMilli()).Scan(&found); err != nil {
				t.Fatal(err)
			}
			if got := reads.Swap(0); got != int64(history) {
				t.Fatalf("history probe read %d rows, want %d", got, history)
			}
			if err := st.BindLane(ctx, key, "drive", "lane", started); err != nil {
				t.Fatal(err)
			}
			if got := reads.Load(); got != 0 {
				t.Fatalf("already-bound lane read %d history rows, want 0", got)
			}
			t.Logf("history=%d repeated_binding_history_rows=%d", history, reads.Load())
		})
	}
}

func BenchmarkBindLaneExistingCursor(b *testing.B) {
	for _, history := range []int{64, 4096} {
		b.Run(fmt.Sprint(history), func(b *testing.B) {
			st, err := Open(context.Background(), b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = st.Close() })
			seedLaneHistory(b, st, history)
			key := PeerKey{Session: "session", Agent: "agent"}
			started := time.UnixMilli(1_000_000)
			if err := st.BindLane(context.Background(), key, "drive", "lane", started); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for b.Loop() {
				if err := st.BindLane(context.Background(), key, "drive", "lane", started); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func seedLaneHistory(t testing.TB, st *Store, rows int) {
	t.Helper()
	if _, err := st.db.ExecContext(context.Background(), `WITH RECURSIVE history(n) AS (
	VALUES(1) UNION ALL SELECT n + 1 FROM history WHERE n < ?
)
INSERT INTO records (drive, lane, kind, at, text, source, hash, origin_seq)
SELECT 'drive', 'old-lane', 'note', n, 'history', 'post', 'history-' || n, n FROM history`, rows); err != nil {
		t.Fatal(err)
	}
}

func TestLaneHistorySeedPreservesIdentityTriggerRows(t *testing.T) {
	ctx := t.Context()
	stores := make([]*Store, 0, 2)
	for range 2 {
		st, err := Open(ctx, t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = st.Close() })
		if _, err := st.db.ExecContext(ctx, `CREATE TABLE seed_identity_updates (n INTEGER NOT NULL);
INSERT INTO seed_identity_updates VALUES (0);
CREATE TRIGGER seed_identity_updates_count AFTER UPDATE OF origin_seq ON records
BEGIN UPDATE seed_identity_updates SET n=n+1; END`); err != nil {
			t.Fatal(err)
		}
		stores = append(stores, st)
	}
	if _, err := stores[0].db.ExecContext(ctx, `WITH RECURSIVE history(n) AS (
	VALUES(1) UNION ALL SELECT n + 1 FROM history WHERE n < ?
)
INSERT INTO records (drive, lane, kind, at, text, source, hash)
SELECT 'drive', 'old-lane', 'note', n, 'history', 'post', 'history-' || n FROM history`, 32); err != nil {
		t.Fatal(err)
	}
	seedLaneHistory(t, stores[1], 32)
	for i, want := range []int{32, 0} {
		var got int
		if err := stores[i].db.QueryRowContext(ctx, "SELECT n FROM seed_identity_updates").Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("seed %d identity updates = %d, want %d", i, got, want)
		}
	}
	snapshot := func(st *Store, query string) [][]any {
		rows, err := st.db.QueryContext(ctx, query)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = rows.Close() }()
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		var result [][]any
		for rows.Next() {
			values := make([]any, len(columns))
			targets := make([]any, len(columns))
			for i := range values {
				targets[i] = &values[i]
			}
			if err := rows.Scan(targets...); err != nil {
				t.Fatal(err)
			}
			result = append(result, values)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return result
	}
	for _, query := range []string{
		"SELECT * FROM records ORDER BY seq",
		"SELECT * FROM record_recipients ORDER BY address, source_seq",
		"SELECT * FROM record_also_resolves ORDER BY target, source_seq",
		"SELECT * FROM record_json_errors ORDER BY field, source_seq",
	} {
		want, got := snapshot(stores[0], query), snapshot(stores[1], query)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("seed changed %s: got %v, want %v", query, got, want)
		}
	}
}
