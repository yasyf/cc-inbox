package store

import (
	"context"
	"database/sql/driver"
	"fmt"
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
INSERT INTO records (drive, lane, kind, at, text, source, hash)
SELECT 'drive', 'old-lane', 'note', n, 'history', 'post', 'history-' || n FROM history`, rows); err != nil {
		t.Fatal(err)
	}
}
