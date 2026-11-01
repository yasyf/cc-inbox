package inbox_test

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
	"unsafe"

	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
)

func tailPageAssemblyStore(tb testing.TB, population int) *store.Store {
	tb.Helper()
	home := tb.TempDir()
	st, err := store.Open(tb.Context(), home)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = st.Close() })
	st.Now = func() time.Time { return time.UnixMilli(4102444800000).UTC() }
	db, err := sql.Open("sqlite", filepath.Join(home, store.DBFile))
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(tb.Context(), `WITH RECURSIVE history(n) AS (
VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n<?
)
INSERT INTO records(seq,drive,lane,kind,at,text,source,hash)
SELECT 148000+n,'durable-agents-rebuild','desk-runner',
CASE n%3 WHEN 0 THEN 'blocker' WHEN 1 THEN 'report' ELSE 'note' END,
4102444800000,'row '||n,'post','page-'||n FROM history`, population); err != nil {
		tb.Fatal(err)
	}
	return st
}

func tailPageAssemblyBenchmark(b *testing.B, st *store.Store, population int, oldest bool) {
	b.Helper()
	opts := inbox.TailOptions{Filter: store.Filter{Drive: "durable-agents-rebuild", Lanes: []string{"desk-runner"}, Kinds: []kinds.Kind{kinds.Blocker, kinds.Report, kinds.Note}, Peer: "tail-page-private-session", After: 148000}, Budget: 4000000, Width: 700, Oldest: oldest, Notes: io.Discard}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := inbox.Tail(b.Context(), st, opts, io.Discard)
		if err != nil {
			b.Fatal(err)
		}
		if result.Printed != population || result.Last != int64(148000+population) || result.More != 0 {
			b.Fatalf("full Tail result=%+v, want %d records through #%d and zero more", result, population, 148000+population)
		}
	}
	b.ReportMetric(float64(population), "records/op")
}

func TestTailPageAssemblyAllocationScaling(t *testing.T) {
	if os.Getenv("CCI_TAIL_PAGE_ALLOCATION") != "1" {
		t.Skip("CI runs the immutable full Tail allocation control")
	}
	var newest [2]int64
	for i, population := range []int{10000, 20000} {
		t.Run(fmt.Sprintf("rows=%d", population), func(t *testing.T) {
			st := tailPageAssemblyStore(t, population)
			measure := func(oldest bool) testing.BenchmarkResult {
				result := testing.Benchmark(func(b *testing.B) {
					defer func() {
						if b.Failed() {
							t.Errorf("full Tail benchmark failed: oldest=%t", oldest)
						}
					}()
					tailPageAssemblyBenchmark(b, st, population, oldest)
				})
				if t.Failed() {
					t.FailNow()
				}
				return result
			}
			ascending, descending := measure(true), measure(false)
			newest[i] = descending.AllocedBytesPerOp()
			bound := ascending.AllocedBytesPerOp() + int64(population)*int64(4*unsafe.Sizeof(store.Record{}))
			t.Logf("TAIL_PAGE_ALLOCATION rows=%d newest_bytes=%d oldest_bytes=%d bound_bytes=%d newest_allocs=%d oldest_allocs=%d CPU_gain=UNKNOWN", population, newest[i], ascending.AllocedBytesPerOp(), bound, descending.AllocsPerOp(), ascending.AllocsPerOp())
			if newest[i] > bound {
				t.Fatalf("TAIL_PAGE_ALLOCATION_BOUND_EXCEEDED rows=%d newest_bytes=%d bound_bytes=%d", population, newest[i], bound)
			}
		})
	}
	if newest[1] > 3*newest[0] {
		t.Fatalf("TAIL_PAGE_ALLOCATION_SCALING_EXCEEDED rows=10000/20000 bytes=%d/%d max_ratio=3", newest[0], newest[1])
	}
	t.Logf("TAIL_PAGE_ALLOCATION_SCALING rows=10000/20000 bytes=%d/%d max_ratio=3 CPU_gain=UNKNOWN", newest[0], newest[1])
}

func BenchmarkTailPageAssembly(b *testing.B) {
	for _, population := range []int{10000, 20000} {
		b.Run(fmt.Sprintf("rows=%d", population), func(b *testing.B) {
			st := tailPageAssemblyStore(b, population)
			tailPageAssemblyBenchmark(b, st, population, false)
		})
	}
}
