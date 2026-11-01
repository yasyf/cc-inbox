package inbox_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func TestWatchUnchangedPollWork(t *testing.T) {
	for _, count := range []int{32, 10000} {
		for _, drive := range []string{"d", "foreign"} {
			t.Run(fmt.Sprintf("%d/%s", count, drive), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					st, clock := testutil.Store(t)
					db, err := sql.Open("sqlite", "file:"+filepath.Join(st.HomeDir(), store.DBFile))
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = db.Close() }()
					_, err = db.ExecContext(t.Context(), `WITH RECURSIVE history(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n<?)
INSERT INTO records(drive,lane,kind,at,text,source,hash,recipients,origin,origin_seq,id)
SELECT ?,'noise','state',1,'ignored','post','watch-'||n,'["noise"]',?,n,'watch-'||n FROM history`, count+1, drive, st.Self())
					if err != nil {
						t.Fatal(err)
					}
					if err := st.SetCursor(t.Context(), "root-channel", "d", 1); err != nil {
						t.Fatal(err)
					}
					var queries atomic.Int64
					st.Now = func() time.Time {
						queries.Add(1)
						return clock.Now()
					}
					filter := (store.Subscription{Drive: "d", Session: "missing", Reader: "root", Kinds: []kinds.Kind{kinds.Note}, ToKinds: store.DefaultToKinds("root")}).Filter()
					filter.After = 1
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					done := make(chan error, 1)
					var emitted atomic.Int64
					go func() {
						done <- inbox.Watch(ctx, st, inbox.WatchOptions{Filter: filter, Cursor: "root-channel", Interval: time.Second}, func(store.Record, string) error {
							emitted.Add(1)
							return nil
						})
					}()
					synctest.Wait()
					for range 2 {
						time.Sleep(time.Second)
						synctest.Wait()
					}
					cancel()
					if err := <-done; err != nil {
						t.Fatal(err)
					}
					t.Logf("WATCH_POLL_WORK population=%d placement=%s polls=3 record_queries=%d emitted=%d", count, drive, queries.Load(), emitted.Load())
					if got := queries.Load(); got != 3 {
						t.Fatalf("record query starts = %d, want 3", got)
					}
					if got := emitted.Load(); got != 0 {
						t.Fatalf("emitted = %d, want 0", got)
					}
					cursor, ok, err := st.Cursor(t.Context(), "root-channel", "d")
					if err != nil || !ok || cursor != 1 {
						t.Fatalf("cursor = %d, %v, %v; want 1, true, nil", cursor, ok, err)
					}
				})
			})
		}
	}
}
