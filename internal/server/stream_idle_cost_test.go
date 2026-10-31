package server_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/server"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

type streamResponse struct {
	header     http.Header
	body       bytes.Buffer
	flushes    int
	afterFlush func(int)
}

func (w *streamResponse) Header() http.Header { return w.header }
func (w *streamResponse) WriteHeader(int)     {}
func (w *streamResponse) Write(p []byte) (int, error) {
	return w.body.Write(p)
}

func (w *streamResponse) Flush() {
	w.flushes++
	if w.afterFlush != nil {
		w.afterFlush(w.flushes)
	}
}

func TestStreamIdleQueriesDoNotRepeatUnmatchedHistory(t *testing.T) {
	for _, count := range []int{32, 10000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			st, clock := testutil.Store(t)
			db, err := sql.Open("sqlite", "file:"+filepath.Join(st.HomeDir(), store.DBFile))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			_, err = db.ExecContext(t.Context(), `WITH RECURSIVE history(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n<?)
INSERT INTO records(drive,lane,kind,at,text,source,hash,recipients,origin,origin_seq,id)
SELECT 'd','noise','note',1,'ignored','post','idle-'||n,'["noise"]',?,n,'idle-'||n FROM history`, count+1, st.Self())
			if err != nil {
				t.Fatal(err)
			}
			var queries atomic.Int64
			st.Now = func() time.Time {
				queries.Add(1)
				return clock.Now()
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			response := &streamResponse{header: make(http.Header), afterFlush: func(n int) {
				if n == 4 {
					cancel()
				}
			}}
			request := httptest.NewRequestWithContext(ctx, http.MethodGet, "/v1/stream?drive=d&to=missing&since=1", nil)
			server.New(st, time.Nanosecond).Handler().ServeHTTP(response, request)
			if response.flushes != 4 || response.body.Len() != 0 || response.header.Get("Content-Type") != "text/event-stream" {
				t.Fatalf("idle stream: flushes=%d bytes=%d headers=%v", response.flushes, response.body.Len(), response.header)
			}
			t.Logf("population=%d idle_polls=3 record_queries=%d body_bytes=%d", count, queries.Load(), response.body.Len())
			if got := queries.Load(); got != 1 {
				t.Fatalf("idle record queries %d for %d unmatched rows, want 1", got, count)
			}
			var cursors int
			if err := db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM cursors").Scan(&cursors); err != nil || cursors != 0 {
				t.Fatalf("SSE cursors = %d, %v", cursors, err)
			}
		})
	}
}
