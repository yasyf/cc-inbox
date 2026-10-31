package server_test

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/server"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func streamRows(t *testing.T, response *streamResponse) []store.Record {
	t.Helper()
	var records []store.Record
	scanner := bufio.NewScanner(&response.body)
	for scanner.Scan() {
		if data, ok := strings.CutPrefix(scanner.Text(), "data: "); ok {
			var record store.Record
			if err := json.Unmarshal([]byte(data), &record); err != nil {
				t.Fatal(err)
			}
			records = append(records, record)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return records
}

func streamJSON(records []store.Record) []string {
	out := make([]string, len(records))
	for i, record := range records {
		out[i] = render.JSON(record)
	}
	return out
}

func TestStreamChangesPreserveSameSequenceMutationAndLateAppend(t *testing.T) {
	for _, method := range []string{"reparse", "external SQL"} {
		t.Run(method, func(t *testing.T) {
			st, clock := testutil.Store(t)
			testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "before requested cursor"})
			item := store.Ingestion{Record: store.Record{Drive: "d", Lane: "importer", Kind: kinds.Note, Text: "initially unmatched", Source: "import:/private.md", At: clock.Now()}, LineHash: "stream-import", Line: "initially unmatched"}
			if n, err := st.IngestAll(t.Context(), []store.Ingestion{item}); err != nil || n != 1 {
				t.Fatalf("IngestAll = %d, %v", n, err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var late store.Record
			response := &streamResponse{header: make(http.Header), afterFlush: func(n int) {
				switch n {
				case 2:
					item.Record.Kind = kinds.Hold
					if method == "reparse" {
						if n, _, err := st.Reparse(t.Context(), []store.Ingestion{item}, false); err != nil || n != 1 {
							t.Fatalf("Reparse = %d, %v", n, err)
						}
					} else {
						db, err := sql.Open("sqlite", "file:"+filepath.Join(st.HomeDir(), store.DBFile))
						if err != nil {
							t.Fatal(err)
						}
						defer func() { _ = db.Close() }()
						if _, err := db.ExecContext(t.Context(), "UPDATE records SET kind='hold' WHERE line_hash='stream-import'"); err != nil {
							t.Fatal(err)
						}
					}
				case 3:
					late = testutil.Post(t, st, store.Record{Kind: kinds.Hold, Text: "late matching append", To: []string{"root"}, Refs: store.Refs{Targets: []string{"private"}}})
				case 4:
					cancel()
				}
			}}
			request := httptest.NewRequestWithContext(ctx, http.MethodGet, "/v1/stream?drive=d&kind=hold&since=1&expired=1", nil)
			server.New(st, time.Nanosecond).Handler().ServeHTTP(response, request)
			records := streamRows(t, response)
			stored, err := st.Get(t.Context(), late.Seq)
			if err != nil {
				t.Fatal(err)
			}
			if len(records) != 2 || records[0].Seq != 2 || records[0].Kind != kinds.Hold || render.JSON(records[1]) != render.JSON(stored) {
				t.Fatalf("streamed records = %+v, want mutated #2 then %+v", records, late)
			}
		})
	}
}

func TestStreamChangesDrainFullPagesWithoutNewCommits(t *testing.T) {
	st, _ := testutil.Store(t)
	anchor := testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "before requested cursor"})
	for i := range 5 {
		testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: fmt.Sprint(i)})
	}
	want, err := st.Query(t.Context(), store.Filter{Drive: "d", After: anchor.Seq})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	response := &streamResponse{header: make(http.Header), afterFlush: func(n int) {
		if n == 4 {
			cancel()
		}
	}}
	request := httptest.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("/v1/stream?drive=d&since=%d&limit=2", anchor.Seq), nil)
	server.New(st, time.Nanosecond).Handler().ServeHTTP(response, request)
	if got := streamRows(t, response); !reflect.DeepEqual(streamJSON(got), streamJSON(want)) {
		t.Fatalf("streamed pages = %+v, want %+v", got, want)
	}
}

func TestStreamChangesKeepaliveAndCancellationAfterExpiry(t *testing.T) {
	st, clock := testutil.Store(t)
	before := testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "requested cursor anchor"})
	want, _, err := st.Post(t.Context(), store.Record{Drive: "d", Lane: "root", Kind: kinds.Note, Text: "expires after delivery"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	response := &streamResponse{header: make(http.Header), afterFlush: func(n int) {
		if n == 2 {
			clock.Advance(time.Hour)
		}
		if n == 4 {
			cancel()
		}
	}}
	request := httptest.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("/v1/stream?drive=d&since=%d", before.Seq), nil)
	server.New(st, time.Nanosecond).Handler().ServeHTTP(response, request)
	stored, err := st.Get(t.Context(), want.Seq)
	if err != nil {
		t.Fatal(err)
	}
	if got := streamRows(t, response); !reflect.DeepEqual(streamJSON(got), streamJSON([]store.Record{stored})) || response.flushes != 4 {
		t.Fatalf("expiry stream = %+v, flushes=%d", got, response.flushes)
	}
}
