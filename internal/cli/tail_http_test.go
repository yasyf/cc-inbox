package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/cli"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
)

type nativeTailEnvelope struct {
	Code int    `json:"returncode"`
	Out  []byte `json:"stdout_b64"`
	Err  []byte `json:"stderr_b64"`
}

func nativeTailBody(t *testing.T, fixture nativeTailFixture, call nativeTailCall) []byte {
	t.Helper()
	session := call.claude
	if session == "" {
		session = call.codex
	}
	raw, err := json.Marshal(map[string]any{
		"argv":    append([]string{"cci"}, call.args...),
		"session": session,
		"agent":   call.agent,
		"home":    fixture.home,
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func nativeTailHandler(t *testing.T, fixture nativeTailFixture) http.Handler {
	t.Helper()
	handler, err := cli.TailHandler(fixture.st, fixture.home)
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func nativeTailResponse(t *testing.T, handler http.Handler, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/v1/tail", bytes.NewReader(body)).WithContext(t.Context())
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func nativeTailResultFromHTTP(t *testing.T, response *httptest.ResponseRecorder) nativeTailResult {
	t.Helper()
	if response.Code != http.StatusOK {
		t.Fatalf("native response status=%d body=%q", response.Code, response.Body.String())
	}
	var envelope nativeTailEnvelope
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	return nativeTailResult{Code: envelope.Code, Out: envelope.Out, Err: envelope.Err}
}

func TestNativeTailHTTPParity(t *testing.T) {
	if os.Getenv("CCI_NATIVE_TAIL_SNAPSHOTS") == "" {
		t.Skip("CI supplies immutable native Tail reference snapshots")
	}
	for _, variant := range tailVariants {
		for _, call := range nativeTailCalls() {
			t.Run(variant.name+"/"+call.name, func(t *testing.T) {
				fixture := newNativeTailFixture(t, call, variant.sets)
				body := nativeTailBody(t, fixture, call)
				t.Setenv("CLAUDE_CODE_SESSION_ID", "daemon-must-not-supply-peer")
				t.Setenv("CODEX_THREAD_ID", "daemon-codex-must-not-supply-peer")
				t.Setenv("CCI_AGENT_ID", "daemon-agent")
				handler := nativeTailHandler(t, fixture)
				got := normalizeNativeTail(nativeTailResultFromHTTP(t, nativeTailResponse(t, handler, body)), fixture.node)
				raw, err := os.ReadFile(nativeTailSnapshotPath(variant.name + "/" + call.name))
				if err != nil {
					t.Fatal(err)
				}
				var want nativeTailResult
				if err := json.Unmarshal(raw, &want); err != nil {
					t.Fatal(err)
				}
				if got.Code != want.Code || !bytes.Equal(got.Out, want.Out) || !bytes.Equal(got.Err, want.Err) {
					t.Fatalf("native HTTP differs from immutable CLI: code=%d/%d stdout=%q/%q stderr=%q/%q", got.Code, want.Code, got.Out, want.Out, got.Err, want.Err)
				}
				if os.Getenv("CLAUDE_CODE_SESSION_ID") != "daemon-must-not-supply-peer" || os.Getenv("CODEX_THREAD_ID") != "daemon-codex-must-not-supply-peer" || os.Getenv("CCI_AGENT_ID") != "daemon-agent" {
					t.Fatal("native request mutated process identity")
				}
			})
		}
	}
}

func forbidNativeTailWrites(t *testing.T, fixture nativeTailFixture) {
	t.Helper()
	rows, err := fixture.db.QueryContext(t.Context(), "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	for i, name := range names {
		for _, operation := range []string{"INSERT", "UPDATE", "DELETE"} {
			query := fmt.Sprintf("CREATE TRIGGER native_write_%d_%s BEFORE %s ON %q BEGIN SELECT RAISE(FAIL,'native tail attempted a state write'); END", i, operation, operation, name)
			if _, err := fixture.db.ExecContext(t.Context(), query); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestNativeTailRejectsUnsafeModesWithoutWrites(t *testing.T) {
	call := nativeTailCall{claude: "reader", args: []string{"tail", "--drive", "d", "--since", "0", "--json"}}
	fixture := newNativeTailFixture(t, call, nil)
	handler := nativeTailHandler(t, fixture)
	forbidNativeTailWrites(t, fixture)
	for _, args := range [][]string{
		call.args,
		{"tail", "--drive", "d", "--since", "", "--cursor", "another", "--json"},
		{"tail", "--drive", "d", "--since=0", "--cursor", "another", "--reader", "root", "--json"},
		{"tail", "--drive", "d", "--since", "not-a-time", "--json"},
	} {
		got := nativeTailResultFromHTTP(t, nativeTailResponse(t, handler, nativeTailBody(t, fixture, nativeTailCall{claude: "reader", args: args})))
		if bytes.Contains(got.Err, []byte("attempted a state write")) {
			t.Fatalf("request wrote state: %q", got.Err)
		}
		if len(got.Out) == 0 && got.Code == 0 {
			t.Fatal("accepted fixture request did not execute Tail")
		}
	}
	for _, args := range [][]string{
		{"tail", "--drive", "d", "--cursor", "another"},
		{"tail", "--drive", "d", "--help"},
		{"tail", "--since", "0"},
		{"tail", "--drive", "", "--since", "0"},
		{"tail", "--drive", "d", "--topic", "--since"},
	} {
		response := nativeTailResponse(t, handler, nativeTailBody(t, fixture, nativeTailCall{args: args}))
		if response.Code != http.StatusBadRequest {
			t.Fatalf("unsafe mode %q status=%d body=%q", args, response.Code, response.Body.String())
		}
	}
}

func TestNativeTailProtocolAndHome(t *testing.T) {
	call := nativeTailCall{args: []string{"tail", "--drive", "d", "--since", "0", "--json"}}
	fixture := newNativeTailFixture(t, call, nil)
	handler := nativeTailHandler(t, fixture)
	forbidNativeTailWrites(t, fixture)
	body := nativeTailBody(t, fixture, call)
	for _, bad := range [][]byte{
		[]byte("null"),
		[]byte("{}"),
		append(append([]byte(nil), body...), []byte("{}")...),
		bytes.Replace(body, []byte(`"argv":`), []byte(`"unknown":`), 1),
		bytes.Replace(body, []byte(`"session":""`), []byte(`"session":null`), 1),
		bytes.Replace(body, []byte(`"session":""`), []byte(`"session":false`), 1),
		bytes.Replace(body, []byte(`"cci","tail"`), []byte(`"cci","post"`), 1),
		bytes.Replace(body, []byte(`"cci","tail"`), []byte(`"cci\u0000","tail"`), 1),
	} {
		if response := nativeTailResponse(t, handler, bad); response.Code != http.StatusBadRequest {
			t.Fatalf("invalid request status=%d body=%q", response.Code, response.Body.String())
		}
	}
	var mismatch map[string]any
	if err := json.Unmarshal(body, &mismatch); err != nil {
		t.Fatal(err)
	}
	otherHome := filepath.Join(fixture.home, "must-not-be-opened")
	mismatch["home"] = otherHome
	raw, err := json.Marshal(mismatch)
	if err != nil {
		t.Fatal(err)
	}
	if response := nativeTailResponse(t, handler, raw); response.Code != http.StatusConflict {
		t.Fatalf("home mismatch status=%d body=%q", response.Code, response.Body.String())
	}
	if _, err := os.Stat(otherHome); !os.IsNotExist(err) {
		t.Fatalf("request opened a different home: %v", err)
	}
	get := httptest.NewRequest(http.MethodGet, "/v1/tail", nil).WithContext(t.Context())
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, get)
	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET native tail = %d", response.Code)
	}
}

func TestNativeTailConcurrentRequestPeersStayIsolated(t *testing.T) {
	call := nativeTailCall{args: []string{"tail", "--drive", "d", "--since", "0", "--json"}}
	fixture := newNativeTailFixture(t, call, nil)
	handler := nativeTailHandler(t, fixture)
	forbidNativeTailWrites(t, fixture)
	var group sync.WaitGroup
	for i := range 32 {
		group.Go(func() {
			requestCall := call
			requestCall.claude = "other"
			if i%2 == 0 {
				requestCall.claude = "reader"
				requestCall.agent = "worker"
			}
			got := nativeTailResultFromHTTP(t, nativeTailResponse(t, handler, nativeTailBody(t, fixture, requestCall)))
			if got.Code != 0 || bytes.Contains(got.Out, []byte("worker mail")) != (i%2 == 0) {
				t.Errorf("request %d identity leaked: code=%d output=%q error=%q", i, got.Code, got.Out, got.Err)
			}
		})
	}
	group.Wait()
}

func TestNativeTailCanceledContextReachesBorrowedStore(t *testing.T) {
	call := nativeTailCall{args: []string{"tail", "--drive", "d", "--since", "0", "--json"}}
	fixture := newNativeTailFixture(t, call, nil)
	forbidNativeTailWrites(t, fixture)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	request := httptest.NewRequest(http.MethodPost, "/v1/tail", bytes.NewReader(nativeTailBody(t, fixture, call))).WithContext(ctx)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	nativeTailHandler(t, fixture).ServeHTTP(response, request)
	got := nativeTailResultFromHTTP(t, response)
	if got.Code != 1 || len(got.Out) != 0 || !bytes.Contains(got.Err, []byte(context.Canceled.Error())) {
		t.Fatalf("cancellation did not reach Tail: code=%d output=%q error=%q", got.Code, got.Out, got.Err)
	}
}

func TestNativeTailBorrowedStoreOperationCounts(t *testing.T) {
	for _, population := range []int{32, 10000} {
		for _, field := range []string{"recipients", "also_resolves"} {
			t.Run(fmt.Sprintf("irrelevant=%d/field=%s", population, field), func(t *testing.T) {
				db := tailCostStore(t)
				at := time.Now().UnixMilli()
				if _, err := db.ExecContext(t.Context(), "INSERT INTO records(drive,lane,kind,at,text,source,hash) VALUES('d','root','hold',?,'hold #29020','post','selected')", at); err != nil {
					t.Fatal(err)
				}
				if _, err := db.ExecContext(t.Context(), `INSERT INTO records(drive,lane,kind,at,text,recipients,also_resolves,source,hash) VALUES('foreign','root','note',?,'addressed','["peer:tail-cost-session"]','[1]','post','addressed')`, at); err != nil {
					t.Fatal(err)
				}
				tailCostHistory(t, db, population, at)
				reads := tailCostColumn(t, db, field)
				home := os.Getenv("CCI_HOME")
				st, err := store.Open(t.Context(), home)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = st.Close() })
				fixture := nativeTailFixture{home: home, st: st, db: db}
				handler := nativeTailHandler(t, fixture)
				call := nativeTailCall{claude: "tail-cost-session", args: []string{"tail", "--drive", "d", "--since", "36h", "--json", "--budget", "4000000"}}
				body := nativeTailBody(t, fixture, call)
				for request := range 2 {
					reads.Store(0)
					got := nativeTailResultFromHTTP(t, nativeTailResponse(t, handler, body))
					if got.Code != 0 {
						t.Fatalf("native request failed: %q", got.Err)
					}
					if bytes.Count(got.Out, []byte("\n")) != 2 || !bytes.Contains(got.Out, []byte(`"status":"closed"`)) {
						t.Fatalf("native output lost peer or resolution: %q", got.Out)
					}
					count := reads.Load()
					if count == 0 || count > 8 {
						t.Fatalf("native %s reads=%d for irrelevant=%d, want 1..8", field, count, population)
					}
					t.Logf("actual_native_http_tail irrelevant=%d field=%s request=%d output_rows=2 column_reads=%d store_open_before_requests=1", population, field, request, count)
				}
			})
		}
	}
}

func TestNativeTailInputBoundIncludesTrailingAndChunked(t *testing.T) {
	const limit = 8 << 20
	call := nativeTailCall{args: []string{"tail", "--drive", "d", "--since", "0", "--json"}}
	fixture := newNativeTailFixture(t, call, nil)
	handler := nativeTailHandler(t, fixture)
	forbidNativeTailWrites(t, fixture)
	body := nativeTailBody(t, fixture, call)
	exact := append(append([]byte(nil), body...), bytes.Repeat([]byte(" "), limit-len(body))...)
	if got := nativeTailResultFromHTTP(t, nativeTailResponse(t, handler, exact)); got.Code != 0 {
		t.Fatalf("exact frame failed: %q", got.Err)
	}
	over := append(exact, ' ')
	if response := nativeTailResponse(t, handler, over); response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("declared oversize = %d", response.Code)
	}
	srv := httptest.NewServer(handler)
	defer srv.Close()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL+"/v1/tail", io.NopCloser(bytes.NewReader(over)))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	if request.ContentLength != 0 {
		t.Fatalf("fixture is not chunked: content length=%d", request.ContentLength)
	}
	response, err := srv.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("chunked oversized trailing whitespace = %d", response.StatusCode)
	}
}

type nativeTailCountingWriter struct {
	*httptest.ResponseRecorder
	largest int
}

func (w *nativeTailCountingWriter) Write(p []byte) (int, error) {
	w.largest = max(w.largest, len(p))
	return w.ResponseRecorder.Write(p)
}

func TestNativeTailStreamsFirstWholeRecordBeyondInputFrame(t *testing.T) {
	call := nativeTailCall{args: []string{"tail", "--drive", "large", "--since", "0", "--json", "--budget", "1"}}
	fixture := newNativeTailFixture(t, call, nil)
	if _, err := fixture.db.ExecContext(t.Context(), "INSERT INTO records(drive,lane,kind,at,text,source,hash) VALUES('large','one','note',4102444800000,?,'post','large')", strings.Repeat("x", 9<<20)); err != nil {
		t.Fatal(err)
	}
	want := runNativeTailReference(t, call.args)
	if want.Code != 0 || len(want.Out) <= 8<<20 {
		t.Fatalf("CLI did not emit its first whole record: code=%d bytes=%d error=%q", want.Code, len(want.Out), want.Err)
	}
	handler := nativeTailHandler(t, fixture)
	body := nativeTailBody(t, fixture, call)
	request := httptest.NewRequest(http.MethodPost, "/v1/tail", bytes.NewReader(body)).WithContext(t.Context())
	request.Header.Set("Content-Type", "application/json")
	writer := &nativeTailCountingWriter{ResponseRecorder: httptest.NewRecorder()}
	handler.ServeHTTP(writer, request)
	got := nativeTailResultFromHTTP(t, writer.ResponseRecorder)
	if got.Code != want.Code || !bytes.Equal(got.Out, want.Out) || !bytes.Equal(got.Err, want.Err) {
		t.Fatalf("whole-record byte parity failed: code=%d/%d bytes=%d/%d", got.Code, want.Code, len(got.Out), len(want.Out))
	}
	if writer.largest > 16<<10 {
		t.Fatalf("response buffered a whole encoding: largest write=%d", writer.largest)
	}
}

func TestNativeTailFreshMutableHold(t *testing.T) {
	call := nativeTailCall{args: []string{"tail", "--drive", "fresh", "--since", "0", "--kind=hold", "--json"}}
	fixture := newNativeTailFixture(t, call, nil)
	seq, _, err := fixture.st.Post(t.Context(), store.Record{Drive: "fresh", Lane: "root", Kind: kinds.Hold, Text: "hold #29020", Refs: store.Refs{PRs: []store.PR{{Number: 29020}}}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	handler := nativeTailHandler(t, fixture)
	read := func() []store.Record {
		got := nativeTailResultFromHTTP(t, nativeTailResponse(t, handler, nativeTailBody(t, fixture, call)))
		if got.Code != 0 {
			t.Fatalf("fresh read failed: %q", got.Err)
		}
		var records []store.Record
		for line := range bytes.Lines(got.Out) {
			var record store.Record
			if err := json.Unmarshal(line, &record); err != nil {
				t.Fatal(err)
			}
			records = append(records, record)
		}
		return records
	}
	if records := read(); len(records) != 1 || records[0].Status != "open" {
		t.Fatalf("initial hold = %+v", records)
	}
	if _, _, err := fixture.st.Post(t.Context(), store.Record{Drive: "fresh", Lane: "root", Kind: kinds.Done, Text: "resolved", Resolves: seq}, 0); err != nil {
		t.Fatal(err)
	}
	if records := read(); len(records) != 1 || records[0].Status != "closed" {
		t.Fatalf("resolved hold stale = %+v", records)
	}
	if _, err := fixture.db.ExecContext(t.Context(), "UPDATE records SET expires_at=1 WHERE seq=?", seq); err != nil {
		t.Fatal(err)
	}
	if records := read(); len(records) != 0 {
		t.Fatalf("expired hold stale = %+v", records)
	}
}
