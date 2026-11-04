package cli_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/cli"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func hookRequest(body string, marked bool) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/v1/hook/post-tool-use", strings.NewReader(body))
	if marked {
		r.Header.Set(cli.HookHeader, "1")
	}
	return r
}

func TestHookHandlerDeliversMailAndMovesTheCursorPastIt(t *testing.T) {
	st, _ := testutil.Store(t)
	key := store.PeerKey{Session: "s"}
	if err := st.ClaimLane(t.Context(), key, "d", "reader", st.Now()); err != nil {
		t.Fatal(err)
	}
	testutil.Post(t, st, store.Record{Lane: "writer", Kind: kinds.Note, Text: "for the reader", To: []string{"reader"}})
	handler := cli.HookHandler(st, st.HomeDir())
	body := `{"session_id":"s","hook_event_name":"PostToolUse"}`
	first := httptest.NewRecorder()
	handler.ServeHTTP(first, hookRequest(body, true))
	if first.Code != http.StatusOK || !strings.Contains(first.Body.String(), "for the reader") {
		t.Fatalf("first hook = %d %q", first.Code, first.Body)
	}
	second := httptest.NewRecorder()
	handler.ServeHTTP(second, hookRequest(body, true))
	if second.Code != http.StatusOK || strings.Contains(second.Body.String(), "for the reader") {
		t.Fatalf("second hook = %d %q, want the mail delivered once", second.Code, second.Body)
	}
}

func TestHookHandlerRefusesARequestWithoutItsHeader(t *testing.T) {
	st, _ := testutil.Store(t)
	got := httptest.NewRecorder()
	cli.HookHandler(st, st.HomeDir()).ServeHTTP(got, hookRequest(`{"session_id":"s"}`, false))
	if got.Code != http.StatusForbidden {
		t.Fatalf("code = %d, want forbidden", got.Code)
	}
}
