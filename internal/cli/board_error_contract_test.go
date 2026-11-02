package cli

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
)

func TestBoardOriginalRulingErrorBytes(t *testing.T) { boardOriginalErrorControl(t, false) }
func TestBoardOriginalRulingDriveOrder(t *testing.T) { boardOriginalErrorControl(t, true) }

func boardOriginalErrorControl(t *testing.T, twoDrives bool) {
	t.Helper()
	if os.Getenv("CCI_BOARD_ERROR_CONTRACT") != "1" {
		t.Skip("set CCI_BOARD_ERROR_CONTRACT=1 for exact original board error controls")
	}
	home := t.TempDir()
	st, err := store.Open(t.Context(), home)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	if err := st.InstallBoardProjection(t.Context()); err != nil {
		t.Fatal(err)
	}
	session, agent := "board-error-session", "board-error-agent"
	peer := store.PeerKey{Session: session, Agent: agent}.String()
	post := func(record store.Record) store.Record {
		t.Helper()
		got, _, err := st.Post(t.Context(), record, 0)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	var foreign store.Record
	if twoDrives {
		foreign = post(store.Record{Drive: "z", Lane: "root", Kind: kinds.Decide, Text: "unselected #999", Refs: store.Refs{Targets: []string{"z-target"}}, To: []string{store.PeerAddress(peer)}})
	}
	local := post(store.Record{Drive: "a", Lane: "root", Kind: kinds.Decide, Text: "unselected #998", Refs: store.Refs{Targets: []string{"a-target"}}})
	var corrupt []int64
	if twoDrives {
		closer := post(store.Record{Drive: "z", Lane: "root", Kind: kinds.Withdraw, Text: "withdraw", Re: foreign.Seq})
		corrupt = append(corrupt, closer.Seq)
	}
	expected := post(store.Record{Drive: "a", Lane: "root", Kind: kinds.Withdraw, Text: "withdraw", Re: local.Seq})
	corrupt = append(corrupt, expected.Seq)
	budget := "4000000"
	if value := os.Getenv("CCI_BOARD_CONTRACT_BASELINE_BUDGET"); value != "" {
		if value != "0" {
			t.Fatal("baseline budget must be literal0")
		}
		budget = value
	}
	argv := func(kinds ...string) []string {
		args := []string{"cci", "tail", "--drive", "a", "--since-seq", "0", "--json", "--budget", budget}
		for _, kind := range kinds {
			args = append(args, "--kind="+kind)
		}
		return args
	}
	ruling := argv("answer", "decide", "decision", "go")
	review := argv("head", "review")
	withdrawn := append(argv("withdraw"), "--lane", "root")
	body := map[string]any{"contract": "board-query-v1", "home": home, "session": session, "agent": agent, "drive": "a", "repo": "owner/repo", "cwd": t.TempDir(), "go_lanes": []string{}, "ruling_argv": ruling, "review_argv": review, "withdraw_argv": withdrawn, "prs": []string{"123"}, "row_ids": []string{}}
	handler, err := BoardRecordsHandler(st, home)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = handler.Close() }()
	call := func(target http.Handler, path string, value any) *httptest.ResponseRecorder {
		t.Helper()
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw)).WithContext(t.Context())
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		target.ServeHTTP(response, request)
		return response
	}
	provision := call(handler, "/v1/board-provision", body)
	if provision.Code != http.StatusOK {
		t.Fatalf("provision status=%d body=%s", provision.Code, provision.Body.String())
	}
	db, err := sql.Open("sqlite", "file:"+filepath.Join(home, store.DBFile))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	for _, seq := range corrupt {
		result, err := db.ExecContext(t.Context(), "UPDATE records SET refs='{' WHERE seq=?", seq)
		if err != nil {
			t.Fatal(err)
		}
		if count, err := result.RowsAffected(); err != nil || count != 1 {
			t.Fatalf("corruption rows=%d error=%v", count, err)
		}
	}
	original, err := TailHandler(st, home)
	if err != nil {
		t.Fatal(err)
	}
	originalError := func(args []string) string {
		t.Helper()
		exact := append([]string(nil), args...)
		for i, arg := range exact {
			if arg == "--budget" {
				exact[i+1] = "4000000"
			}
		}
		response := call(original, "/v1/tail", map[string]any{"argv": exact, "home": home, "session": session, "agent": agent})
		if response.Code != http.StatusOK {
			t.Fatalf("original status=%d body=%s", response.Code, response.Body.String())
		}
		var result struct {
			Code int    `json:"returncode"`
			Err  string `json:"stderr_b64"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		stderr, err := base64.StdEncoding.DecodeString(result.Err)
		if err != nil {
			t.Fatal(err)
		}
		if result.Code != 1 || strings.Count(string(stderr), "decode refs of #") != 1 {
			t.Fatalf("original error control is vacuous: code=%d error=%q", result.Code, stderr)
		}
		return strings.TrimSuffix(strings.TrimPrefix(string(stderr), "cci: "), "\n")
	}
	want := originalError(ruling)
	if !strings.Contains(want, fmt.Sprintf("decode refs of #%d", expected.Seq)) {
		t.Fatalf("original did not select local a-drive closer first: error=%q expected=%d", want, expected.Seq)
	}
	response := call(handler, "/v1/board-records", body)
	if response.Code != http.StatusOK {
		t.Fatalf("board status=%d body=%s", response.Code, response.Body.String())
	}
	var result struct {
		Rulings struct {
			Error *string `json:"error"`
		} `json:"rulings_withdrawals"`
		Reviews struct {
			Error *string `json:"error"`
		} `json:"reviews"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Rulings.Error == nil || *result.Rulings.Error != want {
		t.Fatalf("ORIGINAL_RULING_ERROR_PARITY_MISMATCH got=%v want=%q", result.Rulings.Error, want)
	}
	if twoDrives {
		wantReview := originalError(review)
		if result.Reviews.Error == nil || *result.Reviews.Error != wantReview {
			t.Fatalf("ORIGINAL_REVIEW_ERROR_PARITY_MISMATCH got=%v want=%q", result.Reviews.Error, wantReview)
		}
	} else if result.Reviews.Error != nil {
		t.Fatalf("independent reviews acquired ruling error: %q", *result.Reviews.Error)
	}
	t.Logf("BOARD_ORIGINAL_ERROR_PARITY two_drives=%t local_closer=%d error=%q", twoDrives, expected.Seq, want)
}
