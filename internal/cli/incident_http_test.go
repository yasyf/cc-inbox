package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
)

func TestIncidentHTTPRejectsMalformedFramesBeforeQuery(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if _, err := st.ProvisionIncidentScope(ctx, "d"); err != nil {
		t.Fatal(err)
	}
	handler, err := IncidentHoldsHandler(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = handler.Close() })
	var calls int
	now := st.Now
	st.Now = func() time.Time { calls++; return now() }
	home, _ := json.Marshal(st.HomeDir())
	valid := `{"home":` + string(home) + `,"drive":"d","prs":[]}`
	for _, test := range []struct {
		name, body string
		length     int64
		want       int
	}{
		{"null list", `{"home":` + string(home) + `,"drive":"d","prs":null}`, -1, 400},
		{"null element", `{"home":` + string(home) + `,"drive":"d","prs":[null]}`, -1, 400},
		{"numeric element", `{"home":` + string(home) + `,"drive":"d","prs":[42]}`, -1, 400},
		{"unknown field", valid[:len(valid)-1] + `,"path":"/other"}`, -1, 400},
		{"trailing", valid + ` null`, -1, 400},
		{"declared oversize", valid, nativeTailInputBytes + 1, 413},
		{"chunked oversize", valid + strings.Repeat(" ", nativeTailInputBytes), -1, 413},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/v1/incident-holds", strings.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			request.ContentLength = test.length
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.want || calls != 0 {
				t.Fatalf("status=%d calls=%d body=%s", response.Code, calls, response.Body)
			}
		})
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/incident-holds", strings.NewReader(valid))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != 200 || calls != 1 || strings.TrimSpace(response.Body.String()) != "{}" {
		t.Fatalf("positive status=%d calls=%d body=%s", response.Code, calls, response.Body)
	}
}

func TestIncidentHTTPUsesActualStoreAndTypedAdmission(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	record, _, err := st.Post(ctx, store.Record{Drive: "d", Lane: "root", Kind: kinds.Incident, Text: "hold #42", Source: "post"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.ProvisionIncidentScope(ctx, "d"); err != nil {
		t.Fatal(err)
	}
	handler, err := IncidentHoldsHandler(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = handler.Close() })
	for _, test := range []struct {
		home, drive string
		status      int
		code        string
	}{
		{st.HomeDir(), "d", 200, ""},
		{st.HomeDir(), "unadmitted", 503, "scope_not_admitted"},
		{st.HomeDir() + "-other", "d", 409, "home_mismatch"},
	} {
		body, _ := json.Marshal(map[string]any{"home": test.home, "drive": test.drive, "prs": []string{"42"}})
		request := httptest.NewRequest(http.MethodPost, "/v1/incident-holds", bytes.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != test.status {
			t.Fatalf("status=%d body=%s", response.Code, response.Body)
		}
		if test.code != "" {
			var failure map[string]string
			if err := json.Unmarshal(response.Body.Bytes(), &failure); err != nil || failure["code"] != test.code {
				t.Fatalf("error=%v, %v", failure, err)
			}
		} else {
			var got map[string]store.Record
			if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil || len(got) != 1 || got["42"].Seq != record.Seq || got["42"].Status != "open" {
				t.Fatalf("holds=%v, %v", got, err)
			}
		}
	}
}
