package server_test

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/server"
	"github.com/yasyf/cc-inbox/internal/store"
)

func TestOpenAmongCandidatePreservesSelectedClosure(t *testing.T) {
	for _, name := range []string{"foreign nonmatching typed suffix", "raw topic bytes", "Go-valid deep refs"} {
		t.Run(name, func(t *testing.T) {
			home := t.TempDir()
			st, err := store.Open(t.Context(), home)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			st.Now = func() time.Time { return time.UnixMilli(1000) }
			db, err := sql.Open("sqlite", filepath.Join(home, store.DBFile))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			opener := store.Record{Drive: "d", Lane: "selected", Kind: kinds.Ask, Text: "selected", Source: "post"}
			if name == "raw topic bytes" {
				opener.Topic = string([]byte{0xff})
			}
			if name == "Go-valid deep refs" {
				opener.Kind = kinds.Incident
				opener.Refs = store.Refs{Targets: []string{"api"}}
			}
			posted, _, err := st.Post(t.Context(), opener, 0)
			if err != nil {
				t.Fatal(err)
			}
			switch name {
			case "foreign nonmatching typed suffix":
				if _, err := db.ExecContext(t.Context(), `INSERT INTO records(drive,lane,kind,at,text,also_resolves,source,hash) VALUES('foreign','other','note',1000,'resolver','[1,"bad"]','post','foreign')`); err != nil {
					t.Fatal(err)
				}
			case "raw topic bytes":
				if _, _, err := st.Post(t.Context(), store.Record{Drive: "d", Lane: "closer", Kind: kinds.Answer, Text: "closer", Topic: opener.Topic, Source: "post"}, 0); err != nil {
					t.Fatal(err)
				}
			case "Go-valid deep refs":
				closer, _, err := st.Post(t.Context(), store.Record{Drive: "d", Lane: "closer", Kind: kinds.FixLive, Text: "closer", Source: "post"}, 0)
				if err != nil {
					t.Fatal(err)
				}
				refs := `{"targets":["api"],"extra":` + strings.Repeat("[", 1100) + "0" + strings.Repeat("]", 1100) + "}"
				var decoded store.Refs
				if err := json.Unmarshal([]byte(refs), &decoded); err != nil || len(decoded.Targets) != 1 || decoded.Targets[0] != "api" {
					t.Fatalf("fixture is not valid Go closure metadata: %+v, %v", decoded, err)
				}
				if _, err := db.ExecContext(t.Context(), "UPDATE records SET refs=? WHERE seq=?", refs, closer.Seq); err != nil {
					t.Fatal(err)
				}
			}
			response := httptest.NewRecorder()
			server.New(st, time.Second).Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/records?drive=d&kind="+string(opener.Kind)+"&limit=1", nil).WithContext(t.Context()))
			var records []store.Record
			if err := json.Unmarshal(response.Body.Bytes(), &records); err != nil {
				t.Fatalf("legacy selected closure changed to error: status=%d body=%s decode=%v", response.Code, response.Body.String(), err)
			}
			if response.Code != http.StatusOK || len(records) != 1 || records[0].Seq != posted.Seq || records[0].Status != "closed" {
				t.Fatalf("legacy selected closure changed: status=%d records=%+v", response.Code, records)
			}
			t.Logf("selected closure case=%s status=200 closed=true", name)
		})
	}
}
