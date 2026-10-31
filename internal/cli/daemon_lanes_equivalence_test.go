package cli_test

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/server"
	"github.com/yasyf/cc-inbox/internal/store"
)

type daemonLaneSnapshot struct {
	Status int
	Body   []byte
}

func TestDaemonLanePreservesNativeOutputAndErrors(t *testing.T) {
	directory := os.Getenv("CCI_LANE_SNAPSHOTS")
	if directory == "" {
		t.Skip("CI supplies immutable current-main lane snapshots")
	}
	for _, fixture := range []struct {
		name string
		sql  []string
	}{
		{"empty", nil},
		{"backdated", []string{`INSERT INTO records(seq,drive,lane,kind,at,text,source,hash) VALUES(1,'d','one','note',3,'winner','post','a'),(2,'d','one','note',1,'later seq','post','b'),(3,'d','two','note',2,'other','post','c')`}},
		{"integer-score-tie", []string{`INSERT INTO records(seq,drive,lane,kind,at,text,source,hash) VALUES(1,'d','one','note',2,'first','post','a'),(1000001,'d','one','ask',1,'equal score','post','b')`}},
		{"real-score-plateau", []string{`INSERT INTO records(seq,drive,lane,kind,at,text,source,hash) VALUES(7,'d','one','note',253402300000000,'last inserted first','post','a'),(5,'d','one','note',253402300000000,'first seq','post','b'),(6,'d','one','note',253402300000000,'middle','post','c')`}},
		{"expired-and-digest", []string{`INSERT INTO records(seq,drive,lane,kind,at,text,expires_at,source,hash) VALUES(1,'d','one','note',2,'expired winner',1,'post','a'),(2,'d','one','digest',3,'excluded digest',NULL,'post','b'),(3,'x','one','note',4,'other drive',NULL,'post','c')`}},
		{"empty-and-unicode-lanes", []string{`INSERT INTO records(seq,drive,lane,kind,at,text,source,hash) VALUES(1,'d','','note',1,'empty lane','post','a'),(2,'d','é','note',2,'unicode lane','post','b'),(3,'d','a','note',3,'ascii lane','post','c')`}},
		{"losing-corrupt-refs", []string{`INSERT INTO records(seq,drive,lane,kind,at,text,refs,source,hash) VALUES(1,'d','one','note',1,'loser','{','post','a'),(2,'d','one','note',2,'winner','{}','post','b')`}},
		{"winning-corrupt-refs", []string{`INSERT INTO records(seq,drive,lane,kind,at,text,refs,source,hash) VALUES(1,'d','one','note',2,'winner','{','post','a'),(2,'d','one','note',1,'loser','{}','post','b')`}},
		{"winning-corrupt-fields", []string{`INSERT INTO records(seq,drive,lane,kind,at,text,fields,source,hash) VALUES(1,'d','one','note',1,'winner','[','post','a')`}},
		{"winning-corrupt-recipients", []string{`INSERT INTO records(seq,drive,lane,kind,at,text,recipients,source,hash) VALUES(1,'d','one','note',1,'winner','[','post','a')`}},
		{"other-drive-corrupt", []string{`INSERT INTO records(seq,drive,lane,kind,at,text,refs,source,hash) VALUES(1,'x','one','note',1,'foreign','{','post','a'),(2,'d','one','note',2,'selected','{}','post','b')`}},
		{"limit-frontier-unselected-error", []string{daemonLaneManyRows, `UPDATE records SET refs='{' WHERE seq=21`}},
		{"limit-frontier-selected-error", []string{daemonLaneManyRows, `UPDATE records SET refs='{' WHERE seq=22`}},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			home := t.TempDir()
			st, err := store.Open(t.Context(), home)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			db, err := sql.Open("sqlite", filepath.Join(home, store.DBFile))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			for _, query := range fixture.sql {
				if _, err := db.ExecContext(t.Context(), query); err != nil {
					t.Fatal(err)
				}
			}
			response := httptest.NewRecorder()
			server.New(st, time.Second).Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/lanes?drive=d", nil).WithContext(t.Context()))
			got := daemonLaneSnapshot{Status: response.Code, Body: response.Body.Bytes()}
			path := filepath.Join(directory, fixture.name+".json")
			if os.Getenv("CCI_LANE_SNAPSHOT_WRITE") == "1" {
				raw, err := json.Marshal(got)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, raw, 0o600); err != nil {
					t.Fatal(err)
				}
				return
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var want daemonLaneSnapshot
			if err := json.Unmarshal(raw, &want); err != nil {
				t.Fatal(err)
			}
			if got.Status != want.Status || !bytes.Equal(got.Body, want.Body) {
				t.Fatalf("lane output changed: status=%d/%d body=%q/%q", got.Status, want.Status, got.Body, want.Body)
			}
			t.Logf("actual_http_lanes case=%s status=%d output_bytes=%d", fixture.name, got.Status, len(got.Body))
		})
	}
}

const daemonLaneManyRows = `WITH RECURSIVE lanes(n) AS (
VALUES(1) UNION ALL SELECT n+1 FROM lanes WHERE n < 521
)
INSERT INTO records(seq,drive,lane,kind,at,text,source,hash)
SELECT n,'d',printf('lane-%04d',n),'note',1,'winner','post','row-' || n FROM lanes`

func TestDaemonLaneScoreTieIsNotMaximumSequence(t *testing.T) {
	home := t.TempDir()
	st, err := store.Open(t.Context(), home)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	db, err := sql.Open("sqlite", filepath.Join(home, store.DBFile))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(t.Context(), `INSERT INTO records(seq,drive,lane,kind,at,text,source,hash) VALUES(1,'d','one','note',2,'first','post','a'),(1000001,'d','one','note',1,'tie','post','b')`); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	server.New(st, time.Second).Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/lanes?drive=d", nil).WithContext(t.Context()))
	var rows []store.Record
	if err := json.Unmarshal(response.Body.Bytes(), &rows); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || len(rows) != 1 || rows[0].Seq != 1 {
		t.Fatalf("score tie changed: status=%d rows=%v", response.Code, rows)
	}
	t.Logf("actual_http_lanes equal_score_first_seq=%d later_seq=1000001", rows[0].Seq)
}
