package server_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"modernc.org/sqlite"

	"github.com/yasyf/cc-inbox/internal/server"
	"github.com/yasyf/cc-inbox/internal/store"
)

var openAmongCounterID atomic.Uint64

func openAmongStore(t *testing.T) (*store.Store, *sql.DB) {
	t.Helper()
	home := t.TempDir()
	st, err := store.Open(context.Background(), home)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	db, err := sql.Open("sqlite", filepath.Join(home, store.DBFile))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return st, db
}

func openAmongInsert(t *testing.T, db *sql.DB, history int) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO records (drive,lane,kind,at,text,source,hash)
VALUES ('d','selected','ask',1,'selected','post','selected')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`WITH RECURSIVE history(n) AS (
VALUES(1) UNION ALL SELECT n + 1 FROM history WHERE n < ?
)
INSERT INTO records (drive,lane,kind,at,text,topic,re,source,hash)
SELECT 'd','history','answer',n+1,'unrelated','unrelated-'||n,1000000+n,'post','history-'||n FROM history`, history); err != nil {
		t.Fatal(err)
	}
}

func openAmongCountRefs(t *testing.T, db *sql.DB) *atomic.Int64 {
	t.Helper()
	reads := &atomic.Int64{}
	function := fmt.Sprintf("cci_openamong_refs_%d", openAmongCounterID.Add(1))
	if err := sqlite.RegisterScalarFunction(function, 1, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
		reads.Add(1)
		return args[0], nil
	}); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query("PRAGMA table_info(records)")
	if err != nil {
		t.Fatal(err)
	}
	var selection []string
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, kind string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &kind, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatal(err)
		}
		column := name
		if name == "refs" {
			column = function + "(refs) AS refs"
		}
		selection = append(selection, column)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		"ALTER TABLE records RENAME TO counted_openamong_history",
		"CREATE VIEW records AS SELECT " + strings.Join(selection, ", ") + " FROM counted_openamong_history",
	} {
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	return reads
}

func TestOpenAmongHTTPHistoryBaseline(t *testing.T) {
	global := os.Getenv("CCI_OPENAMONG_EXPECT_GLOBAL_HISTORY") == "1"
	for _, history := range []int{32, 10000} {
		for _, resolved := range []bool{false, true} {
			t.Run(fmt.Sprintf("history=%d/cross-drive-resolved=%t", history, resolved), func(t *testing.T) {
				st, db := openAmongStore(t)
				openAmongInsert(t, db, history)
				if resolved {
					if _, err := db.Exec(`INSERT INTO records (drive,lane,kind,at,text,also_resolves,source,hash)
VALUES ('foreign','other','note',2,'cross-drive resolver','[1]','post','resolver')`); err != nil {
						t.Fatal(err)
					}
				}
				reads := openAmongCountRefs(t, db)
				home := st.HomeDir()
				if err := st.Close(); err != nil {
					t.Fatal(err)
				}
				reopened, err := store.Open(context.Background(), home)
				if err != nil {
					t.Fatal(err)
				}
				st = reopened
				t.Cleanup(func() { _ = reopened.Close() })
				handler := server.New(st, time.Second).Handler()
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/records?drive=d&kind=ask&limit=1", nil))
				if response.Code != http.StatusOK {
					t.Fatalf("HTTP error before cost observation: %d %s", response.Code, response.Body.String())
				}
				var records []store.Record
				if err := json.Unmarshal(response.Body.Bytes(), &records); err != nil {
					t.Fatal(err)
				}
				wantStatus := "open"
				if resolved {
					wantStatus = "closed"
				}
				if response.Code != http.StatusOK || len(records) != 1 || records[0].Seq != 1 || records[0].Status != wantStatus {
					t.Fatalf("HTTP records = %d %s, want selected #1 %s", response.Code, response.Body.String(), wantStatus)
				}
				wantReads := int64(2)
				if global {
					wantReads += int64(history)
				}
				if got := reads.Load(); got != wantReads {
					t.Fatalf("actual HTTP annotation refs reads=%d, want%d with global=%t", got, wantReads, global)
				}
				t.Logf("OPENAMONG_BASELINE history=%d selected=1 cross_drive_resolved=%t refs_reads=%d", history, resolved, reads.Load())
				reads.Store(0)
				response = httptest.NewRecorder()
				handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/records?drive=d&kind=ask&since=1&limit=1", nil))
				if response.Code != http.StatusOK || strings.TrimSpace(response.Body.String()) != "[]" || reads.Load() != 0 {
					t.Fatalf("empty HTTP result = %d %s refs_reads=%d, want [] and0reads", response.Code, response.Body.String(), reads.Load())
				}
				open, err := st.OpenAmong(context.Background(), "d", nil)
				wantEmptyReads := int64(0)
				if global {
					wantEmptyReads = int64(history)
				}
				if err != nil || len(open) != 0 || reads.Load() != wantEmptyReads {
					t.Fatalf("empty direct OpenAmong = %+v %v refs_reads=%d, want empty and%d", open, err, reads.Load(), history)
				}
				t.Logf("OPENAMONG_EMPTY_BASELINE history=%d http_refs_reads=0 direct_refs_reads=%d", history, reads.Load())
			})
		}
	}
}

func TestOpenAmongUnrelatedCorruptionBaseline(t *testing.T) {
	global := os.Getenv("CCI_OPENAMONG_EXPECT_GLOBAL_HISTORY") == "1"
	for _, field := range []string{"recipients", "refs", "fields", "also_resolves"} {
		for _, selected := range []string{"unrelated", "closer", "opener"} {
			t.Run(field+"/"+selected, func(t *testing.T) {
				st, db := openAmongStore(t)
				openAmongInsert(t, db, 32)
				seq := 2
				if selected == "opener" {
					seq = 1
				}
				if selected == "closer" {
					if _, err := db.Exec("UPDATE records SET re=1 WHERE seq=2"); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := db.Exec("UPDATE records SET "+field+" = '[' WHERE seq = ?", seq); err != nil {
					t.Fatal(err)
				}
				handler := server.New(st, time.Second).Handler()
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/records?drive=d&kind=ask&limit=1", nil))
				if global || selected != "unrelated" {
					var failure map[string]string
					if err := json.Unmarshal(response.Body.Bytes(), &failure); err != nil {
						t.Fatal(err)
					}
					if response.Code != http.StatusInternalServerError || !strings.Contains(failure["error"], fmt.Sprintf("decode %s of #%d", field, seq)) {
						t.Fatalf("%s %s corruption = %d %s, want existing candidate decode error", selected, field, response.Code, response.Body.String())
					}
				} else {
					var records []store.Record
					if err := json.Unmarshal(response.Body.Bytes(), &records); err != nil {
						t.Fatal(err)
					}
					if response.Code != http.StatusOK || len(records) != 1 || records[0].Status != "open" {
						t.Fatalf("unrelated corruption affected selection: %d %s", response.Code, response.Body.String())
					}
				}
				open, err := st.OpenAmong(context.Background(), "d", nil)
				if global && selected != "opener" {
					if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("decode %s of #2", field)) {
						t.Fatalf("empty global read error=%v", err)
					}
				} else if global && selected == "opener" && field == "also_resolves" {
					if err == nil {
						t.Fatal("global empty resolver JSON error missing")
					}
				} else if err != nil || len(open) != 0 {
					t.Fatalf("empty selection=%+v %v", open, err)
				}
				t.Logf("OPENAMONG_CORRUPTION field=%s scope=%s global=%t http_status=%d", field, selected, global, response.Code)
			})
		}
	}
}

func TestOpenAmongResolverCandidateErrors(t *testing.T) {
	for _, row := range []struct {
		name, also, refs string
		direct           int64
		wantError        bool
	}{
		{"foreign nonmatching typed suffix", `[1,"bad"]`, `{}`, 0, false},
		{"malformed array on direct candidate", `[`, `{}`, 1, true},
		{"foreign refs do not enter resolver decoding", `[1]`, `[`, 0, false},
	} {
		t.Run(row.name, func(t *testing.T) {
			st, db := openAmongStore(t)
			openAmongInsert(t, db, 32)
			if _, err := db.Exec(`INSERT INTO records(drive,lane,kind,at,text,also_resolves,resolves,refs,source,hash) VALUES('foreign','other','note',2,'resolver',?,?,?,'post','resolver')`, row.also, row.direct, row.refs); err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			server.New(st, time.Second).Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/records?drive=d&kind=ask&limit=1", nil))
			if row.wantError {
				if response.Code != http.StatusInternalServerError {
					t.Fatalf("candidate resolver error suppressed: %d %s", response.Code, response.Body.String())
				}
				return
			}
			var records []store.Record
			if err := json.Unmarshal(response.Body.Bytes(), &records); err != nil {
				t.Fatal(err)
			}
			if response.Code != http.StatusOK || len(records) != 1 || records[0].Status != "closed" {
				t.Fatalf("foreign resolver changed: %d %s", response.Code, response.Body.String())
			}
		})
	}
}
