package cli_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"modernc.org/sqlite"

	"github.com/yasyf/cc-inbox/internal/server"
	"github.com/yasyf/cc-inbox/internal/store"
)

func TestDaemonLaneHistoryBaseline(t *testing.T) {
	for _, population := range []int{32, 10000} {
		t.Run(fmt.Sprintf("history=%d", population), func(t *testing.T) {
			var comparisons atomic.Int64
			collation := fmt.Sprintf("cci_daemon_lane_drive_%d", tailCounterID.Add(1))
			if err := sqlite.RegisterCollationUtf8(collation, func(a, b string) int {
				comparisons.Add(1)
				return strings.Compare(a, b)
			}); err != nil {
				t.Fatal(err)
			}
			db := tailComparedStore(t, "records", "drive", collation)
			if _, err := db.ExecContext(t.Context(), `WITH RECURSIVE history(n) AS (
VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n < ?
)
INSERT INTO records(drive,lane,kind,at,text,source,hash)
SELECT 'd','lane-' || (n%8),'note',1,'old','post','old-' || n FROM history`, population); err != nil {
				t.Fatal(err)
			}
			for lane := range 8 {
				if _, err := db.ExecContext(t.Context(), "INSERT INTO records(drive,lane,kind,at,text,source,hash) VALUES('d',?,'note',2,'latest','post',?)", fmt.Sprintf("lane-%d", lane), fmt.Sprintf("latest-%d", lane)); err != nil {
					t.Fatal(err)
				}
			}
			st, err := store.Open(t.Context(), os.Getenv("CCI_HOME"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			handler := server.New(st, time.Second).Handler()
			comparisons.Store(0)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/lanes?drive=d", nil).WithContext(t.Context()))
			if response.Code != http.StatusOK {
				t.Fatalf("lanes status=%d body=%q", response.Code, response.Body.String())
			}
			var records []store.Record
			if err := json.Unmarshal(response.Body.Bytes(), &records); err != nil {
				t.Fatal(err)
			}
			if len(records) != 8 {
				t.Fatalf("lanes rows=%d, want8", len(records))
			}
			for i, record := range records {
				if record.Seq != int64(population+8-i) || record.Text != "latest" {
					t.Fatalf("lane winner changed: %+v", record)
				}
			}
			count := comparisons.Load()
			t.Logf("actual_daemon_http_lanes history=%d selected_lanes=8 output_bytes=%d native_drive_comparisons=%d", population, response.Body.Len(), count)
			if count < int64(population/2) {
				t.Fatalf("baseline did not reproduce linear historical drive work: comparisons=%d history=%d", count, population)
			}
		})
	}
}
