package cli_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

type changedWatchObservation struct {
	queries int64
	reads   [3]int64
	records []store.Record
	lines   []string
	cursor  int64
}

func TestWatchChangedGenerationRecipientWork(t *testing.T) {
	if os.Getenv("CCI_WATCH_CHANGED_GENERATION_DIAGNOSTIC") != "1" {
		t.Skip("set CCI_WATCH_CHANGED_GENERATION_DIAGNOSTIC=1 to run changed-generation Watch diagnostics")
	}
	for _, population := range []int{32, 10000} {
		for _, placement := range []string{"same-drive", "foreign-drive"} {
			t.Run(fmt.Sprintf("%d/%s", population, placement), func(t *testing.T) {
				native := changedWatchObservationFor(t, population, placement, "native")
				identity := changedWatchObservationFor(t, population, placement, "identity")
				counted := changedWatchObservationFor(t, population, placement, "counted")
				if !reflect.DeepEqual(native.records, identity.records) || !reflect.DeepEqual(native.records, counted.records) || !reflect.DeepEqual(native.lines, identity.lines) || !reflect.DeepEqual(native.lines, counted.lines) || native.cursor != identity.cursor || native.cursor != counted.cursor {
					t.Fatalf("watch observations differ: native=%+v identity=%+v counted=%+v", native, identity, counted)
				}
				total := counted.reads[0] + counted.reads[1] + counted.reads[2]
				t.Logf("WATCH_CHANGED_GENERATION_WORK population=%d placement=%s polls=3 query_starts=%d emitted=%d cursor=%d poll1_recipient_materializations=%d poll2_recipient_materializations=%d poll3_recipient_materializations=%d recipient_materializations=%d native_query_starts=%d native_emitted=%d native_cursor=%d identity_query_starts=%d identity_emitted=%d identity_cursor=%d", population, placement, counted.queries, len(counted.records), counted.cursor, counted.reads[0], counted.reads[1], counted.reads[2], total, native.queries, len(native.records), native.cursor, identity.queries, len(identity.records), identity.cursor)
			})
		}
	}
}

func changedWatchObservationFor(t *testing.T, population int, placement, mode string) changedWatchObservation {
	t.Helper()
	var observed changedWatchObservation
	synctest.Test(t, func(t *testing.T) {
		st, clock := testutil.Store(t)
		db, err := sql.Open("sqlite", "file:"+filepath.Join(st.HomeDir(), store.DBFile))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		db.SetMaxOpenConns(1)
		if _, err := db.ExecContext(t.Context(), `INSERT INTO records(seq,drive,lane,kind,at,text,source,hash,recipients,origin,origin_seq,id)
VALUES(1,'d','noise','ask',?,'control-0','post','watch-control','["other"]',?,1,'watch-control')`, clock.Now().UnixMilli(), st.Self()); err != nil {
			t.Fatal(err)
		}
		drive := "d"
		if placement == "foreign-drive" {
			drive = "foreign"
		}
		if _, err := db.ExecContext(t.Context(), `WITH RECURSIVE history(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n<?)
INSERT INTO records(seq,drive,lane,kind,at,text,source,hash,recipients,origin,origin_seq,id)
SELECT n+1,?,'noise','ask',?,'ignored','post','changed-watch-'||n,'["other"]',?,n+1,'changed-watch-'||n FROM history`, population, drive, clock.Now().UnixMilli(), st.Self()); err != nil {
			t.Fatal(err)
		}
		if err := st.SetCursor(t.Context(), "changed-root-channel", "d", 1); err != nil {
			t.Fatal(err)
		}
		home := st.HomeDir()
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}
		for _, query := range []string{"DROP TRIGGER IF EXISTS record_root_audience_insert", "DROP TRIGGER IF EXISTS record_root_audience_update", "DROP TRIGGER IF EXISTS record_root_audience_delete", "DROP INDEX IF EXISTS record_root_audience_stream", "DROP INDEX IF EXISTS record_root_audience_kind_stream", "DROP TABLE IF EXISTS record_root_audience"} {
			if _, err := db.ExecContext(t.Context(), query); err != nil {
				t.Fatal(err)
			}
		}
		var rootObjects int
		if err := db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM sqlite_schema WHERE name IN ('record_root_audience','record_root_audience_stream','record_root_audience_kind_stream','record_root_audience_insert','record_root_audience_update','record_root_audience_delete')").Scan(&rootObjects); err != nil || rootObjects != 0 {
			t.Fatalf("unenrolled root audience objects=%d, error=%v", rootObjects, err)
		}
		t.Log("WATCH_CHANGED_GENERATION_UNENROLLED_SCOPE root_audience_objects=0 route=general unbounded_reference=true")
		table := "records"
		var reads *atomic.Int64
		switch mode {
		case "counted":
			reads = tailCostColumn(t, db, "recipients")
			table = "counted_tail_history"
		case "identity":
			for _, query := range []string{"ALTER TABLE records RENAME TO counted_tail_history", "CREATE VIEW records AS SELECT * FROM counted_tail_history"} {
				if _, err := db.ExecContext(t.Context(), query); err != nil {
					t.Fatal(err)
				}
			}
			table = "counted_tail_history"
		case "native":
		default:
			t.Fatalf("unknown observation mode %q", mode)
		}
		st, err = store.Open(t.Context(), home)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = st.Close() }()
		if reads != nil {
			reads.Store(0)
		}
		var queries atomic.Int64
		st.Now = func() time.Time {
			queries.Add(1)
			return clock.Now()
		}
		sub := store.Subscription{Drive: "d", Session: "missing", Reader: "root", Kinds: []kinds.Kind{kinds.Incident, kinds.Decide, kinds.Ask, kinds.Defect, kinds.Blocker}, ToKinds: store.DefaultToKinds("root")}
		filter := sub.Filter()
		filter.After = 1
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() {
			done <- inbox.Watch(ctx, st, inbox.WatchOptions{Filter: filter, Cursor: "changed-root-channel", Interval: time.Second, Untagged: true}, func(record store.Record, line string) error {
				observed.records = append(observed.records, record)
				observed.lines = append(observed.lines, line)
				return nil
			})
		}()
		defer func() {
			cancel()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		}()
		var previous int64
		for poll := range 3 {
			if poll > 0 {
				result, err := db.ExecContext(t.Context(), "UPDATE "+table+" SET text = ? WHERE seq = 1", fmt.Sprintf("control-%d", poll))
				if err != nil {
					t.Fatal(err)
				}
				if affected, err := result.RowsAffected(); err != nil || affected != 1 {
					t.Fatalf("control mutation affected %d rows: %v", affected, err)
				}
				if reads != nil {
					previous = reads.Load()
				}
				time.Sleep(time.Second)
			}
			synctest.Wait()
			if got := queries.Load(); got != int64(poll+1) {
				t.Fatalf("mode=%s poll=%d query starts=%d, want %d", mode, poll+1, got, poll+1)
			}
			if len(observed.records) != 0 || len(observed.lines) != 0 {
				t.Fatalf("mode=%s poll=%d emitted records=%v lines=%v", mode, poll+1, observed.records, observed.lines)
			}
			cursor, ok, err := st.Cursor(t.Context(), "changed-root-channel", "d")
			if err != nil || !ok || cursor != 1 {
				t.Fatalf("mode=%s poll=%d cursor=%d found=%t error=%v", mode, poll+1, cursor, ok, err)
			}
			observed.cursor = cursor
			if reads != nil {
				current := reads.Load()
				observed.reads[poll] = current - previous
				previous = current
			}
		}
		observed.queries = queries.Load()
	})
	return observed
}
