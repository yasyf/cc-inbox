package hook_test

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/hook"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"modernc.org/sqlite"
)

var owedCounterID atomic.Uint64

func TestHookOwedDoesNotDecodeUnrelatedCloserHistory(t *testing.T) {
	for _, history := range []int{32, 10000} {
		for _, resolved := range []bool{false, true} {
			t.Run(fmt.Sprintf("history=%d/resolved=%t", history, resolved), func(t *testing.T) {
				var reads atomic.Int64
				name := fmt.Sprintf("cci_hook_owed_refs_%d", owedCounterID.Add(1))
				if err := sqlite.RegisterDeterministicScalarFunction(name, 1, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
					reads.Add(1)
					return args[0], nil
				}); err != nil {
					t.Fatal(err)
				}
				ctx := t.Context()
				home := t.TempDir()
				st, err := store.Open(ctx, home)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = st.Close() }()
				now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
				st.Now = func() time.Time { return now }
				db, err := sql.Open("sqlite", filepath.Join(home, store.DBFile))
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = db.Close() }()
				ask, _, err := st.Post(ctx, store.Record{Drive: "d", Lane: "root", Kind: kinds.Ask, Text: "selected question", To: []string{"api"}, Source: "post"}, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := db.ExecContext(ctx, `WITH RECURSIVE n(x) AS(VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<?)
INSERT INTO records(drive,lane,kind,at,text,re,topic,source,hash)
SELECT 'd','history','answer',x,'unrelated',1000000+x,'other-'||x,'post','noise-'||x FROM n`, history); err != nil {
					t.Fatal(err)
				}
				if resolved {
					if _, err := db.ExecContext(ctx, `INSERT INTO records(drive,lane,kind,at,text,also_resolves,source,hash) VALUES('foreign','resolver','note',1,'resolved',?,'post','resolver')`, fmt.Sprintf("[%d]", ask.Seq)); err != nil {
						t.Fatal(err)
					}
				}
				key := store.PeerKey{Session: "hook-owed"}
				if err := st.BindLane(ctx, key, "d", "api", time.Time{}); err != nil {
					t.Fatal(err)
				}
				head, err := st.MaxSeq(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if err := st.MoveCursor(ctx, store.LaneCursor(key), "d", head); err != nil {
					t.Fatal(err)
				}
				if _, _, err := st.Post(ctx, store.Record{Drive: "d", Lane: "root", Kind: kinds.Note, Text: "delivery trigger", To: []string{"api"}, Source: "post"}, 0); err != nil {
					t.Fatal(err)
				}

				definitions, err := db.QueryContext(ctx, "SELECT name, sql FROM sqlite_schema WHERE type='trigger' AND sql IS NOT NULL ORDER BY rowid")
				if err != nil {
					t.Fatal(err)
				}
				var triggers [][2]string
				for definitions.Next() {
					var definition [2]string
					if err := definitions.Scan(&definition[0], &definition[1]); err != nil {
						t.Fatal(err)
					}
					triggers = append(triggers, definition)
				}
				if err := definitions.Err(); err != nil {
					t.Fatal(err)
				}
				if err := definitions.Close(); err != nil {
					t.Fatal(err)
				}
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = tx.Rollback() }()
				for _, definition := range triggers {
					if _, err := tx.ExecContext(ctx, fmt.Sprintf("DROP TRIGGER %q", definition[0])); err != nil {
						t.Fatal(err)
					}
				}
				for _, query := range []string{"ALTER TABLE records RENAME COLUMN refs TO counted_refs", "ALTER TABLE records ADD COLUMN refs TEXT GENERATED ALWAYS AS (" + name + "(counted_refs)) VIRTUAL"} {
					if _, err := tx.ExecContext(ctx, query); err != nil {
						t.Fatal(err)
					}
				}
				for _, definition := range triggers {
					if _, err := tx.ExecContext(ctx, definition[1]); err != nil {
						t.Fatal(err)
					}
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}

				if err := st.Close(); err != nil {
					t.Fatal(err)
				}
				st, err = store.Open(ctx, home)
				if err != nil {
					t.Fatal(err)
				}
				st.Now = func() time.Time { return now }
				reads.Store(0)
				var out bytes.Buffer
				err = hook.PostToolUse(ctx, st, hook.Payload{SessionID: key.Session, Event: "PostToolUse"}, func() (store.Window, error) { return store.Window{}, nil }, &out)
				if err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(out.String(), "delivery trigger") || strings.Contains(out.String(), "selected question") == resolved {
					t.Fatalf("actual hook output=%s", out.String())
				}
				want := int64(2)
				if os.Getenv("CCI_HOOK_OWED_EXPECT_GLOBAL") == "1" {
					want += int64(history)
				}
				if got := reads.Load(); got != want {
					t.Fatalf("hook owed refs_reads=%d want=%d history=%d", got, want, history)
				}
				if snapshots := os.Getenv("CCI_HOOK_OWED_SNAPSHOTS"); snapshots != "" {
					p := filepath.Join(snapshots, fmt.Sprintf("%d-%t.json", history, resolved))
					if os.Getenv("CCI_HOOK_OWED_SNAPSHOT_WRITE") == "1" {
						if err := os.WriteFile(p, out.Bytes(), 0o600); err != nil {
							t.Fatal(err)
						}
					} else {
						old, err := os.ReadFile(p)
						if err != nil || !bytes.Equal(old, out.Bytes()) {
							t.Fatalf("hook output changed: old=%s current=%s err=%v", old, out.Bytes(), err)
						}
					}
				}
				t.Logf("HOOK_OWED_WORK history=%d cross_drive_resolved=%t actual_refs_reads=%d output_bytes=%d", history, resolved, reads.Load(), out.Len())
			})
		}
	}
}
