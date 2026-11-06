package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"modernc.org/sqlite"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/pacific"
)

var (
	writeHook     sync.Once
	writeCounting sync.Map
)

type largestWrites struct {
	mu      sync.Mutex
	largest map[string]int
}

func (w *largestWrites) of(table string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.largest[table]
}

func (w *largestWrites) reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	clear(w.largest)
}

func countLargestWrites(t *testing.T, home string) *largestWrites {
	t.Helper()
	writeHook.Do(func() {
		sqlite.RegisterConnectionHook(func(conn sqlite.ExecQuerierContext, dsn string) error {
			path, _, _ := strings.Cut(strings.TrimPrefix(dsn, "file:"), "?")
			counted, ok := writeCounting.Load(filepath.Dir(path))
			if !ok {
				return nil
			}
			w := counted.(*largestWrites)
			changed := map[string]int{}
			hooks := conn.(sqlite.HookRegisterer)
			hooks.RegisterPreUpdateHook(func(d sqlite.SQLitePreUpdateData) { changed[d.TableName]++ })
			hooks.RegisterCommitHook(func() int32 {
				w.mu.Lock()
				for table, n := range changed {
					w.largest[table] = max(w.largest[table], n)
				}
				w.mu.Unlock()
				clear(changed)
				return 0
			})
			hooks.RegisterRollbackHook(func() { clear(changed) })
			return nil
		})
	})
	w := &largestWrites{largest: map[string]int{}}
	writeCounting.Store(home, w)
	t.Cleanup(func() { writeCounting.Delete(home) })
	return w
}

func pagedStore(t *testing.T) (*Store, *largestWrites) {
	t.Helper()
	home := t.TempDir()
	w := countLargestWrites(t, home)
	return openMembershipStore(t.Context(), t, home), w
}

var pagesAtN = []int{1, 100}

func foreignNotes(n int, at time.Time) []Row {
	rows := make([]Row, n)
	for i := range rows {
		rows[i] = Row{Origin: "0123456789abcdef", OriginSeq: int64(i + 1), Drive: "d", Lane: "root", Kind: kinds.Note, At: at, Text: fmt.Sprintf("note %d", i), Source: "post"}
	}
	return rows
}

func lineItems(n int, at time.Time) []Ingestion {
	items := make([]Ingestion, n)
	for i := range items {
		line := fmt.Sprintf("line %d", i)
		items[i] = Ingestion{Record: Record{Drive: "d", Lane: "importer", Kind: kinds.Note, At: at, Text: line, Source: "import:/inbox.md"}, LineHash: fmt.Sprintf("hash-%d", i), Line: line}
	}
	return items
}

func TestSettleStoresAPageOfHeldRowsPerTransaction(t *testing.T) {
	root := Pointer{Origin: "fedcba9876543210", Seq: 1}
	shapes := map[string]func(rows []Row) []Pointer{
		"each reply waits on its own absent record": func(rows []Row) []Pointer {
			absent := make([]Pointer, len(rows))
			for i := range rows {
				absent[i] = Pointer{Origin: root.Origin, Seq: int64(i + 1)}
				rows[i].Re = &absent[i]
			}
			return absent
		},
		"every reply waits on one absent record": func(rows []Row) []Pointer {
			for i := range rows {
				rows[i].Re = &root
			}
			return []Pointer{root}
		},
		"a chain of replies waits on one absent record": func(rows []Row) []Pointer {
			rows[0].Re = &root
			for i := 1; i < len(rows); i++ {
				rows[i].Re = &Pointer{Origin: rows[i-1].Origin, Seq: rows[i-1].OriginSeq}
			}
			return []Pointer{root}
		},
	}
	for name, shape := range shapes {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			largest := map[int]int{}
			for _, pages := range pagesAtN {
				st, w := pagedStore(t)
				n := pages * writePage
				rows := foreignNotes(n, time.Now().Add(-time.Hour))
				absent := shape(rows)
				if a, err := st.Apply(ctx, rows, nil); err != nil || a.Buffered != n {
					t.Fatalf("apply of %d replies = %+v, %v; want every one held", n, a, err)
				}
				w.reset()
				settled, err := st.Settle(ctx, absent)
				if err != nil || settled.Released != n {
					t.Fatalf("settle of %d held replies = %+v, %v; want every one stored", n, settled, err)
				}
				largest[pages] = w.of("records")
			}
			if largest[1] == 0 || largest[1] != largest[100] {
				t.Fatalf("the largest settle transaction stored %d records at 1 page and %d at 100; want the same page either way", largest[1], largest[100])
			}
		})
	}
}

func TestApplyWritesEveryBodyBeforeItTakesTheWriteLock(t *testing.T) {
	ctx := t.Context()
	for _, pages := range pagesAtN {
		st, _ := pagedStore(t)
		n := pages * writePage
		rows := foreignNotes(n, time.Now().Add(-time.Hour))
		for i := range rows {
			rows[i].PathBody = fmt.Sprintf("body %d", i)
		}
		held := strings.Repeat("a body larger than a pipe holds ", 1<<12)
		rows[n-1].PathBody = held
		sum := sha256.Sum256([]byte(redact(held)))
		dir := filepath.Join(st.HomeDir(), blobDir)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		fifo := filepath.Join(dir, hex.EncodeToString(sum[:8])+".txt")
		if err := syscall.Mkfifo(fifo, 0o600); err != nil {
			t.Fatal(err)
		}
		writer, err := sql.Open("sqlite", "file:"+filepath.Join(st.HomeDir(), DBFile)+"?_pragma=busy_timeout(0)&_txlock=immediate")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = writer.Close() })
		type probe struct {
			read int64
			err  error
		}
		probed := make(chan probe, 1)
		go func() {
			body, err := os.Open(fifo)
			if err != nil {
				probed <- probe{err: err}
				return
			}
			defer func() { _ = body.Close() }()
			tx, locked := writer.BeginTx(context.Background(), nil)
			if locked == nil {
				locked = tx.Rollback()
			}
			read, drained := io.Copy(io.Discard, body)
			probed <- probe{read: read, err: errors.Join(locked, drained)}
		}()
		applied, err := st.Apply(ctx, rows, nil)
		if unblock, openErr := os.OpenFile(fifo, os.O_WRONLY|syscall.O_NONBLOCK, 0); openErr == nil {
			_ = unblock.Close()
		}
		got := <-probed
		if err != nil || applied.Inserted != n {
			t.Fatalf("apply of %d rows with bodies = %+v, %v; want every row", n, applied, err)
		}
		if got.err != nil || got.read != int64(len(held)) {
			t.Fatalf("while an apply of %d rows wrote a %d-byte body, another writer read %d bytes and found the store %v; want the whole body and the store unlocked", n, len(held), got.read, got.err)
		}
	}
}

func TestIngestAllStoresAPageOfLinesPerTransaction(t *testing.T) {
	ctx := t.Context()
	largest := map[int]int{}
	for _, pages := range pagesAtN {
		st, w := pagedStore(t)
		n := pages * writePage
		inserted, err := st.IngestAll(ctx, lineItems(n, time.Now().Add(-time.Hour)))
		if err != nil || inserted != n {
			t.Fatalf("ingest of %d lines = %d, %v; want every line", n, inserted, err)
		}
		largest[pages] = w.of("records")
	}
	if largest[1] == 0 || largest[1] != largest[100] {
		t.Fatalf("the largest ingest transaction stored %d records at 1 page and %d at 100; want the same page either way", largest[1], largest[100])
	}
}

func TestReparseStoresAPageOfLinesPerTransaction(t *testing.T) {
	ctx := t.Context()
	largest := map[int]int{}
	for _, pages := range pagesAtN {
		st, w := pagedStore(t)
		n := pages * writePage
		updated, inserted, err := st.Reparse(ctx, lineItems(n, time.Now().Add(-time.Hour)), true)
		if err != nil || updated != 0 || inserted != n {
			t.Fatalf("fresh reparse of %d lines = %d updated, %d inserted, %v; want every line inserted", n, updated, inserted, err)
		}
		largest[pages] = w.of("records")
	}
	if largest[1] == 0 || largest[1] != largest[100] {
		t.Fatalf("the largest reparse transaction stored %d records at 1 page and %d at 100; want the same page either way", largest[1], largest[100])
	}
}

func TestReparseCarriesASplitLinesParentAcrossPages(t *testing.T) {
	ctx := t.Context()
	st, _ := pagedStore(t)
	parentAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	items := lineItems(writePage+1, time.Now().Add(-time.Hour))
	parent := &items[writePage-1]
	parent.Record.At, parent.Record.Text = parentAt, "the parent line and a split line"
	if inserted, err := st.IngestAll(ctx, []Ingestion{*parent}); err != nil || inserted != 1 {
		t.Fatalf("ingest of the parent = %d, %v", inserted, err)
	}
	split := &items[writePage]
	split.Line, split.Record.Text = "a split line", "a split line"
	updated, inserted, err := st.Reparse(ctx, items, false)
	if err != nil || updated != 1 || inserted != 1 {
		t.Fatalf("reparse = %d updated, %d inserted, %v; want the parent updated and the split line stored", updated, inserted, err)
	}
	var at int64
	if err := st.db.QueryRowContext(ctx, "SELECT at FROM records WHERE line_hash = ?", split.LineHash).Scan(&at); err != nil {
		t.Fatal(err)
	}
	if got := time.UnixMilli(at).UTC(); !got.Equal(parentAt) {
		t.Fatalf("the split line on the next page is stamped %s, want its parent's %s", got, parentAt)
	}
}

func TestCompactFoldsAPageOfRecordsPerTransaction(t *testing.T) {
	ctx := t.Context()
	largest := map[int]int{}
	day := time.Date(2026, 1, 2, 12, 0, 0, 0, pacific.Zone)
	for _, pages := range pagesAtN {
		st, w := pagedStore(t)
		n := pages * writePage
		if inserted, err := st.IngestAll(ctx, lineItems(n, day)); err != nil || inserted != n {
			t.Fatalf("ingest of %d lines = %d, %v", n, inserted, err)
		}
		w.reset()
		res, err := st.Compact(ctx, "d", 0)
		if err != nil || res.Folded != n || res.Digests != 1 {
			t.Fatalf("compact of %d records on one day = %+v, %v; want every record in one digest", n, res, err)
		}
		largest[pages] = w.of("records")
	}
	if largest[1] == 0 || largest[1] != largest[100] {
		t.Fatalf("the largest compact transaction changed %d records at 1 page and %d at 100; want the same page either way", largest[1], largest[100])
	}
}

func TestAReparseRetriedAfterAFailedPageStillStampsASplitLine(t *testing.T) {
	ctx := t.Context()
	st, _ := pagedStore(t)
	parentAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	items := lineItems(writePage+1, time.Now().Add(-time.Hour))
	parent := &items[writePage-1]
	parent.Record.At, parent.Record.Text = parentAt, "the parent line and a split line"
	if inserted, err := st.IngestAll(ctx, []Ingestion{*parent}); err != nil || inserted != 1 {
		t.Fatalf("ingest of the parent = %d, %v", inserted, err)
	}
	parent.Record.Text = "the parent line"
	split := &items[writePage]
	split.Line, split.Record.Text = "a split line", "a split line"
	if _, err := st.db.ExecContext(ctx, "CREATE TRIGGER interrupt BEFORE INSERT ON records WHEN NEW.text = 'a split line' BEGIN SELECT RAISE(ABORT, 'interrupted'); END"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.Reparse(ctx, items, false); err == nil {
		t.Fatal("the interrupted reparse succeeded")
	}
	if _, err := st.db.ExecContext(ctx, "DROP TRIGGER interrupt"); err != nil {
		t.Fatal(err)
	}
	if _, inserted, err := st.Reparse(ctx, items, false); err != nil || inserted != 1 {
		t.Fatalf("the retried reparse = %d inserted, %v; want the split line stored", inserted, err)
	}
	var at int64
	if err := st.db.QueryRowContext(ctx, "SELECT at FROM records WHERE line_hash = ?", split.LineHash).Scan(&at); err != nil {
		t.Fatal(err)
	}
	if got := time.UnixMilli(at).UTC(); !got.Equal(parentAt) {
		t.Fatalf("the split line is stamped %s after the retry, want its parent's %s", got, parentAt)
	}
}

func TestAFoldCountsOnlyTheRecordsItDeletes(t *testing.T) {
	ctx := t.Context()
	st, _ := pagedStore(t)
	day := time.Date(2026, 1, 2, 12, 0, 0, 0, pacific.Zone)
	if inserted, err := st.IngestAll(ctx, lineItems(3, day)); err != nil || inserted != 3 {
		t.Fatalf("ingest = %d, %v", inserted, err)
	}
	old, err := st.Query(ctx, Filter{Drive: "d", IncludeExpired: true})
	if err != nil || len(old) != 3 {
		t.Fatalf("query = %d records, %v", len(old), err)
	}
	if _, err := st.db.ExecContext(ctx, "DELETE FROM records WHERE seq = ?", old[0].Seq); err != nil {
		t.Fatal(err)
	}
	folded, err := st.foldPage(ctx, "d", day.Format(time.DateOnly), old)
	if err != nil || folded != 2 {
		t.Fatalf("fold of 3 candidates, one already gone = %d, %v; want 2", folded, err)
	}
	digests, err := st.Query(ctx, Filter{Drive: "d", Kinds: []kinds.Kind{kinds.Digest}, IncludeExpired: true})
	if err != nil || len(digests) != 1 || digests[0].Fields.Counts["total"] != 2 {
		t.Fatalf("digests = %+v, %v; want one counting 2 records", digests, err)
	}
}
