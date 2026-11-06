package daemon_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/daemon"
	"github.com/yasyf/cc-inbox/internal/hook"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
)

func note(text string) store.Write {
	return store.Write{Records: []store.Record{{Drive: "d", Lane: "lane-a", Kind: kinds.Note, Text: text}}, At: time.Now()}
}

func commit(w store.Writer, write store.Write, within time.Duration) (store.Written, error) {
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	return w.Commit(ctx, write)
}

func awaitRouted(t *testing.T, w store.Writer) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, err := commit(w, store.Write{Touch: []store.PeerKey{{Session: "nobody"}}}, 2*time.Second)
		if err == nil {
			return
		}
		if !errors.Is(err, store.ErrUnrouted) || time.Now().After(deadline) {
			t.Fatalf("the daemon never took a write: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func texts(t *testing.T, st *store.Store, drive string) []string {
	t.Helper()
	records, err := st.Query(context.Background(), store.Filter{Drive: drive})
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, 0, len(records))
	for _, r := range records {
		out = append(out, r.Text)
	}
	slices.Sort(out)
	return out
}

func TestDaemonCommitsConcurrentClientWritesAndRefusesAnotherStores(t *testing.T) {
	home := t.TempDir()
	st, err := store.Open(context.Background(), home)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	d, _, _ := serve(t, home)
	w := daemon.Writes(d, home, st.Self())
	awaitRouted(t, w)

	const n = 24
	seqs, errs := make([]int64, n), make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			done, err := commit(w, note(fmt.Sprintf("concurrent %02d", i)), 10*time.Second)
			if errs[i] = err; err == nil {
				seqs[i] = done.Posted[0].Record.Seq
			}
		})
	}
	wg.Wait()
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("write %d = %v", i, errs[i])
		}
		got, err := st.Get(context.Background(), seqs[i])
		if err != nil || got.Text != fmt.Sprintf("concurrent %02d", i) {
			t.Fatalf("record #%d the daemon answered write %d with = %+v, %v", seqs[i], i, got, err)
		}
	}
	if slices.Sort(seqs); len(slices.Compact(seqs)) != n {
		t.Fatalf("seqs = %v, want %d distinct", seqs, n)
	}

	for _, other := range []store.Writer{daemon.Writes(d, t.TempDir(), st.Self()), daemon.Writes(d, home, "another-node")} {
		if _, err := commit(other, note("elsewhere"), 5*time.Second); !errors.Is(err, store.ErrUnrouted) {
			t.Fatalf("a write for another store or node = %v, want ErrUnrouted", err)
		}
	}
	if got := texts(t, st, "d"); slices.Contains(got, "elsewhere") {
		t.Fatalf("stored texts = %q, want no write the daemon refused", got)
	}
}

func TestDaemonAppliesNothingOfAWriteThatMissedItsTurn(t *testing.T) {
	home := t.TempDir()
	st, err := store.Open(context.Background(), home)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	d, _, _ := serve(t, home)
	w := daemon.Writes(d, home, st.Self())
	awaitRouted(t, w)

	release := holdStore(t, home)
	if _, err := commit(w, note("missed its turn"), 3*time.Second); !store.Busy(err) || errors.Is(err, store.ErrUnconfirmed) {
		t.Fatalf("a write behind a held store = %v, want the daemon to answer busy before the caller's deadline", err)
	}
	release()
	if _, err := commit(w, note("after the hold"), 10*time.Second); err != nil {
		t.Fatalf("a write after the store freed = %v", err)
	}
	if got := texts(t, st, "d"); !slices.Equal(got, []string{"after the hold"}) {
		t.Fatalf("stored texts = %q, want only the write that made its turn", got)
	}
}

func TestDaemonRecordsAPromptTheHookKept(t *testing.T) {
	home := t.TempDir()
	st, err := store.Open(context.Background(), home)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.Bind(context.Background(), store.PeerKey{Session: "root"}, "d", true); err != nil {
		t.Fatal(err)
	}
	if err := hook.KeepPrompt(home, []byte(`{"session_id":"root","hook_event_name":"UserPromptSubmit","prompt":"record me once the store answers"}`), time.Now()); err != nil {
		t.Fatal(err)
	}
	serve(t, home)
	deadline := time.Now().Add(15 * time.Second)
	for {
		records, err := st.Query(context.Background(), store.Filter{Drive: "d", Kinds: []kinds.Kind{kinds.Owner}})
		if err != nil {
			t.Fatal(err)
		}
		kept, _ := filepath.Glob(filepath.Join(home, "prompts", "*"))
		if len(records) == 1 && records[0].Text == "record me once the store answers" && len(kept) == 0 {
			return
		}
		if len(records) > 1 || time.Now().After(deadline) {
			t.Fatalf("owner records = %+v with %v still kept, want the kept prompt recorded once", records, kept)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestDaemonRecordsTheMigrationItRanAtStart(t *testing.T) {
	home := t.TempDir()
	st, err := store.Open(context.Background(), home)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	db, err := sql.Open("sqlite", "file:"+filepath.Join(home, store.DBFile))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(context.Background(), "DROP INDEX records_re"); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	serve(t, home)
	deadline := time.Now().Add(15 * time.Second)
	for {
		got := texts(t, st, daemon.MaintenanceDrive)
		if len(got) == 1 && strings.HasPrefix(got[0], "built the store's query indexes in ") {
			return
		}
		if len(got) > 1 || time.Now().After(deadline) {
			t.Fatalf("maintenance records = %q, want the index build the daemon ran", got)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func holdStore(t *testing.T, home string) func() {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(home, store.DBFile))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(t.Context(), "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	release := sync.OnceFunc(func() {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
	})
	t.Cleanup(release)
	return release
}

func TestTheDaemonCommitsPagedWritesOnItsQueue(t *testing.T) {
	home := t.TempDir()
	st, err := store.Open(context.Background(), home)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	d, _, _ := serve(t, home)
	w := daemon.Writes(d, home, st.Self())
	awaitRouted(t, w)
	line := store.Ingestion{Record: store.Record{Drive: "d", Lane: "importer", Kind: kinds.Note, At: time.Now(), Text: "an imported line", Source: "import:/inbox.md"}, LineHash: "imported", Line: "an imported line"}
	ingested, err := commit(w, store.Write{Ingest: []store.Ingestion{line}}, 10*time.Second)
	if err != nil || ingested.Ingested != 1 {
		t.Fatalf("an import page through the daemon = %+v, %v; want its line stored", ingested, err)
	}
	reply := store.Row{Origin: "0123456789abcdef", OriginSeq: 1, Drive: "d", Lane: "root", Kind: kinds.Note, At: time.Now(), Text: "a reply to a record no node holds", Source: "post", Re: &store.Pointer{Origin: "fedcba9876543210", Seq: 1}}
	applied, err := commit(w, store.Write{Apply: []store.Row{reply}}, 10*time.Second)
	if err != nil || applied.Applied.Buffered != 1 {
		t.Fatalf("an apply page through the daemon = %+v, %v; want the reply held", applied, err)
	}
	settled, err := commit(w, store.Write{Settle: &store.SettlePage{Waiting: []store.Pointer{*reply.Re}, Absent: []store.Pointer{*reply.Re}}}, 10*time.Second)
	if err != nil || settled.Applied.Released != 1 || len(settled.Waiting) != 0 {
		t.Fatalf("a settle page through the daemon = %+v, %v; want the reply stored and nothing left waiting", settled, err)
	}
	if got := texts(t, st, "d"); !slices.Equal(got, []string{"a reply to a record no node holds", "an imported line"}) {
		t.Fatalf("records = %q, want the imported line and the settled reply", got)
	}
}
