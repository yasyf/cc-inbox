package daemon

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
)

func TestRefusalTellsAppliedNothingFromUnknown(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		is   error
	}{
		{"an old daemon", &daemonkit.ProductError{Message: fmt.Sprintf("unknown op %q", OpWrite)}, store.ErrUnrouted},
		{"another store", &daemonkit.ProductError{Code: codeUnrouted, Message: "this daemon writes /elsewhere"}, store.ErrUnrouted},
		{"expired", &daemonkit.ProductError{Code: codeExpired, Message: "late"}, store.ErrExpired},
		{"busy", &daemonkit.ProductError{Code: codeBusy, Message: "locked"}, store.ErrBusy},
		{"lost reply", context.DeadlineExceeded, store.ErrUnconfirmed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := refusal(tt.err); !errors.Is(got, tt.is) {
				t.Fatalf("refusal(%v) = %v, want %v", tt.err, got, tt.is)
			}
		})
	}
	got := refusal(&daemonkit.ProductError{Message: "withdraw --re #9 names no record in drive d"})
	if got.Error() != "withdraw --re #9 names no record in drive d" || errors.Is(got, store.ErrUnconfirmed) || store.Busy(got) {
		t.Fatalf("refusal of a product error = %v, want its message alone", got)
	}
}

func TestQueueCommitsWhatWasQueuedBeforeItCloses(t *testing.T) {
	home := t.TempDir()
	ctx := context.Background()
	st, err := store.Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	q, err := newQueue(st)
	if err != nil {
		t.Fatal(err)
	}
	release := holdWriter(t, home)
	done := make(chan committed, 1)
	q.jobs <- job{ctx: ctx, w: store.Write{Records: []store.Record{{Drive: "d", Lane: "lane-a", Kind: kinds.Note, Text: "queued before the drain"}}, At: time.Now()}, done: done}
	closed := make(chan error, 1)
	go func() {
		closeCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		closed <- q.close(closeCtx)
	}()
	time.Sleep(100 * time.Millisecond)
	release()
	if c := <-done; c.err != nil || len(c.written.Posted) != 1 {
		t.Fatalf("the write queued before close = %+v", c)
	}
	if err := <-closed; err != nil {
		t.Fatalf("close() = %v", err)
	}
	if n, err := st.Count(ctx, store.Filter{Drive: "d"}); err != nil || n != 1 {
		t.Fatalf("records after the drain = %d, %v, want the queued one", n, err)
	}
	if _, err := q.Commit(ctx, store.Write{Touch: []store.PeerKey{{Session: "s"}}}); !errors.Is(err, store.ErrUnrouted) {
		t.Fatalf("Commit() after close = %v, want ErrUnrouted so the caller writes directly", err)
	}
}

func holdWriter(t *testing.T, home string) func() {
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
	return func() {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
	}
}
