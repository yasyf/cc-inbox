package daemon_test

import (
	"context"
	"net"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/daemon"
	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/server"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func watchSeqs(ctx context.Context, st *store.Store, f store.Filter, interval time.Duration) (<-chan int64, <-chan error) {
	seqs := make(chan int64, 16)
	done := make(chan error, 1)
	go func() {
		done <- inbox.Watch(ctx, st, inbox.WatchOptions{Filter: f, Interval: interval}, func(r store.Record, _ string) error {
			seqs <- r.Seq
			return nil
		})
	}()
	return seqs, done
}

func awaitSeq(t *testing.T, seqs <-chan int64, want int64) {
	t.Helper()
	select {
	case got := <-seqs:
		if got != want {
			t.Fatalf("watched #%d, want #%d", got, want)
		}
	case <-time.After(time.Minute):
		t.Fatalf("#%d was not watched within a minute", want)
	}
}

func TestWatchReadsOnlyWhatTheDaemonSaysPasses(t *testing.T) {
	st, _ := testutil.Store(t)
	feed := server.New(st, time.Millisecond)
	listening := httptest.NewServer(feed.Handler())
	defer listening.Close()
	defer feed.Close()
	st.FollowThrough(daemon.Follows(st.HomeDir(), listening.Listener.Addr().String()))
	anchor := testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "anchor"})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	seqs, done := watchSeqs(ctx, st, store.Filter{Drive: "d", To: "reader", After: anchor.Seq}, time.Hour)
	testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "to nobody here"})
	wanted := testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "first", To: []string{"reader"}})
	awaitSeq(t, seqs, wanted.Seq)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Watch = %v", err)
	}
}

func TestWatchReadsTheStoreItselfWithNoDaemon(t *testing.T) {
	st, _ := testutil.Store(t)
	closed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := closed.Addr().String()
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	st.FollowThrough(daemon.Follows(st.HomeDir(), addr))
	anchor := testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "anchor"})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	seqs, done := watchSeqs(ctx, st, store.Filter{Drive: "d", To: "reader", After: anchor.Seq}, time.Millisecond)
	wanted := testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "first", To: []string{"reader"}})
	awaitSeq(t, seqs, wanted.Seq)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Watch = %v", err)
	}
}

func TestWatchRefusedByADaemonOfAnotherStoreReadsTheStoreItself(t *testing.T) {
	st, _ := testutil.Store(t)
	other, _ := testutil.Store(t)
	feed := server.New(other, time.Millisecond)
	listening := httptest.NewServer(feed.Handler())
	defer listening.Close()
	defer feed.Close()
	st.FollowThrough(daemon.Follows(st.HomeDir(), listening.Listener.Addr().String()))
	anchor := testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "anchor"})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	seqs, done := watchSeqs(ctx, st, store.Filter{Drive: "d", To: "reader", After: anchor.Seq}, time.Millisecond)
	wanted := testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "first", To: []string{"reader"}})
	awaitSeq(t, seqs, wanted.Seq)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Watch = %v", err)
	}
}
