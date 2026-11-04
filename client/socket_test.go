package client_test

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/client"
	"github.com/yasyf/cc-inbox/internal/cli"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/wire"
)

type countingListener struct {
	net.Listener
	accepted atomic.Int64
}

func (l *countingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		l.accepted.Add(1)
	}
	return conn, err
}

func daemonHome(t *testing.T) (*countingListener, func()) {
	t.Helper()
	home, err := os.MkdirTemp("", "cci")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("CCI_HOME", home)
	st, err := store.Open(context.Background(), home)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	raw, err := net.Listen("unix", filepath.Join(home, wire.SocketName))
	if err != nil {
		t.Fatal(err)
	}
	ln := &countingListener{Listener: raw}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- cli.ServeCommands(ctx, ln, st, home) }()
	var stopped atomic.Bool
	stop := func() {
		if stopped.CompareAndSwap(false, true) {
			cancel()
			if err := <-done; err != nil {
				t.Error(err)
			}
		}
	}
	t.Cleanup(stop)
	return ln, stop
}

func TestTheClientReadsAndWritesThroughTheDaemonAndAloneAlike(t *testing.T) {
	ln, stop := daemonHome(t)
	ctx := t.Context()
	ask, err := client.Post(ctx, client.Draft{Drive: "d", Lane: "worker", Kind: "ask", Text: "ship it?", To: []string{"root"}})
	if err != nil {
		t.Fatal(err)
	}
	if ln.accepted.Load() != 1 {
		t.Fatalf("accepted = %d; want the post to reach the daemon", ln.accepted.Load())
	}
	answer, err := client.Post(ctx, client.Draft{Drive: "d", Lane: "root", Kind: "answer", Text: "yes", Re: ask.Seq, Body: nil})
	if err != nil {
		t.Fatal(err)
	}
	read := func() map[string]any {
		tail, err := client.Tail(ctx, client.Query{Drive: "d"})
		if err != nil {
			t.Fatal(err)
		}
		among, err := client.Among(ctx, "d", []int64{ask.Seq, answer.Seq})
		if err != nil {
			t.Fatal(err)
		}
		open, err := client.Open(ctx, "d", "ask")
		if err != nil {
			t.Fatal(err)
		}
		generations, err := client.Generations(ctx, "d", "root")
		if err != nil {
			t.Fatal(err)
		}
		who, err := client.Who(ctx, "d")
		if err != nil {
			t.Fatal(err)
		}
		return map[string]any{"tail": tail, "among": among, "open": open, "generations": generations, "who": who}
	}
	through := read()
	if ln.accepted.Load() < 7 {
		t.Fatalf("accepted = %d; want each read to reach the daemon", ln.accepted.Load())
	}
	if tail := through["tail"].([]client.Record); len(tail) != 2 || tail[0].Seq != ask.Seq || tail[1].ReNum == "" {
		t.Fatalf("tail through the daemon = %+v", tail)
	}
	stop()
	if err := os.Remove(filepath.Join(os.Getenv("CCI_HOME"), wire.SocketName)); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	alone := read()
	a, _ := json.Marshal(through)
	b, _ := json.Marshal(alone)
	if string(a) != string(b) || !reflect.DeepEqual(through["tail"].([]client.Record)[1].ReNum, alone["tail"].([]client.Record)[1].ReNum) {
		t.Fatalf("through the daemon = %s\nalone = %s", a, b)
	}
}

func TestFollowRunsOnTheDaemon(t *testing.T) {
	ln, stop := daemonHome(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	start, err := client.Post(ctx, client.Draft{Drive: "d", Lane: "worker", Kind: "note", Text: "before the watch", To: []string{"root"}})
	if err != nil {
		t.Fatal(err)
	}
	first, err := client.Post(ctx, client.Draft{Drive: "d", Lane: "worker", Kind: "note", Text: "after its start", To: []string{"root"}})
	if err != nil {
		t.Fatal(err)
	}
	seen := make(chan client.Record, 4)
	followed := make(chan error, 1)
	go func() {
		followed <- client.Follow(ctx, client.Watch{Drive: "d", Reader: "root", After: start.Seq}, func(r client.Record) error {
			seen <- r
			return nil
		})
	}()
	select {
	case r := <-seen:
		if r.Seq != first.Seq {
			t.Fatalf("followed %+v", r)
		}
	case <-ctx.Done():
		t.Fatal("nothing followed")
	}
	if ln.accepted.Load() < 3 {
		t.Fatalf("accepted = %d; want the follow on the daemon", ln.accepted.Load())
	}
	cancel()
	if err := <-followed; err != nil {
		t.Fatalf("Follow ended by its context = %v, want nil", err)
	}
	stop()
}
