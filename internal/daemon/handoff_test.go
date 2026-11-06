package daemon_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/daemon"
)

func TestADaemonMoveHandsItsListenerOverSoNoDialIsRefused(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "cci-move")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	old, err := daemon.Listen(home, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := old.Addr().String()
	_, stopOld := serveOn(t, home, old)
	client := &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	get := func() error {
		resp, err := client.Get("http://" + addr + "/v1/drives")
		if err != nil {
			return err
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("answered %s", resp.Status)
		}
		return nil
	}

	moving, stopDialing := context.WithCancel(context.Background())
	var answered, refused atomic.Int64
	var dialing sync.WaitGroup
	for range 4 {
		dialing.Go(func() {
			for moving.Err() == nil {
				err := get()
				if err == nil {
					answered.Add(1)
					continue
				}
				if errors.Is(err, syscall.ECONNREFUSED) {
					refused.Add(1)
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
	t.Cleanup(func() {
		stopDialing()
		dialing.Wait()
	})
	waitAnswered(t, &answered, 1)

	held, err := daemon.Hold(home)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = held.Close() })
	if err := stopOld(); err != nil {
		t.Fatalf("the old daemon's drain = %v", err)
	}
	between := make(chan error, 4)
	for range cap(between) {
		go func() { between <- get() }()
	}
	select {
	case err := <-between:
		t.Fatalf("a dial between the two daemons returned %v before the new daemon started, want it to wait for the new daemon", err)
	case <-time.After(300 * time.Millisecond):
	}
	ln, err := daemon.Listen(home, addr)
	if err != nil {
		t.Fatalf("the new daemon could not take the held listener: %v", err)
	}
	serveOn(t, home, ln)
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	for range cap(between) {
		if err := <-between; err != nil {
			t.Errorf("a dial between the two daemons got %v, want the new daemon to serve it", err)
		}
	}
	waitAnswered(t, &answered, answered.Load()+20)

	stopDialing()
	dialing.Wait()
	if n := refused.Load(); n > 0 {
		t.Fatalf("%d dials through the move were refused, want none", n)
	}
}

func waitAnswered(t *testing.T, answered *atomic.Int64, want int64) {
	t.Helper()
	for deadline := time.Now().Add(20 * time.Second); answered.Load() < want; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("the daemon answered %d requests, want %d", answered.Load(), want)
		}
	}
}
