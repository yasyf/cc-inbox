package testutil

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/store"
)

type Clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func Store(t *testing.T) (*store.Store, *Clock) {
	t.Helper()
	st, err := store.Open(context.Background(), t.TempDir())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	c := &Clock{now: time.Date(2026, 10, 4, 22, 0, 0, 0, time.UTC)}
	st.Now = c.Now
	return st, c
}

func Post(t *testing.T, st *store.Store, r store.Record) store.Record {
	t.Helper()
	if r.Drive == "" {
		r.Drive = "d"
	}
	if r.Lane == "" {
		r.Lane = "lane-a"
	}
	got, _, err := st.Post(context.Background(), r, 0)
	if err != nil {
		t.Fatalf("Post(%+v) error = %v", r, err)
	}
	return got
}
