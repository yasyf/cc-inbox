package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/yasyf/daemonkit"

	"github.com/yasyf/cc-inbox/internal/store"
)

// OpWrite is the business op that commits one client's store.Write in the
// daemon's writer.
const OpWrite = "write"

const (
	codeUnrouted = "unrouted"
	codeExpired  = "expired"
	codeBusy     = "busy"

	queueDepth  = 256
	batchLimit  = 64
	commitGrace = 2 * time.Second
)

// WriteRequest is one client's Write for the store at Home, written as the
// node Origin names.
type WriteRequest struct {
	Home   string      `json:"home"`
	Origin string      `json:"origin"`
	Write  store.Write `json:"write"`
}

type job struct {
	ctx  context.Context
	w    store.Write
	done chan<- committed
}

type committed struct {
	written store.Written
	err     error
}

type queue struct {
	st      *store.Store
	jobs    chan job
	mu      sync.RWMutex
	closed  bool
	stopped chan struct{}
}

func newQueue(st *store.Store) *queue {
	q := &queue{st: st, jobs: make(chan job, queueDepth), stopped: make(chan struct{})}
	go q.run()
	return q
}

func (q *queue) Commit(ctx context.Context, w store.Write) (store.Written, error) {
	done := make(chan committed, 1)
	q.mu.RLock()
	if q.closed {
		q.mu.RUnlock()
		return store.Written{}, store.ErrUnrouted
	}
	select {
	case q.jobs <- job{ctx: ctx, w: w, done: done}:
		q.mu.RUnlock()
	case <-ctx.Done():
		q.mu.RUnlock()
		return store.Written{}, fmt.Errorf("%w: the writer queue stayed full: %w", store.ErrBusy, ctx.Err())
	}
	select {
	case c := <-done:
		return c.written, c.err
	case <-ctx.Done():
		return store.Written{}, fmt.Errorf("%w: %w", store.ErrUnconfirmed, ctx.Err())
	}
}

func (q *queue) run() {
	defer close(q.stopped)
	for first := range q.jobs {
		batch := []job{first}
	gather:
		for len(batch) < batchLimit {
			select {
			case j, ok := <-q.jobs:
				if !ok {
					break gather
				}
				batch = append(batch, j)
			default:
				break gather
			}
		}
		q.commit(batch)
	}
}

func (q *queue) commit(batch []job) {
	var (
		live   []job
		writes []store.Write
	)
	for _, j := range batch {
		if err := j.ctx.Err(); err != nil {
			j.done <- committed{err: fmt.Errorf("%w: %w", store.ErrExpired, err)}
			continue
		}
		live = append(live, j)
		writes = append(writes, j.w)
	}
	if len(live) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), store.ClientBudget)
	defer cancel()
	written, errs := q.st.CommitAll(ctx, writes)
	for i, j := range live {
		j.done <- committed{written: written[i], err: errs[i]}
	}
}

func (q *queue) close(ctx context.Context) error {
	q.mu.Lock()
	if !q.closed {
		q.closed = true
		close(q.jobs)
	}
	q.mu.Unlock()
	select {
	case <-q.stopped:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("drain writes: %w", ctx.Err())
	}
}

func (p *Product) write(ctx context.Context, body []byte) (daemonkit.Reply, error) {
	var in WriteRequest
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil {
		return daemonkit.Reply{}, &daemonkit.ProductError{Code: codeUnrouted, Message: fmt.Sprintf("decode write: %v", err)}
	}
	if filepath.Clean(in.Home) != filepath.Clean(p.home) || in.Origin != p.st.Self() {
		return daemonkit.Reply{}, &daemonkit.ProductError{Code: codeUnrouted, Message: fmt.Sprintf("this daemon writes %s as %s", p.home, p.st.Self())}
	}
	written, err := p.writes.Commit(ctx, in.Write)
	switch {
	case errors.Is(err, store.ErrUnrouted):
		return daemonkit.Reply{}, &daemonkit.ProductError{Code: codeUnrouted, Message: err.Error()}
	case errors.Is(err, store.ErrExpired):
		return daemonkit.Reply{}, &daemonkit.ProductError{Code: codeExpired, Message: err.Error()}
	case errors.Is(err, store.ErrUnconfirmed):
		return daemonkit.Reply{}, err
	case store.Busy(err):
		return daemonkit.Reply{}, &daemonkit.ProductError{Code: codeBusy, Message: err.Error()}
	case err != nil:
		return daemonkit.Reply{}, err
	}
	reply, err := json.Marshal(written)
	if err != nil {
		return daemonkit.Reply{}, fmt.Errorf("encode write: %w", err)
	}
	return daemonkit.Reply{Body: reply}, nil
}

// Writes routes a store's writes to the daemon d runs for home, written as
// the node origin names. A daemon that is absent, draining, too old to know
// OpWrite, or serving another store answers store.ErrUnrouted, so the store
// applies the write itself.
func Writes(d daemonkit.Daemon, home, origin string) store.Writer {
	return &remote{daemon: d, home: home, origin: origin}
}

type remote struct {
	daemon       daemonkit.Daemon
	home, origin string
	once         sync.Once
	business     *daemonkit.Business
	opened       error
}

func (r *remote) Commit(ctx context.Context, w store.Write) (store.Written, error) {
	r.once.Do(func() {
		var client *daemonkit.Client
		if client, r.opened = daemonkit.Open(r.daemon); r.opened == nil {
			r.business = client.Business()
		}
	})
	if r.opened != nil {
		return store.Written{}, fmt.Errorf("%w: %w", store.ErrUnrouted, r.opened)
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, store.ClientBudget)
		defer cancel()
	}
	deadline, _ := ctx.Deadline()
	w.StartBy = deadline.Add(-min(time.Until(deadline)/3, commitGrace))
	body, err := json.Marshal(WriteRequest{Home: r.home, Origin: r.origin, Write: w})
	if err != nil {
		return store.Written{}, fmt.Errorf("encode write: %w", err)
	}
	reply, err := r.business.Call(ctx, OpWrite, body)
	if err != nil {
		return store.Written{}, refusal(err)
	}
	var written store.Written
	if err := json.Unmarshal(reply.Body, &written); err != nil {
		return store.Written{}, fmt.Errorf("%w: decode the daemon's reply: %w", store.ErrUnconfirmed, err)
	}
	return written, nil
}

func refusal(err error) error {
	if daemonkit.Undispatched(err) {
		return fmt.Errorf("%w: %w", store.ErrUnrouted, err)
	}
	var refused *daemonkit.ProductError
	if !errors.As(err, &refused) {
		return fmt.Errorf("%w: %w", store.ErrUnconfirmed, err)
	}
	switch {
	case refused.Code == codeUnrouted, refused.Code == "" && refused.Message == fmt.Sprintf("unknown op %q", OpWrite):
		return fmt.Errorf("%w: %s", store.ErrUnrouted, refused.Message)
	case refused.Code == codeExpired:
		return store.ErrExpired
	case refused.Code == codeBusy:
		return fmt.Errorf("%w: %s", store.ErrBusy, refused.Message)
	}
	return errors.New(refused.Message)
}
