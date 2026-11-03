package inbox

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/yasyf/cc-inbox/internal/store"
)

// Hub reads every follower's filter from one goroutine and one snapshot when
// new records commit, so a follower whose filter passes nothing does no work.
// It runs only while it has followers.
type Hub struct {
	st        *store.Store
	interval  time.Duration
	kick      chan struct{}
	done      chan struct{}
	closing   sync.Once
	mu        sync.Mutex
	followers map[*Follower]struct{}
	running   bool
	tip       store.Tip
	read      bool
}

// NewHub returns a Hub that looks for commits Kick did not report every interval.
func NewHub(st *store.Store, interval time.Duration) *Hub {
	return &Hub{st: st, interval: interval, kick: make(chan struct{}, 1), done: make(chan struct{}), followers: map[*Follower]struct{}{}}
}

// Follower is one reader's place in a Hub. Rows past floor are unread; base is
// the last place it was woken from, which a changed record or a new alias sends
// the floor back to, as a poll from the last delivered record would have seen them.
type Follower struct {
	hub     *Hub
	filter  store.Filter
	base    int64
	aliases map[string]bool
	mu      sync.Mutex
	pending store.Wake
	ready   chan struct{}
}

// Follow starts reading f past floor; a changed record rewinds to f.After.
func (h *Hub) Follow(f store.Filter, floor int64) *Follower {
	follower := &Follower{hub: h, filter: f, base: f.After, ready: make(chan struct{}, 1)}
	follower.filter.After = floor
	h.mu.Lock()
	h.followers[follower] = struct{}{}
	if !h.running {
		h.running = true
		go h.run()
	}
	h.mu.Unlock()
	h.Kick()
	return follower
}

// Kick reports a commit, so followers hear it before the next interval.
func (h *Hub) Kick() {
	select {
	case h.kick <- struct{}{}:
	default:
	}
}

// Close stops the Hub and tells every reader through Done.
func (h *Hub) Close() {
	h.closing.Do(func() { close(h.done) })
}

// Done closes when the Hub stops.
func (h *Hub) Done() <-chan struct{} {
	return h.done
}

// Ready receives when Take has a Wake.
func (f *Follower) Ready() <-chan struct{} {
	return f.ready
}

// Take returns the Wake waiting, merging every wake since the last Take.
func (f *Follower) Take() store.Wake {
	f.mu.Lock()
	defer f.mu.Unlock()
	wake := f.pending
	f.pending = store.Wake{}
	return wake
}

// Leave stops following.
func (f *Follower) Leave() {
	f.hub.mu.Lock()
	delete(f.hub.followers, f)
	f.hub.mu.Unlock()
}

func (f *Follower) wake(first, head int64) {
	f.mu.Lock()
	if f.pending.Head == 0 {
		f.pending.First = first
	}
	f.pending.First = min(f.pending.First, first)
	f.pending.Head = head
	f.mu.Unlock()
	select {
	case f.ready <- struct{}{}:
	default:
	}
}

func (f *Follower) advance(m store.FirstMatch, head int64) (rewound bool) {
	seen := f.aliases
	f.aliases = make(map[string]bool, len(m.Aliases))
	for _, name := range m.Aliases {
		f.aliases[name] = true
	}
	if seen != nil && slices.ContainsFunc(m.Aliases, func(name string) bool { return !seen[name] }) {
		f.filter.After = f.base
		return true
	}
	if m.Seq != 0 {
		f.base = m.Seq - 1
		f.wake(m.Seq, head)
	}
	f.filter.After = head
	return false
}

func (h *Hub) followed() ([]*Follower, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.followers) == 0 {
		h.running = false
		return nil, false
	}
	followers := make([]*Follower, 0, len(h.followers))
	for f := range h.followers {
		followers = append(followers, f)
	}
	return followers, true
}

func (h *Hub) run() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-h.done:
			cancel()
		case <-ctx.Done():
		}
	}()
	tick := time.NewTicker(h.interval)
	defer tick.Stop()
	var (
		changes *store.ChangeVersion
		version int64
		dirty   bool
	)
	defer func() {
		if changes != nil {
			_ = changes.Close()
		}
	}()
	for {
		select {
		case <-h.done:
			h.mu.Lock()
			h.running = false
			h.mu.Unlock()
			return
		case <-h.kick:
			dirty = true
		case <-tick.C:
		}
		followers, live := h.followed()
		if !live {
			return
		}
		if changes == nil {
			var err error
			if changes, err = h.st.OpenChangeVersion(ctx); err != nil {
				slog.Warn("cci hub could not watch the store", "err", err)
				changes = nil
				continue
			}
			dirty = true
		}
		now, err := changes.Read(ctx)
		if err != nil {
			slog.Warn("cci hub could not read the store's version", "err", err)
			continue
		}
		if !dirty && now == version {
			continue
		}
		version = now
		if err := h.readFollowers(ctx, followers); err != nil {
			if ctx.Err() == nil {
				slog.Warn("cci hub could not read its followers' records", "err", err)
			}
			dirty = true
			continue
		}
		dirty = false
	}
}

func (h *Hub) readFollowers(ctx context.Context, followers []*Follower) error {
	tip, err := h.st.RecordTip(ctx)
	if err != nil {
		return err
	}
	mutated := !tip.Exact || h.read && tip.Mutations != h.tip.Mutations
	h.tip, h.read = tip, true
	var behind []*Follower
	var filters []store.Filter
	for _, f := range followers {
		if mutated {
			f.filter.After = f.base
		}
		if f.aliases == nil || f.filter.After < tip.Head {
			behind = append(behind, f)
			filters = append(filters, f.filter)
		}
	}
	if len(behind) == 0 {
		return nil
	}
	matches, err := h.st.FirstMatches(ctx, filters, tip.Head)
	if err != nil {
		return err
	}
	for i, m := range matches {
		if behind[i].advance(m, tip.Head) {
			h.Kick()
		}
	}
	return nil
}
