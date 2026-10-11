package relay

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/yasyf/cc-inbox/e2e"
	"github.com/yasyf/cc-inbox/internal/store"
)

const (
	PushInterval   = time.Second
	PullWait       = 20 * time.Second
	ConfigInterval = 5 * time.Second
	maxBackoff     = time.Minute
)

type Loop struct {
	home      string
	st        *store.Store
	id        e2e.Identity
	onControl ControlHandler
}

func NewLoop(home string, st *store.Store, id e2e.Identity) *Loop {
	return &Loop{home: home, st: st, id: id}
}

func (l *Loop) OnControl(h ControlHandler) { l.onControl = h }

func (l *Loop) Run(ctx context.Context) {
	var wg sync.WaitGroup
	defer wg.Wait()
	running := map[string]context.CancelFunc{}
	defer func() {
		for _, stop := range running {
			stop()
		}
	}()
	tick := time.NewTicker(ConfigInterval)
	defer tick.Stop()
	for {
		configs, err := Configs(l.home)
		if err != nil {
			slog.Warn("relay configs unreadable", "err", err)
		}
		live := map[string]bool{}
		for _, c := range configs {
			live[c.Mesh] = true
			if _, ok := running[c.Mesh]; ok {
				continue
			}
			mctx, stop := context.WithCancel(ctx)
			running[c.Mesh] = stop
			wg.Add(2)
			go func() { defer wg.Done(); l.every(mctx, c.Mesh, "push", l.push) }()
			go func() { defer wg.Done(); l.every(mctx, c.Mesh, "pull", l.pull) }()
		}
		for mesh, stop := range running {
			if !live[mesh] {
				stop()
				delete(running, mesh)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (l *Loop) node(mesh string) (*Node, []Config, bool, error) {
	configs, err := Configs(l.home)
	if err != nil {
		return nil, nil, false, err
	}
	for _, c := range configs {
		if c.Mesh == mesh {
			n, err := Open(l.home, l.st, l.id, c)
			return n, configs, err == nil, err
		}
	}
	return nil, nil, false, nil
}

func (l *Loop) push(ctx context.Context, n *Node, configs []Config) error {
	_, err := n.Push(ctx, configs)
	if err == nil {
		return sleep(ctx, PushInterval)
	}
	return err
}

func (l *Loop) pull(ctx context.Context, n *Node, _ []Config) error {
	_, err := n.Pull(ctx, PullWait, l.onControl)
	return err
}

func (l *Loop) every(ctx context.Context, mesh, what string, step func(context.Context, *Node, []Config) error) {
	backoff := time.Second
	for ctx.Err() == nil {
		n, configs, ok, err := l.node(mesh)
		if err == nil && !ok {
			return
		}
		if err == nil {
			err = step(ctx, n, configs)
		}
		if err == nil || ctx.Err() != nil {
			backoff = time.Second
			continue
		}
		slog.Warn("relay "+what+" failed", "mesh", mesh, "err", err, "retry", backoff)
		if sleep(ctx, backoff) != nil {
			return
		}
		backoff = min(2*backoff, maxBackoff)
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
