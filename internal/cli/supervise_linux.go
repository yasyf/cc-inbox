package cli

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/daemonkit/supervise"

	"github.com/yasyf/cc-inbox/internal/daemon"
)

func runSupervisor(ctx context.Context) error {
	for wait := time.Second; ; wait = min(2*wait, 30*time.Second) {
		err := superviseOnce(ctx)
		if !errors.Is(err, supervise.ErrBusy) {
			return err
		}
		slog.Info("another cci supervisor holds the label; waiting for it", "retry", wait)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
	}
}

func superviseOnce(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	supervised := make(chan error, 1)
	go func() { supervised <- daemonkit.Supervise(ctx, daemon.Label) }()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case err := <-supervised:
			return err
		case <-tick.C:
		}
		_, err := ensureDaemon(ctx, time.Minute)
		if errors.Is(err, supervise.ErrNoSupervisor) {
			continue
		}
		if err != nil && ctx.Err() == nil {
			cancel()
			return errors.Join(err, <-supervised)
		}
		return <-supervised
	}
}
