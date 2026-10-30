package cli

import (
	"context"
	"errors"
	"time"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/daemonkit/supervise"

	"github.com/yasyf/cc-inbox/internal/daemon"
)

func runSupervisor(ctx context.Context) error {
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
		if err != nil {
			cancel()
			return errors.Join(err, <-supervised)
		}
		return <-supervised
	}
}
