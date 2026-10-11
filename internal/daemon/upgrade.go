package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/yasyf/cc-inbox/internal/version"
)

const UpgradeInterval = 30 * time.Second

type Upgrader struct {
	Roots   version.Roots
	Running string
	tried   version.Install
}

func (u *Upgrader) Check() error {
	if !version.IsRelease(u.Running) {
		return nil
	}
	newest, err := u.Roots.Newest()
	if err != nil {
		return err
	}
	if newest == u.tried || !version.Newer(newest.Version, u.Running) {
		return nil
	}
	u.tried = newest
	cmd := exec.Command(newest.Path, "serve") //nolint:gosec // G204: the path is a cci install under the plugin cache or Caskroom.
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		slog.Warn("cci daemon upgrade failed to start", "from", u.Running, "to", newest.Version, "path", newest.Path, "err", err)
		return nil
	}
	slog.Info("cci daemon handing off to a newer install", "from", u.Running, "to", newest.Version, "path", newest.Path, "pid", cmd.Process.Pid)
	go func() {
		if err := cmd.Wait(); err != nil {
			slog.Warn("cci daemon upgrade failed", "from", u.Running, "to", newest.Version, "path", newest.Path, "err", err)
		}
	}()
	return nil
}

func (u *Upgrader) Follow(ctx context.Context, every time.Duration) error {
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
		if err := u.Check(); err != nil {
			return fmt.Errorf("follow cci upgrades: %w", err)
		}
	}
}
