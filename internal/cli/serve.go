package cli

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/daemonkit"

	"github.com/yasyf/cc-inbox/internal/daemon"
	"github.com/yasyf/cc-inbox/internal/hook"
	"github.com/yasyf/cc-inbox/internal/peers"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/version"
)

func newServeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Start or reuse the cci daemon, which serves read-only JSON over HTTP from a hot store",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ensured, err := ensureDaemon(cmd.Context(), 60*time.Second)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "cci daemon pid %d serving http://%s/v1\n", ensured.After.PID, daemon.Addr)
			return err
		},
	}
}

func newUpgradeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "upgrade",
		Short: "Upgrade every cci install on this machine, then move the daemon onto the newest one",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			roots, err := version.DefaultRoots()
			if err != nil {
				return err
			}
			lines, err := roots.UpgradeCommands(os.Getenv("PATH"))
			if err != nil {
				return err
			}
			for _, line := range lines {
				if err := runShown(cmd, "/bin/sh", "-c", line); err != nil {
					return err
				}
			}
			newest, err := roots.Newest()
			if err != nil {
				return err
			}
			for _, path := range version.Standalone(os.Getenv("PATH")) {
				out, err := exec.CommandContext(cmd.Context(), path, "--version").Output() //nolint:gosec // G204: a standalone cci found on PATH.
				if err != nil {
					return fmt.Errorf("%s --version: %w", path, err)
				}
				if v := strings.TrimSpace(string(out)); version.Newer(v, newest.Version) {
					newest = version.Install{Path: path, Version: v}
				}
			}
			if newest.Path == "" {
				return errors.New("no cci install found to serve from")
			}
			return runShown(cmd, newest.Path, "serve")
		},
	}
}

func runShown(cmd *cobra.Command, name string, args ...string) error {
	line := strings.Join(append([]string{name}, args...), " ")
	if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "$ %s\n", line); err != nil {
		return err
	}
	c := exec.CommandContext(cmd.Context(), name, args...) //nolint:gosec // G204: an install path cci found or an upgrade command it composed.
	c.Stdout = cmd.OutOrStdout()
	c.Stderr = cmd.ErrOrStderr()
	if err := c.Run(); err != nil {
		return fmt.Errorf("%s: %w", line, err)
	}
	return nil
}

func ensureDaemon(ctx context.Context, timeout time.Duration) (daemonkit.Ensured, error) {
	d, err := daemon.Definition()
	if err != nil {
		return daemonkit.Ensured{}, err
	}
	client, err := daemonkit.Open(d)
	if err != nil {
		return daemonkit.Ensured{}, fmt.Errorf("open cci daemon: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ensured, err := client.Ensure(ctx)
	if err != nil {
		return daemonkit.Ensured{}, fmt.Errorf("ensure cci daemon: %w", err)
	}
	return ensured, nil
}

func ensureNewestDaemon(ctx context.Context) error {
	roots, err := version.DefaultRoots()
	if err != nil {
		return err
	}
	newest, err := roots.Newest()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if version.IsRelease(version.String()) && version.Newer(newest.Version, version.String()) {
		return exec.CommandContext(ctx, newest.Path, "serve").Run() //nolint:gosec // G204: a cci install under the plugin cache or Caskroom.
	}
	_, err = ensureDaemon(ctx, 10*time.Second)
	return err
}

func newDaemonCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "daemon", Short: "The daemonkit-managed cci daemon", Hidden: true}
	cmd.AddCommand(&cobra.Command{
		Use:  "run",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			d, err := daemon.Definition()
			if err != nil {
				return err
			}
			home, err := store.Home()
			if err != nil {
				return err
			}
			ln, err := net.Listen("tcp", daemon.Addr)
			if err != nil {
				return fmt.Errorf("listen %s: %w", daemon.Addr, err)
			}
			rt := daemon.New(home, ln, func(st *store.Store) (http.Handler, error) {
				return TailHandler(st, home)
			})
			_, err = daemonkit.Serve(cmd.Context(), d, rt.Start)
			return cmp.Or(err, rt.Err())
		},
	})
	return cmd
}

func newHookCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "hook", Short: "Claude Code hook entry points (read the hook payload on stdin)", Hidden: true}
	cmd.AddCommand(
		&cobra.Command{
			Use:  "session-start",
			Args: cobra.NoArgs,
			RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
				p, err := hook.Read(cmd.InOrStdin())
				if err != nil {
					return err
				}
				window, windowErr := thisWindow()
				if err := hook.SessionStart(cmd.Context(), st, p, window, cmd.OutOrStdout()); err != nil {
					return err
				}
				return errors.Join(windowErr, ensureNewestDaemon(cmd.Context()))
			}),
		},
		&cobra.Command{
			Use:  "announce",
			Args: cobra.NoArgs,
			RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
				p, err := hook.Read(cmd.InOrStdin())
				if err != nil {
					return err
				}
				window, err := thisWindow()
				if err != nil {
					return err
				}
				_, err = peers.Announce(cmd.Context(), st, peers.Options{Session: p.SessionID, Window: window, Cwd: p.Cwd})
				return err
			}),
		},
		&cobra.Command{
			Use:  "post-tool-use",
			Args: cobra.NoArgs,
			RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
				p, err := hook.Read(cmd.InOrStdin())
				if err != nil {
					return err
				}
				return hook.PostToolUse(cmd.Context(), st, p, thisWindow, cmd.OutOrStdout())
			}),
		},
		&cobra.Command{
			Use:  "stop",
			Args: cobra.NoArgs,
			RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
				p, err := hook.Read(cmd.InOrStdin())
				if err != nil {
					return err
				}
				return hook.Stop(cmd.Context(), st, p, cmd.OutOrStdout())
			}),
		},
		&cobra.Command{
			Use:  "subagent-stop",
			Args: cobra.NoArgs,
			RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
				p, err := hook.Read(cmd.InOrStdin())
				if err != nil {
					return err
				}
				return hook.SubagentStop(cmd.Context(), st, p)
			}),
		},
		&cobra.Command{
			Use:  "prompt",
			Args: cobra.NoArgs,
			RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
				p, err := hook.Read(cmd.InOrStdin())
				if err != nil {
					return err
				}
				return hook.Prompt(cmd.Context(), st, p)
			}),
		},
	)
	return cmd
}
