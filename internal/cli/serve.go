package cli

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/daemonkit"

	"github.com/yasyf/cc-inbox/internal/daemon"
	"github.com/yasyf/cc-inbox/internal/hook"
	"github.com/yasyf/cc-inbox/internal/store"
)

func newServeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Start or reuse the cci daemon, which serves read-only JSON over HTTP from a hot store",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			d, err := daemon.Definition()
			if err != nil {
				return err
			}
			client, err := daemonkit.Open(d)
			if err != nil {
				return fmt.Errorf("open cci daemon: %w", err)
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 60*time.Second)
			defer cancel()
			ensured, err := client.Ensure(ctx)
			if err != nil {
				return fmt.Errorf("ensure cci daemon: %w", err)
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "cci daemon pid %d serving http://%s/v1\n", ensured.After.PID, daemon.Addr)
			return err
		},
	}
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
			_, err = daemonkit.Serve(cmd.Context(), d, daemon.Start(home, ln, daemon.ImportEvery))
			return err
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
				return hook.SessionStart(cmd.Context(), st, p, cmd.OutOrStdout())
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
		&cobra.Command{
			Use:  "post-tool",
			Args: cobra.NoArgs,
			RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
				return hook.PostTool(cmd.Context(), st)
			}),
		},
	)
	return cmd
}
