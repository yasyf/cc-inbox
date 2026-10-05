package cli

import (
	"fmt"
	"net"
	"time"

	"github.com/spf13/cobra"

	"github.com/yasyf/cc-inbox/internal/hook"
	"github.com/yasyf/cc-inbox/internal/server"
	"github.com/yasyf/cc-inbox/internal/store"
)

func newServeCmd() *cobra.Command {
	var (
		addr     string
		interval time.Duration
	)
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Serve read-only JSON over HTTP for dashboards",
		Args:  cobra.NoArgs,
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
			ln, err := net.Listen("tcp", addr)
			if err != nil {
				return fmt.Errorf("listen %s: %w", addr, err)
			}
			if _, err := fmt.Fprintf(cmd.OutOrStdout(), "cci serving http://%s/v1\n", ln.Addr()); err != nil {
				return err
			}
			return server.New(st, interval).Serve(cmd.Context(), ln)
		}),
	}
	cmd.Flags().StringVar(&addr, "addr", "127.0.0.1:7377", "listen address")
	cmd.Flags().DurationVar(&interval, "interval", time.Second, "poll interval for /v1/stream")
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
