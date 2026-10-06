package cli

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/shirou/gopsutil/v4/process"
	"github.com/spf13/cobra"
	"github.com/yasyf/cc-interact/procs"

	"github.com/yasyf/cc-inbox/internal/channel"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
)

func newSubscribeCmd() *cobra.Command {
	var (
		drive  string
		reader string
		kindsF []string
		cursor string
	)
	cmd := &cobra.Command{
		Use:   "subscribe",
		Short: "Deliver this session's records over the cci channel: records addressed to --reader plus broadcasts of each --kind",
		Args:  cobra.NoArgs,
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
			window, session, err := thisSession()
			if err != nil {
				return err
			}
			d, err := resolveDrive(cmd.Context(), st, drive)
			if err != nil {
				return err
			}
			sub := store.Subscription{Session: session, Window: window, Drive: d, Reader: reader, Cursor: cursor}
			for _, k := range kindsF {
				kind, err := kinds.Parse(k)
				if err != nil {
					return err
				}
				sub.Kinds = append(sub.Kinds, kind)
			}
			if sub.Cursor == "" {
				sub.Cursor = "channel-" + session
			}
			if _, err := st.Subscribe(cmd.Context(), sub); err != nil {
				return err
			}
			from, _, err := st.Cursor(cmd.Context(), sub.Cursor, d)
			if err != nil {
				return err
			}
			start := fmt.Sprintf("cursor %s at #%d", sub.Cursor, from)
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "window %d (session %s) subscribed to %s as %s, kinds %s, %s; records arrive as <channel source=%q> tags when this session was launched with --channels plugin:cc-inbox@cc-inbox\n",
				window.PID, session, d, reader, kindList(sub.Kinds), start, channel.Source)
			return err
		}),
	}
	cmd.Flags().StringVar(&drive, "drive", "", "drive to deliver (default: the drive bound to this session)")
	cmd.Flags().StringVar(&reader, "reader", "", "deliver records addressed to this lane, as cci tail --reader does")
	cmd.Flags().StringSliceVar(&kindsF, "kind", nil, "also deliver other lanes' broadcasts of these kinds (repeatable)")
	cmd.Flags().StringVar(&cursor, "cursor", "", "resume from and advance this cursor, used by no other reader (default: channel-<session>, starting at the current head)")
	_ = cmd.MarkFlagRequired("reader")
	return cmd
}

func newUnsubscribeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "unsubscribe",
		Short: "Stop delivering records over this session's cci channel",
		Args:  cobra.NoArgs,
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
			window, _, err := thisSession()
			if err != nil {
				return err
			}
			removed, err := st.Unsubscribe(cmd.Context(), window)
			if err != nil {
				return err
			}
			if !removed {
				_, err = fmt.Fprintf(cmd.OutOrStdout(), "window %d has no subscription\n", window.PID)
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "window %d unsubscribed\n", window.PID)
			return err
		}),
	}
}

func newChannelCmd() *cobra.Command {
	return &cobra.Command{
		Use:    "channel",
		Short:  "Run the cci channel MCP server (stdio) for this Claude Code window",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
			window, err := thisWindow()
			if err != nil {
				return err
			}
			return channel.Serve(cmd.Context(), st, window, cmd.InOrStdin(), cmd.OutOrStdout())
		}),
	}
}

var errNoSession = errors.New("run this from a Claude Code session: no claude ancestor process or " + sessionEnv)

func thisSession() (store.Window, string, error) {
	window, err := thisWindow()
	session := os.Getenv(sessionEnv)
	if err != nil || session == "" {
		return store.Window{}, "", errors.Join(errNoSession, err)
	}
	return window, session, nil
}

func thisWindow() (store.Window, error) {
	pid := procs.ClaudePID()
	if pid == 0 {
		return store.Window{}, errNoSession
	}
	p, err := process.NewProcess(int32(pid)) //nolint:gosec // G115: OS pids fit in int32.
	if err != nil {
		return store.Window{}, fmt.Errorf("open claude pid %d: %w", pid, err)
	}
	started, err := p.CreateTime()
	if err != nil {
		return store.Window{}, fmt.Errorf("start time of claude pid %d: %w", pid, err)
	}
	return store.Window{PID: pid, Started: started}, nil
}

func kindList(ks []kinds.Kind) string {
	if len(ks) == 0 {
		return "none"
	}
	out := make([]string, len(ks))
	for i, k := range ks {
		out[i] = string(k)
	}
	return strings.Join(out, ",")
}
