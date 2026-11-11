package cli

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/shirou/gopsutil/v4/process"
	"github.com/spf13/cobra"

	"github.com/yasyf/cc-inbox/internal/channel"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
)

func newSubscribeCmd() *cobra.Command {
	var (
		drive  string
		reader string
		kindsF []string
		muteF  []string
		toF    []string
		batchF []string
		topicF []string
		follow []string
		cursor string
	)
	cmd := &cobra.Command{
		Use:   "subscribe",
		Short: "Deliver this session's records over the cci channel: records addressed to --reader of each --to-kind, plus broadcasts of each --kind",
		Long: `Deliver this session's records over the cci channel.

A record addressed to --reader arrives when its kind is a --to-kind; other
lanes' broadcasts arrive when their kind is a --kind. For reader root (or main)
--to-kind defaults to ` + kindList(kinds.RootAddressed()) + `, the kinds a
drive's root acts on; for any other reader every addressed kind arrives.
Records the channel holds back still show in cci tail, cci digest and the stop
hook's overdue-ask nudge.

A record addressed to --reader of a --batch-kinds kind wakes nobody on arrival:
the channel carries those together in one tag, at most every five minutes and
at once ahead of any other record, and the PostToolUse hook leaves them to it.
A tag keeps the newest record per lane and topic, and per pull request for
head, within about 3 KB, and counts the rest in a last line naming the cci tail
that reads them. For reader root (or main) in a window launched with channels,
--batch-kinds defaults to done.

A record of a --topic-kinds kind arrives whoever it is addressed to when its
topic is one a record addressed to --reader has carried: being addressed on a
topic is how a reader comes to follow its results. It only widens what
arrives; it claims, holds and reassigns nothing.`,
		Args: cobra.NoArgs,
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
			window, session, err := thisSession(cmd.Context())
			if err != nil {
				return err
			}
			d, err := resolveDrive(cmd.Context(), st, drive)
			if err != nil {
				return err
			}
			sub := store.Subscription{Session: session, Window: window, Drive: d, Reader: reader, Cursor: cursor}
			if sub.Kinds, err = kinds.ParseAll(kindsF); err != nil {
				return err
			}
			if sub.Mute, err = kinds.ParseAll(muteF); err != nil {
				return err
			}
			if sub.ToKinds, err = kinds.ParseAll(toF); err != nil {
				return err
			}
			if len(sub.ToKinds) == 0 {
				sub.ToKinds = store.DefaultToKinds(reader)
			}
			if sub.Batch, err = kinds.ParseAll(batchF); err != nil {
				return err
			}
			if sub.TopicKinds, err = kinds.ParseAll(topicF); err != nil {
				return err
			}
			channeled := channelsOn(window.PID)
			if len(sub.Batch) > 0 && !channeled {
				return errors.New("--batch-kinds needs this window launched with --channels plugin:cc-inbox@cc-inbox: only the channel delivers those kinds")
			}
			if len(sub.Batch) == 0 && channeled {
				sub.Batch = store.DefaultBatch(reader)
			}
			for _, spec := range follow {
				other, list, ok := strings.Cut(spec, "=")
				if !ok || other == "" || list == "" {
					return fmt.Errorf("--follow %q is not <drive>=<kind>[,<kind>...], e.g. oncall=incident", spec)
				}
				ks, err := kinds.ParseAll(strings.Split(list, ","))
				if err != nil {
					return err
				}
				sub.Follow = append(sub.Follow, store.Follow{Drive: other, Kinds: ks})
			}
			if sub.Cursor == "" {
				sub.Cursor = "channel-" + session
			}
			self := store.PeerKey{Session: session}
			moves, err := st.MovesLane(cmd.Context(), sub)
			if err != nil {
				return err
			}
			if moves {
				if err := refuseHeldLane(cmd.Context(), st, self, window, d, reader, st.Now()); err != nil {
					return err
				}
			}
			if _, err := st.Subscribe(cmd.Context(), sub); err != nil {
				return err
			}
			if moves {
				if err := st.ClaimLane(cmd.Context(), self, d, reader, time.UnixMilli(window.Started)); err != nil {
					return err
				}
			}
			from, _, err := st.Cursor(cmd.Context(), sub.Cursor, d)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "window %d (session %s) subscribed: %s, cursor %s at #%d; records arrive as <channel source=%q> tags when this session was launched with --channels plugin:cc-inbox@cc-inbox\n",
				window.PID, session, describe(sub), sub.Cursor, from, channel.Source)
			return err
		}),
	}
	cmd.Flags().StringVar(&drive, "drive", "", "drive to deliver (default: the drive bound to this session)")
	cmd.Flags().StringVar(&reader, "reader", "", "deliver records addressed to this lane, as cci tail --reader does")
	cmd.Flags().StringSliceVar(&kindsF, "kind", nil, "also deliver other lanes' broadcasts of these kinds (repeatable)")
	cmd.Flags().StringSliceVar(&muteF, "mute-kinds", nil, "never deliver these kinds, even addressed to --reader; cci tail still reads them (repeatable)")
	cmd.Flags().StringSliceVar(&toF, "to-kind", nil, "deliver records addressed to --reader only of these kinds; cci tail still reads the rest (repeatable; default for root: "+kindList(kinds.RootAddressed())+", otherwise every kind)")
	cmd.Flags().StringSliceVar(&batchF, "batch-kinds", nil, "deliver records addressed to --reader of these kinds together, one tag at most every five minutes, never on arrival (repeatable; default for root: "+kindList(store.DefaultBatch("root"))+", otherwise none)")
	cmd.Flags().StringSliceVar(&topicF, "topic-kinds", nil, "also deliver records of these kinds, whoever they are addressed to, on any topic a record addressed to --reader has carried (repeatable)")
	cmd.Flags().StringArrayVar(&follow, "follow", nil, "also deliver another drive's broadcasts of these kinds, as <drive>=<kind>[,<kind>...], e.g. oncall=incident (repeatable)")
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
			window, _, err := thisSession(cmd.Context())
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
	var resumed bool
	cmd := &cobra.Command{
		Use:    "channel",
		Short:  "Run the cci channel MCP server (stdio) for this Claude Code window",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
			window, err := thisWindow(cmd.Context())
			if err != nil {
				return err
			}
			proc, err := windowOf(callerPID(cmd.Context()))
			if err != nil {
				return err
			}
			return channel.Serve(cmd.Context(), st, window, proc, channelsOn(window.PID), resumed, cmd.InOrStdin(), cmd.OutOrStdout(), channelTools()...)
		}),
	}
	cmd.Flags().BoolVar(&resumed, channel.Resumed, false, "stream at once: the cci this replaced already finished the MCP handshake")
	return cmd
}

var (
	errNoSession    = errors.New("run this from a Claude Code session: no claude ancestor process or " + sessionEnv)
	errAgentChannel = errors.New("a subagent or teammate cannot subscribe or unsubscribe (" + agentEnv + " is set): the channel belongs to the Claude Code window and delivers only to its main thread, so this would replace the root's subscription; the PostToolUse hook already delivers records addressed to your lane, and cci tail --reader <lane> reads them")
)

func thisSession(ctx context.Context) (store.Window, string, error) {
	if getenv(ctx, agentEnv) != "" {
		return store.Window{}, "", errAgentChannel
	}
	window, err := thisWindow(ctx)
	session := getenv(ctx, sessionEnv)
	if err != nil || session == "" {
		return store.Window{}, "", errors.Join(errNoSession, err)
	}
	return window, session, nil
}

func thisWindow(ctx context.Context) (store.Window, error) {
	pid := claudePID(ctx)
	if pid == 0 {
		return store.Window{}, errNoSession
	}
	return windowOf(pid)
}

func windowOf(pid int) (store.Window, error) {
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

func channelsOn(pid int) bool {
	p, err := process.NewProcess(int32(pid)) //nolint:gosec // G115: OS pids fit in int32.
	if err != nil {
		slog.Warn("cci channel: read claude's arguments", "pid", pid, "err", err)
		return false
	}
	args, err := p.CmdlineSlice()
	if err != nil {
		slog.Warn("cci channel: read claude's arguments", "pid", pid, "err", err)
		return false
	}
	return slices.Contains(channelArgs(args), "plugin:cc-inbox@")
}

func channelArgs(args []string) []string {
	var out []string
	listing := false
	for _, a := range args {
		flag, value, inline := strings.Cut(a, "=")
		switch {
		case flag == "--channels" || flag == "--dangerously-load-development-channels":
			listing = !inline
			if inline {
				out = append(out, strings.Split(value, ",")...)
			}
		case strings.HasPrefix(a, "-"):
			listing = false
		case listing:
			out = append(out, a)
		}
	}
	for i, c := range out {
		if name, _, ok := strings.Cut(c, "@"); ok {
			out[i] = name + "@"
		}
	}
	return out
}

func describe(sub store.Subscription) string {
	followed := make([]string, len(sub.Follow))
	for i, f := range sub.Follow {
		followed[i] = f.Drive + "=" + kindList(f.Kinds)
	}
	return fmt.Sprintf("drive %s as %s, addressed kinds %s, kinds %s, muted %s, batched %s, topic kinds %s, followed %s, urgent repeats held %s", sub.Drive, sub.Reader, addressedList(sub.ToKinds), kindList(sub.Kinds), kindList(sub.Mute), kindList(sub.Batch), kindList(sub.TopicKinds), cmp.Or(strings.Join(followed, " "), "none"), store.UrgentRepeat)
}

func addressedList(ks []kinds.Kind) string {
	if len(ks) == 0 {
		return "all"
	}
	return kindList(ks)
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
