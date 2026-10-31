package cli

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"slices"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/yasyf/cc-inbox/internal/importer"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/node"
	"github.com/yasyf/cc-inbox/internal/relay"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/version"
)

const (
	sessionEnv      = "CLAUDE_CODE_SESSION_ID"
	codexSessionEnv = "CODEX_THREAD_ID"
	agentEnv        = "CCI_AGENT_ID"
	driveEnv        = "CLAUDE_LONG_RUNNING_DRIVE"
)

var errNoPeerSession = errors.New("run this from a Claude Code or Codex session: neither " + sessionEnv + " nor " + codexSessionEnv + " is set")

func NewRootCmd() *cobra.Command {
	root := rootCommand()
	root.AddCommand(
		newPostCmd(),
		newTailCmd(),
		newWatchCmd(),
		newDigestCmd(),
		newGrepCmd(),
		newShowCmd(),
		newStateCmd(),
		newImportCmd(),
		newCompactCmd(),
		newMaintainIndexesCmd(),
		newDriveCmd(),
		newAnnounceCmd(),
		newMeCmd(),
		newWhoCmd(),
		newClaimCmd(),
		newUnclaimCmd(),
		newServeCmd(),
		newSuperviseCmd(),
		newUpgradeCmd(),
		newDaemonCmd(),
		newHookCmd(),
		newSubscribeCmd(),
		newUnsubscribeCmd(),
		newChannelCmd(),
		newNodeCmd(),
		newCursorCmd(),
		newMeshCmd(),
		newRelayCmd(),
		newPairCmd(),
		newUnpairCmd(),
		newPeersCmd(),
	)
	return root
}

func rootCommand() *cobra.Command {
	root := &cobra.Command{
		Use:           "cci",
		Short:         "Typed, bounded coordination records for long-running agent drives",
		Version:       version.String(),
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.SetVersionTemplate("{{.Version}}\n")
	return root
}

var number = regexp.MustCompile(`^#?[0-9]+(@[0-9a-f]+)?$`)

func openStore(ctx context.Context) (*store.Store, error) {
	home, err := store.Home()
	if err != nil {
		return nil, err
	}
	st, err := store.Open(ctx, home)
	if err != nil {
		return nil, err
	}
	if err := configureStoreOwner(st, home); err != nil {
		_ = st.Close()
		return nil, err
	}
	return st, nil
}

func configureStoreOwner(st *store.Store, home string) error {
	configs, err := relay.Configs(home)
	if err != nil {
		return err
	}
	st.SetOwner(!slices.ContainsFunc(configs, func(c relay.Config) bool { return c.Tier != string(node.TierOwner) }))
	return nil
}

func resolveDrive(ctx context.Context, st *store.Store, drive string) (string, error) {
	if drive != "" {
		return drive, nil
	}
	if self := selfPeer(); self.Session != "" {
		b, ok, err := st.Binding(ctx, self)
		if err == nil && !ok {
			_, b, ok, err = st.Owner(ctx, self)
		}
		if err != nil {
			return "", err
		}
		if ok {
			return b.Drive, nil
		}
	}
	if d := os.Getenv(driveEnv); d != "" {
		return d, nil
	}
	return "", store.ErrNoDrive
}

type readFlags struct {
	drive        string
	origin       string
	kinds        []string
	mute         []string
	toKinds      []string
	lanes        []string
	to           string
	reader       string
	topics       []string
	stack        string
	target       string
	pr           store.PR
	since        string
	sinceSeq     int64
	sinceSeqFlag *pflag.Flag
	limit        int
	budget       int
	width        int
	json         bool
}

func (f *readFlags) register(cmd *cobra.Command, since string, filters bool, budget int) {
	cmd.Flags().StringVar(&f.drive, "drive", "", "drive to read (default: the drive bound to this session)")
	if filters {
		cmd.Flags().StringSliceVar(&f.kinds, "kind", nil, "only these kinds (repeatable)")
		cmd.Flags().StringSliceVar(&f.mute, "mute-kinds", nil, "never these kinds, even addressed to --reader (repeatable)")
		cmd.Flags().StringSliceVar(&f.toKinds, "to-kind", nil, "only these kinds among records addressed to --reader or --to; broadcasts still follow --kind (repeatable)")
		cmd.Flags().StringSliceVar(&f.lanes, "lane", nil, "only these lanes (repeatable)")
		cmd.Flags().StringVar(&f.to, "to", "", "only records addressed to this lane")
		cmd.Flags().StringVar(&f.reader, "reader", "", "deliver to this lane: records addressed to it, plus other lanes' broadcasts that match --kind, --lane and --topic")
		cmd.Flags().StringSliceVar(&f.topics, "topic", nil, "only these topics (repeatable)")
		cmd.Flags().StringVar(&f.stack, "stack", "", "only records about this <project>/<env> stack")
		cmd.Flags().StringVar(&f.target, "target", "", "only records about this release target")
		cmd.Flags().Var((*prFlag)(&f.pr), "pr", "only records about this pull request: a number, owner/repo#N, or URL")
		cmd.Flags().StringVar(&f.origin, "origin", "", "only records written on this node (cci node prints its id)")
	}
	cmd.Flags().StringVar(&f.since, "since", since, "a seq (#123 or 123), a duration (2h), a Pacific clock time (9:00 AM, 21:30, 12:0x), or an RFC3339 time")
	cmd.Flags().Int64Var(&f.sinceSeq, "since-seq", 0, "read after this internal store sequence")
	f.sinceSeqFlag = cmd.Flags().Lookup("since-seq")
	_ = cmd.Flags().MarkHidden("since-seq")
	cmd.MarkFlagsMutuallyExclusive("since", "since-seq")
	cmd.Flags().IntVar(&f.budget, "budget", budget, "output budget in bytes")
	cmd.Flags().BoolVar(&f.json, "json", false, "one JSON record per line")
}

func (f *readFlags) registerWidth(cmd *cobra.Command) {
	cmd.Flags().IntVar(&f.width, "width", 0, "clip each text record line to this many characters (default: whole records)")
}

func (f *readFlags) registerLimit(cmd *cobra.Command, usage string) {
	cmd.Flags().IntVarP(&f.limit, "limit", "n", 0, usage)
}

func (f *readFlags) filter(ctx context.Context, st *store.Store) (store.Filter, error) {
	return f.filterPeer(ctx, st, selfPeer())
}

func (f *readFlags) filterPeer(ctx context.Context, st *store.Store, peer store.PeerKey) (store.Filter, error) {
	drive, err := resolveDrive(ctx, st, f.drive)
	if err != nil {
		return store.Filter{}, err
	}
	out := store.Filter{Drive: drive, Origin: f.origin, Lanes: f.lanes, To: f.to, For: f.reader, Topics: f.topics, Stack: f.stack, Target: f.target, PR: f.pr}
	if f.to == "" {
		out.Peer = peer.String()
	}
	if out.Kinds, err = kinds.ParseAll(f.kinds); err != nil {
		return store.Filter{}, err
	}
	if out.Mute, err = kinds.ParseAll(f.mute); err != nil {
		return store.Filter{}, err
	}
	if out.ToKinds, err = kinds.ParseAll(f.toKinds); err != nil {
		return store.Filter{}, err
	}
	if f.sinceSeqFlag.Changed {
		if f.sinceSeq < 0 {
			return store.Filter{}, errors.New("--since-seq must be nonnegative")
		}
		out.After = f.sinceSeq
	} else {
		if out.After, out.Since, err = parseSince(ctx, st, f.since); err != nil {
			return store.Filter{}, err
		}
	}
	return out, nil
}

func parseSince(ctx context.Context, st *store.Store, s string) (int64, time.Time, error) {
	if s == "" {
		return 0, time.Time{}, nil
	}
	now := st.Now()
	if number.MatchString(s) {
		seq, err := st.Resolve(ctx, s)
		return seq, time.Time{}, err
	}
	if d, err := time.ParseDuration(s); err == nil {
		return 0, now.Add(-d), nil
	}
	if c := importer.ParseClock(s); c != nil {
		return 0, c.Latest(now), nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return 0, t, nil
	}
	return 0, time.Time{}, fmt.Errorf("--since %q is not a seq, a duration, a clock time (9:00 AM, 21:30), or an RFC3339 time", s)
}

func selfPeer() store.PeerKey {
	session := cmp.Or(os.Getenv(sessionEnv), os.Getenv(codexSessionEnv))
	if session == "" {
		return store.PeerKey{}
	}
	return store.PeerKey{Session: session, Agent: os.Getenv(agentEnv)}
}

func withStore(fn func(cmd *cobra.Command, st *store.Store, args []string) error) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		st, err := openStore(cmd.Context())
		if err != nil {
			return err
		}
		defer func() { _ = st.Close() }()
		return fn(cmd, st, args)
	}
}
