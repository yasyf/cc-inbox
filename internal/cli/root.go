package cli

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/version"
)

const sessionEnv = "CLAUDE_CODE_SESSION_ID"

func NewRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "cci",
		Short:         "Typed, bounded coordination records for long-running agent drives",
		Version:       version.String(),
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.SetVersionTemplate("{{.Version}}\n")
	root.AddCommand(
		newPostCmd(),
		newTailCmd(),
		newWatchCmd(),
		newDigestCmd(),
		newGrepCmd(),
		newStateCmd(),
		newImportCmd(),
		newCompactCmd(),
		newDriveCmd(),
		newServeCmd(),
		newDaemonCmd(),
		newHookCmd(),
	)
	return root
}

func openStore(ctx context.Context) (*store.Store, error) {
	home, err := store.Home()
	if err != nil {
		return nil, err
	}
	return store.Open(ctx, home)
}

func resolveDrive(ctx context.Context, st *store.Store, drive string) (string, error) {
	if drive != "" {
		return drive, nil
	}
	session := os.Getenv(sessionEnv)
	if session == "" {
		return "", store.ErrNoDrive
	}
	b, ok, err := st.Binding(ctx, session)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", store.ErrNoDrive
	}
	return b.Drive, nil
}

type readFlags struct {
	drive  string
	kinds  []string
	lanes  []string
	to     string
	reader string
	topics []string
	stack  string
	target string
	pr     int
	since  string
	budget int
	width  int
	json   bool
}

func (f *readFlags) register(cmd *cobra.Command, since string, filters bool) {
	cmd.Flags().StringVar(&f.drive, "drive", "", "drive to read (default: the drive bound to this session)")
	if filters {
		cmd.Flags().StringSliceVar(&f.kinds, "kind", nil, "only these kinds (repeatable)")
		cmd.Flags().StringSliceVar(&f.lanes, "lane", nil, "only these lanes (repeatable)")
		cmd.Flags().StringVar(&f.to, "to", "", "only records addressed to this lane")
		cmd.Flags().StringVar(&f.reader, "reader", "", "deliver to this lane: records addressed to it, plus other lanes' broadcasts that match --kind, --lane and --topic")
		cmd.Flags().StringSliceVar(&f.topics, "topic", nil, "only these topics (repeatable)")
		cmd.Flags().StringVar(&f.stack, "stack", "", "only records about this <project>/<env> stack")
		cmd.Flags().StringVar(&f.target, "target", "", "only records about this release target")
		cmd.Flags().IntVar(&f.pr, "pr", 0, "only records about this pull request")
	}
	cmd.Flags().StringVar(&f.since, "since", since, "a seq (#123 or 123), a duration (2h), or an RFC3339 time")
	cmd.Flags().IntVar(&f.budget, "budget", 0, "output budget in bytes (default 6144)")
	cmd.Flags().BoolVar(&f.json, "json", false, "one JSON record per line")
}

func (f *readFlags) registerWidth(cmd *cobra.Command) {
	cmd.Flags().IntVar(&f.width, "width", 0, "clip each text record line to this many characters (default: whole records)")
}

func (f *readFlags) filter(ctx context.Context, st *store.Store) (store.Filter, error) {
	drive, err := resolveDrive(ctx, st, f.drive)
	if err != nil {
		return store.Filter{}, err
	}
	out := store.Filter{Drive: drive, Lanes: f.lanes, To: f.to, For: f.reader, Topics: f.topics, Stack: f.stack, Target: f.target, PR: f.pr}
	for _, k := range f.kinds {
		kind, err := kinds.Parse(k)
		if err != nil {
			return store.Filter{}, err
		}
		out.Kinds = append(out.Kinds, kind)
	}
	if out.After, out.Since, err = parseSince(f.since, st.Now()); err != nil {
		return store.Filter{}, err
	}
	return out, nil
}

func parseSince(s string, now time.Time) (int64, time.Time, error) {
	if s == "" {
		return 0, time.Time{}, nil
	}
	if seq, err := strconv.ParseInt(strings.TrimPrefix(s, "#"), 10, 64); err == nil {
		return seq, time.Time{}, nil
	}
	if d, err := time.ParseDuration(s); err == nil {
		return 0, now.Add(-d), nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return 0, t, nil
	}
	return 0, time.Time{}, fmt.Errorf("--since %q is not a seq, a duration, or an RFC3339 time", s)
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
