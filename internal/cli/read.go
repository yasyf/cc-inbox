package cli

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/spf13/cobra"

	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
)

func newTailCmd() *cobra.Command {
	cmd, flags := tailCommand(selfPeer().String())
	cmd.RunE = withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
		return flags.run(cmd.Context(), cmd, st, selfPeer())
	})
	return cmd
}

type tailFlags struct {
	rf     readFlags
	cursor string
	all    bool
}

func tailCommand(peer string) (*cobra.Command, *tailFlags) {
	f := &tailFlags{}
	cmd := &cobra.Command{
		Use:   "tail",
		Short: "Print records after this session's cursor, within a byte budget",
		Args:  cobra.NoArgs,
	}
	cmd.Flags().BoolVar(&f.all, "all", false, fmt.Sprintf("print resolved records whole; by default a record a later one resolved is cut to %d characters", render.FoldWidth))
	f.rf.register(cmd, "", true, render.DefaultBudget)
	f.rf.registerWidth(cmd)
	f.rf.registerLimit(cmd, "print only the newest this many records, like tail -n")
	cmd.Flags().StringVar(&f.cursor, "cursor", peer, "cursor name, read and advanced when --since is absent")
	return cmd, f
}

func (f *tailFlags) run(ctx context.Context, cmd *cobra.Command, st *store.Store, peer store.PeerKey) error {
	filter, err := f.rf.filterPeer(ctx, st, peer)
	if err != nil {
		return err
	}
	name := f.cursor
	if cmd.Flags().Changed("since") || cmd.Flags().Changed("since-seq") {
		name = ""
	}
	fold := render.FoldWidth
	if f.all {
		fold = 0
	}
	start, resumed, err := st.Cursor(ctx, name, filter.Drive)
	if err != nil {
		return err
	}
	res, err := inbox.Tail(ctx, st, inbox.TailOptions{Filter: filter, Cursor: name, Limit: f.rf.limit, Budget: f.rf.budget, Width: f.rf.width, JSON: f.rf.json, Fold: fold, Notes: cmd.ErrOrStderr()}, cmd.OutOrStdout())
	if err != nil || name == "" || !resumed || f.rf.limit > 0 {
		return err
	}
	return advanceLaneCursor(ctx, st, filter, start, res.Last)
}

func advanceLaneCursor(ctx context.Context, st *store.Store, f store.Filter, start, last int64) error {
	self := selfPeer()
	b, bound, err := st.Binding(ctx, self)
	if err != nil || !bound || b.Lane == "" || !store.SameLane(b.Lane, f.For) || b.Drive != f.Drive {
		return err
	}
	if f.To != "" || f.Origin != "" || f.Stack != "" || f.Target != "" || f.PR.Number != 0 || len(f.Mute) > 0 || len(f.ToKinds) > 0 {
		return nil
	}
	seq, _, err := st.Cursor(ctx, store.LaneCursor(self), f.Drive)
	if err != nil || start > seq || seq >= last {
		return err
	}
	return st.SetCursor(ctx, store.LaneCursor(self), f.Drive, last)
}

func newWatchCmd() *cobra.Command {
	var (
		rf       readFlags
		cursor   string
		interval time.Duration
		limit    time.Duration
	)
	cmd := &cobra.Command{
		Use:   "watch",
		Short: "Print one line per new matching record, for a Monitor",
		Long: `Print one line per new matching record, for a Monitor.

Watching for reader root (or main), records addressed to it arrive only of the
kinds a drive's root acts on, as cci subscribe delivers them; --to-kind names
others. cci tail reads every kind.`,
		Args: cobra.NoArgs,
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
			f, err := rf.filter(cmd.Context(), st)
			if err != nil {
				return err
			}
			if len(f.ToKinds) == 0 {
				f.ToKinds = store.DefaultToKinds(cmp.Or(f.For, f.To))
			}
			return inbox.Watch(cmd.Context(), st, inbox.WatchOptions{Filter: f, Cursor: cursor, Interval: interval, For: limit, Width: rf.width, JSON: rf.json}, inbox.WriteLines(cmd.OutOrStdout(), rf.json))
		}),
	}
	rf.register(cmd, "", true, render.DefaultBudget)
	rf.registerWidth(cmd)
	cmd.Flags().StringVar(&cursor, "cursor", "", "resume from and advance this cursor")
	cmd.Flags().DurationVar(&interval, "interval", time.Second, "poll interval")
	cmd.Flags().DurationVar(&limit, "for", 0, "exit after this long; 0 watches until interrupted")
	return cmd
}

func newDigestCmd() *cobra.Command {
	var rf readFlags
	cmd := &cobra.Command{
		Use:   "digest",
		Short: "One-screen summary: counts, open asks, blockers, holds, incidents, latest line per lane",
		Args:  cobra.NoArgs,
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
			f, err := rf.filter(cmd.Context(), st)
			if err != nil {
				return err
			}
			v, err := inbox.Digest(cmd.Context(), st, f.Drive, f.Since)
			if err != nil {
				return err
			}
			if rf.json {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(v)
			}
			v.Write(cmd.OutOrStdout(), st.Now(), rf.budget, rf.width)
			return nil
		}),
	}
	rf.register(cmd, "24h", false, inbox.DigestBudget)
	rf.registerWidth(cmd)
	return cmd
}

func newGrepCmd() *cobra.Command {
	var (
		rf         readFlags
		fixed      bool
		ignoreCase bool
	)
	cmd := &cobra.Command{
		Use:   "grep PATTERN",
		Short: "Search whole record lines (kind, lane, topic, text, refs), newest first",
		Args:  cobra.ExactArgs(1),
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, args []string) error {
			expr := args[0]
			if fixed {
				expr = regexp.QuoteMeta(expr)
			}
			if ignoreCase {
				expr = "(?i)" + expr
			}
			pattern, err := regexp.Compile(expr)
			if err != nil {
				return fmt.Errorf("pattern: %w", err)
			}
			f, err := rf.filter(cmd.Context(), st)
			if err != nil {
				return err
			}
			f.IncludeExpired = true
			return inbox.Grep(cmd.Context(), st, f, pattern, rf.limit, rf.budget, rf.json, cmd.OutOrStdout())
		}),
	}
	rf.register(cmd, "", true, render.DefaultBudget)
	cmd.Flags().BoolVarP(&ignoreCase, "ignore-case", "i", true, "match case-insensitively; --ignore-case=false matches case exactly")
	cmd.Flags().BoolVarP(&fixed, "fixed-strings", "F", false, "match PATTERN literally; the default reads it as a regular expression, so | alternates")
	rf.registerLimit(cmd, "print at most this many matches, newest first")
	return cmd
}

func newShowCmd() *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "show SEQ...",
		Short: "Print records whole by seq: the full line, then the full text or body file the record names",
		Args:  cobra.MinimumNArgs(1),
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, args []string) error {
			seqs := make([]int64, len(args))
			for i, arg := range args {
				seq, err := st.Resolve(cmd.Context(), arg)
				if err != nil {
					return fmt.Errorf("show: %w", err)
				}
				seqs[i] = seq
			}
			return inbox.Show(cmd.Context(), st, seqs, asJSON, cmd.OutOrStdout())
		}),
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "one JSON record per line, the body file not inlined")
	return cmd
}

func newStateCmd() *cobra.Command {
	var rf readFlags
	cmd := &cobra.Command{
		Use:   "state",
		Short: "Latest head, contract, and state record per lane and topic, minus withdrawn ones",
		Args:  cobra.NoArgs,
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
			f, err := rf.filter(cmd.Context(), st)
			if err != nil {
				return err
			}
			records, err := inbox.State(cmd.Context(), st, f)
			if err != nil {
				return err
			}
			if len(records) == 0 && !rf.json {
				_, err := fmt.Fprintf(cmd.OutOrStdout(), "no head, contract, or state records on %s\n", f.Drive)
				return err
			}
			inbox.WriteState(cmd.OutOrStdout(), records, st.Now(), rf.budget, rf.json)
			return nil
		}),
	}
	rf.register(cmd, "", true, render.DefaultBudget)
	return cmd
}
