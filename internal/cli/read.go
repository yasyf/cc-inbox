package cli

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/node"
	"github.com/yasyf/cc-inbox/internal/relay"
	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
)

func newTailCmd() *cobra.Command {
	cmd, flags := tailCommand("")
	cmd.RunE = withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
		self := selfPeer(cmd.Context())
		if !cmd.Flags().Changed("cursor") {
			flags.cursor = self.String()
		}
		return flags.run(cmd.Context(), cmd, st, self)
	})
	return cmd
}

type tailFlags struct {
	rf     readFlags
	cursor string
	seed   string
	all    bool
	oldest bool
	mail   bool
	seqs   []string
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
	cmd.Flags().StringVar(&f.seed, "seed", "", "start a --cursor that does not exist yet where this cursor stands, or at the newest record when this one does not exist either, instead of an hour back")
	cmd.Flags().BoolVar(&f.oldest, "oldest", false, "when the budget cuts a text --since window, keep its oldest records instead of its newest, as --json reads always do")
	cmd.Flags().BoolVar(&f.mail, "mail", false, "when this thread is bound as --to, read its lane mail through the cursor the PostToolUse hook advances, so a record reaches the window once")
	cmd.Flags().StringArrayVar(&f.seqs, "seqs", nil, "read these internal store sequences")
	_ = cmd.Flags().MarkHidden("seqs")
	return cmd, f
}

func (f *tailFlags) run(ctx context.Context, cmd *cobra.Command, st *store.Store, peer store.PeerKey) error {
	if cmd.Flags().Changed("seqs") {
		return f.runSelected(ctx, cmd, st)
	}
	if f.mail {
		delivered, err := f.runMail(ctx, cmd, st, peer)
		if delivered || err != nil {
			return err
		}
	}
	filter, err := f.rf.filterPeer(ctx, st, peer)
	var older []store.Record
	if errors.Is(err, store.ErrNoNumber) {
		filter, older, err = f.rf.sinceFromHub(ctx, st, peer, err)
	}
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
	res, err := inbox.Tail(ctx, st, inbox.TailOptions{Filter: filter, Cursor: name, Seed: f.seed, Limit: f.rf.limit, Budget: f.rf.budget, Width: f.rf.width, JSON: f.rf.json, Oldest: f.oldest, Fold: fold, Notes: tailNotes(cmd, f.rf.json), Older: older}, cmd.OutOrStdout())
	if err != nil || name == "" || !resumed || f.rf.limit > 0 {
		return err
	}
	return advanceLaneCursor(ctx, st, filter, start, res.Last)
}

func (f *tailFlags) runMail(ctx context.Context, cmd *cobra.Command, st *store.Store, peer store.PeerKey) (bool, error) {
	drive, err := resolveDrive(ctx, st, f.rf.drive)
	if err != nil {
		return false, err
	}
	b, bound, err := st.Binding(ctx, peer)
	if err != nil || !bound || b.Drive != drive || !store.SameLane(b.Lane, f.rf.to) {
		return false, err
	}
	mail, err := st.WindowMail(ctx, peer, b)
	if err != nil {
		return false, err
	}
	_, err = inbox.Tail(ctx, st, inbox.TailOptions{Filter: mail, Cursor: store.LaneCursor(peer), Seed: f.seed, Budget: f.rf.budget, Compact: true, Reader: b.Lane, Open: true, Notes: tailNotes(cmd, false)}, cmd.OutOrStdout())
	return true, err
}

func (f *tailFlags) runSelected(ctx context.Context, cmd *cobra.Command, st *store.Store) error {
	if f.rf.drive == "" || !cmd.Flags().Changed("since-seq") || f.rf.sinceSeq != 0 || !f.rf.json || f.rf.budget != 0 {
		return errors.New("--seqs requires --drive, --since-seq 0, --json, and --budget 0")
	}
	var unsupported []string
	cmd.Flags().Visit(func(flag *pflag.Flag) {
		switch flag.Name {
		case "drive", "since-seq", "seqs", "json", "budget":
		default:
			unsupported = append(unsupported, flag.Name)
		}
	})
	if len(unsupported) > 0 {
		return fmt.Errorf("--seqs does not support --%s", unsupported[0])
	}
	seqs := make([]int64, 0, len(f.seqs))
	for _, argument := range f.seqs {
		if argument == "" {
			continue
		}
		for _, value := range strings.Split(argument, ",") {
			seq, err := strconv.ParseInt(value, 0, 64)
			if err != nil || seq <= 0 {
				return errors.New("--seqs requires positive internal store sequences")
			}
			seqs = append(seqs, seq)
		}
	}
	records, err := st.RecordsAmong(ctx, f.rf.drive, seqs)
	if err != nil {
		return err
	}
	if err := inbox.Annotate(ctx, st, records); err != nil {
		return err
	}
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(cmd.OutOrStdout(), render.JSON(record)); err != nil {
			return err
		}
	}
	return nil
}

func advanceLaneCursor(ctx context.Context, st *store.Store, f store.Filter, start, last int64) error {
	self := selfPeer(ctx)
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
		Use:     "grep PATTERN",
		Aliases: []string{"search"},
		Short:   "Search whole record lines (kind, lane, topic, text, refs), newest first",
		Args:    cobra.ExactArgs(1),
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
			return inbox.Grep(cmd.Context(), st, f, pattern, rf.limit, rf.budget, rf.width, rf.json, cmd.OutOrStdout(), tailNotes(cmd, rf.json))
		}),
	}
	rf.register(cmd, "", true, render.DefaultBudget)
	rf.registerWidth(cmd)
	cmd.Flags().BoolVarP(&ignoreCase, "ignore-case", "i", true, "match case-insensitively; --ignore-case=false matches case exactly")
	cmd.Flags().BoolVarP(&fixed, "fixed-strings", "F", false, "match PATTERN literally; the default reads it as a regular expression, so | alternates")
	rf.registerLimit(cmd, "print at most this many matches, newest first")
	return cmd
}

func newFindCmd() *cobra.Command {
	var (
		asJSON bool
		drive  string
	)
	cmd := &cobra.Command{
		Use:   "find ALIAS",
		Short: "Print the record an alias names, as --re would take it: t:<topic>, ccn:<id or text prefix>, card:<id>, pr#N, or last:<kind>[:<lane>]",
		Args:  cobra.ExactArgs(1),
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, args []string) error {
			n, err := store.ParseNamedRef(args[0])
			if err != nil {
				return err
			}
			if drive, err = resolveDrive(cmd.Context(), st, drive); err != nil {
				return err
			}
			seq, err := st.ResolveNamed(cmd.Context(), n, drive, false)
			if err != nil {
				return err
			}
			r, err := st.Get(cmd.Context(), seq)
			if err != nil {
				return err
			}
			if asJSON {
				_, err = fmt.Fprintln(cmd.OutOrStdout(), render.JSON(r))
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s %s\n", n, render.Line(r, st.Now()))
			return err
		}),
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "the record as one JSON line")
	cmd.Flags().StringVar(&drive, "drive", "", "drive to look in (default: the drive bound to this session)")
	return cmd
}

func newShowCmd() *cobra.Command {
	var (
		asJSON bool
		drive  string
		budget int
	)
	cmd := &cobra.Command{
		Use:     "show SEQ...",
		Aliases: []string{"read"},
		Short:   "Print records by seq, each once: the full line, then the rest of the text or body file the record names",
		Args:    cobra.MinimumNArgs(1),
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, args []string) error {
			records := make([]store.Record, len(args))
			for i, arg := range args {
				r, err := showRecord(cmd.Context(), st, drive, arg)
				if err != nil {
					return fmt.Errorf("show: %w", err)
				}
				records[i] = r
			}
			return inbox.Show(cmd.Context(), st, records, drive, budget, asJSON, cmd.OutOrStdout())
		}),
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "one JSON record per line, the body file not inlined")
	cmd.Flags().StringVar(&drive, "drive", "", "refuse a seq that is not on this drive")
	cmd.Flags().IntVar(&budget, "budget", 0, "bytes of body to print, split evenly across the seqs asked; each cut body names the show that reads it whole (default: whole bodies)")
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

func tailNotes(cmd *cobra.Command, asJSON bool) io.Writer {
	if asJSON {
		return cmd.ErrOrStderr()
	}
	if inv := invoked(cmd.Context()); inv != nil {
		if inv.stdoutChar {
			return nil
		}
		return cmd.ErrOrStderr()
	}
	out, ok := cmd.OutOrStdout().(*os.File)
	if !ok {
		return nil
	}
	info, err := out.Stat()
	if err != nil || info.Mode()&os.ModeCharDevice != 0 {
		return nil
	}
	return cmd.ErrOrStderr()
}

func showRecord(ctx context.Context, st *store.Store, drive, ref string) (store.Record, error) {
	seq, err := st.Resolve(ctx, ref)
	if errors.Is(err, store.ErrNoNumber) {
		_, row, err := findOnHub(ctx, st, drive, ref, err)
		if err != nil {
			return store.Record{}, err
		}
		records, err := st.Unstored(ctx, []store.Row{row})
		if err != nil {
			return store.Record{}, err
		}
		return records[0], nil
	}
	if err != nil {
		return store.Record{}, err
	}
	r, err := st.Get(ctx, seq)
	if errors.Is(err, sql.ErrNoRows) {
		return store.Record{}, fmt.Errorf("no record #%d", seq)
	}
	return r, err
}

func (f *readFlags) sinceFromHub(ctx context.Context, st *store.Store, peer store.PeerKey, missing error) (store.Filter, []store.Record, error) {
	rest := *f
	rest.since = ""
	filter, err := rest.filterPeer(ctx, st, peer)
	if err != nil {
		return store.Filter{}, nil, err
	}
	hub, row, err := findOnHub(ctx, st, filter.Drive, f.since, missing)
	if err != nil {
		return store.Filter{}, nil, err
	}
	before, err := st.Oldest(ctx, filter.Drive, row.Origin)
	if err != nil {
		return store.Filter{}, nil, err
	}
	rows, err := hub.Range(ctx, filter.Drive, row.Origin, row.OriginSeq, before)
	if err != nil {
		return store.Filter{}, nil, err
	}
	unstored, err := st.Unstored(ctx, rows)
	if err != nil {
		return store.Filter{}, nil, err
	}
	older, err := st.Selected(ctx, filter, unstored)
	if err != nil || len(older) == 0 {
		return filter, nil, err
	}
	filter.After = older[0].Seq - 1
	return filter, older, nil
}

func findOnHub(ctx context.Context, st *store.Store, drive, ref string, missing error) (*relay.Node, store.Row, error) {
	configs, err := relay.Configs(st.HomeDir())
	if err != nil {
		return nil, store.Row{}, err
	}
	drive, err = resolveDrive(ctx, st, drive)
	if errors.Is(err, store.ErrNoDrive) {
		drive, err = relayDrive(configs, missing)
	}
	if err != nil {
		return nil, store.Row{}, err
	}
	mesh := relay.Route(configs, drive)
	i := slices.IndexFunc(configs, func(c relay.Config) bool { return c.Mesh == mesh })
	if i < 0 {
		return nil, store.Row{}, missing
	}
	id, err := node.Load(st.HomeDir())
	if err != nil {
		return nil, store.Row{}, err
	}
	hub, err := relay.Open(st.HomeDir(), st, id, configs[i])
	if err != nil {
		return nil, store.Row{}, err
	}
	num, err := store.ParseNumber(ref)
	if err != nil {
		return nil, store.Row{}, err
	}
	row, ok, err := hub.Find(ctx, drive, num)
	if err != nil {
		return nil, store.Row{}, err
	}
	if !ok {
		return nil, store.Row{}, missing
	}
	return hub, row, nil
}

func relayDrive(configs []relay.Config, missing error) (string, error) {
	var drives []string
	for _, c := range configs {
		named := c.Routes
		if c.Delegation != nil {
			named = slices.Concat(named, c.Delegation.Drives)
		}
		for _, d := range named {
			if !strings.HasSuffix(d, "*") && !slices.Contains(drives, d) {
				drives = append(drives, d)
			}
		}
	}
	switch len(drives) {
	case 0:
		return "", missing
	case 1:
		return drives[0], nil
	}
	return "", fmt.Errorf("this node's relay carries drives %s; pass --drive", strings.Join(drives, ", "))
}
