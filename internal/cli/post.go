package cli

import (
	"bytes"
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/yasyf/cc-inbox/internal/importer"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/peers"
	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
)

var (
	typedStamp      = regexp.MustCompile(`^\s*R\d+[a-z]? \(([^()]+)\)`)
	leadingLabel    = regexp.MustCompile(`^\s*((?:\S+\s+){0,4}?)(` + clockLabel + `)(:?)`)
	proseWithdrawal = regexp.MustCompile(`(?i)\bwithdraw(?:s|n|ing)?:?\s+(?:the\s+|root\s+|my\s+)?go(?:s|\s+records?)?\b[^.;]*`)
	seqRef          = regexp.MustCompile(`#(\d+)\b`)
	notGoRef        = regexp.MustCompile(`(?i)\([^)]*\)|\b(?:on|for|of)\s+#\d+\b`)
)

const clockLabel = `\b\d{1,2}:\d{2}(?::\d{2})? ?[AaPp][Mm]\b`

type postPlan struct {
	write    store.Write
	resolved []store.Record
	asJSON   bool
}

func newPostCmd() *cobra.Command {
	cmd, _ := postCommand()
	return cmd
}

func postCommand() (*cobra.Command, func(*cobra.Command, *store.Store, []string, time.Time) (postPlan, error)) {
	var (
		drive, lane, kind, text string
		quoted                  string
		parsedKind              kinds.Kind
		r                       store.Record
		counts                  map[string]string
		census, bodyFile        string
		successor, ownedBy      string
		heads                   []store.Head
		ttl                     time.Duration
		asJSON, force, orHeir   bool
		pairwise                bool
		re, resolves, namedPRs  []string
		reSeqs, resolvesSeqs    []int64
		named                   = map[int64]string{}
		batch                   string
	)
	cmd := &cobra.Command{
		Use:   "post [TEXT]",
		Short: "Append one record",
		Args: func(cmd *cobra.Command, args []string) error {
			if cmd.Flags().Changed(batchFlag) {
				return batchOnly(cmd, args)
			}
			switch {
			case len(args) > 1:
				return fmt.Errorf("post takes its text as one quoted argument or --text; got %d arguments", len(args))
			case len(args) == 1 && cmd.Flags().Changed("text"):
				return fmt.Errorf("post takes its text once: as an argument or --text, not both")
			case len(args) == 0 && !cmd.Flags().Changed("text") && !cmd.Flags().Changed("path") && !cmd.Flags().Changed("head") && !cmd.Flags().Changed("quoted"):
				return fmt.Errorf("post needs its text, as one quoted argument, --text, or a --path file whose first line becomes the text")
			}
			literal := slices.Clone(args)
			cmd.Flags().Visit(func(f *pflag.Flag) { literal = append(literal, f.Value.String()) })
			if err := refuseCredential("the command line", []byte(strings.Join(literal, "\n"))); err != nil {
				return err
			}
			if !cmd.Flags().Changed("kind") {
				return errors.New(`required flag(s) "kind" not set`)
			}
			var err error
			if parsedKind, err = kinds.Parse(kind); err != nil {
				return err
			}
			if successor != "" && (parsedKind != kinds.StandDown || len(r.To) != 1) {
				return errors.New("--successor names who takes over the one --to lane of a --kind stand-down record")
			}
			if ownedBy != "" && (parsedKind != kinds.Decide || len(r.To) != 1) {
				return errors.New("--owned-by names the lane that owns the one --to lane of a --kind decide record")
			}
			for _, seq := range append(slices.Clone(reSeqs), resolvesSeqs...) {
				if seq < 0 {
					return errors.New("--re-seq and --resolves-seq must be nonnegative")
				}
			}
			if err := validateTargets(len(re)+len(reSeqs), len(resolves)+len(resolvesSeqs), pairwise); err != nil {
				return err
			}
			return nil
		},
	}
	plan := func(cmd *cobra.Command, st *store.Store, args []string, at time.Time) (postPlan, error) {
		var (
			err      error
			fromPath bool
		)
		if lane == "" {
			if lane, err = threadLane(cmd.Context(), st); err != nil {
				return postPlan{}, err
			}
		}
		if r.Refs.Path != "" {
			if r.Refs.Path, err = keepPath(cmd, st, r.Refs.Path); err != nil {
				return postPlan{}, err
			}
		}
		switch {
		case len(args) == 1 || cmd.Flags().Changed("text"):
			if len(args) == 1 {
				text = args[0]
			}
			if text == "-" {
				stdin, err := io.ReadAll(cmd.InOrStdin())
				if err != nil {
					return postPlan{}, fmt.Errorf("post -: %w", err)
				}
				if text = strings.TrimSpace(string(stdin)); text == "" {
					return postPlan{}, errors.New("post -: stdin is empty")
				}
				if err := refuseCredential("stdin", stdin); err != nil {
					return postPlan{}, err
				}
			}
			if text, err = stamped(cmd.ErrOrStderr(), text, lane, at); err != nil {
				return postPlan{}, err
			}
		case r.Refs.Path != "":
			body, err := os.ReadFile(r.Refs.Path)
			if err != nil {
				return postPlan{}, fmt.Errorf("--path: %w", err)
			}
			if !utf8.Valid(body) {
				return postPlan{}, fmt.Errorf("--path %s is a binary file, so it cannot be the text; say what it shows with --text", r.Refs.Path)
			}
			text, fromPath = firstLine(string(body)), true
		}
		k := parsedKind
		if clause := proseWithdrawal.FindString(text); clause != "" && k != kinds.Withdraw {
			var re []string
			for _, m := range seqRef.FindAllStringSubmatch(notGoRef.ReplaceAllString(clause, ""), -1) {
				re = append(re, "--re "+m[1])
			}
			return postPlan{}, fmt.Errorf("a withdrawn go is one withdraw record every lane reads as gone, never a sentence in a %s record: post cci post --kind withdraw %s --text '<why>', then this record without the withdrawal", cmp.Or(string(k), "note"), cmp.Or(strings.Join(re, " "), "--re <go seq>"))
		}
		if err := refuseProseEnd(k, text, lane); err != nil {
			return postPlan{}, err
		}
		switch {
		case quoted == "":
		case text == "":
			text = quoted
		default:
			text += " " + quoted
		}
		if len(heads) > 0 {
			if text, err = withHeads(&r, k, heads, text); err != nil {
				return postPlan{}, err
			}
		}
		if (k == kinds.Opened || k == kinds.Head && r.Refs.CCN == "") && len(r.Refs.PRs) == 0 {
			if r.Refs.PRs, err = namedPR(cmd.ErrOrStderr(), store.Record{Kind: k, Topic: r.Topic, Text: text}); err != nil {
				return postPlan{}, err
			}
		}
		if fromPath {
			text = store.Clip(text)
		} else if utf8.RuneCountInString(text) > store.MaxText {
			full := text
			if r.Refs.Path != "" {
				attached, err := os.ReadFile(r.Refs.Path)
				if err != nil {
					return postPlan{}, fmt.Errorf("--path: %w", err)
				}
				full += "\n\n" + string(attached)
			}
			excerpt, path, err := st.Fit(full)
			if err != nil {
				return postPlan{}, err
			}
			text, r.Refs.Path = excerpt, path
			if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "cci: text over %d characters: the record carries its first %d, and the full text is at %s\n", store.MaxText, store.MaxText, path); err != nil {
				return postPlan{}, err
			}
		}
		if r.Drive, err = resolveDrive(cmd.Context(), st, drive); err != nil {
			return postPlan{}, err
		}
		if lane, err = aliasedLane(cmd.Context(), st, r.Drive, lane); err != nil {
			return postPlan{}, err
		}
		self, window, identityErr := peerIdentity(cmd.Context())
		binds := getenv(cmd.Context(), unsentEnv) == "" && getenv(cmd.Context(), parentSessionEnv) == "" && identityErr == nil && window.PID != 0 && bindsCaller(cmd.Context(), lane)
		if binds {
			if lane, err = registeredLane(cmd.Context(), st, self, window, r.Drive, lane); err != nil {
				return postPlan{}, err
			}
		}
		r.Lane, r.Kind, r.Text, r.Source = lane, k, text, "post"
		if census != "" {
			r.Fields.Census = &store.Census{}
			if err := json.Unmarshal([]byte(census), r.Fields.Census); err != nil {
				return postPlan{}, fmt.Errorf("--census: %w", err)
			}
		}
		if bodyFile != "" {
			if r.Body, err = readBody(cmd, bodyFile); err != nil {
				return postPlan{}, err
			}
		}
		if len(counts) > 0 {
			r.Fields.Counts = map[string]int{}
			for key, v := range counts {
				n, err := strconv.Atoi(v)
				if err != nil {
					return postPlan{}, fmt.Errorf("--count %s=%s: %w", key, v, err)
				}
				r.Fields.Counts[key] = n
			}
		}
		to := r.To
		r.To, err = resolveRecipients(cmd.Context(), st, r.Drive, to, k, force, cmd.ErrOrStderr())
		var needsLane []string
		for retired := (*RetiredLane)(nil); orHeir && errors.As(err, &retired); {
			gone := slices.Index(to, retired.to)
			to = slices.Clone(to)
			if slices.Contains(to, retired.heir) {
				to = slices.Delete(to, gone, gone+1)
			} else {
				to[gone] = retired.heir
			}
			needsLane = append(needsLane, fmt.Sprintf("NEEDS-LANE %s, so this reaches %s to route", retired.down, retired.heir))
			r.Text = needsLane[len(needsLane)-1] + ": " + r.Text
			r.To, err = resolveRecipients(cmd.Context(), st, r.Drive, to, k, force, cmd.ErrOrStderr())
		}
		if err != nil {
			var retired *RetiredLane
			if errors.As(err, &retired) && (k == kinds.Owner || store.Stem(lane) == ownerRelay) {
				_, _, raised := st.Post(cmd.Context(), store.Record{Drive: r.Drive, Lane: "cci", Kind: kinds.Blocker, To: []string{ownerRelay}, Text: store.Clip(fmt.Sprintf("%s; the refused %s record from %s read: %s", retired, cmp.Or(string(k), "note"), lane, text)), Source: "post"}, 0)
				return postPlan{}, errors.Join(err, raised)
			}
			return postPlan{}, err
		}
		if utf8.RuneCountInString(r.Text) > store.MaxText {
			attached := text
			if r.Refs.Path != "" {
				body, err := os.ReadFile(r.Refs.Path)
				if err != nil {
					return postPlan{}, fmt.Errorf("--path: %w", err)
				}
				attached = string(body)
			}
			path, err := st.Blob(strings.Join(needsLane, "\n") + "\n\n" + attached)
			if err != nil {
				return postPlan{}, err
			}
			r.Text, r.Refs.Path = store.Clip(fmt.Sprintf("NEEDS-LANE %d stood-down lanes, named in the body: %s", len(needsLane), text)), path
			if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "cci: naming the %d stood-down lanes would put the text over %d characters, so they are named in the body at %s\n", len(needsLane), store.MaxText, path); err != nil {
				return postPlan{}, err
			}
		}
		switch {
		case k != kinds.StandDown:
			if r.To, err = forwarded(cmd.Context(), st, r.Drive, r.To, k); err != nil {
				return postPlan{}, err
			}
		case successor != "":
			if r.Fields.Attrs == nil {
				r.Fields.Attrs = map[string]string{}
			}
			r.Fields.Attrs["successor"] = successor
		}
		if ownedBy != "" {
			if r.Fields.Attrs == nil {
				r.Fields.Attrs = map[string]string{}
			}
			r.Fields.Attrs[store.OwnedByAttr] = ownedBy
		}
		if cmd.Flags().Changed("re") {
			if reSeqs, err = resolveAll(cmd.Context(), st, r.Drive, re, false, named); err != nil {
				return postPlan{}, err
			}
		}
		if cmd.Flags().Changed("resolves") {
			if resolvesSeqs, err = resolveAll(cmd.Context(), st, r.Drive, resolves, true, named); err != nil {
				return postPlan{}, err
			}
		}
		var resolved []store.Record
		for _, seq := range resolvesSeqs {
			if slices.ContainsFunc(resolved, func(o store.Record) bool { return o.Seq == seq }) {
				continue
			}
			o, err := st.Get(cmd.Context(), seq)
			if err != nil {
				return postPlan{}, fmt.Errorf("--resolves #%d: %w", seq, err)
			}
			if o.Num, err = st.Number(cmd.Context(), o); err != nil {
				return postPlan{}, err
			}
			if !o.Kind.Opens() {
				return postPlan{}, fmt.Errorf("--resolves %s: it is a %s from %s, which opens nothing to resolve; name the ask, decide, blocker, blocked, defect, hold or incident this record resolves", o.Name(), o.Kind, o.Lane)
			}
			resolved = append(resolved, o)
		}
		for _, ref := range namedPRs {
			prs, err := namedRefPRs(cmd.Context(), st, r.Drive, ref)
			if err != nil {
				return postPlan{}, err
			}
			r.Refs.PRs = append(r.Refs.PRs, prs...)
		}
		if k == kinds.Hold && len(r.Refs.PRs) > 0 {
			if unheld := store.UnboundPRs(store.Record{Topic: r.Topic, Text: text, Refs: r.Refs}); len(unheld) > 0 {
				held, named := make([]string, len(r.Refs.PRs)), make([]string, len(unheld))
				for i, pr := range r.Refs.PRs {
					held[i] = "#" + strconv.Itoa(pr.Number)
				}
				for i, n := range unheld {
					named[i] = "#" + strconv.Itoa(n)
				}
				if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "cci: this hold holds %s only; its text also names %s, which it does not hold\n", strings.Join(held, ", "), strings.Join(named, ", ")); err != nil {
					return postPlan{}, err
				}
			}
		}
		records, err := perTarget(r, reSeqs, resolvesSeqs, pairwise)
		if err != nil {
			return postPlan{}, err
		}
		if k == kinds.Lift {
			if err := refuseStrayLift(cmd.Context(), st, records); err != nil {
				return postPlan{}, err
			}
		}
		for i := range records {
			if len(records[i].To) == 0 && records[i].Re != 0 {
				if records[i].To, err = replyTo(cmd.Context(), st, records[i]); err != nil {
					return postPlan{}, err
				}
			}
			if len(records[i].To) > 0 && !slices.ContainsFunc(records[i].To, func(t string) bool { return !addressesSelf(t, lane, records[i].Drive) }) {
				return postPlan{}, selfAddressed(cmd.Context(), st, records[i], lane)
			}
			if k != kinds.StandDown && ownedBy == "" {
				if records[i].To, err = stemmed(cmd.Context(), st, records[i].Drive, records[i].To); err != nil {
					return postPlan{}, err
				}
			}
		}
		var (
			bind   *store.LaneBinding
			binder string
		)
		if binds {
			holds, err := refuseSecondBinder(cmd.Context(), st, self, window, r.Drive, lane, at)
			if err != nil {
				return postPlan{}, err
			}
			if holds {
				binder = self.String()
			}
			bind = &store.LaneBinding{Key: self, Drive: r.Drive, Lane: lane, Started: time.UnixMilli(window.Started)}
		}
		for i := range records {
			records[i].Fields.Attrs = namedAttrs(records[i], named, binder)
		}
		return postPlan{write: store.Write{Bind: bind, Records: records, TTL: ttl, At: at}, resolved: resolved, asJSON: asJSON}, nil
	}
	cmd.RunE = untilStored(withStore(func(cmd *cobra.Command, st *store.Store, args []string) error {
		if batch != "" {
			return postBatch(cmd, st, batch, lane, drive)
		}
		p, err := plan(cmd, st, args, st.Now())
		if err != nil {
			return err
		}
		return commitPosts(cmd, st, []postPlan{p})
	}))
	f := cmd.Flags()
	f.StringVar(&batch, batchFlag, "", "post each record of a JSON list, from a file or - for stdin: each an object of this command's flags by name, text and to and re among them, plus body for its full text; --lane and --drive are each record's default. Every record is checked before any is stored, and the list stores in one step: each prints as one post does, while a refusal prints each refused record's position, stores none of the list, and exits 1")
	f.StringVar(&drive, "drive", "", "drive of this one record (default: the drive bound to this session); it never moves the binding, which only cci drive use does")
	f.StringVar(&lane, "lane", "", "the writing lane (default: the lane this thread is bound to; never root, so the root passes --lane root)")
	f.StringVar(&kind, "kind", "", "record kind: "+strings.Join(kinds.Names(), ", "))
	f.StringVar(&text, "text", "", "the record text, also accepted as the one argument, and read from stdin when it is a bare -; {at} anywhere in it becomes the record's own Pacific time as readers print it, and a clock typed in a leading R<n> (<clock>) stamp, such as R84 (10:52 AM), is replaced with that time; a leading H:MM AM/PM label is replaced with it too, with a hint on stderr: one that opens the text followed by a colon or the lane's own name, one that follows the lane's own name, or one that follows at most four words and ends in a colon; any other time, such as a deadline that opens the text, is stored as typed; over 400 characters it spills to a file under $CCI_HOME/blobs, followed by the --path file when one is named, and the record carries its first 400 characters, cut at a word and ended with …; without either, the --path file's first line, stored as written")
	f.StringVar(&ownedBy, "owned-by", "", "with --kind decide, the lane that owns the one --to lane, such as the lane that maintains a script posting as that lane: stored as fields.attrs.owned-by: later asks, decides and defects addressed to the owned lane reach its owner, and the owner's successors, too; the newest such decide wins")
	f.StringVar(&successor, "successor", "", "with --kind stand-down, the lane taking over the one --to lane: stored as fields.attrs.successor: later records addressed to the stood-down lane reach the successor too, and cci who marks the lane stood down")
	f.StringVar(&r.Topic, "topic", "", "pairing key, e.g. #30427 or an incident name")
	f.StringSliceVar(&r.To, "to", nil, "addressed lanes (repeatable); peer:<session prefix or name>[/<agent id or name>] reaches one session's root thread or one of its subagents here, peer:<agent id or name> one of this session's own subagents, a Workflow agent named by its label, peer:<node>/<session>[/<agent>] one on another node, and <lane>@<drive> a lane in another drive; a bare lane that no live thread reads in this drive, but one does in exactly one other drive, is addressed <lane>@<that drive>, and in several the post is refused; a bare lane no live thread reads, no thread is bound as on this drive, and no record on it was posted as is accepted and read once that lane registers, unless the name looks like a slip for a live lane, such as one or two edits from it or an older number of it, which is refused naming that lane unless --force; a lane that stood down, or a stem whose every lane stood down, is refused before anything is written, naming its live successor or root, and an owner record or an owner-relay post so refused also raises a blocker to owner-relay; a name ending -<n> answers under its stem, the name without it: --to <stem> reaches the stem's newest live lane, a numbered name is stored as its stem while the stem reaches that same lane, and the post prints one line when no live lane reads a name or several running lanes share its stem; owner and all always pass; a record every --to of which names the posting lane itself is refused, naming who wrote the record it answers, or, from an agent whose session holds that lane, naming the agent's own lane to post as, such as --lane handler or --lane builder")
	f.StringVar(&r.ID, "id", "", "idempotency key; a repost with a key already in the drive returns the stored record as a duplicate")
	f.StringSliceVar(&re, "re", nil, "number (#N or #N@node) or alias (t:<topic>, ccn:<id or text prefix>, card:<id>, pr#N, last:<kind>[:<lane>]) of the record this one answers or closes; an alias names its one open record, else its one record, and refuses naming the candidates; repeated, each gets its own record; with no --to, each is addressed to the lane that wrote the record it answers, as <lane>@<drive> when that record is on another drive")
	f.StringSliceVar(&resolves, "resolves", nil, "number (#N or #N@node) or alias, as for --re but naming an open record, of the ask, decide, blocker, blocked, defect, hold or incident this record resolves; repeated or a comma list, the one record resolves every one")
	f.Int64SliceVar(&reSeqs, "re-seq", nil, "internal store sequences this record answers")
	f.Int64SliceVar(&resolvesSeqs, "resolves-seq", nil, "internal store sequences this record resolves")
	_ = f.MarkHidden("re-seq")
	_ = f.MarkHidden("resolves-seq")
	// TODO: delete once no live brief passes --stamp; every record is stamped now, and refusing it stopped lanes' holds mid-incident.
	f.Bool("stamp", false, "ignored: cci stamps every record")
	_ = f.MarkHidden("stamp")
	cmd.MarkFlagsMutuallyExclusive("re", "re-seq")
	cmd.MarkFlagsMutuallyExclusive("resolves", "resolves-seq")
	f.BoolVar(&pairwise, "pairwise", false, "pair equal non-empty --re and --resolves lists in order, posting one record per pair")
	f.StringVar(&r.Refs.Path, "path", "", "file holding the full body, or - for stdin; stdin, /dev/stdin, and other pipes are copied to a file under $CCI_HOME/blobs, which the record names; its first line is the text when no text is given, stored exactly as written with no clock or {at} replaced, so the record's text equals that line")
	f.StringVar(&r.Refs.CCN, "ccn", "", "cc-notes id")
	f.Var(&prsFlag{prs: &r.Refs.PRs, named: &namedPRs}, "pr", "pull requests as numbers, pr#N, owner/repo#N, or URLs, or an alias as for --re, whose record's pull requests this one carries (repeatable)")
	f.StringSliceVar(&r.Refs.Builds, "build", nil, "build URLs or ids (repeatable)")
	f.StringSliceVar(&r.Refs.Stacks, "stack", nil, "deploy stacks as <project>/<env> (repeatable)")
	f.StringSliceVar(&r.Refs.Targets, "target", nil, "release targets (repeatable)")
	f.StringSliceVar(&r.Refs.Lanes, "lane-ref", nil, "lanes this record is about (repeatable)")
	f.StringVar(&r.Refs.URL, "url", "", "any other URL")
	f.StringVar(&r.Refs.Board, "board", "", "cc-present board URL")
	f.StringSliceVar(&r.Fields.Envs, "env", nil, "environments or clusters (repeatable)")
	f.StringVar(&r.Fields.Mode, "mode", "", "how a release or apply started: platy, cli, manual, walker")
	f.StringVar(&r.Fields.Outcome, "outcome", "", "passed, failed, pending, cancelled")
	f.StringVar(&r.Fields.Commit, "commit", "", "full commit sha")
	f.Var(&headsFlag{heads: &heads}, "head", "on a head record, one pull request's new head as <pr>=<sha> or <pr>=<was>..<sha>, then an optional note after a space, such as \"45001=4423264..c3b44a3 patch-identical\"; repeated, one record carries every head of a restack: each lands in fields.attrs as head:<pr> = [<was>..]<sha>[ <note>], each PR joins its refs, and its text opens with HEAD <n> PRs: #<pr> <short sha>, ...")
	f.StringVar(&census, "census", "", `census JSON: {"n":232,"denominator":293,"head":"<sha>","drift":0,"stacks_clean":["<project>/<env>"]}`)
	f.StringToStringVar(&counts, "count", nil, "named counts, e.g. --count deletes=0")
	f.Var(&attrsFlag{attrs: &r.Fields.Attrs}, "attr", "named string attributes (repeatable), e.g. --attr actor=platy")
	f.StringVar(&bodyFile, "body-file", "", "JSON body of at most 64 KiB, from a file or - for stdin")
	f.DurationVar(&ttl, "ttl", 0, "expiry override (default: the kind's TTL)")
	f.BoolVar(&asJSON, "json", false, "print the stored record as JSON")
	f.StringVar(&quoted, "quoted", "", "words another writer wrote, such as a relayed Slack page, appended to the text after one space, or the whole text when no other is given; cci replaces no {at} and checks no withdrawal in them, and keeps checking the text's own words")
	f.BoolVar(&orHeir, "or-heir", false, "readdress each --to lane that stood down to its successor, or to root when it named none, and open the text with NEEDS-LANE naming the stood-down lane, or name them in the body when naming them in the text would put it over 400 characters, for a sender that takes the lane from a stored row rather than from a person's typing")
	f.BoolVar(&force, "force", false, "address a bare --to lane that looks like a slip for a live lane as typed; any other lane that has not started yet needs no flag")
	cmd.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		if name, ok := strings.CutPrefix(err.Error(), "unknown flag: --"); ok && slices.Contains(textFlagSlips, name) {
			return fmt.Errorf("%w: a record has no %s of its own; its one line is the text, given as the one quoted argument or --text, and a longer body goes in a --path file", err, name)
		}
		return err
	})
	return cmd, plan
}

var textFlagSlips = []string{"subject", "title", "message", "summary"}

const batchFlag = "batch"

func batchOnly(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		return errors.New("--batch takes its text from the list, not an argument")
	}
	var other []string
	cmd.Flags().Visit(func(f *pflag.Flag) {
		if f.Name != batchFlag && f.Name != "lane" && f.Name != "drive" {
			other = append(other, "--"+f.Name)
		}
	})
	if len(other) > 0 {
		return fmt.Errorf("--batch takes only --lane and --drive; %s belongs in the list", strings.Join(other, " "))
	}
	return nil
}

func postBatch(cmd *cobra.Command, st *store.Store, path, lane, drive string) error {
	raw, err := readBatch(cmd, path)
	if err != nil {
		return err
	}
	var records []toolRecord
	if err := json.Unmarshal(raw, &records); err != nil {
		return fmt.Errorf("--batch: not a JSON list of records: %w", err)
	}
	if len(records) == 0 {
		return errors.New("--batch: the list is empty")
	}
	at := st.Now()
	plans := make([]postPlan, 0, len(records))
	var said strings.Builder
	refused := 0
	for i, record := range records {
		p, notes, err := planRecord(cmd, st, at, record, lane, drive)
		if err == nil && len(plans) > 0 && p.write.TTL != plans[0].write.TTL {
			err = fmt.Errorf("its --ttl %s differs from the list's %s, and the list stores in one step under one ttl: post it apart", p.write.TTL, plans[0].write.TTL)
		}
		if err != nil {
			refused++
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "cci: --batch record %d: %s\n", i+1, err)
			continue
		}
		plans = append(plans, p)
		said.WriteString(notes)
	}
	if refused > 0 {
		return fmt.Errorf("--batch: %d of %d records refused, so none posted", refused, len(records))
	}
	if _, err := io.WriteString(cmd.ErrOrStderr(), said.String()); err != nil {
		return err
	}
	return commitPosts(cmd, st, plans)
}

func planRecord(batch *cobra.Command, st *store.Store, at time.Time, record toolRecord, lane, drive string) (postPlan, string, error) {
	args, stdin, err := record.args(lane, drive)
	if err != nil {
		return postPlan{}, "", err
	}
	post, plan := postCommand()
	var p postPlan
	post.RunE = func(cmd *cobra.Command, operands []string) (err error) {
		p, err = plan(cmd, st, operands, at)
		return err
	}
	var said bytes.Buffer
	root := rootCommand()
	root.AddCommand(post)
	root.SetArgs(args)
	root.SetIn(strings.NewReader(stdin))
	root.SetOut(&said)
	root.SetErr(&said)
	err = root.ExecuteContext(batch.Context())
	return p, said.String(), err
}

func commitPosts(cmd *cobra.Command, st *store.Store, plans []postPlan) error {
	w := plans[0].write
	for _, p := range plans[1:] {
		w.Bind = cmp.Or(w.Bind, p.write.Bind)
		w.Records = append(w.Records, p.write.Records...)
	}
	posted, err := st.PostAll(cmd.Context(), w.Bind, w.Records, w.TTL, w.At)
	if errors.Is(err, store.ErrUnconfirmed) {
		if getenv(cmd.Context(), unsentEnv) != "" {
			return &BusDown{Cause: err}
		}
		return keepWrite(w, err)
	}
	if err != nil {
		return err
	}
	for _, p := range plans {
		for _, got := range posted[:len(p.write.Records)] {
			if got.Record.Num, err = st.Number(cmd.Context(), got.Record); err != nil {
				return err
			}
			if err := printPosted(cmd.OutOrStdout(), got.Record, got.Duplicate, p.asJSON); err != nil {
				return err
			}
		}
		posted = posted[len(p.write.Records):]
		for _, o := range p.resolved {
			if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "cci: resolved %s %s %s: %s\n", o.Name(), o.Kind, o.Lane, render.Clip(o.Text, 80)); err != nil {
				return err
			}
		}
	}
	return nil
}

func readBatch(cmd *cobra.Command, path string) ([]byte, error) {
	if path == "-" {
		return io.ReadAll(cmd.InOrStdin())
	}
	raw, err := os.ReadFile(callerPath(cmd.Context(), path)) //nolint:gosec // G304: the caller names its own batch file.
	if err != nil {
		return nil, fmt.Errorf("--batch: %w", err)
	}
	return raw, nil
}

var errNoLane = errors.New("post needs --lane: this thread is bound to no lane and was spawned with no name, and a post never defaults to root, so the root passes --lane root; after one post with --lane, later posts default to it")

func threadLane(ctx context.Context, st *store.Store) (string, error) {
	self := SelfPeer(ctx)
	if self.Session == "" {
		return "", errNoLane
	}
	b, bound, err := st.Binding(ctx, self)
	if err != nil {
		return "", err
	}
	if bound && b.Lane != "" {
		if b.Lane == "root" {
			return "", errNoLane
		}
		return b.Lane, nil
	}
	lane, err := namedLane(ctx, st, self)
	if err != nil || lane == "" {
		return "", cmp.Or(err, errNoLane)
	}
	return lane, nil
}

func namedLane(ctx context.Context, st *store.Store, self store.PeerKey) (string, error) {
	if self.Agent == "" {
		return "", nil
	}
	p, err := st.Peer(ctx, self)
	if errors.Is(err, sql.ErrNoRows) || p.Role == peers.RoleWorkflow {
		return "", nil
	}
	return p.AgentName, err
}

func replyTo(ctx context.Context, st *store.Store, reply store.Record) ([]string, error) {
	answered, err := st.Get(ctx, reply.Re)
	if errors.Is(err, sql.ErrNoRows) || err == nil && (answered.Lane == "" || answered.Lane == reply.Lane && answered.Drive == reply.Drive) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("--re #%d: %w", reply.Re, err)
	}
	to := answered.Lane
	if answered.Drive != reply.Drive {
		to += "@" + answered.Drive
	}
	return forwarded(ctx, st, reply.Drive, []string{to}, reply.Kind)
}

func stemmed(ctx context.Context, st *store.Store, drive string, to []string) ([]string, error) {
	out := make([]string, 0, len(to))
	for _, t := range to {
		if stem := store.Stem(t); stem != t && !strings.HasPrefix(t, "peer:") && !strings.Contains(t, "@") {
			named, err := st.Reach(ctx, drive, t)
			if errors.Is(err, store.ErrNoLaneRegistry) {
				return to, nil
			}
			if err != nil {
				return nil, err
			}
			held, err := st.Reach(ctx, drive, stem)
			if err != nil {
				return nil, err
			}
			if named.Lane != "" && held.Lane == named.Lane {
				t = stem
			}
		}
		if !slices.Contains(out, t) {
			out = append(out, t)
		}
	}
	return out, nil
}

func addressesSelf(t, lane, drive string) bool {
	addressed, onDrive, cross := strings.Cut(t, "@")
	return store.SameLane(addressed, lane) && (!cross || onDrive == drive)
}

func selfAddressed(ctx context.Context, st *store.Store, r store.Record, lane string) error {
	to := strings.Join(r.To, ",")
	why := fmt.Sprintf("--to %s addresses only %s, the lane posting this record", to, lane)
	self := SelfPeer(ctx)
	session, bound, err := st.Binding(ctx, store.PeerKey{Session: self.Session})
	if err != nil {
		return err
	}
	if self.Agent != "" && bound && session.Drive == r.Drive && (store.SameLane(session.Lane, lane) || session.Root && store.RootName(lane)) {
		return fmt.Errorf("%s, and %s is the lane of this agent's session, which reads a record from its own lane as one it wrote: post it as this agent's own lane, such as --lane handler or --lane builder, keeping --to %s", why, lane, to)
	}
	if r.Re == 0 {
		return errors.New(why + "; name the lane that should read it")
	}
	answered, err := st.Get(ctx, r.Re)
	if err != nil || answered.Lane == "" || store.SameLane(answered.Lane, lane) && answered.Drive == r.Drive {
		return fmt.Errorf("%s; name the lane that should read this answer to #%d", why, r.Re)
	}
	wrote := answered.Lane
	if answered.Drive != r.Drive {
		wrote += "@" + answered.Drive
	}
	return fmt.Errorf("%s; #%d came from %s, which a reply with no --to reaches", why, r.Re, wrote)
}

const ownerRelay = "owner-relay"

var ownerForwarded = []kinds.Kind{kinds.Ask, kinds.Decide, kinds.Defect}

func forwarded(ctx context.Context, st *store.Store, drive string, to []string, kind kinds.Kind) ([]string, error) {
	out := slices.Clone(to)
	for _, t := range to {
		lane, laneDrive, cross := strings.Cut(t, "@")
		if strings.HasPrefix(t, "peer:") || store.RootName(lane) || t == "owner" || t == "all" {
			continue
		}
		if !cross {
			laneDrive = drive
		}
		chain, _, err := st.Successors(ctx, laneDrive, lane)
		if err != nil {
			return nil, err
		}
		live, err := liveLane(ctx, st, laneDrive, lane)
		if err != nil {
			return nil, err
		}
		if live {
			chain = nil
		}
		owner := ""
		if slices.Contains(ownerForwarded, kind) {
			if owner, err = st.LaneOwner(ctx, laneDrive, lane); err != nil {
				return nil, err
			}
		}
		if owner != "" {
			reach, err := st.Reach(ctx, laneDrive, owner)
			if err != nil && !errors.Is(err, store.ErrNoLaneRegistry) {
				return nil, err
			}
			owners, _, err := st.Successors(ctx, laneDrive, owner)
			if err != nil {
				return nil, err
			}
			if reach.Started && !reach.Down {
				owners = nil
			}
			if !reach.Started || !reach.Down || len(owners) > 0 {
				chain = append(append(chain, owner), owners...)
			}
		}
		for _, successor := range chain {
			if cross {
				successor += "@" + laneDrive
			}
			if !slices.Contains(out, successor) {
				out = append(out, successor)
			}
		}
	}
	return out, nil
}

func liveLane(ctx context.Context, st *store.Store, drive, lane string) (bool, error) {
	reach, err := st.Reach(ctx, drive, lane)
	if errors.Is(err, store.ErrNoLaneRegistry) {
		return false, nil
	}
	return reach.Started && !reach.Down, err
}

var errManyTargets = errors.New("post takes several --re or several --resolves, not both: each seq of a repeated --re becomes its own record")

func validateTargets(re, resolves int, pairwise bool) error {
	if pairwise {
		if re == 0 || re != resolves {
			return fmt.Errorf("--pairwise needs equal non-empty --re and --resolves lists; got %d and %d", re, resolves)
		}
		return nil
	}
	if re > 1 && resolves > 1 {
		return errManyTargets
	}
	return nil
}

func perTarget(r store.Record, re, resolves []int64, pairwise bool) ([]store.Record, error) {
	if err := validateTargets(len(re), len(resolves), pairwise); err != nil {
		return nil, err
	}
	if pairwise {
		out := make([]store.Record, len(re))
		for i, seq := range re {
			out[i] = r
			out[i].Re, out[i].Resolves = seq, resolves[i]
			if r.ID != "" {
				out[i].ID = fmt.Sprintf("%s#%d:%d", r.ID, seq, resolves[i])
			}
		}
		return out, nil
	}
	for _, seq := range resolves {
		switch {
		case r.Resolves == 0:
			r.Resolves = seq
		case seq != r.Resolves && !slices.Contains(r.AlsoResolves, seq):
			r.AlsoResolves = append(r.AlsoResolves, seq)
		}
	}
	if len(re) <= 1 {
		if len(re) == 1 {
			r.Re = re[0]
		}
		return []store.Record{r}, nil
	}
	out := make([]store.Record, len(re))
	for i, seq := range re {
		out[i] = r
		out[i].Re = seq
		if r.ID != "" {
			out[i].ID = fmt.Sprintf("%s#%d", r.ID, seq)
		}
	}
	return out, nil
}

func resolveAll(ctx context.Context, st *store.Store, drive string, refs []string, resolving bool, named map[int64]string) ([]int64, error) {
	seqs := make([]int64, len(refs))
	for i, ref := range refs {
		if !store.IsNamedRef(ref) {
			seq, err := st.Resolve(ctx, ref)
			if errors.Is(err, store.ErrNoNumber) {
				seq, err = storeFromHub(ctx, st, drive, ref, err)
			}
			if err != nil {
				return nil, err
			}
			seqs[i] = seq
			continue
		}
		n, err := store.ParseNamedRef(ref)
		if err != nil {
			return nil, err
		}
		if seqs[i], err = st.ResolveNamed(ctx, n, drive, resolving); err != nil {
			return nil, err
		}
		named[seqs[i]] = ref
	}
	return seqs, nil
}

func storeFromHub(ctx context.Context, st *store.Store, drive, ref string, missing error) (int64, error) {
	hub, row, err := findOnHub(ctx, st, drive, ref, missing)
	if err != nil {
		return 0, err
	}
	applied, err := st.Backfill(ctx, []store.Row{row}, nil)
	if err == nil && applied.Buffered > 0 {
		_, err = hub.Pull(ctx, 0, nil)
	}
	if err != nil {
		return 0, err
	}
	return st.Resolve(ctx, ref)
}

func namedRefPRs(ctx context.Context, st *store.Store, drive, ref string) ([]store.PR, error) {
	n, err := store.ParseNamedRef(ref)
	if err != nil {
		return nil, err
	}
	seq, err := st.ResolveNamed(ctx, n, drive, false)
	if err != nil {
		return nil, err
	}
	r, err := st.Get(ctx, seq)
	if err != nil {
		return nil, err
	}
	if len(r.Refs.PRs) == 0 {
		return nil, fmt.Errorf("--pr %s names %s, which carries no pull request", ref, r.Name())
	}
	return r.Refs.PRs, nil
}

func namedAttrs(r store.Record, named map[int64]string, binder string) map[string]string {
	attrs := maps.Clone(r.Fields.Attrs)
	set := func(key, alias string) {
		if alias == "" {
			return
		}
		if attrs == nil {
			attrs = map[string]string{}
		}
		attrs[key] = alias
	}
	set(store.ReAliasAttr, named[r.Re])
	for i, seq := range r.Closes() {
		set(store.ResolvesAliasAttr(i), named[seq])
	}
	set(store.BinderAttr, binder)
	return attrs
}

func printPosted(w io.Writer, got store.Record, dup, asJSON bool) error {
	if asJSON {
		_, err := fmt.Fprintln(w, render.JSON(got))
		return err
	}
	suffix := ""
	if dup {
		suffix = " (duplicate)"
	}
	_, err := fmt.Fprintf(w, "%s%s\n", got.Name(), suffix)
	return err
}

func stamped(w io.Writer, text, lane string, at time.Time) (string, error) {
	stamp := render.Stamp(at, at)
	if m := typedStamp.FindStringSubmatchIndex(text); m != nil && importer.ParseClock(text[m[2]:m[3]]) != nil {
		text = text[:m[2]] + stamp + text[m[3]:]
	} else if m := leadingLabel.FindStringSubmatchIndex(text); m != nil && labels(text[m[2]:m[3]], text[m[6]:m[7]], text[m[7]:], lane) {
		if _, err := fmt.Fprintf(w, "cci: label %s became the post time %s; cci stamps every record, so leave the time out\n", text[m[4]:m[5]], stamp); err != nil {
			return "", err
		}
		text = text[:m[4]] + stamp + text[m[5]:]
	}
	return strings.ReplaceAll(text, "{at}", stamp), nil
}

func labels(prefix, colon, rest, lane string) bool {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		next, _, _ := strings.Cut(strings.TrimSpace(rest), " ")
		return colon != "" || strings.TrimSuffix(next, ":") == lane
	}
	if prefix == lane {
		return true
	}
	words := strings.Fields(prefix)
	return colon != "" && !strings.ContainsAny(prefix, ".;") && !clockPrepositions[strings.ToLower(words[len(words)-1])]
}

var clockPrepositions = map[string]bool{"since": true, "at": true, "until": true, "till": true, "before": true, "after": true, "by": true, "from": true, "to": true, "around": true, "about": true}

func namedPR(w io.Writer, r store.Record) ([]store.PR, error) {
	named := store.UnboundPRs(r)
	if len(named) != 1 || named[0] == 0 {
		if r.Kind == kinds.Head {
			return nil, fmt.Errorf("a %s record needs --pr or --ccn: the finder and landing desks key a pull request's head by --pr 43922, and a head that names a note, such as an experiment's method note, takes --ccn <note id>", r.Kind)
		}
		return nil, fmt.Errorf("a %s record needs --pr: the finder and landing desks key it by pull request, so name the one it is for, e.g. --pr 43922", r.Kind)
	}
	if _, err := fmt.Fprintf(w, "cci: %s names #%d; recorded as --pr %d\n", r.Kind, named[0], named[0]); err != nil {
		return nil, err
	}
	return []store.PR{{Number: named[0]}}, nil
}

func withHeads(r *store.Record, k kinds.Kind, heads []store.Head, text string) (string, error) {
	if k != kinds.Head {
		return "", fmt.Errorf("--head is for a head record, not %s", k)
	}
	if r.Fields.Commit != "" {
		return "", errors.New("--head carries each pull request's commit; drop --commit")
	}
	if r.Fields.Attrs == nil {
		r.Fields.Attrs = map[string]string{}
	}
	for _, h := range heads {
		key, value := h.Attr()
		if _, twice := r.Fields.Attrs[key]; twice {
			return "", fmt.Errorf("--head names %s twice", h.PR)
		}
		r.Fields.Attrs[key] = value
		if !slices.Contains(r.Refs.PRs, h.PR) {
			r.Refs.PRs = append(r.Refs.PRs, h.PR)
		}
	}
	preview := store.HeadsPreview(heads)
	if text == "" {
		return preview, nil
	}
	return preview + ": " + text, nil
}

func firstLine(text string) string {
	for line := range strings.Lines(text) {
		if line = strings.TrimSpace(line); line != "" {
			return line
		}
	}
	return ""
}

func keepPath(cmd *cobra.Command, st *store.Store, path string) (string, error) {
	var (
		body []byte
		err  error
	)
	if path == "-" {
		body, err = io.ReadAll(cmd.InOrStdin())
	} else {
		path = callerPath(cmd.Context(), path)
		info, statErr := os.Stat(path)
		if statErr != nil {
			return "", fmt.Errorf("--path: %w", statErr)
		}
		abs, absErr := filepath.Abs(path)
		if absErr != nil {
			return "", fmt.Errorf("--path: %w", absErr)
		}
		body, err = os.ReadFile(path) //nolint:gosec // G304: the caller names its own body file.
		if err == nil && info.Mode().IsRegular() && !strings.HasPrefix(abs, "/dev/") && !strings.HasPrefix(abs, "/proc/") {
			return abs, refuseCredential("--path", body)
		}
	}
	if err != nil {
		return "", fmt.Errorf("--path %s: %w", path, err)
	}
	if err := refuseCredential("--path", body); err != nil {
		return "", err
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return "", fmt.Errorf("--path %s: empty body", path)
	}
	return st.Blob(string(body))
}

func refuseCredential(where string, said []byte) error {
	shape, found := store.CredentialShape(said)
	if !found {
		return nil
	}
	return fmt.Errorf("%s holds %s, and a record cannot be taken back: post what the credential is for and where it went, never its value", where, shape)
}

func readBody(cmd *cobra.Command, path string) (json.RawMessage, error) {
	var (
		raw []byte
		err error
	)
	if path == "-" {
		raw, err = io.ReadAll(io.LimitReader(cmd.InOrStdin(), store.MaxBody+1))
	} else {
		raw, err = os.ReadFile(callerPath(cmd.Context(), path)) //nolint:gosec // G304: the caller names its own body file.
	}
	if err != nil {
		return nil, fmt.Errorf("--body-file: %w", err)
	}
	if len(raw) > store.MaxBody {
		return nil, fmt.Errorf("--body-file: body exceeds %d bytes", store.MaxBody)
	}
	if err := refuseCredential("--body-file", raw); err != nil {
		return nil, err
	}
	body := bytes.TrimSpace(raw)
	if len(body) == 0 || !json.Valid(body) {
		return nil, errors.New("--body-file: not a JSON value")
	}
	return json.RawMessage(body), nil
}
