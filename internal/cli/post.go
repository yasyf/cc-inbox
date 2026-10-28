package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/spf13/cobra"

	"github.com/yasyf/cc-inbox/internal/importer"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
)

var (
	typedStamp   = regexp.MustCompile(`^\s*R\d+[a-z]? \(([^()]+)\)`)
	leadingLabel = regexp.MustCompile(`^\s*((?:\S+\s+){0,4}?)(` + clockLabel + `)(:?)`)
	anyLabel     = regexp.MustCompile(clockLabel)
)

const clockLabel = `\b\d{1,2}:\d{2} ?[AaPp][Mm]\b`

func newPostCmd() *cobra.Command {
	var (
		drive, lane, kind, text string
		r                       store.Record
		counts                  map[string]string
		census, bodyFile        string
		ttl                     time.Duration
		asJSON                  bool
		re, resolves            []int64
	)
	cmd := &cobra.Command{
		Use:   "post [TEXT]",
		Short: "Append one record",
		Args: func(cmd *cobra.Command, args []string) error {
			switch {
			case len(args) > 1:
				return fmt.Errorf("post takes its text as one quoted argument or --text; got %d arguments", len(args))
			case len(args) == 1 && cmd.Flags().Changed("text"):
				return fmt.Errorf("post takes its text once: as an argument or --text, not both")
			case len(args) == 0 && !cmd.Flags().Changed("text") && !cmd.Flags().Changed("path"):
				return fmt.Errorf("post needs its text, as one quoted argument, --text, or a --path file whose first line becomes the text")
			}
			return nil
		},
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, args []string) error {
			at := st.Now()
			var (
				err      error
				fromPath bool
			)
			if lane == "" {
				if lane, err = threadLane(cmd.Context(), st); err != nil {
					return err
				}
			}
			if r.Refs.Path != "" {
				if r.Refs.Path, err = keepPath(cmd, st, r.Refs.Path); err != nil {
					return err
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
						return fmt.Errorf("post -: %w", err)
					}
					if text = strings.TrimSpace(string(stdin)); text == "" {
						return errors.New("post -: stdin is empty")
					}
				}
				if text, err = stamped(cmd.ErrOrStderr(), text, lane, at); err != nil {
					return err
				}
			default:
				body, err := os.ReadFile(r.Refs.Path)
				if err != nil {
					return fmt.Errorf("--path: %w", err)
				}
				text, fromPath = firstLine(string(body)), true
			}
			k, err := kinds.Parse(kind)
			if err != nil {
				return err
			}
			if (k == kinds.Head || k == kinds.Opened) && len(r.Refs.PRs) == 0 {
				if r.Refs.PRs, err = namedPR(cmd.ErrOrStderr(), store.Record{Kind: k, Topic: r.Topic, Text: text}); err != nil {
					return err
				}
			}
			if fromPath {
				text = store.Clip(text)
			} else if utf8.RuneCountInString(text) > store.MaxText && r.Refs.Path == "" {
				excerpt, path, err := st.Fit(text)
				if err != nil {
					return err
				}
				text, r.Refs.Path = excerpt, path
				if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "cci: text over %d characters: the record carries its first %d, and the full text is at %s\n", store.MaxText, store.MaxText, path); err != nil {
					return err
				}
			}
			if r.Drive, err = resolveDrive(cmd.Context(), st, drive); err != nil {
				return err
			}
			r.Lane, r.Kind, r.Text, r.Source = lane, k, text, "post"
			if census != "" {
				r.Fields.Census = &store.Census{}
				if err := json.Unmarshal([]byte(census), r.Fields.Census); err != nil {
					return fmt.Errorf("--census: %w", err)
				}
			}
			if bodyFile != "" {
				if r.Body, err = readBody(cmd, bodyFile); err != nil {
					return err
				}
			}
			if len(counts) > 0 {
				r.Fields.Counts = map[string]int{}
				for key, v := range counts {
					n, err := strconv.Atoi(v)
					if err != nil {
						return fmt.Errorf("--count %s=%s: %w", key, v, err)
					}
					r.Fields.Counts[key] = n
				}
			}
			if r.To, err = resolveRecipients(cmd.Context(), st, r.Drive, r.To, cmd.ErrOrStderr()); err != nil {
				return err
			}
			records, err := perTarget(r, re, resolves)
			if err != nil {
				return err
			}
			for _, rec := range records {
				got, dup, err := st.PostAt(cmd.Context(), rec, ttl, at)
				if err != nil {
					return err
				}
				if err := printPosted(cmd.OutOrStdout(), got, dup, asJSON); err != nil {
					return err
				}
			}
			if self, window, err := peerIdentity(); err == nil && window.PID != 0 {
				return st.BindLane(cmd.Context(), self, r.Drive, lane, time.UnixMilli(window.Started))
			}
			return nil
		}),
	}
	f := cmd.Flags()
	f.StringVar(&drive, "drive", "", "drive of this one record (default: the drive bound to this session); it never moves the binding, which only cci drive use does")
	f.StringVar(&lane, "lane", "", "the writing lane (default: the lane this thread is bound to; never root, so the root passes --lane root)")
	f.StringVar(&kind, "kind", "", "record kind: "+strings.Join(kinds.Names(), ", "))
	f.StringVar(&text, "text", "", "the record text, also accepted as the one argument, and read from stdin when it is a bare -; {at} anywhere in it becomes the record's own time as readers print it, and a clock typed in a leading R<n> (<clock>) stamp, such as R84 (10:52 AM), is replaced with that time, as is a leading H:MM AM/PM label more than a minute ahead of it, one that opens the text or follows at most four words and ends in a colon or follows the lane's own name, and any other such time is stored as typed with a warning on stderr; over 400 characters it spills to a file under $CCI_HOME/blobs and the record carries its first 400 characters, cut at a word and ended with …; without either, the --path file's first line, stored as written")
	f.StringVar(&r.Topic, "topic", "", "pairing key, e.g. #30427 or an incident name")
	f.StringSliceVar(&r.To, "to", nil, "addressed lanes (repeatable); peer:<session prefix or name>[/<agent id or name>] reaches one session's root thread or one of its subagents here, peer:<agent id or name> one of this session's own subagents, a Workflow agent named by its label, peer:<node>/<session>[/<agent>] one on another node, and <lane>@<drive> a lane in another drive; a bare lane that no live thread reads in this drive, but one does in exactly one other drive, is addressed <lane>@<that drive>, and in several the post is refused")
	f.StringVar(&r.ID, "id", "", "idempotency key; a repost with a key already in the drive returns the stored record as a duplicate")
	f.Int64SliceVar(&re, "re", nil, "seq this record answers or closes; repeated, each seq gets its own record")
	f.Int64SliceVar(&resolves, "resolves", nil, "seq of the ask, decide, blocker, blocked, defect, hold or incident this record resolves; repeated or a comma list, the one record resolves every seq")
	f.StringVar(&r.Refs.Path, "path", "", "file holding the full body, or - for stdin; stdin, /dev/stdin, and other pipes are copied to a file under $CCI_HOME/blobs, which the record names; its first line is the text when no text is given, stored exactly as written with no clock or {at} replaced, so the record's text equals that line")
	f.StringVar(&r.Refs.CCN, "ccn", "", "cc-notes id")
	f.Var(&prsFlag{&r.Refs.PRs}, "pr", "pull requests as numbers, owner/repo#N, or URLs (repeatable)")
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
	f.StringVar(&census, "census", "", `census JSON: {"n":232,"denominator":293,"head":"<sha>","drift":0,"stacks_clean":["<project>/<env>"]}`)
	f.StringToStringVar(&counts, "count", nil, "named counts, e.g. --count deletes=0")
	f.StringToStringVar(&r.Fields.Attrs, "attr", nil, "named string attributes (repeatable), e.g. --attr actor=platy")
	f.StringVar(&bodyFile, "body-file", "", "JSON body of at most 64 KiB, from a file or - for stdin")
	f.DurationVar(&ttl, "ttl", 0, "expiry override (default: the kind's TTL)")
	f.BoolVar(&asJSON, "json", false, "print the stored record as JSON")
	_ = cmd.MarkFlagRequired("kind")
	return cmd
}

var errNoLane = errors.New("post needs --lane: this thread is bound to no lane, and a post never defaults to root, so the root passes --lane root; after one post with --lane, later posts default to it")

func threadLane(ctx context.Context, st *store.Store) (string, error) {
	self := selfPeer()
	if self.Session == "" {
		return "", errNoLane
	}
	b, bound, err := st.Binding(ctx, self)
	if err != nil {
		return "", err
	}
	if !bound || b.Lane == "" || b.Lane == "root" {
		return "", errNoLane
	}
	return b.Lane, nil
}

var errManyTargets = errors.New("post takes several --re or several --resolves, not both: each seq of a repeated --re becomes its own record")

func perTarget(r store.Record, re, resolves []int64) ([]store.Record, error) {
	if len(re) > 1 && len(resolves) > 1 {
		return nil, errManyTargets
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

func printPosted(w io.Writer, got store.Record, dup, asJSON bool) error {
	if asJSON {
		_, err := fmt.Fprintln(w, render.JSON(got))
		return err
	}
	suffix := ""
	if dup {
		suffix = " (duplicate)"
	}
	_, err := fmt.Fprintf(w, "#%d%s\n", got.Seq, suffix)
	return err
}

func stamped(w io.Writer, text, lane string, at time.Time) (string, error) {
	stamp := render.Stamp(at, at)
	now := at.Truncate(time.Minute)
	later := func(typed string) bool {
		c := importer.ParseClock(typed)
		return c != nil && c.Latest(now.Add(12*time.Hour)).Sub(now) > time.Minute
	}
	if m := typedStamp.FindStringSubmatchIndex(text); m != nil && importer.ParseClock(text[m[2]:m[3]]) != nil {
		text = text[:m[2]] + stamp + text[m[3]:]
	} else if m := leadingLabel.FindStringSubmatchIndex(text); m != nil && later(text[m[4]:m[5]]) && labels(text[m[2]:m[3]], text[m[6]:m[7]], lane) {
		if _, err := fmt.Fprintf(w, "cci: time label %s rewritten to %s\n", text[m[4]:m[5]], stamp); err != nil {
			return "", err
		}
		text = text[:m[4]] + stamp + text[m[5]:]
	}
	for _, typed := range anyLabel.FindAllString(text, -1) {
		if !later(typed) {
			continue
		}
		if _, err := fmt.Fprintf(w, "cci: time %s in the text is later than the post time %s; stored as typed\n", typed, stamp); err != nil {
			return "", err
		}
	}
	return strings.ReplaceAll(text, "{at}", stamp), nil
}

func labels(prefix, colon, lane string) bool {
	prefix = strings.TrimSpace(prefix)
	return prefix == "" || colon != "" || prefix == lane
}

func namedPR(w io.Writer, r store.Record) ([]store.PR, error) {
	named := store.UnboundPRs(r)
	if len(named) != 1 || named[0] == 0 {
		return nil, fmt.Errorf("a %s record needs --pr: the finder and landing desks key it by pull request, so name the one it is for, e.g. --pr 43922", r.Kind)
	}
	if _, err := fmt.Fprintf(w, "cci: %s names #%d; recorded as --pr %d\n", r.Kind, named[0], named[0]); err != nil {
		return nil, err
	}
	return []store.PR{{Number: named[0]}}, nil
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
		info, statErr := os.Stat(path)
		if statErr != nil {
			return "", fmt.Errorf("--path: %w", statErr)
		}
		abs, absErr := filepath.Abs(path)
		if absErr != nil {
			return "", fmt.Errorf("--path: %w", absErr)
		}
		if info.Mode().IsRegular() && !strings.HasPrefix(abs, "/dev/") && !strings.HasPrefix(abs, "/proc/") {
			return abs, nil
		}
		body, err = os.ReadFile(path) //nolint:gosec // G304: the caller names its own body file.
	}
	if err != nil {
		return "", fmt.Errorf("--path %s: %w", path, err)
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return "", fmt.Errorf("--path %s: empty body", path)
	}
	return st.Blob(string(body))
}

func readBody(cmd *cobra.Command, path string) (json.RawMessage, error) {
	var (
		raw []byte
		err error
	)
	if path == "-" {
		raw, err = io.ReadAll(io.LimitReader(cmd.InOrStdin(), store.MaxBody+1))
	} else {
		raw, err = os.ReadFile(path) //nolint:gosec // G304: the caller names its own body file.
	}
	if err != nil {
		return nil, fmt.Errorf("--body-file: %w", err)
	}
	if len(raw) > store.MaxBody {
		return nil, fmt.Errorf("--body-file: body exceeds %d bytes", store.MaxBody)
	}
	body := bytes.TrimSpace(raw)
	if len(body) == 0 || !json.Valid(body) {
		return nil, errors.New("--body-file: not a JSON value")
	}
	return json.RawMessage(body), nil
}
