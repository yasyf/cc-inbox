package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
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

var typedStamp = regexp.MustCompile(`^\s*R\d+[a-z]? \(([^()]+)\)`)

func newPostCmd() *cobra.Command {
	var (
		drive, lane, kind, text string
		r                       store.Record
		counts                  map[string]string
		census, bodyFile        string
		ttl                     time.Duration
		asJSON                  bool
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
			switch {
			case len(args) == 1:
				text = stamped(args[0], at)
			case cmd.Flags().Changed("text"):
				text = stamped(text, at)
			default:
				body, err := os.ReadFile(r.Refs.Path)
				if err != nil {
					return fmt.Errorf("--path: %w", err)
				}
				text = store.Clip(firstLine(string(body)))
			}
			if utf8.RuneCountInString(text) > store.MaxText && r.Refs.Path == "" {
				_, path, err := st.Fit(text)
				if err != nil {
					return err
				}
				text, r.Refs.Path = store.Clip(firstLine(text)), path
				if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "cci: text over %d characters: the record carries its first line, and the full text is at %s\n", store.MaxText, path); err != nil {
					return err
				}
			}
			k, err := kinds.Parse(kind)
			if err != nil {
				return err
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
			if r.To, err = resolveRecipients(cmd.Context(), st, r.To); err != nil {
				return err
			}
			got, dup, err := st.PostAt(cmd.Context(), r, ttl, at)
			if err != nil {
				return err
			}
			if window, _, err := thisSession(); err == nil {
				if err := st.BindLane(cmd.Context(), selfPeer(), r.Drive, lane, time.UnixMilli(window.Started)); err != nil {
					return err
				}
			}
			if asJSON {
				_, err = fmt.Fprintln(cmd.OutOrStdout(), render.JSON(got))
				return err
			}
			suffix := ""
			if dup {
				suffix = " (duplicate)"
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "#%d%s\n", got.Seq, suffix)
			return err
		}),
	}
	f := cmd.Flags()
	f.StringVar(&drive, "drive", "", "drive (default: the drive bound to this session)")
	f.StringVar(&lane, "lane", "", "the writing lane (required)")
	f.StringVar(&kind, "kind", "", "record kind: "+strings.Join(kinds.Names(), ", "))
	f.StringVar(&text, "text", "", "the record text, also accepted as the one argument; {at} anywhere in it becomes the record's own time as readers print it, and a clock typed in a leading R<n> (<clock>) stamp, such as R84 (10:52 AM), is replaced with that time; over 400 characters it spills to a file under $CCI_HOME/blobs and the record carries its first line; without either, the --path file's first line, stored as written")
	f.StringVar(&r.Topic, "topic", "", "pairing key, e.g. #30427 or an incident name")
	f.StringSliceVar(&r.To, "to", nil, "addressed lanes (repeatable); peer:<session prefix or name>[/<agent id or name>] reaches one session's root thread or one of its subagents here, peer:<node>/<session>[/<agent>] one on another node, and <lane>@<drive> a lane in another drive")
	f.StringVar(&r.ID, "id", "", "idempotency key; a repost with a key already in the drive returns the stored record as a duplicate")
	f.Int64Var(&r.Re, "re", 0, "seq this record answers or closes")
	f.Int64Var(&r.Resolves, "resolves", 0, "seq of the ask, decide, blocker, blocked, defect, hold or incident this record resolves")
	f.StringVar(&r.Refs.Path, "path", "", "file holding the full body; its first line is the text when no text is given, stored exactly as written with no clock or {at} replaced, so the record's text equals that line")
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
	for _, name := range []string{"lane", "kind"} {
		_ = cmd.MarkFlagRequired(name)
	}
	return cmd
}

func stamped(text string, at time.Time) string {
	stamp := render.Stamp(at, at)
	if m := typedStamp.FindStringSubmatchIndex(text); m != nil && importer.ParseClock(text[m[2]:m[3]]) != nil {
		text = text[:m[2]] + stamp + text[m[3]:]
	}
	return strings.ReplaceAll(text, "{at}", stamp)
}

func firstLine(text string) string {
	for line := range strings.Lines(text) {
		if line = strings.TrimSpace(line); line != "" {
			return line
		}
	}
	return ""
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
