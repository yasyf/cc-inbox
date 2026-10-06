package cli

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
)

func newPostCmd() *cobra.Command {
	var (
		drive, lane, kind, text string
		r                       store.Record
		counts                  map[string]string
		census                  string
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
			case len(args) == 0 && !cmd.Flags().Changed("text"):
				return fmt.Errorf("post needs its text, as one quoted argument or --text")
			}
			return nil
		},
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, args []string) error {
			if len(args) == 1 {
				text = args[0]
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
			got, dup, err := st.Post(cmd.Context(), r, ttl)
			if err != nil {
				return err
			}
			if window, session, err := thisSession(); err == nil {
				if err := st.BindLane(cmd.Context(), session, r.Drive, lane, time.UnixMilli(window.Started)); err != nil {
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
	f.StringVar(&text, "text", "", "at most 400 characters, also accepted as the one argument; longer bodies go in a file passed with --path")
	f.StringVar(&r.Topic, "topic", "", "pairing key, e.g. #30427 or an incident name")
	f.StringSliceVar(&r.To, "to", nil, "addressed lanes (repeatable)")
	f.Int64Var(&r.Re, "re", 0, "seq this record answers or closes")
	f.Int64Var(&r.Resolves, "resolves", 0, "seq of the ask, decide, blocker, blocked, defect, hold or incident this record resolves")
	f.StringVar(&r.Refs.Path, "path", "", "file holding the full body")
	f.StringVar(&r.Refs.CCN, "ccn", "", "cc-notes id")
	f.IntSliceVar(&r.Refs.PRs, "pr", nil, "pull request numbers (repeatable)")
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
	f.DurationVar(&ttl, "ttl", 0, "expiry override (default: the kind's TTL)")
	f.BoolVar(&asJSON, "json", false, "print the stored record as JSON")
	for _, name := range []string{"lane", "kind"} {
		_ = cmd.MarkFlagRequired(name)
	}
	return cmd
}
