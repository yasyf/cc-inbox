package cli

import (
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
		stack                   []int
		counts                  map[string]string
		ttl                     time.Duration
		asJSON                  bool
	)
	cmd := &cobra.Command{
		Use:   "post",
		Short: "Append one record",
		Args:  cobra.NoArgs,
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
			k, err := kinds.Parse(kind)
			if err != nil {
				return err
			}
			if r.Drive, err = resolveDrive(cmd.Context(), st, drive); err != nil {
				return err
			}
			r.Lane, r.Kind, r.Text, r.Source = lane, k, text, "post"
			r.Fields.Stack = stack
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
	f.StringVar(&text, "text", "", "at most 400 characters; longer bodies go in a file passed with --path")
	f.StringVar(&r.Topic, "topic", "", "pairing key, e.g. #30427 or an incident name")
	f.StringSliceVar(&r.To, "to", nil, "addressed lanes (repeatable)")
	f.Int64Var(&r.Re, "re", 0, "seq this record answers or closes")
	f.StringVar(&r.Refs.Path, "path", "", "file holding the full body")
	f.StringVar(&r.Refs.CCN, "ccn", "", "cc-notes id")
	f.IntVar(&r.Refs.PR, "pr", 0, "pull request number")
	f.StringVar(&r.Refs.Build, "build", "", "build URL or id")
	f.StringVar(&r.Refs.URL, "url", "", "any other URL")
	f.IntSliceVar(&stack, "stack", nil, "PR numbers of a stack")
	f.StringVar(&r.Fields.Env, "env", "", "environment or cluster")
	f.StringToStringVar(&counts, "count", nil, "named counts, e.g. --count deletes=0")
	f.DurationVar(&ttl, "ttl", 0, "expiry override (default: the kind's TTL)")
	f.BoolVar(&asJSON, "json", false, "print the stored record as JSON")
	for _, name := range []string{"lane", "kind", "text"} {
		_ = cmd.MarkFlagRequired(name)
	}
	return cmd
}
