package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"time"

	"github.com/spf13/cobra"

	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/store"
)

func newTailCmd() *cobra.Command {
	var (
		rf     readFlags
		cursor string
	)
	cmd := &cobra.Command{
		Use:   "tail",
		Short: "Print records after this session's cursor, within a byte budget",
		Args:  cobra.NoArgs,
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
			f, err := rf.filter(cmd.Context(), st)
			if err != nil {
				return err
			}
			name := cursor
			if cmd.Flags().Changed("since") {
				name = ""
			}
			_, err = inbox.Tail(cmd.Context(), st, inbox.TailOptions{Filter: f, Cursor: name, Budget: rf.budget, JSON: rf.json}, cmd.OutOrStdout())
			return err
		}),
	}
	rf.register(cmd, "", true)
	cmd.Flags().StringVar(&cursor, "cursor", os.Getenv(sessionEnv), "cursor name, read and advanced when --since is absent")
	return cmd
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
		Args:  cobra.NoArgs,
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
			f, err := rf.filter(cmd.Context(), st)
			if err != nil {
				return err
			}
			return inbox.Watch(cmd.Context(), st, inbox.WatchOptions{Filter: f, Cursor: cursor, Interval: interval, For: limit, JSON: rf.json}, cmd.OutOrStdout())
		}),
	}
	rf.register(cmd, "", true)
	cmd.Flags().StringVar(&cursor, "cursor", "", "resume from and advance this cursor")
	cmd.Flags().DurationVar(&interval, "interval", time.Second, "poll interval")
	cmd.Flags().DurationVar(&limit, "for", 29*time.Minute, "exit after this long so a Monitor re-arms")
	return cmd
}

func newDigestCmd() *cobra.Command {
	var rf readFlags
	cmd := &cobra.Command{
		Use:   "digest",
		Short: "One-screen summary: counts, open asks, holds, incidents, latest line per lane",
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
			v.Write(cmd.OutOrStdout(), st.Now(), rf.budget)
			return nil
		}),
	}
	rf.register(cmd, "24h", false)
	return cmd
}

func newGrepCmd() *cobra.Command {
	var rf readFlags
	cmd := &cobra.Command{
		Use:   "grep PATTERN",
		Short: "Search record text with a regular expression, newest first",
		Args:  cobra.ExactArgs(1),
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, args []string) error {
			pattern, err := regexp.Compile("(?i)" + args[0])
			if err != nil {
				return fmt.Errorf("pattern: %w", err)
			}
			f, err := rf.filter(cmd.Context(), st)
			if err != nil {
				return err
			}
			f.IncludeExpired = true
			return inbox.Grep(cmd.Context(), st, f, pattern, rf.budget, rf.json, cmd.OutOrStdout())
		}),
	}
	rf.register(cmd, "", true)
	return cmd
}
