package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/yasyf/cc-inbox/internal/importer"
	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
)

func newImportCmd() *cobra.Command {
	var drive, lane string
	cmd := &cobra.Command{
		Use:   "import FILE...",
		Short: "Ingest markdown inbox lines as records, incrementally and idempotently",
		Args:  cobra.MinimumNArgs(1),
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, args []string) error {
			d := drive
			if d == "" {
				var err error
				if d, err = resolveDrive(cmd.Context(), st, ""); err != nil {
					return err
				}
			}
			for _, path := range args {
				res, err := importer.Import(cmd.Context(), st, path, d, lane)
				if err != nil {
					return err
				}
				if _, err := fmt.Fprintf(cmd.OutOrStdout(), "%s: %d entries, %d new, %d reparsed\n", res.Path, res.Entries, res.Inserted, res.Reparsed); err != nil {
					return err
				}
			}
			return nil
		}),
	}
	cmd.Flags().StringVar(&drive, "drive", "", "drive the lines belong to (default: the drive bound to this session)")
	cmd.Flags().StringVar(&lane, "lane", "", "lane for lines that name none (default: the file name)")
	return cmd
}

func newCompactCmd() *cobra.Command {
	var (
		drive string
		keep  time.Duration
	)
	cmd := &cobra.Command{
		Use:   "compact",
		Short: "Fold records older than --keep into one digest record per day",
		Args:  cobra.NoArgs,
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
			d, err := resolveDrive(cmd.Context(), st, drive)
			if err != nil {
				return err
			}
			res, err := st.Compact(cmd.Context(), d, keep)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "folded %d records into %d day digests\n", res.Folded, res.Digests)
			return err
		}),
	}
	cmd.Flags().StringVar(&drive, "drive", "", "drive to compact (default: the drive bound to this session)")
	cmd.Flags().DurationVar(&keep, "keep", 48*time.Hour, "leave records newer than this untouched")
	return cmd
}

func newDriveCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "drive", Short: "Bind this session to a drive, or list drives"}
	var root bool
	use := &cobra.Command{
		Use:   "use DRIVE",
		Short: "Bind this session to DRIVE so --drive defaults to it",
		Args:  cobra.ExactArgs(1),
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, args []string) error {
			session := os.Getenv(sessionEnv)
			if session == "" {
				return errors.New(sessionEnv + " is not set; run this from a Claude Code session")
			}
			if err := st.Bind(cmd.Context(), session, args[0], root); err != nil {
				return err
			}
			_, err := fmt.Fprintf(cmd.OutOrStdout(), "session %s -> %s\n", session, args[0])
			return err
		}),
	}
	use.Flags().BoolVar(&root, "root", false, "this session is the drive's root: its prompts are recorded as owner records")
	var asJSON bool
	ls := &cobra.Command{
		Use:   "ls",
		Short: "List drives by most recent activity",
		Args:  cobra.NoArgs,
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
			drives, err := st.Drives(cmd.Context())
			if err != nil {
				return err
			}
			if asJSON {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(drives)
			}
			for _, d := range drives {
				if _, err := fmt.Fprintf(cmd.OutOrStdout(), "%s %d records, last %s\n", d.Drive, d.Records, render.Stamp(d.Last, st.Now())); err != nil {
					return err
				}
			}
			return nil
		}),
	}
	ls.Flags().BoolVar(&asJSON, "json", false, "print JSON")
	cmd.AddCommand(use, ls)
	return cmd
}
