package cli

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/yasyf/cc-inbox/internal/store"
)

func newCursorCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "cursor", Short: "Read or move a named reader cursor"}
	var drive string
	set := &cobra.Command{
		Use:   "set NAME SEQ",
		Short: "Move cursor NAME to SEQ, backwards included, so a restarted reader replays from there",
		Args:  cobra.ExactArgs(2),
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, args []string) error {
			seq, err := strconv.ParseInt(strings.TrimPrefix(args[1], "#"), 10, 64)
			if err != nil || seq < 0 {
				return fmt.Errorf("seq %q is not a record number", args[1])
			}
			d, err := resolveDrive(cmd.Context(), st, drive)
			if err != nil {
				return err
			}
			if err := st.MoveCursor(cmd.Context(), args[0], d, seq); err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "cursor %s in %s -> #%d\n", args[0], d, seq)
			return err
		}),
	}
	set.Flags().StringVar(&drive, "drive", "", "drive (default: the drive bound to this session)")
	cmd.AddCommand(set)
	return cmd
}
