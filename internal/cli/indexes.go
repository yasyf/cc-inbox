package cli

import (
	"encoding/json"

	"github.com/spf13/cobra"

	"github.com/yasyf/cc-inbox/internal/store"
)

func newMaintainIndexesCmd() *cobra.Command {
	return &cobra.Command{
		Use:    "maintain-indexes",
		Short:  "Build local lane-registration indexes and report storage cost",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			home, err := store.Home()
			if err != nil {
				return err
			}
			report, err := store.MaintainIndexes(cmd.Context(), home)
			if err != nil {
				return err
			}
			return json.NewEncoder(cmd.OutOrStdout()).Encode(report)
		},
	}
}
