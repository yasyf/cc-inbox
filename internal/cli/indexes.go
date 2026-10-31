package cli

import (
	"encoding/json"
	"errors"

	"github.com/spf13/cobra"

	"github.com/yasyf/cc-inbox/internal/store"
)

func newMaintainIndexesCmd() *cobra.Command {
	var drive string
	cmd := &cobra.Command{
		Use:    "maintain-indexes",
		Short:  "Build local lane-registration indexes and record memberships and report storage cost",
		Hidden: true,
		Args: func(cmd *cobra.Command, args []string) error {
			if err := cobra.NoArgs(cmd, args); err != nil { return err }
			if cmd.Flags().Changed("incident-drive") && drive == "" { return errors.New("--incident-drive requires a nonempty drive") }
			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			home, err := store.Home()
			if err != nil {
				return err
			}
			report, err := store.MaintainIndexes(cmd.Context(), home)
			if err != nil {
				return err
			}
			if drive != "" {
				st, err := store.Open(cmd.Context(), home)
				if err != nil { return err }
				defer func() { _ = st.Close() }()
				incident, err := st.ProvisionIncidentScope(cmd.Context(), drive)
				if err != nil { return err }
				return json.NewEncoder(cmd.OutOrStdout()).Encode(struct {
					Indexes store.IndexMaintenanceReport `json:"indexes"`
					Incident store.IncidentProvisionReport `json:"incident"`
				}{report, incident})
			}
			return json.NewEncoder(cmd.OutOrStdout()).Encode(report)
		},
	}
	cmd.Flags().StringVar(&drive, "incident-drive", "", "explicitly provision native incident membership for one drive")
	return cmd
}
