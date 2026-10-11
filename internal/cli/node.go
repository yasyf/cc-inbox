package cli

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/yasyf/cc-inbox/internal/node"
	"github.com/yasyf/cc-inbox/internal/store"
)

func newNodeCmd() *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "node",
		Short: "Print this machine's node id and key fingerprint",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			home, err := store.Home()
			if err != nil {
				return err
			}
			n, err := node.Load(home)
			if err != nil {
				return err
			}
			e, endorsed := n.Endorsement()
			if asJSON {
				out := struct {
					ID          string            `json:"id"`
					Fingerprint string            `json:"fingerprint"`
					PublicKey   string            `json:"public_key"`
					Endorsement *node.Endorsement `json:"endorsement,omitempty"`
				}{ID: n.ID(), Fingerprint: n.Fingerprint(), PublicKey: base64.StdEncoding.EncodeToString(n.PublicKey())}
				if endorsed {
					out.Endorsement = &e
				}
				return json.NewEncoder(cmd.OutOrStdout()).Encode(out)
			}
			by := "no"
			if endorsed {
				by = e.KeyID
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "id %s\nfingerprint %s\nendorsed %s\n", n.ID(), n.Fingerprint(), by)
			return err
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "print the node as JSON")
	cmd.AddCommand(&cobra.Command{
		Use:   "endorse",
		Short: "Endorse this node's key with the Secure Enclave key, one Touch ID tap (macOS, authkit)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			home, err := store.Home()
			if err != nil {
				return err
			}
			n, err := node.Load(home)
			if err != nil {
				return err
			}
			e, err := n.Endorse(cmd.Context(), home)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "endorsed %s by %s\n", n.ID(), e.KeyID)
			return err
		},
	})
	return cmd
}
