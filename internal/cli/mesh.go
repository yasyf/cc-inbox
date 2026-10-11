package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"slices"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/synckit/delivery"
	"github.com/yasyf/synckit/hostregistry"

	"github.com/yasyf/cc-inbox/internal/mesh"
	"github.com/yasyf/cc-inbox/internal/store"
)

func newMeshCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mesh",
		Short: "Replicate records across the owner's machines through the synckit mesh",
	}
	cmd.AddCommand(newMeshEnableCmd(), newMeshStatusCmd(), &cobra.Command{
		Use:    "helper",
		Short:  "Serve the synckit consumer cci (spawned by the cci daemon)",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			home, err := store.Home()
			if err != nil {
				return err
			}
			return mesh.RunHelper(cmd.Context(), home)
		},
	})
	return cmd
}

func newMeshEnableCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "enable",
		Short: "Register cci with synckitd and start the mesh helper in the cci daemon",
		Long: `enable writes the synckit manifest for service cci, registers it with synckitd,
and ensures the cci daemon, which starts the resident mesh helper. Peers pin each
other's node keys when they join the mesh:

    synckitd host add <user@node>     a new peer
    synckitd host enroll <user@node>  a peer already in the mesh`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			synckitd, err := exec.LookPath("synckitd")
			if err != nil {
				return fmt.Errorf("synckitd is not on PATH; install it with `brew install yasyf/tap/synckitd`: %w", err)
			}
			st, err := openStore(cmd.Context())
			if err != nil {
				return err
			}
			path, err := mesh.WriteManifest(st.HomeDir(), st.SchemaFingerprint())
			_ = st.Close()
			if err != nil {
				return err
			}
			if out, err := exec.CommandContext(cmd.Context(), synckitd, "register", path).CombinedOutput(); err != nil { //nolint:gosec // fixed synckitd subcommand on cci's own manifest
				return fmt.Errorf("synckitd register %s: %w: %s", path, err, out)
			}
			if _, err := ensureDaemon(cmd.Context(), 60*time.Second); err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "registered %s with synckitd; the cci daemon serves the mesh helper\n", path)
			return err
		},
	}
}

type meshPeer struct {
	Host    string            `json:"host"`
	Node    string            `json:"node,omitempty"`
	Acked   int64             `json:"acked"`
	State   delivery.State    `json:"state,omitempty"`
	Pending *delivery.Pending `json:"pending,omitempty"`
	Error   string            `json:"error,omitempty"`
}

type meshReport struct {
	Node    string     `json:"node"`
	Self    string     `json:"self"`
	Latest  int64      `json:"latest"`
	Enabled bool       `json:"enabled"`
	Peers   []meshPeer `json:"peers"`
}

func newMeshStatusCmd() *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show this node, its latest record, and what each mesh host has acknowledged",
		Args:  cobra.NoArgs,
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
			report, err := meshStatus(cmd.Context(), st)
			if err != nil {
				return err
			}
			if asJSON {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(report)
			}
			return printMeshReport(cmd.OutOrStdout(), report)
		}),
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "print the report as JSON")
	return cmd
}

func meshStatus(ctx context.Context, st *store.Store) (meshReport, error) {
	enabled, err := mesh.Enabled(st.HomeDir())
	if err != nil {
		return meshReport{}, err
	}
	reg, err := hostregistry.Mesh.Load()
	if err != nil {
		return meshReport{}, err
	}
	statuses, err := delivery.Status(ctx, mesh.Service)
	if err != nil {
		return meshReport{}, err
	}
	members, err := st.Members(ctx)
	if err != nil {
		return meshReport{}, err
	}
	report := meshReport{Node: st.Self(), Self: reg.Self, Enabled: enabled}
	floor := int64(-1)
	for _, host := range reg.Hosts {
		p := meshPeer{Host: host}
		for _, m := range members {
			if m.Via == "mesh" && m.Host == host {
				p.Node = m.Node
			}
		}
		if i := slices.IndexFunc(statuses, func(s delivery.PeerStatus) bool { return s.Peer == host }); i >= 0 {
			s := statuses[i]
			acked, err := mesh.AckedSeq(st.HomeDir(), s.Acked)
			if err != nil {
				return meshReport{}, err
			}
			p.Acked, p.State, p.Pending, p.Error = acked, s.State, s.Pending, s.LastError
		}
		if floor < 0 || p.Acked < floor {
			floor = p.Acked
		}
		report.Peers = append(report.Peers, p)
	}
	rows, err := st.Export(ctx, st.Self(), max(floor, 0), 0)
	if err != nil {
		return meshReport{}, err
	}
	report.Latest = max(floor, 0)
	if len(rows) > 0 {
		report.Latest = rows[len(rows)-1].OriginSeq
	}
	return report, nil
}

func printMeshReport(w io.Writer, r meshReport) error {
	enabled := "enabled"
	if !r.Enabled {
		enabled = "disabled (run `cci mesh enable`)"
	}
	if _, err := fmt.Fprintf(w, "node %s on %s, latest #%d, mesh %s\n", r.Node, r.Self, r.Latest, enabled); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 2, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "HOST\tNODE\tACKED\tSTATE\tERROR")
	for _, p := range r.Peers {
		node, state := p.Node, string(p.State)
		if node == "" {
			node = "not enrolled"
		}
		if p.Acked == r.Latest && p.Pending == nil && p.Node != "" {
			state = "acked"
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t#%d\t%s\t%s\n", p.Host, node, p.Acked, state, p.Error)
	}
	return tw.Flush()
}
