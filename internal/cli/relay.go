package cli

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/yasyf/cc-inbox/internal/node"
	"github.com/yasyf/cc-inbox/internal/relay"
	"github.com/yasyf/cc-inbox/internal/store"
)

func newRelayCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "relay", Short: "Replicate records end-to-end encrypted through the wide-area relay"}
	cmd.AddCommand(newRelayJoinCmd(), newRelayPushCmd(), newRelayPullCmd(), newRelaySyncCmd(), newRelayRotateCmd(), newRelayStatusCmd())
	return cmd
}

func relayNodes(st *store.Store, mesh string) ([]*relay.Node, []relay.Config, error) {
	id, err := node.Load(st.HomeDir())
	if err != nil {
		return nil, nil, err
	}
	configs, err := relay.Configs(st.HomeDir())
	if err != nil {
		return nil, nil, err
	}
	var nodes []*relay.Node
	for _, c := range configs {
		if mesh != "" && c.Mesh != mesh {
			continue
		}
		n, err := relay.Open(st.HomeDir(), st, id, c)
		if err != nil {
			return nil, nil, err
		}
		nodes = append(nodes, n)
	}
	if len(nodes) == 0 {
		return nil, nil, fmt.Errorf("no relay mesh %q joined: run `cci relay join --url <relay url>`", mesh)
	}
	return nodes, configs, nil
}

func newRelayJoinCmd() *cobra.Command {
	var (
		cfg      relay.Config
		instance bool
		drives   []string
		kinds    []string
		ttl      time.Duration
		owners   []string
	)
	cmd := &cobra.Command{
		Use:   "join",
		Short: "Join a relay mesh: pin the relay key, open this node's queue, fetch the group key",
		Args:  cobra.NoArgs,
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
			id, err := node.Load(st.HomeDir())
			if err != nil {
				return err
			}
			have, ok, err := relay.LoadConfig(st.HomeDir(), cfg.Mesh)
			if err != nil {
				return err
			}
			if ok {
				cfg.RelayPub = have.RelayPub
				if cfg.Tier == "" {
					cfg.Tier = have.Tier
				}
			}
			for _, o := range owners {
				pub, err := base64.StdEncoding.DecodeString(o)
				if err != nil {
					return fmt.Errorf("--owner %q is not base64: %w", o, err)
				}
				if err := st.PutMember(cmd.Context(), store.Member{
					Node: node.IDOf(pub), PublicKey: pub, Tier: node.TierOwner, Via: "relay", By: "relay join --owner", At: time.Now(),
				}); err != nil {
					return err
				}
			}
			n, err := relay.Open(st.HomeDir(), st, id, cfg)
			if err != nil {
				return err
			}
			if instance {
				if err := n.Delegate(drives, kinds, ttl); err != nil {
					return err
				}
				n.Config.Tier = string(node.TierService)
			}
			if n.Config.Tier == "" {
				return errors.New("pass --tier owner|service|paired: the tier this node was enrolled at, which the relay must confirm")
			}
			if err := n.Join(cmd.Context()); err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "joined mesh %s at %s as %s %s, group key epoch %d\n",
				n.Config.Mesh, n.Config.URL, n.Config.Tier, id.ID(), n.Keyring.Latest(n.Config.Mesh))
			return err
		}),
	}
	cmd.Flags().StringVar(&cfg.URL, "url", "", "relay base URL")
	cmd.Flags().StringVar(&cfg.Mesh, "mesh", relay.DefaultMesh, "mesh to join")
	cmd.Flags().StringSliceVar(&cfg.Routes, "route", nil, "drives this mesh carries (exact or trailing *); a mesh without routes carries every other drive")
	cmd.Flags().StringVar(&cfg.Tier, "tier", "", "the tier this node was enrolled at (owner|service|paired); kept from an earlier join")
	cmd.Flags().StringVar(&cfg.Tap, "tap", "", "pull from the per-drive tap NAME instead of this node's queue (service members)")
	cmd.Flags().BoolVar(&instance, "instance", false, "join as an instance delegated by the service key in $CCI_SERVICE_KEY (CI jobs sharing one service identity)")
	cmd.Flags().StringSliceVar(&drives, "drive", nil, "drives the instance may write (exact or trailing *); required with --instance")
	cmd.Flags().StringSliceVar(&kinds, "kind", nil, "kinds the instance may write (default: the service's scope)")
	cmd.Flags().DurationVar(&ttl, "ttl", 6*time.Hour, "how long the instance delegation lasts")
	cmd.Flags().StringSliceVar(&owners, "owner", nil, "pin an owner node by its base64 X9.63 public key before joining (a fresh node has no other way to trust group-key wraps)")
	_ = cmd.MarkFlagRequired("url")
	return cmd
}

func newRelayPushCmd() *cobra.Command {
	var (
		mesh, drive string
		asJSON      bool
	)
	cmd := &cobra.Command{
		Use:   "push",
		Short: "Push this node's records the relay has not acknowledged",
		Args:  cobra.NoArgs,
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
			nodes, configs, err := relayNodes(st, mesh)
			if err != nil {
				return err
			}
			total := relay.PushResult{Rejected: []relay.Rejection{}}
			for _, n := range nodes {
				res, err := n.Push(cmd.Context(), configs)
				if err != nil {
					return err
				}
				total.Pushed += res.Pushed
				for _, rj := range res.Rejected {
					if drive == "" || rj.Drive == drive {
						total.Rejected = append(total.Rejected, rj)
					}
				}
				if asJSON {
					continue
				}
				if _, err := fmt.Fprintf(cmd.OutOrStdout(), "%s: pushed %d, refused %d\n", n.Config.Mesh, res.Pushed, len(res.Rejected)); err != nil {
					return err
				}
			}
			if asJSON {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(total)
			}
			for _, rj := range total.Rejected {
				if _, err := fmt.Fprintf(cmd.OutOrStdout(), "refused %s #%d %s: %s\n", rj.Drive, rj.OriginSeq, rj.ID, rj.Reason); err != nil {
					return err
				}
			}
			return nil
		}),
	}
	cmd.Flags().StringVar(&mesh, "mesh", "", "mesh to push (default: every joined mesh)")
	cmd.Flags().StringVar(&drive, "drive", "", "report refusals for this drive only (every unpushed record is still sent)")
	cmd.Flags().BoolVar(&asJSON, "json", false, `print {"pushed":N,"rejected":[{"id","drive","origin_seq","reason"}]}`)
	return cmd
}

func newRelayPullCmd() *cobra.Command {
	var (
		mesh string
		wait time.Duration
	)
	cmd := &cobra.Command{
		Use:   "pull",
		Short: "Long-poll the relay once: verify, decrypt and apply records, then acknowledge them",
		Args:  cobra.NoArgs,
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
			nodes, _, err := relayNodes(st, mesh)
			if err != nil {
				return err
			}
			for _, n := range nodes {
				a, err := n.Pull(cmd.Context(), wait, nil)
				if err != nil {
					return err
				}
				if _, err := fmt.Fprintf(cmd.OutOrStdout(), "%s: %d new, %d duplicate, %d held, %d released\n",
					n.Config.Mesh, a.Inserted, a.Duplicate, a.Buffered, a.Released); err != nil {
					return err
				}
			}
			return nil
		}),
	}
	cmd.Flags().StringVar(&mesh, "mesh", "", "mesh to pull (default: every joined mesh)")
	cmd.Flags().DurationVar(&wait, "wait", relay.PullWait, "long-poll wait, at most 20s")
	return cmd
}

func newRelaySyncCmd() *cobra.Command {
	var (
		mesh, drive string
		asJSON      bool
	)
	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Backfill a drive from the relay's log: every origin, verified, decrypted and applied idempotently",
		Args:  cobra.NoArgs,
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
			nodes, _, err := relayNodes(st, mesh)
			if err != nil {
				return err
			}
			if len(nodes) != 1 {
				return errors.New("pass --mesh: this node joined more than one mesh")
			}
			a, err := nodes[0].Sync(cmd.Context(), drive)
			if err != nil {
				return err
			}
			if asJSON {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(a)
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s %s: %d new, %d duplicate, %d held, %d released\n",
				nodes[0].Config.Mesh, drive, a.Inserted, a.Duplicate, a.Buffered, a.Released)
			return err
		}),
	}
	cmd.Flags().StringVar(&mesh, "mesh", "", "mesh to read (default: the only joined mesh)")
	cmd.Flags().StringVar(&drive, "drive", "", "drive to backfill")
	cmd.Flags().BoolVar(&asJSON, "json", false, "print the apply counts as JSON")
	_ = cmd.MarkFlagRequired("drive")
	return cmd
}

func newRelayRotateCmd() *cobra.Command {
	var mesh string
	cmd := &cobra.Command{
		Use:   "rotate",
		Short: "Owner: mint the next group key and wrap it to every pinned member",
		Args:  cobra.NoArgs,
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
			nodes, _, err := relayNodes(st, mesh)
			if err != nil {
				return err
			}
			for _, n := range nodes {
				epoch, err := n.Rotate(cmd.Context())
				if err != nil {
					return err
				}
				if _, err := fmt.Fprintf(cmd.OutOrStdout(), "%s: group key epoch %d\n", n.Config.Mesh, epoch); err != nil {
					return err
				}
			}
			return nil
		}),
	}
	cmd.Flags().StringVar(&mesh, "mesh", relay.DefaultMesh, "mesh to rotate")
	return cmd
}

func newRelayStatusCmd() *cobra.Command {
	var (
		mesh   string
		asJSON bool
	)
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show each joined mesh: tier, group key epoch, unpushed records, relay queue depth",
		Args:  cobra.NoArgs,
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
			nodes, _, err := relayNodes(st, mesh)
			if err != nil {
				return err
			}
			var reps []relay.StatusReport
			for _, n := range nodes {
				rep, err := n.Status(cmd.Context())
				if err != nil {
					return err
				}
				reps = append(reps, rep)
			}
			if asJSON {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(reps)
			}
			for _, r := range reps {
				remote := r.RemoteErr
				if r.Remote != nil {
					remote = fmt.Sprintf("queue %s depth %d, relay epoch %d", r.Remote.Queue, r.Remote.QueueDepth, r.Remote.Epoch)
				}
				routes := "*"
				if len(r.Routes) > 0 {
					routes = strings.Join(r.Routes, ",")
				}
				if _, err := fmt.Fprintf(cmd.OutOrStdout(), "%s %s node %s %s, epoch %d, drives %s, pushed through %d, %d unpushed; %s\n",
					r.Mesh, r.URL, r.Node, r.Tier, r.Epoch, routes, r.Pushed, r.Unpushed, remote); err != nil {
					return err
				}
			}
			return nil
		}),
	}
	cmd.Flags().StringVar(&mesh, "mesh", "", "mesh to show (default: every joined mesh)")
	cmd.Flags().BoolVar(&asJSON, "json", false, "print JSON")
	return cmd
}
