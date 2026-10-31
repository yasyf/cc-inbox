package cli

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/yasyf/cc-inbox/internal/node"
	"github.com/yasyf/cc-inbox/internal/pairing"
	"github.com/yasyf/cc-inbox/internal/relay"
	"github.com/yasyf/cc-inbox/internal/store"
)

func newRelayCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "relay", Short: "Replicate records end-to-end encrypted through the wide-area relay"}
	cmd.AddCommand(newRelayFoundCmd(), newRelayEnrollCmd(), newRelayJoinCmd(), newRelayPushCmd(), newRelayPullCmd(), newRelaySyncCmd(), newRelayRotateCmd(), newRelayStatusCmd())
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
		services []string
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
			if err := pinMembers(cmd, st, "owner", node.TierOwner, owners); err != nil {
				return err
			}
			if err := pinMembers(cmd, st, "service", node.TierService, services); err != nil {
				return err
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
	cmd.Flags().BoolVar(&instance, "instance", false, "join as an instance delegated by the service key in $CCI_SERVICE_KEY, which the join keeps in the mesh's service.pem so later calls and the daemon need no key in their environment (CI jobs sharing one service identity)")
	cmd.Flags().StringSliceVar(&drives, "drive", nil, "drives the instance may write (exact or trailing *); required with --instance")
	cmd.Flags().StringSliceVar(&kinds, "kind", nil, "kinds the instance may write (default: the service's scope)")
	cmd.Flags().DurationVar(&ttl, "ttl", 6*time.Hour, "how long the instance delegation lasts")
	cmd.Flags().StringSliceVar(&services, "service", nil, "pin a service node by its base64 X9.63 public key before joining, so its records and delegated instances verify")
	cmd.Flags().StringSliceVar(&owners, "owner", nil, "pin an owner node by its base64 X9.63 public key before joining (a fresh node has no other way to trust group-key wraps)")
	_ = cmd.MarkFlagRequired("url")
	return cmd
}

func newRelayFoundCmd() *cobra.Command {
	var (
		cfg         relay.Config
		name, proof string
	)
	cmd := &cobra.Command{
		Use:   "found",
		Short: "Found a relay mesh as its first owner with the STS proof `ci cci-relay prove` prints, then join it",
		Args:  cobra.NoArgs,
		RunE: withNode(func(cmd *cobra.Command, home string, st *store.Store, self node.Node, _ []string) error {
			raw, err := readProof(cmd, proof)
			if err != nil {
				return err
			}
			if name == "" {
				if name, err = os.Hostname(); err != nil {
					return err
				}
			}
			return pairing.Found(cmd.Context(), home, st, self, cfg, name, raw, cmd.OutOrStdout())
		}),
	}
	cmd.Flags().StringVar(&cfg.URL, "url", "", "relay base URL")
	cmd.Flags().StringVar(&cfg.Mesh, "mesh", relay.DefaultMesh, "mesh to found")
	cmd.Flags().StringSliceVar(&cfg.Routes, "route", nil, "drives this mesh carries (exact or trailing *); a mesh without routes carries every other drive")
	cmd.Flags().StringVar(&proof, "proof", "", "file holding the output of `ci cci-relay prove --mesh <mesh> --pub <this node's public_key>`, or - for stdin")
	cmd.Flags().StringVar(&name, "name", "", "this node's name in the relay's directory (default: the hostname)")
	_ = cmd.MarkFlagRequired("url")
	_ = cmd.MarkFlagRequired("proof")
	return cmd
}

func readProof(cmd *cobra.Command, path string) ([]byte, error) {
	if path == "-" {
		return io.ReadAll(cmd.InOrStdin())
	}
	return os.ReadFile(path) //nolint:gosec // G304: the operator names the proof file.
}

func newRelayEnrollCmd() *cobra.Command {
	var (
		mesh, tier, allow, label, name, pub, endorsement string
		synckitd                                         string
	)
	cmd := &cobra.Command{
		Use:   "enroll [NODE]",
		Short: "Owner: admit a pinned owner NODE or a scoped service key (--pub) to the relay mesh with Touch ID, and share the group key with it",
		Args:  cobra.MaximumNArgs(1),
		RunE: withNode(func(cmd *cobra.Command, home string, st *store.Store, self node.Node, args []string) error {
			opts := pairing.EnrollOptions{Tier: node.Tier(tier), Name: name}
			switch opts.Tier {
			case node.TierOwner:
				if len(args) != 1 || pub != "" || allow != "" || label != "" {
					return errors.New("--tier owner takes the pinned owner's NODE id and neither --pub, --allow nor --label")
				}
				opts.Target = args[0]
				if endorsement != "" {
					raw, err := os.ReadFile(endorsement) //nolint:gosec // G304: the operator names the endorsement file.
					if err != nil {
						return err
					}
					var e node.Endorsement
					if err := json.Unmarshal(raw, &e); err != nil {
						return fmt.Errorf("--endorsement %s: %w", endorsement, err)
					}
					opts.Endorsement = &e
				}
			case node.TierService:
				if len(args) != 0 || pub == "" || label == "" || endorsement != "" {
					return errors.New("--tier service takes --pub, --allow and --label, and no NODE or --endorsement")
				}
				key, err := base64.StdEncoding.DecodeString(pub)
				if err != nil {
					return fmt.Errorf("--pub %q is not base64: %w", pub, err)
				}
				opts.Pub = key
				if opts.Allow, err = pairing.ParseAllow(allow, label); err != nil {
					return err
				}
				if opts.Name == "" {
					opts.Name = label
				}
			default:
				return fmt.Errorf("--tier %q: enroll admits owner or service; paired nodes join through cci pair", tier)
			}
			cfg, err := joinedConfig(home, mesh)
			if err != nil {
				return err
			}
			_, err = pairing.Enroll(cmd.Context(), home, st, self, cfg, pairing.Synckitd{Path: synckitd}, opts, cmd.OutOrStdout())
			return err
		}),
	}
	cmd.Flags().StringVar(&mesh, "mesh", relay.DefaultMesh, "the mesh to enroll into")
	cmd.Flags().StringVar(&tier, "tier", "", "owner (a node pinned here as owner) or service (a scoped writer key)")
	cmd.Flags().StringVar(&pub, "pub", "", "the service key, base64 X9.63, as `cci node --json` prints it")
	cmd.Flags().StringVar(&allow, "allow", "", "service scope as <drives>|<kinds>, comma-separated, trailing * globs, e.g. 'page:*|oncall.say,oncall.report'")
	cmd.Flags().StringVar(&label, "label", "", "service writer name stamped on its records")
	cmd.Flags().StringVar(&name, "name", "", "the member's name in the relay's directory (default: the label, or the node id)")
	cmd.Flags().StringVar(&endorsement, "endorsement", "", "the owner's $CCI_HOME/node/endorsement.json, copied from its machine, so it can approve pairings with Touch ID")
	cmd.Flags().StringVar(&synckitd, "synckitd", "synckitd", "the synckitd binary that routes the Touch ID prompt")
	_ = cmd.Flags().MarkHidden("synckitd")
	_ = cmd.MarkFlagRequired("tier")
	return cmd
}

func pinMembers(cmd *cobra.Command, st *store.Store, flag string, tier node.Tier, keys []string) error {
	for _, k := range keys {
		pub, err := base64.StdEncoding.DecodeString(k)
		if err != nil {
			return fmt.Errorf("--%s %q is not base64: %w", flag, k, err)
		}
		if err := st.PutMember(cmd.Context(), store.Member{
			Node: node.IDOf(pub), PublicKey: pub, Tier: tier, Via: "relay", By: "relay join --" + flag, At: time.Now(),
		}); err != nil {
			return err
		}
	}
	return nil
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
				a, err := n.Pull(cmd.Context(), wait, pairing.Handler(n.Home, st, n.Identity))
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
