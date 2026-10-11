package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/yasyf/cc-inbox/internal/node"
	"github.com/yasyf/cc-inbox/internal/pairing"
	"github.com/yasyf/cc-inbox/internal/relay"
	"github.com/yasyf/cc-inbox/internal/store"
)

func newPairCmd() *cobra.Command {
	var (
		opts pairing.JoinOptions
		wait time.Duration
	)
	cmd := &cobra.Command{
		Use:   "pair",
		Short: "Join an owner's mesh as a read-only peer: print a one-time code and this node's fingerprint, then wait for an owner to accept",
		Args:  cobra.NoArgs,
		RunE: withNode(func(cmd *cobra.Command, home string, st *store.Store, self node.Node, _ []string) error {
			if opts.URL == "" {
				cfg, ok, err := relay.LoadConfig(home, opts.Mesh)
				if err != nil {
					return err
				}
				if !ok {
					return errors.New("pass --url, the relay URL the owner gave you")
				}
				opts.URL = cfg.URL
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), wait)
			defer cancel()
			_, err := pairing.Join(ctx, home, st, self, opts, cmd.OutOrStdout())
			return err
		}),
	}
	cmd.Flags().StringVar(&opts.URL, "url", "", "the relay URL (default: the URL this node already joined for --mesh)")
	cmd.Flags().StringVar(&opts.Mesh, "mesh", relay.DefaultMesh, "the mesh to join")
	cmd.Flags().DurationVar(&wait, "wait", 10*time.Minute, "how long to wait for an owner to accept")
	cmd.Flags().DurationVar(&opts.Poll, "poll", 2*time.Second, "how often to ask the relay whether the code was accepted")
	_ = cmd.Flags().MarkHidden("poll")
	cmd.AddCommand(newPairAcceptCmd())
	return cmd
}

func newPairAcceptCmd() *cobra.Command {
	var (
		mesh, tier, allow, label string
		synckitd                 string
	)
	cmd := &cobra.Command{
		Use:   "accept CODE",
		Short: "Owner side of pairing: show the joining node's fingerprint and approve it with Touch ID, here or on a live owner host",
		Args:  cobra.ExactArgs(1),
		RunE: withNode(func(cmd *cobra.Command, home string, st *store.Store, self node.Node, args []string) error {
			code, err := pairing.ParseCode(args[0])
			if err != nil {
				return err
			}
			opts := pairing.AcceptOptions{Code: code, Tier: node.Tier(tier)}
			switch opts.Tier {
			case node.TierPaired:
				if allow != "" || label != "" {
					return errors.New("--allow and --label apply only to --tier service")
				}
			case node.TierService:
				if label == "" {
					return errors.New("--tier service needs --label, the writer's name, e.g. an operator email")
				}
				if opts.Allow, err = pairing.ParseAllow(allow, label); err != nil {
					return err
				}
			default:
				return fmt.Errorf("--tier %q: pairing admits paired or service; owner hosts enroll through synckitd host add", tier)
			}
			cfg, err := joinedConfig(home, mesh)
			if err != nil {
				return err
			}
			_, err = pairing.Accept(cmd.Context(), home, st, self, cfg, pairing.Synckitd{Path: synckitd}, opts, cmd.OutOrStdout())
			return err
		}),
	}
	cmd.Flags().StringVar(&mesh, "mesh", relay.DefaultMesh, "the mesh the code was minted for")
	cmd.Flags().StringVar(&tier, "tier", string(node.TierPaired), "paired (read-only) or service (scoped writer)")
	cmd.Flags().StringVar(&allow, "allow", "", "service scope as <drives>|<kinds>, comma-separated, trailing * globs, e.g. 'page:*|oncall.say,oncall.report'")
	cmd.Flags().StringVar(&label, "label", "", "service writer name stamped on its records, e.g. an operator email")
	cmd.Flags().StringVar(&synckitd, "synckitd", "synckitd", "the synckitd binary that routes the Touch ID prompt")
	_ = cmd.Flags().MarkHidden("synckitd")
	return cmd
}

func newUnpairCmd() *cobra.Command {
	var mesh string
	cmd := &cobra.Command{
		Use:   "unpair NODE",
		Short: "Revoke a node through the relay, drop its pin here, and rotate the mesh's group key so it reads nothing new",
		Args:  cobra.ExactArgs(1),
		RunE: withNode(func(cmd *cobra.Command, home string, st *store.Store, self node.Node, args []string) error {
			cfg, err := joinedConfig(home, mesh)
			if err != nil {
				return err
			}
			_, err = pairing.Unpair(cmd.Context(), home, st, self, cfg, args[0], cmd.OutOrStdout())
			return err
		}),
	}
	cmd.Flags().StringVar(&mesh, "mesh", relay.DefaultMesh, "the mesh to unpair the node from")
	return cmd
}

func newPeersCmd() *cobra.Command {
	var withMesh, asJSON bool
	cmd := &cobra.Command{
		Use:   "peers",
		Short: "List the nodes this one trusts, with tier and how each was admitted; --mesh adds the relay's directory",
		Args:  cobra.NoArgs,
		RunE: withNode(func(cmd *cobra.Command, home string, st *store.Store, self node.Node, _ []string) error {
			cfgs, err := relay.Configs(home)
			if err != nil {
				return err
			}
			peers, err := pairing.Peers(cmd.Context(), st, self, cfgs, withMesh)
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			if asJSON {
				enc := json.NewEncoder(out)
				for _, p := range peers {
					if err := enc.Encode(p); err != nil {
						return err
					}
				}
				return nil
			}
			for _, p := range peers {
				if _, err := fmt.Fprintln(out, p.Line(st.Now())); err != nil {
					return err
				}
			}
			return nil
		}),
	}
	cmd.Flags().BoolVar(&withMesh, "mesh", false, "also read every joined relay's member directory and show unpinned or mismatched entries")
	cmd.Flags().BoolVar(&asJSON, "json", false, "one JSON object per node")
	return cmd
}

func joinedConfig(home, mesh string) (relay.Config, error) {
	cfg, ok, err := relay.LoadConfig(home, mesh)
	if err != nil {
		return relay.Config{}, err
	}
	if !ok {
		return relay.Config{}, fmt.Errorf("this node has not joined mesh %s; run cci relay join --mesh %s first", mesh, mesh)
	}
	return cfg, nil
}

func withNode(fn func(cmd *cobra.Command, home string, st *store.Store, self node.Node, args []string) error) func(*cobra.Command, []string) error {
	return withStore(func(cmd *cobra.Command, st *store.Store, args []string) error {
		home, err := store.Home()
		if err != nil {
			return err
		}
		self, err := node.Load(home)
		if err != nil {
			return err
		}
		return fn(cmd, home, st, self, args)
	})
}
