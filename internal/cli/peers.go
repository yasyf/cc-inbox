package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/yasyf/cc-inbox/internal/peers"
	"github.com/yasyf/cc-inbox/internal/store"
)

const codexSessionEnv = "CODEX_THREAD_ID"

func newAnnounceCmd() *cobra.Command {
	var (
		role, description string
		tags              []string
	)
	cmd := &cobra.Command{
		Use:   "announce",
		Short: "Register this session in the machine-wide peer registry so other sessions can find it with cci who",
		Args:  cobra.NoArgs,
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
			session, window, err := peerIdentity()
			if err != nil {
				return err
			}
			cwd, err := os.Getwd()
			if err != nil {
				return fmt.Errorf("resolve cwd: %w", err)
			}
			if _, err := peers.Announce(cmd.Context(), st, peers.Options{Session: session, Window: window, Cwd: cwd, Role: role, Description: description, Tags: tags}); err != nil {
				return err
			}
			return printPeer(cmd, st, session)
		}),
	}
	cmd.Flags().StringVar(&role, "role", "", "root, lane, desk, watcher, codex or session (default: inferred from the drive binding)")
	cmd.Flags().StringVar(&description, "desc", "", "what this session is doing, in one line")
	cmd.Flags().StringSliceVar(&tags, "tag", nil, "free-form tags other sessions can select on (repeatable)")
	return cmd
}

func newMeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "me",
		Short: "Print this session's registry line, the one to paste into a handoff",
		Args:  cobra.NoArgs,
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
			session, _, err := peerIdentity()
			if err != nil {
				return err
			}
			return printPeer(cmd, st, session)
		}),
	}
}

func printPeer(cmd *cobra.Command, st *store.Store, session string) error {
	v, err := peers.One(cmd.Context(), st, peers.Filter{Session: session, All: true})
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("session %s is not in the registry; run cci announce", session)
	}
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(cmd.OutOrStdout(), v.Line(st.Now()))
	return err
}

func newWhoCmd() *cobra.Command {
	var (
		f      peers.Filter
		asJSON bool
	)
	cmd := &cobra.Command{
		Use:   "who",
		Short: "List live peer sessions on this machine with the address SendMessage accepts; every flag narrows",
		Args:  cobra.NoArgs,
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
			views, err := peers.Resolve(cmd.Context(), st, f)
			if err != nil {
				return err
			}
			if asJSON {
				enc := json.NewEncoder(cmd.OutOrStdout())
				for _, v := range views {
					if err := enc.Encode(v); err != nil {
						return err
					}
				}
				return nil
			}
			if len(views) == 0 {
				_, err := fmt.Fprintln(cmd.OutOrStdout(), "no matching peers; cci who --all includes sessions that have ended")
				return err
			}
			for _, v := range views {
				if _, err := fmt.Fprintln(cmd.OutOrStdout(), v.Line(st.Now())); err != nil {
					return err
				}
			}
			return nil
		}),
	}
	fl := cmd.Flags()
	fl.StringVar(&f.Session, "session", "", "session id or its prefix")
	fl.StringVar(&f.Name, "name", "", "the harness name, as ListAgents prints it")
	fl.StringVar(&f.Role, "role", "", "root, lane, desk, watcher, codex or session")
	fl.StringVar(&f.Drive, "drive", "", "sessions bound to this drive")
	fl.StringVar(&f.Lane, "lane", "", "sessions bound as this lane")
	fl.StringVar(&f.Tag, "tag", "", "sessions announced with this tag")
	fl.StringVar(&f.Repo, "repo", "", "sessions in this repository, by owner/name or git common dir")
	fl.StringVar(&f.Worktree, "worktree", "", "sessions whose worktree or cwd is this path")
	fl.StringVar(&f.Branch, "branch", "", "sessions on this branch")
	fl.StringVarP(&f.Query, "query", "q", "", "substring of the name, description, tags, branch, lane, drive or cwd")
	fl.BoolVar(&f.All, "all", false, "include sessions whose process has ended")
	fl.BoolVar(&asJSON, "json", false, "one JSON object per peer")
	return cmd
}

func announceSelf(ctx context.Context, st *store.Store) error {
	session, window, err := peerIdentity()
	if err != nil {
		return err
	}
	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("resolve cwd: %w", err)
	}
	_, err = peers.Announce(ctx, st, peers.Options{Session: session, Window: window, Cwd: cwd})
	return err
}

func peerIdentity() (string, store.Window, error) {
	session := os.Getenv(sessionEnv)
	if session == "" {
		session = os.Getenv(codexSessionEnv)
	}
	if session == "" {
		return "", store.Window{}, errors.New("run this from a Claude Code or Codex session: neither " + sessionEnv + " nor " + codexSessionEnv + " is set")
	}
	window, err := thisWindow()
	if errors.Is(err, errNoSession) {
		return session, store.Window{}, nil
	}
	return session, window, err
}
