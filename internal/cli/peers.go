package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/yasyf/cc-interact/procs"

	"github.com/yasyf/cc-inbox/internal/kinds"
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
			self, window, err := peerIdentity()
			if err != nil {
				return err
			}
			cwd, err := os.Getwd()
			if err != nil {
				return fmt.Errorf("resolve cwd: %w", err)
			}
			if _, err := peers.Announce(cmd.Context(), st, peers.Options{Session: self.Session, Agent: self.Agent, Window: window, Cwd: cwd, Role: role, Description: description, Tags: tags, Speak: true}); err != nil {
				return err
			}
			return printPeer(cmd, st, self)
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
			self, _, err := peerIdentity()
			if err != nil {
				return err
			}
			return printPeer(cmd, st, self)
		}),
	}
}

func printPeer(cmd *cobra.Command, st *store.Store, self store.PeerKey) error {
	v, err := peers.One(cmd.Context(), st, peers.KeyFilter(self))
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("session %s is not in the registry; run cci announce", self)
	}
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(cmd.OutOrStdout(), v.Line(st.Now()))
	return err
}

func newWhoCmd() *cobra.Command {
	var (
		f          peers.Filter
		pr         store.PR
		build, ref string
		asJSON     bool
	)
	cmd := &cobra.Command{
		Use:   "who",
		Short: "List live peer sessions on this machine and replicated nodes with the address that reaches each; every flag narrows",
		Args:  cobra.NoArgs,
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
			refs, err := refArgs(pr, build, ref)
			if err != nil {
				return err
			}
			if len(refs) > 1 {
				return errors.New("pass one of --pr, --build or --ref")
			}
			if len(refs) == 1 {
				f.Kind, f.Key = refs[0].Kind, refs[0].Key
			}
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
				for _, line := range v.ClaimLines(st.Now()) {
					if _, err := fmt.Fprintln(cmd.OutOrStdout(), line); err != nil {
						return err
					}
				}
			}
			return nil
		}),
	}
	fl := cmd.Flags()
	fl.StringVar(&f.Session, "session", "", "session id or its prefix")
	fl.StringVar(&f.Agent, "agent", "", "the subagent or teammate with this agent id or name")
	fl.StringVar(&f.Origin, "origin", "", "sessions on this node, by id or prefix; - for this machine's own")
	fl.StringVar(&f.Name, "name", "", "the harness name, as ListAgents prints it")
	fl.StringVar(&f.Role, "role", "", "root, lane, desk, watcher, codex or session")
	fl.StringVar(&f.Drive, "drive", "", "sessions bound to this drive")
	fl.StringVar(&f.Lane, "lane", "", "sessions bound as this lane")
	fl.StringVar(&f.Tag, "tag", "", "sessions announced with this tag")
	fl.StringVar(&f.Repo, "repo", "", "sessions in this repository, by owner/name or git common dir")
	fl.StringVar(&f.Worktree, "worktree", "", "sessions whose worktree or cwd is this path")
	fl.StringVar(&f.Branch, "branch", "", "sessions on this branch")
	fl.StringVarP(&f.Query, "query", "q", "", "substring of the name, description, tags, branch, lane, drive or cwd")
	fl.Var((*prFlag)(&pr), "pr", "sessions with a claim on this pull request, explicit or derived from their records: a number, owner/repo#N, or URL")
	fl.StringVar(&build, "build", "", "sessions with a claim on this build id or URL")
	fl.StringVar(&ref, "ref", "", "sessions with a claim on this <kind>:<key>, e.g. branch:yasyf/eng-1062 or thread:C0822AHFY3G/1759")
	fl.StringVar(&f.Rel, "rel", "", "only claims with this relation, e.g. opened, watches, started")
	fl.BoolVar(&f.All, "all", false, "include sessions whose process has ended")
	fl.BoolVar(&asJSON, "json", false, "one JSON object per peer")
	return cmd
}

func announceSelf(ctx context.Context, st *store.Store) error {
	self, window, err := peerIdentity()
	if err != nil {
		return err
	}
	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("resolve cwd: %w", err)
	}
	_, err = peers.Announce(ctx, st, peers.Options{Session: self.Session, Agent: self.Agent, Window: window, Cwd: cwd})
	return err
}

func peerIdentity() (store.PeerKey, store.Window, error) {
	self := selfPeer()
	if self.Session == "" {
		self.Session = os.Getenv(codexSessionEnv)
	}
	if self.Session == "" {
		return store.PeerKey{}, store.Window{}, errors.New("run this from a Claude Code or Codex session: neither " + sessionEnv + " nor " + codexSessionEnv + " is set")
	}
	window, err := thisWindow()
	if !errors.Is(err, errNoSession) {
		return self, window, err
	}
	if pid := procs.FindAncestor(os.Getpid(), isCodex); pid != 0 {
		window, err = windowOf(pid)
		return self, window, err
	}
	return self, store.Window{}, nil
}

func isCodex(argv []string) bool {
	for _, a := range argv {
		if filepath.Base(a) == "codex" {
			return true
		}
	}
	return false
}

type claimFlags struct {
	rel       string
	prs       []store.PR
	builds    []string
	branches  []string
	worktrees []string
	issues    []string
	threads   []string
	stacks    []string
	targets   []string
	systems   []string
	refs      []string
}

func (c *claimFlags) register(cmd *cobra.Command) {
	fl := cmd.Flags()
	fl.StringVar(&c.rel, "rel", "owns", "the relation: owns, opened, watches, reviews, started, holds, posted, or any verb")
	fl.Var(&prsFlag{&c.prs}, "pr", "pull requests as numbers, owner/repo#N, or URLs (repeatable)")
	fl.StringSliceVar(&c.builds, "build", nil, "build ids or URLs (repeatable)")
	fl.StringSliceVar(&c.branches, "branch", nil, "branch names (repeatable)")
	fl.StringSliceVar(&c.worktrees, "worktree", nil, "worktree paths (repeatable)")
	fl.StringSliceVar(&c.issues, "issue", nil, "issue keys such as ENG-1062 (repeatable)")
	fl.StringSliceVar(&c.threads, "thread", nil, "Slack threads as <channel>/<ts> (repeatable)")
	fl.StringSliceVar(&c.stacks, "stack", nil, "deploy stacks as <project>/<env> (repeatable)")
	fl.StringSliceVar(&c.targets, "target", nil, "release targets (repeatable)")
	fl.StringSliceVar(&c.systems, "subsystem", nil, "subsystems such as sandsql (repeatable)")
	fl.StringSliceVar(&c.refs, "ref", nil, "any other <kind>:<key> (repeatable)")
}

func (c *claimFlags) claims(self store.PeerKey) ([]store.Claim, error) {
	var out []store.Claim
	add := func(kind string, keys []string) {
		for _, key := range keys {
			out = append(out, store.Claim{Session: self.Session, Agent: self.Agent, Rel: c.rel, Kind: kind, Key: key})
		}
	}
	prs := make([]string, len(c.prs))
	for i, pr := range c.prs {
		prs[i] = pr.String()
	}
	add("pr", prs)
	add("build", c.builds)
	add("branch", c.branches)
	for _, wt := range c.worktrees {
		abs, err := filepath.Abs(wt)
		if err != nil {
			return nil, fmt.Errorf("--worktree %s: %w", wt, err)
		}
		add("worktree", []string{abs})
	}
	add("issue", c.issues)
	add("thread", c.threads)
	add("stack", c.stacks)
	add("target", c.targets)
	add("subsystem", c.systems)
	for _, ref := range c.refs {
		kind, key, err := store.ParseRef(ref)
		if err != nil {
			return nil, err
		}
		add(kind, []string{key})
	}
	if len(out) == 0 {
		return nil, errors.New("name at least one artifact: --pr, --build, --branch, --worktree, --issue, --thread, --stack, --target, --subsystem or --ref")
	}
	return out, nil
}

func refArgs(pr store.PR, build, ref string) ([]store.Claim, error) {
	var out []store.Claim
	if pr.Number != 0 {
		out = append(out, store.Claim{Kind: "pr", Key: pr.String()})
	}
	if build != "" {
		out = append(out, store.Claim{Kind: "build", Key: build})
	}
	if ref != "" {
		kind, key, err := store.ParseRef(ref)
		if err != nil {
			return nil, err
		}
		out = append(out, store.Claim{Kind: kind, Key: key})
	}
	return out, nil
}

func newClaimCmd() *cobra.Command {
	var cf claimFlags
	cmd := &cobra.Command{
		Use:   "claim",
		Short: "Tie this session to artifacts, so cci who --pr, --build or --ref finds it",
		Args:  cobra.NoArgs,
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
			self, _, err := peerIdentity()
			if err != nil {
				return err
			}
			claims, err := cf.claims(self)
			if err != nil {
				return err
			}
			if _, err := st.Peer(cmd.Context(), self); errors.Is(err, sql.ErrNoRows) {
				if err := announceSelf(cmd.Context(), st); err != nil {
					return err
				}
			} else if err != nil {
				return err
			}
			for _, c := range claims {
				got, err := st.Claim(cmd.Context(), c)
				if err != nil {
					return err
				}
				if _, err := fmt.Fprintf(cmd.OutOrStdout(), "%s %s\n", got.Rel, got.Ref()); err != nil {
					return err
				}
			}
			if cf.rel == string(kinds.Opened) {
				return postOpened(cmd.Context(), st, self, cf.prs)
			}
			return nil
		}),
	}
	cf.register(cmd)
	return cmd
}

func postOpened(ctx context.Context, st *store.Store, self store.PeerKey, prs []store.PR) error {
	b, ok, err := st.Binding(ctx, self)
	if err != nil {
		return err
	}
	if !ok && self.Agent != "" {
		if b, ok, err = st.Binding(ctx, store.PeerKey{Session: self.Session}); err != nil {
			return err
		}
		b.Lane = ""
	}
	if !ok {
		return nil
	}
	lane := b.Lane
	switch {
	case lane != "":
	case self.Agent == "":
		lane = "root"
	default:
		lane = self.Agent
	}
	for _, pr := range prs {
		r := store.Record{ID: "opened:" + pr.String(), Drive: b.Drive, Lane: lane, Kind: kinds.Opened, Text: "opened #" + pr.String(), Source: "claim", Refs: store.Refs{PRs: []store.PR{pr}}}
		if _, _, err := st.Post(ctx, r, 0); err != nil {
			return err
		}
	}
	return nil
}

func newUnclaimCmd() *cobra.Command {
	var cf claimFlags
	cmd := &cobra.Command{
		Use:   "unclaim",
		Short: "Drop this session's explicit claims on artifacts; derived ones stay with their records",
		Args:  cobra.NoArgs,
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
			self, _, err := peerIdentity()
			if err != nil {
				return err
			}
			claims, err := cf.claims(self)
			if err != nil {
				return err
			}
			for _, c := range claims {
				removed, err := st.Unclaim(cmd.Context(), c)
				if err != nil {
					return err
				}
				state := "removed"
				if !removed {
					state = "no such claim"
				}
				if _, err := fmt.Fprintf(cmd.OutOrStdout(), "%s %s %s\n", c.Rel, c.Ref(), state); err != nil {
					return err
				}
			}
			return nil
		}),
	}
	cf.register(cmd)
	return cmd
}

func resolveRecipients(ctx context.Context, st *store.Store, to []string) ([]string, error) {
	out := make([]string, len(to))
	for i, t := range to {
		target, ok := strings.CutPrefix(t, "peer:")
		if !ok {
			out[i] = t
			continue
		}
		origin, rest := store.SplitPeer(target)
		if origin == "" || origin == st.Self() {
			origin = peers.LocalOrigin
		}
		session, agent, _ := strings.Cut(rest, "/")
		v, err := peers.One(ctx, st, peers.Filter{Origin: origin, Session: session, Agent: agent, Root: agent == "", All: true})
		if errors.Is(err, sql.ErrNoRows) {
			v, err = peers.One(ctx, st, peers.Filter{Origin: origin, Name: session, Root: true, All: true})
			if err == nil && agent != "" {
				v, err = peers.One(ctx, st, peers.Filter{Origin: origin, Session: v.Session, Agent: agent, All: true})
			}
		}
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("--to %s: no session has that id prefix or name, or no agent of it that id or name; cci who --all lists them", t)
		}
		if err != nil {
			return nil, fmt.Errorf("--to %s: %w", t, err)
		}
		out[i] = store.PeerAddress(v.Key().String())
		if v.Origin != "" {
			out[i] = store.PeerAddressOn(v.Origin, v.Key().String())
		}
	}
	return out, nil
}
