package cli

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"github.com/yasyf/cc-interact/procs"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/peers"
	"github.com/yasyf/cc-inbox/internal/store"
)

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
		Short: "Print this session's registry line, the one to paste into a handoff, and its channel subscription",
		Args:  cobra.NoArgs,
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
			self, _, err := peerIdentity()
			if err != nil {
				return err
			}
			if err := printPeer(cmd, st, self); err != nil {
				return err
			}
			sub, ok, err := st.SessionSubscription(cmd.Context(), self.Session)
			if err != nil || !ok {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "channel subscription of window %d: %s\n", sub.Window.PID, describe(sub))
			return err
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
	fl.StringVar(&f.Role, "role", "", "root, lane, desk, watcher, codex, agent, workflow or session; workflow lists Workflow tool agents by label")
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
		return store.PeerKey{}, store.Window{}, errNoPeerSession
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

const relOwns = "owns"

type claimFlags struct {
	forLane   string
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
	fl.StringVar(&c.forLane, "for", "", "the lane whose claims these are, as <lane>@<drive>, instead of this session's: the one live local session bound as that lane in that drive, else the one live teammate named that lane with no lane yet, which it binds")
	fl.StringVar(&c.rel, "rel", relOwns, "the relation: owns, opened, watches, reviews, started, holds, posted, or any verb")
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

func (c *claimFlags) holder(ctx context.Context, st *store.Store) (store.PeerKey, error) {
	if c.forLane == "" {
		self, _, err := peerIdentity()
		return self, err
	}
	lane, drive, ok := strings.Cut(c.forLane, "@")
	if !ok || lane == "" || drive == "" {
		return store.PeerKey{}, fmt.Errorf("--for %s: name the lane as <lane>@<drive>", c.forLane)
	}
	views, err := peers.Resolve(ctx, st, peers.Filter{Origin: peers.LocalOrigin, Drive: drive, Lane: lane})
	if err != nil {
		return store.PeerKey{}, err
	}
	if len(views) == 0 {
		named, err := peers.Resolve(ctx, st, peers.Filter{Origin: peers.LocalOrigin, Agent: lane})
		if err != nil {
			return store.PeerKey{}, err
		}
		var unbound []peers.View
		for _, v := range named {
			if v.Root || v.Lane != "" || v.Drive != "" && v.Drive != drive {
				continue
			}
			_, b, owned, err := st.Owner(ctx, v.Key())
			if err != nil {
				return store.PeerKey{}, err
			}
			if !owned || b.Drive == drive {
				unbound = append(unbound, v)
			}
		}
		if len(unbound) == 1 {
			return unbound[0].Key(), st.BindLane(ctx, unbound[0].Key(), drive, lane, unbound[0].AnnouncedAt)
		}
	}
	switch len(views) {
	case 1:
		return views[0].Key(), nil
	case 0:
		return store.PeerKey{}, fmt.Errorf("--for %s: no live session on this machine is bound as lane %s in drive %s, and no one live teammate is named %s with no lane yet; cci who --all --drive %s --lane %s lists ended ones", c.forLane, lane, drive, lane, drive, lane)
	}
	keys := make([]string, len(views))
	for i, v := range views {
		keys[i] = v.Key().String()
	}
	return store.PeerKey{}, fmt.Errorf("--for %s: %d live sessions are bound as that lane (%s); claim from the lane's own session", c.forLane, len(views), strings.Join(keys, ", "))
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
		Short: "Tie this session, or with --for another lane's, to artifacts, so cci who --pr, --build or --ref finds it",
		Args:  cobra.NoArgs,
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
			holder, err := cf.holder(cmd.Context(), st)
			if err != nil {
				return err
			}
			claims, err := cf.claims(holder)
			if err != nil {
				return err
			}
			if _, err := st.Peer(cmd.Context(), holder); errors.Is(err, sql.ErrNoRows) {
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
			switch {
			case cf.rel == string(kinds.Opened):
				return postOpened(cmd.Context(), st, holder, cf.prs)
			case cf.rel == relOwns && cf.forLane != "":
				return postAssigned(cmd.Context(), st, holder, cf.prs)
			}
			return nil
		}),
	}
	cf.register(cmd)
	return cmd
}

func postOpened(ctx context.Context, st *store.Store, holder store.PeerKey, prs []store.PR) error {
	drive, lane, ok, err := ownerLane(ctx, st, holder)
	if err != nil || !ok {
		return err
	}
	for _, pr := range prs {
		r := store.Record{ID: "opened:" + pr.String(), Drive: drive, Lane: lane, Kind: kinds.Opened, Text: "opened #" + pr.String(), Source: "claim", Refs: store.Refs{PRs: []store.PR{pr}}}
		if _, _, err := st.Post(ctx, r, 0); err != nil {
			return err
		}
	}
	return nil
}

func postAssigned(ctx context.Context, st *store.Store, holder store.PeerKey, prs []store.PR) error {
	drive, lane, ok, err := ownerLane(ctx, st, holder)
	if err != nil || !ok || len(prs) == 0 {
		return err
	}
	names := make([]string, len(prs))
	for i, pr := range prs {
		names[i] = "#" + pr.String()
	}
	_, _, err = st.Post(ctx, store.Record{Drive: drive, Lane: lane, Kind: kinds.Claim, Text: store.Clip("claim: " + lane + " owns " + strings.Join(names, " ")), Source: "claim", Refs: store.Refs{PRs: prs}}, 0)
	return err
}

func ownerLane(ctx context.Context, st *store.Store, k store.PeerKey) (drive, lane string, ok bool, err error) {
	_, b, ok, err := st.Owner(ctx, k)
	return b.Drive, cmp.Or(b.Lane, "root"), ok, err
}

func newUnclaimCmd() *cobra.Command {
	var cf claimFlags
	cmd := &cobra.Command{
		Use:   "unclaim",
		Short: "Drop this session's, or with --for another lane's, explicit claims on artifacts; derived ones stay with their records",
		Args:  cobra.NoArgs,
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
			holder, err := cf.holder(cmd.Context(), st)
			if err != nil {
				return err
			}
			claims, err := cf.claims(holder)
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

func resolveRecipients(ctx context.Context, st *store.Store, drive string, to []string, notices io.Writer) ([]string, error) {
	out := make([]string, len(to))
	var reading map[string][]string
	for i, t := range to {
		var err error
		switch target, ok := strings.CutPrefix(t, "peer:"); {
		case ok:
			out[i], err = resolvePeer(ctx, st, t, target)
		case strings.Contains(t, "@") || store.RootName(t):
			out[i] = t
		default:
			if reading == nil {
				reading, err = laneDrives(ctx, st)
			}
			if err == nil {
				out[i], err = routeLane(t, drive, reading[t], notices)
			}
		}
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func resolvePeer(ctx context.Context, st *store.Store, t, target string) (string, error) {
	origin, rest := store.SplitPeer(target)
	if origin == "" || origin == st.Self() {
		origin = peers.LocalOrigin
	}
	session, agent, _ := strings.Cut(rest, "/")
	v, err := onePeer(ctx, st, peers.Filter{Origin: origin, Session: session, Agent: agent, Root: agent == ""})
	if errors.Is(err, sql.ErrNoRows) {
		v, err = onePeer(ctx, st, peers.Filter{Origin: origin, Name: session, Root: true})
		if err == nil && agent != "" {
			v, err = onePeer(ctx, st, peers.Filter{Origin: origin, Session: v.Session, Agent: agent})
		}
	}
	if self := selfPeer(); errors.Is(err, sql.ErrNoRows) && origin == peers.LocalOrigin && self.Session != "" {
		v, err = onePeer(ctx, st, peers.Filter{Origin: origin, Session: self.Session, Agent: rest})
	}
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("--to %s: no session has that id prefix or name, no agent of it that id or name, and no agent of this session that id or name; cci who --all lists them", t)
	}
	if err != nil {
		return "", fmt.Errorf("--to %s: %w", t, err)
	}
	if v.Origin != "" {
		return store.PeerAddressOn(v.Origin, v.Key().String()), nil
	}
	return store.PeerAddress(v.Key().String()), nil
}

// onePeer resolves f among live peers first, so a workflow label reused by a
// finished run still names the running agent, then among ended ones.
func onePeer(ctx context.Context, st *store.Store, f peers.Filter) (peers.View, error) {
	v, err := peers.One(ctx, st, f)
	if !errors.Is(err, sql.ErrNoRows) {
		return v, err
	}
	f.All = true
	return peers.One(ctx, st, f)
}

func laneDrives(ctx context.Context, st *store.Store) (map[string][]string, error) {
	views, err := peers.Resolve(ctx, st, peers.Filter{Origin: peers.LocalOrigin})
	if err != nil {
		return nil, err
	}
	out := map[string][]string{}
	for _, v := range views {
		if v.Lane != "" && v.Drive != "" && !slices.Contains(out[v.Lane], v.Drive) {
			out[v.Lane] = append(out[v.Lane], v.Drive)
		}
	}
	return out, nil
}

func routeLane(lane, drive string, reading []string, notices io.Writer) (string, error) {
	switch {
	case len(reading) == 0 || slices.Contains(reading, drive):
		return lane, nil
	case len(reading) > 1:
		slices.Sort(reading)
		return "", fmt.Errorf("--to %s: no live %s reads drive %s, and the live ones read %s; address one as %s@<drive>", lane, lane, drive, strings.Join(reading, ", "), lane)
	}
	addr := lane + "@" + reading[0]
	_, err := fmt.Fprintf(notices, "cci: --to %s: %s reads drive %s, not %s; addressed %s\n", lane, lane, reading[0], drive, addr)
	return addr, err
}
