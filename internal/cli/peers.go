package cli

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/yasyf/cc-interact/procs"

	"github.com/yasyf/cc-inbox/internal/harness"
	"github.com/yasyf/cc-inbox/internal/hook"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/peers"
	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
)

func newAnnounceCmd() *cobra.Command {
	var (
		role, description, lane, drive string
		tags                           []string
	)
	cmd := &cobra.Command{
		Use:   "announce",
		Short: "Register this session in the machine-wide peer registry so other sessions can find it with cci who",
		Args:  cobra.NoArgs,
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
			self, window, err := peerIdentity(cmd.Context())
			if err != nil {
				return err
			}
			cwd, err := getwd(cmd.Context())
			if err != nil {
				return fmt.Errorf("resolve cwd: %w", err)
			}
			if lane != "" {
				if err := claimLane(cmd.Context(), st, self, window, drive, lane); err != nil {
					return err
				}
			}
			if _, err := peers.Announce(cmd.Context(), st, peers.Options{Session: self.Session, Agent: self.Agent, Window: window, Cwd: cwd, Role: role, Description: description, Tags: tags, Speak: true}); err != nil {
				return err
			}
			return printPeer(cmd, st, self)
		}),
	}
	cmd.Flags().StringVar(&lane, "lane", "", "bind this thread to this lane, over any lane its posts or its drive binding gave it, so its hooks deliver and block for this lane alone")
	cmd.Flags().StringVar(&drive, "drive", "", "drive --lane binds on (default: the drive bound to this session, else $"+driveEnv+")")
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
			self, window, err := peerIdentity(cmd.Context())
			if err != nil {
				return err
			}
			if err := announceThisAgent(cmd.Context(), st, self, window); err != nil {
				return err
			}
			if err := bindNamedLane(cmd.Context(), st, self, window); err != nil {
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

func callerIdentity(ctx context.Context) map[string]string {
	env := map[string]string{}
	for _, name := range peers.IdentityEnv {
		if value := getenv(ctx, name); value != "" {
			env[name] = value
		}
	}
	return env
}

func announceThisAgent(ctx context.Context, st *store.Store, self store.PeerKey, window store.Window) error {
	if self.Agent == "" || window.PID == 0 {
		return nil
	}
	sessions, err := harness.Dir(ctx)
	if err != nil {
		return err
	}
	transcripts, err := filepath.Glob(filepath.Join(filepath.Dir(sessions), "projects", "*", self.Session+".jsonl"))
	if err != nil || len(transcripts) == 0 {
		return err
	}
	cwd, err := getwd(ctx)
	if err != nil {
		return fmt.Errorf("resolve cwd: %w", err)
	}
	p := hook.Payload{SessionID: self.Session, AgentID: self.Agent, TranscriptPath: transcripts[0], Cwd: cwd}
	return hook.AnnounceAgent(ctx, st, p, func() (store.Window, error) { return window, nil })
}

func bindNamedLane(ctx context.Context, st *store.Store, self store.PeerKey, window store.Window) error {
	lane, err := namedLane(ctx, st, self)
	if err != nil || lane == "" {
		return err
	}
	drive, err := resolveDrive(ctx, st, "")
	if errors.Is(err, store.ErrNoDrive) {
		return nil
	}
	if err != nil {
		return err
	}
	return st.BindLane(ctx, self, drive, lane, time.UnixMilli(window.Started))
}

func claimLane(ctx context.Context, st *store.Store, self store.PeerKey, window store.Window, drive, lane string) error {
	drive, err := resolveDrive(ctx, st, drive)
	if errors.Is(err, store.ErrNoDrive) {
		return fmt.Errorf("--lane %s needs a drive: pass --drive, set $%s, or run cci drive use", lane, driveEnv)
	}
	if err != nil {
		return err
	}
	if err := refuseHeldLane(ctx, st, self, window, drive, lane, st.Now()); err != nil {
		return err
	}
	return st.ClaimLane(ctx, self, drive, lane, time.UnixMilli(window.Started))
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
		f                            peers.Filter
		pr                           store.PR
		build, ref, successor, since string
		fields                       []string
		asJSON                       bool
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
			if successor != "" {
				return printSuccessor(cmd, st, f.Drive, successor, asJSON)
			}
			if len(refs) == 1 {
				f.Kind, f.Key = refs[0].Kind, refs[0].Key
			}
			if since != "" {
				seq, at, err := parseSince(cmd.Context(), st, since)
				if err != nil {
					return err
				}
				if seq != 0 || at.IsZero() {
					return fmt.Errorf("--since %q: name a duration, a clock time or an RFC3339 time, since sessions have no seq", since)
				}
				f.All, f.Since = true, at
			}
			if unknown := slices.DeleteFunc(slices.Clone(fields), func(k string) bool { return slices.Contains(viewKeys, k) }); len(unknown) > 0 {
				return fmt.Errorf("--fields: no peer key %s; the keys are %s", strings.Join(unknown, ", "), strings.Join(viewKeys, ","))
			}
			views, err := peers.Resolve(cmd.Context(), st, f)
			if err != nil {
				return err
			}
			if err := markStoodDown(cmd.Context(), st, views); err != nil {
				return err
			}
			if asJSON {
				return writePeersJSON(cmd.OutOrStdout(), views, fields)
			}
			spawns, err := unboundSpawns(cmd.Context(), st, f)
			if err != nil {
				return err
			}
			for _, sp := range spawns {
				task := ""
				if sp.Task != "" {
					task = " task=" + sp.Task
				}
				if _, err := fmt.Fprintf(cmd.OutOrStdout(), "%s spawned drive=%s lane=%s by #%d %s brief=%s model=%s%s, not bound yet\n", sp.Lane, sp.Drive, sp.Lane, sp.Seq, render.Stamp(sp.At, st.Now()), sp.Brief, sp.Model, task); err != nil {
					return err
				}
			}
			if len(views) == 0 && len(spawns) == 0 {
				_, err := fmt.Fprintln(cmd.OutOrStdout(), "no matching peers; cci who --all includes sessions that have ended")
				return err
			}
			unnarrowed := f == peers.Filter{Drive: f.Drive}
			var agents, idle int
			for _, v := range views {
				switch {
				case unnarrowed && v.Lane == "" && (v.Role == peers.RoleAgent || v.Role == peers.RoleWorkflow):
					agents++
					continue
				case unnarrowed && st.Now().Sub(v.LastSeen()) > time.Hour:
					idle++
					continue
				}
				if _, err := fmt.Fprintln(cmd.OutOrStdout(), v.Line(st.Now())); err != nil {
					return err
				}
				for _, line := range v.ClaimLines(st.Now()) {
					if _, err := fmt.Fprintln(cmd.OutOrStdout(), line); err != nil {
						return err
					}
				}
			}
			if agents+idle == 0 {
				return nil
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "+%d more: %d subagents or workflow agents holding no lane, %d idle over an hour; --role agent, --role workflow or --all lists them\n", agents+idle, agents, idle)
			return err
		}),
	}
	fl := cmd.Flags()
	fl.StringVar(&f.Session, "session", "", "session id or its prefix")
	fl.StringVar(&f.Agent, "agent", "", "the subagent or teammate with this agent id or name")
	fl.StringVar(&f.Origin, "origin", "", "sessions on this node, by id or prefix; - for this machine's own")
	fl.StringVar(&f.Name, "name", "", "the harness name, as ListAgents prints it")
	fl.StringVar(&f.Role, "role", "", "root, lane, desk, watcher, codex, agent, workflow or session; workflow lists Workflow tool agents by label")
	fl.StringVar(&f.Drive, "drive", "", "sessions bound to this drive")
	fl.StringVar(&f.Alias, "lane", "", "sessions bound as this lane, or under this alias")
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
	fl.StringVar(&since, "since", "", "include sessions whose process has ended that were last seen since this time: a duration back (24h), a Pacific clock time (9:00 AM) or an RFC3339 time")
	fl.StringSliceVar(&fields, "fields", nil, "with --json, only these keys of each peer, e.g. lane,live,seen_at; env prints only when named")
	fl.StringVar(&successor, "successor-of", "", "print the lane that now stands in for this lane on --drive (default: this session's drive), following every stand-down that named a successor, or nothing when it never stood down with one; with --json, one object naming the drive, the lane, whether it stood down, its successor and the chain")
	fl.BoolVar(&asJSON, "json", false, "one JSON object per peer")
	return cmd
}

// unboundSpawns lists the lanes spawn records started on the filter's drive
// that no thread has bound yet, in the last hour or, with --all, ever, or with
// --since since then, when the filter narrows by nothing but drive, lane and
// the lane role.
func unboundSpawns(ctx context.Context, st *store.Store, f peers.Filter) ([]store.Spawn, error) {
	if f.Session != "" || f.Agent != "" || f.Origin != "" || f.Name != "" || f.Role != "" && f.Role != peers.RoleLane || f.Tag != "" || f.Repo != "" || f.Worktree != "" || f.Branch != "" || f.Query != "" || f.Root || f.Kind != "" || f.Key != "" || f.Rel != "" {
		return nil, nil
	}
	drive, err := resolveDrive(ctx, st, f.Drive)
	if err != nil {
		return nil, nil
	}
	since := st.Now().Add(-time.Hour)
	if f.All {
		since = f.Since
	}
	spawns, err := st.UnboundSpawns(ctx, drive, since)
	return slices.DeleteFunc(spawns, func(sp store.Spawn) bool { return f.Alias != "" && !store.AnswersTo(sp.Lane, f.Alias) }), err
}

var viewKeys = jsonKeys(reflect.TypeFor[peers.View]())

func jsonKeys(t reflect.Type) []string {
	var keys []string
	for i := range t.NumField() {
		field := t.Field(i)
		if field.Anonymous {
			keys = append(keys, jsonKeys(field.Type)...)
			continue
		}
		if name, _, _ := strings.Cut(field.Tag.Get("json"), ","); name != "" && name != "-" {
			keys = append(keys, name)
		}
	}
	return keys
}

func writePeersJSON(w io.Writer, views []peers.View, fields []string) error {
	enc := json.NewEncoder(w)
	for _, v := range views {
		if !slices.Contains(fields, "env") {
			v.Env = nil
		}
		if len(fields) == 0 {
			if err := enc.Encode(v); err != nil {
				return err
			}
			continue
		}
		raw, err := json.Marshal(v)
		if err != nil {
			return err
		}
		var all map[string]json.RawMessage
		if err := json.Unmarshal(raw, &all); err != nil {
			return err
		}
		picked := make(map[string]json.RawMessage, len(fields))
		for _, k := range fields {
			if value, ok := all[k]; ok {
				picked[k] = value
			}
		}
		if err := enc.Encode(picked); err != nil {
			return err
		}
	}
	return nil
}

func markStoodDown(ctx context.Context, st *store.Store, views []peers.View) error {
	drives := map[string]store.StandDowns{}
	for i, v := range views {
		if v.Lane == "" || v.Drive == "" {
			continue
		}
		downs, ok := drives[v.Drive]
		if !ok {
			var err error
			if downs, err = st.StandDowns(ctx, v.Drive); err != nil {
				return err
			}
			drives[v.Drive] = downs
		}
		if down := downs[v.Lane]; down.Seq != 0 {
			views[i].StoodDown = !v.LastSeen().After(down.At)
		}
		if chain := downs.Chain(v.Lane); len(chain) > 0 {
			views[i].Successor = chain[len(chain)-1]
		}
	}
	return nil
}

func printSuccessor(cmd *cobra.Command, st *store.Store, drive, lane string, asJSON bool) error {
	drive, err := resolveDrive(cmd.Context(), st, drive)
	if err != nil {
		return err
	}
	chain, standDown, err := st.Successors(cmd.Context(), drive, lane)
	if err != nil {
		return err
	}
	successor := ""
	if len(chain) > 0 {
		successor = chain[len(chain)-1]
	}
	if asJSON {
		if chain == nil {
			chain = []string{}
		}
		return json.NewEncoder(cmd.OutOrStdout()).Encode(struct {
			Drive     string   `json:"drive"`
			Lane      string   `json:"lane"`
			StoodDown bool     `json:"stood_down"`
			Record    int64    `json:"record,omitempty"`
			Successor string   `json:"successor"`
			Chain     []string `json:"chain"`
		}{drive, lane, standDown != 0, standDown, successor, chain})
	}
	if successor == "" {
		return nil
	}
	_, err = fmt.Fprintln(cmd.OutOrStdout(), successor)
	return err
}

func announceSelf(ctx context.Context, st *store.Store) error {
	self, window, err := peerIdentity(ctx)
	if err != nil {
		return err
	}
	cwd, err := getwd(ctx)
	if err != nil {
		return fmt.Errorf("resolve cwd: %w", err)
	}
	_, err = peers.Announce(ctx, st, peers.Options{Env: callerIdentity(ctx), Session: self.Session, Agent: self.Agent, Window: window, Cwd: cwd})
	return err
}

func peerIdentity(ctx context.Context) (store.PeerKey, store.Window, error) {
	self := SelfPeer(ctx)
	if self.Session == "" {
		return store.PeerKey{}, store.Window{}, errNoPeerSession
	}
	window, err := thisWindow(ctx)
	if !errors.Is(err, errNoSession) {
		return self, window, err
	}
	pid := os.Getpid()
	if inv := invoked(ctx); inv != nil {
		pid = inv.pid
	}
	if pid := procs.FindAncestor(pid, isCodex); pid != 0 {
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
	fl.StringVar(&c.forLane, "for", "", "the lane whose claims these are, as <lane>@<drive>, instead of this session's: the one live local session bound as that lane in that drive, else the one live teammate named that lane with no lane yet, which it binds, else the newest ended session bound as that lane, such as a codex lane's")
	fl.StringVar(&c.rel, "rel", relOwns, "the relation: owns, opened, watches, reviews, started, holds, posted, or any verb")
	fl.Var(&prsFlag{prs: &c.prs}, "pr", "pull requests as numbers, owner/repo#N, or URLs (repeatable)")
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
		self, _, err := peerIdentity(ctx)
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
	if len(views) == 0 {
		ended, err := peers.Resolve(ctx, st, peers.Filter{Origin: peers.LocalOrigin, Drive: drive, Lane: lane, All: true})
		if err != nil {
			return store.PeerKey{}, err
		}
		if len(ended) > 0 {
			return slices.MaxFunc(ended, func(a, b peers.View) int { return a.AnnouncedAt.Compare(b.AnnouncedAt) }).Key(), nil
		}
	}
	switch len(views) {
	case 1:
		return views[0].Key(), nil
	case 0:
		return store.PeerKey{}, fmt.Errorf("--for %s: no session on this machine was ever bound as lane %s in drive %s, and no one live teammate is named %s with no lane yet", c.forLane, lane, drive, lane)
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

func resolveRecipients(ctx context.Context, st *store.Store, drive string, to []string, kind kinds.Kind, force bool, notices io.Writer) ([]string, error) {
	out := make([]string, len(to))
	var reading map[string][]string
	for i, t := range to {
		var err error
		switch target, ok := strings.CutPrefix(t, "peer:"); {
		case ok:
			out[i], err = resolvePeer(ctx, st, t, target)
		case strings.Contains(t, "@") || store.RootName(t) || t == "owner" || t == "all":
			out[i] = t
		default:
			if reading == nil {
				var lanes []string
				for _, candidate := range to {
					if strings.HasPrefix(candidate, "peer:") || strings.Contains(candidate, "@") || store.RootName(candidate) || candidate == "owner" || candidate == "all" {
						continue
					}
					lanes = append(lanes, candidate)
				}
				reading, err = laneDrives(ctx, st, lanes)
			}
			var reach store.Reach
			registry := true
			if err == nil {
				if reach, err = st.Reach(ctx, drive, t); errors.Is(err, store.ErrNoLaneRegistry) {
					registry, err = false, nil
				}
			}
			if err == nil && registry && kind != kinds.StandDown {
				err = refuseRetired(ctx, st, drive, t, reach)
			}
			if err == nil && !force && len(reading[t]) == 0 && (!registry || !reach.Started && (t != reach.Stem || len(reach.Holders) == 0)) {
				err = mistypedLane(ctx, st, drive, t)
			}
			if err == nil {
				readers := reading[t]
				if registry && reach.Lane != "" && reach.Lane != t {
					readers = []string{drive}
				}
				out[i], err = routeLane(t, drive, readers, notices)
			}
			if err == nil && registry && out[i] == t && kind != kinds.StandDown && (st.OwnerNode() || reach.Lane != "") {
				err = noticeReach(notices, t, reach)
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
	if self := SelfPeer(ctx); errors.Is(err, sql.ErrNoRows) && origin == peers.LocalOrigin && self.Session != "" {
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

func laneDrives(ctx context.Context, st *store.Store, lanes []string) (map[string][]string, error) {
	views, err := peers.ResolveLanes(ctx, st, lanes)
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

// RetiredLane refuses a post addressed to a lane that stood down, so no order
// waits in a mailbox nobody reads.
type RetiredLane struct{ line, to, down, heir string }

func (e *RetiredLane) Error() string { return e.line }

func refuseRetired(ctx context.Context, st *store.Store, drive, to string, reach store.Reach) error {
	if reach.Lane != "" && (!reach.Down || to == reach.Stem) {
		return nil
	}
	lane, seq, err := st.LastDown(ctx, drive, to)
	if err != nil || seq == 0 {
		return err
	}
	down, err := st.Get(ctx, seq)
	if err != nil {
		return err
	}
	heir, advice := "root", "root"
	if reach.Lane != "" {
		heir, advice = reach.Lane, "its successor "+reach.Lane+", or to root"
	}
	at := fmt.Sprintf("%s stood down at %s (#%d)", lane, render.Stamp(down.At, st.Now()), seq)
	return &RetiredLane{line: fmt.Sprintf("REFUSED: --to %s: %s; post to %s", to, at, advice), to: to, down: at, heir: heir}
}

func noticeReach(w io.Writer, to string, r store.Reach) error {
	var line string
	switch {
	case to == r.Stem && len(r.Holders) > 1 && (!r.Started || !r.Down):
		readers := []string{r.Holders[0]}
		if r.Started && to != r.Holders[0] {
			readers = []string{to, r.Holders[0]}
		}
		line = fmt.Sprintf("%s run under %s at once; this reaches %s", strings.Join(r.Holders, ", "), r.Stem, strings.Join(readers, " and "))
	case r.Started && !r.Down:
	case r.Down:
		line = fmt.Sprintf("%s stood down, so this reaches %s", to, r.Lane)
	case to == r.Stem && len(r.Holders) == 0:
		line = fmt.Sprintf("no lane holds %s; the first lane to register as %s or %s-<n> reads this", to, to, to)
	case to != r.Stem && len(r.Holders) > 0:
		line = fmt.Sprintf("no lane has registered as %s; %s holds %s, which --to %s reaches, and %s reads this once it registers", to, r.Holders[0], r.Stem, r.Stem, to)
	case to != r.Stem:
		line = fmt.Sprintf("no lane has registered as %s or holds %s; %s reads this once it registers", to, r.Stem, to)
	}
	if line == "" {
		return nil
	}
	_, err := fmt.Fprintf(w, "cci: --to %s: %s\n", to, line)
	return err
}

func mistypedLane(ctx context.Context, st *store.Store, drive, lane string) error {
	ok, err := st.Registered(ctx, drive, lane)
	if err != nil || ok {
		return err
	}
	reading, err := laneDrives(ctx, st, nil)
	if err != nil {
		return err
	}
	near := slices.DeleteFunc(slices.Collect(maps.Keys(reading)), func(live string) bool {
		return !mistyped(lane, live) || store.Stem(live) == lane && slices.Contains(reading[live], drive)
	})
	if len(near) == 0 {
		return nil
	}
	if ok, err := st.NamedSuccessor(ctx, drive, lane); err != nil || ok {
		return err
	}
	slices.Sort(near)
	nearest := slices.MinFunc(near, func(a, b string) int {
		return cmp.Compare(distance(lane, a), distance(lane, b))
	})
	return fmt.Errorf("--to %s: no lane on drive %s has registered as %s, which looks like a slip for the live lane %s; --force addresses %s as typed", lane, drive, lane, nearest, lane)
}

var (
	digits     = regexp.MustCompile(`\d+`)
	generation = regexp.MustCompile(`^(.*)-(\d+)$`)
)

func mistyped(lane, live string) bool {
	if digits.ReplaceAllString(lane, "#") == digits.ReplaceAllString(live, "#") {
		family, n, ok := numbered(lane)
		liveFamily, liveN, liveOK := numbered(live)
		return ok && liveOK && family == liveFamily && n < liveN
	}
	return distance(lane, live) <= min(2, len(lane)/4)
}

func numbered(lane string) (string, int, bool) {
	m := generation.FindStringSubmatch(lane)
	if m == nil {
		return "", 0, false
	}
	n, err := strconv.Atoi(m[2])
	return m[1], n, err == nil
}

func distance(a, b string) int {
	prev := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := range len(a) {
		cur := make([]int, len(b)+1)
		cur[0] = i + 1
		for j := range len(b) {
			cost := 1
			if a[i] == b[j] {
				cost = 0
			}
			cur[j+1] = min(prev[j+1]+1, cur[j]+1, prev[j]+cost)
		}
		prev = cur
	}
	return prev[len(b)]
}
