// Package peers is the session registry seen from one session: announcing
// this one, and resolving the others to something a message can reach.
package peers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/yasyf/cc-inbox/internal/gitinfo"
	"github.com/yasyf/cc-inbox/internal/harness"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
)

// StaleAfter is how long a peer with no process to check stays live after its last hook call.
const StaleAfter = 15 * time.Minute

// IdentityEnv names the environment variables an announce copies into the peer's env.
var IdentityEnv = []string{"ORCA_TERMINAL_HANDLE", "ORCA_LAUNCH_RUN", "CLAUDE_LONG_RUNNING_DRIVE", "CODEX_THREAD_ID"}

const (
	RoleRoot    = "root"
	RoleLane    = "lane"
	RoleSession = "session"
)

// Options describes the announcing session. An empty Role is inferred from
// its drive binding; Cwd is where git is read.
type Options struct {
	Session     string
	Window      store.Window
	Cwd         string
	Role        string
	Description string
	Tags        []string
	Speak       bool
}

// Announce records the session in the registry and returns its row. With
// Speak, a session bound to a drive also broadcasts a peer record there, so
// a root subscribed with --kind peer hears it join.
func Announce(ctx context.Context, st *store.Store, o Options) (store.Peer, error) {
	p, err := announce(ctx, st, o)
	if err != nil || !o.Speak || p.Drive == "" {
		return p, err
	}
	v, err := One(ctx, st, Filter{Session: p.Session, All: true})
	if err != nil {
		return store.Peer{}, err
	}
	_, _, err = st.Post(ctx, store.Record{Drive: p.Drive, Lane: RegistryLane, Kind: kinds.Peer, Text: store.Clip(v.Line(st.Now())), Source: "post"}, 0)
	return p, err
}

// RegistryLane writes the peer broadcasts, so a root's own announce still
// reaches its reader, which drops its own lane's broadcasts.
const RegistryLane = "registry"

func announce(ctx context.Context, st *store.Store, o Options) (store.Peer, error) {
	cwd, err := filepath.EvalSymlinks(o.Cwd)
	if err != nil {
		return store.Peer{}, fmt.Errorf("resolve cwd: %w", err)
	}
	info, err := gitinfo.Read(cwd)
	if err != nil {
		return store.Peer{}, err
	}
	role := o.Role
	if role == "" {
		role, err = inferRole(ctx, st, o.Session)
		if err != nil {
			return store.Peer{}, err
		}
	}
	env := map[string]string{}
	for _, name := range IdentityEnv {
		if v := os.Getenv(name); v != "" {
			env[name] = v
		}
	}
	return st.Announce(ctx, store.Peer{
		Session:     o.Session,
		Role:        role,
		Description: o.Description,
		Tags:        o.Tags,
		Cwd:         info.Cwd,
		Repo:        info.Repo,
		Worktree:    info.Worktree,
		Branch:      info.Branch,
		Remote:      info.Remote,
		PID:         o.Window.PID,
		PIDStarted:  o.Window.Started,
		Env:         env,
	})
}

func inferRole(ctx context.Context, st *store.Store, session string) (string, error) {
	existing, err := st.Peer(ctx, session)
	if err == nil && existing.Role != "" {
		return "", nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	b, ok, err := st.Binding(ctx, session)
	switch {
	case err != nil:
		return "", err
	case ok && b.Root:
		return RoleRoot, nil
	case ok && b.Lane != "":
		return RoleLane, nil
	}
	return RoleSession, nil
}

// View is a peer joined with its harness window, if the harness knows one,
// and with the claims a ref selector matched.
type View struct {
	store.Peer
	Name     string        `json:"name,omitempty"`
	Address  string        `json:"address,omitempty"`
	Status   string        `json:"status,omitempty"`
	StatusAt time.Time     `json:"status_at,omitzero"`
	Live     bool          `json:"live"`
	Claims   []store.Claim `json:"claims,omitempty"`
}

// Filter selects peers. Every set field must match; Session matches a uuid
// prefix. Kind, Key and Rel select by claim, and a matched peer carries the
// claims that selected it.
type Filter struct {
	Session  string
	Name     string
	Role     string
	Drive    string
	Lane     string
	Tag      string
	Repo     string
	Worktree string
	Branch   string
	Query    string
	Kind     string
	Key      string
	Rel      string
	All      bool
}

func (f Filter) byClaim() bool {
	return f.Kind != "" || f.Key != "" || f.Rel != ""
}

// Resolve joins the registry with the harness windows and applies f. Without
// All, only live peers are returned.
func Resolve(ctx context.Context, st *store.Store, f Filter) ([]View, error) {
	rows, err := st.Peers(ctx)
	if err != nil {
		return nil, err
	}
	var claimed map[string][]store.Claim
	if f.byClaim() {
		claims, err := st.Claims(ctx, store.ClaimFilter{Rel: f.Rel, Kind: f.Kind, Key: f.Key})
		if err != nil {
			return nil, err
		}
		claimed = make(map[string][]store.Claim, len(claims))
		for _, c := range claims {
			claimed[c.Session] = append(claimed[c.Session], c)
		}
	}
	dir, err := harness.Dir()
	if err != nil {
		return nil, err
	}
	windows, err := harness.Sessions(dir)
	if err != nil {
		return nil, err
	}
	now := st.Now()
	views := make([]View, 0, len(rows)+len(windows))
	for _, p := range rows {
		v := View{Peer: p, Live: live(p, now), Claims: claimed[p.Session]}
		if w, ok := windows[p.Session]; ok {
			v.Name, v.Address, v.Status, v.Live = w.Name, w.Address(), w.Status, w.Alive() && !p.Superseded
			v.StatusAt = time.UnixMilli(w.StatusUpdatedAt).UTC()
			if v.Cwd == "" {
				v.Cwd = w.Cwd
			}
			delete(windows, p.Session)
		}
		views = append(views, v)
	}
	for _, w := range windows {
		views = append(views, View{
			Peer:     store.Peer{Session: w.SessionID, Role: RoleSession, Cwd: w.Cwd, PID: w.PID, SeenAt: time.UnixMilli(w.StatusUpdatedAt).UTC()},
			Name:     w.Name,
			Address:  w.Address(),
			Status:   w.Status,
			StatusAt: time.UnixMilli(w.StatusUpdatedAt).UTC(),
			Live:     w.Alive(),
			Claims:   claimed[w.SessionID],
		})
	}
	views = slices.DeleteFunc(views, func(v View) bool { return !f.matches(v) })
	slices.SortFunc(views, func(a, b View) int {
		if a.Live != b.Live {
			if a.Live {
				return -1
			}
			return 1
		}
		return strings.Compare(b.lastSeen().Format(time.RFC3339Nano), a.lastSeen().Format(time.RFC3339Nano))
	})
	return views, nil
}

func live(p store.Peer, now time.Time) bool {
	switch {
	case p.Superseded:
		return false
	case p.PID != 0:
		return harness.ProcessAlive(p.PID, p.PIDStarted)
	}
	return now.Sub(p.SeenAt) < StaleAfter
}

func (f Filter) matches(v View) bool {
	if !f.All && !v.Live || f.byClaim() && len(v.Claims) == 0 {
		return false
	}
	worktree := f.Worktree
	if worktree != "" {
		worktree, _ = filepath.Abs(worktree)
		if real, err := filepath.EvalSymlinks(worktree); err == nil {
			worktree = real
		}
	}
	q := strings.ToLower(f.Query)
	return (f.Session == "" || strings.HasPrefix(v.Session, f.Session)) &&
		(f.Name == "" || v.Name == f.Name) &&
		(f.Role == "" || v.Role == f.Role) &&
		(f.Drive == "" || v.Drive == f.Drive) &&
		(f.Lane == "" || v.Lane == f.Lane) &&
		(f.Tag == "" || slices.Contains(v.Tags, f.Tag)) &&
		(f.Repo == "" || v.Repo == f.Repo || v.Remote == f.Repo) &&
		(worktree == "" || v.Worktree == worktree || v.Cwd == worktree) &&
		(f.Branch == "" || v.Branch == f.Branch) &&
		(q == "" || strings.Contains(strings.ToLower(strings.Join(append([]string{v.Name, v.Description, v.Branch, v.Lane, v.Drive, v.Cwd}, v.Tags...), " ")), q))
}

func (v View) lastSeen() time.Time {
	if v.StatusAt.After(v.SeenAt) {
		return v.StatusAt
	}
	return v.SeenAt
}

// Line renders one peer for a terminal or a handoff note. Its trailing
// address is the string SendMessage accepts.
func (v View) Line(now time.Time) string {
	var b strings.Builder
	name := v.Name
	if name == "" {
		name = "-"
	}
	fmt.Fprintf(&b, "%s %s", name, v.Role)
	if v.Drive != "" {
		fmt.Fprintf(&b, " drive=%s", v.Drive)
	}
	if v.Lane != "" {
		fmt.Fprintf(&b, " lane=%s", v.Lane)
	}
	if v.Branch != "" {
		fmt.Fprintf(&b, " branch=%s", v.Branch)
	}
	switch {
	case v.Worktree != "":
		fmt.Fprintf(&b, " wt=%s", v.Worktree)
	case v.Cwd != "":
		fmt.Fprintf(&b, " cwd=%s", v.Cwd)
	}
	switch {
	case !v.Live:
		fmt.Fprintf(&b, " gone, last seen %s", render.Stamp(v.lastSeen(), now))
	case v.Status != "":
		fmt.Fprintf(&b, " %s %s", v.Status, age(now.Sub(v.lastSeen())))
	default:
		fmt.Fprintf(&b, " seen %s", age(now.Sub(v.lastSeen())))
	}
	if v.Description != "" {
		fmt.Fprintf(&b, " %q", v.Description)
	}
	for _, t := range v.Tags {
		fmt.Fprintf(&b, " #%s", t)
	}
	fmt.Fprintf(&b, " session=%s", Short(v.Session))
	if v.Address != "" {
		fmt.Fprintf(&b, " -> %s", v.Address)
	}
	return b.String()
}

// ClaimLines renders the claims that selected the peer, one per line, indented under Line.
func (v View) ClaimLines(now time.Time) []string {
	out := make([]string, len(v.Claims))
	for i, c := range v.Claims {
		out[i] = fmt.Sprintf("  %s %s %s", c.Rel, c.Ref(), render.Stamp(c.At, now))
		if c.Source != "claim" {
			out[i] += " (" + c.Source + ")"
		}
	}
	return out
}

// Short is the eight-character prefix agents cite a session by.
func Short(session string) string {
	if len(session) > 8 {
		return session[:8]
	}
	return session
}

func age(d time.Duration) string {
	switch {
	case d < time.Minute:
		return "now"
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dd", int(d.Hours()/24))
}

// One resolves exactly one peer for f, or an error naming the candidates.
func One(ctx context.Context, st *store.Store, f Filter) (View, error) {
	views, err := Resolve(ctx, st, f)
	if err != nil {
		return View{}, err
	}
	switch len(views) {
	case 0:
		return View{}, sql.ErrNoRows
	case 1:
		return views[0], nil
	}
	names := make([]string, len(views))
	for i, v := range views {
		names[i] = Short(v.Session) + " " + v.Name
	}
	return View{}, errors.New("ambiguous: " + strings.Join(names, ", "))
}
