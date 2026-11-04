package hook

import (
	"bytes"
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/peers"
	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
)

const (
	digestBudget = 2500
	tailBudget   = 1500
	recordWidth  = 400
	openShown    = 5
	openWidth    = 300
	overdue      = 10 * time.Minute
)

var askKinds = []kinds.Kind{kinds.Ask, kinds.Decide, kinds.Blocker}

type Payload struct {
	SessionID      string `json:"session_id"`
	AgentID        string `json:"agent_id"`
	TranscriptPath string `json:"transcript_path"`
	Cwd            string `json:"cwd"`
	Event          string `json:"hook_event_name"`
	Source         string `json:"source"`
	Prompt         string `json:"prompt"`
	ToolName       string `json:"tool_name"`
	LastMessage    string `json:"last_assistant_message"`
	Error          string `json:"error"`
}

// Key is the session thread the hook fired in: the root thread, or the
// subagent or teammate the payload's agent_id names.
func (p Payload) Key() store.PeerKey {
	return store.PeerKey{Session: p.SessionID, Agent: p.AgentID}
}

// Agent describes a subagent from the metadata the harness writes beside the
// session transcript: a teammate's under subagents/, a workflow agent's under
// its workflow's directory there, named by its label, with the agent that
// spawned it. An agent with no metadata has no name and no parent.
type Agent struct {
	Name     string
	Workflow bool
	Parent   string
	Spawned  time.Time
}

const workflowAgentType = "workflow-subagent"

func (p Payload) Agent() (Agent, error) {
	dir := strings.TrimSuffix(p.TranscriptPath, ".jsonl") + "/subagents"
	file := "agent-" + p.AgentID + ".meta.json"
	path := filepath.Join(dir, file)
	if _, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
		matches, err := filepath.Glob(filepath.Join(dir, "workflows", "*", file))
		if err != nil {
			return Agent{}, fmt.Errorf("find workflow agent metadata: %w", err)
		}
		if len(matches) == 0 {
			return Agent{}, nil
		}
		path = matches[0]
	}
	raw, err := os.ReadFile(path) //nolint:gosec // G304: the path is the harness's transcript directory from the hook payload.
	if err != nil {
		return Agent{}, fmt.Errorf("read agent metadata: %w", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return Agent{}, fmt.Errorf("stat agent metadata: %w", err)
	}
	var meta struct {
		AgentType   string `json:"agentType"`
		Name        string `json:"name"`
		Description string `json:"description"`
		Parent      string `json:"parentAgentId"`
	}
	if err := json.Unmarshal(raw, &meta); err != nil {
		return Agent{}, fmt.Errorf("decode %s: %w", path, err)
	}
	if meta.AgentType == workflowAgentType {
		return Agent{Name: meta.Description, Workflow: true, Parent: meta.Parent, Spawned: info.ModTime()}, nil
	}
	return Agent{Name: meta.Name, Parent: meta.Parent, Spawned: info.ModTime()}, nil
}

func Read(r io.Reader) (Payload, error) {
	var p Payload
	if err := json.NewDecoder(r).Decode(&p); err != nil {
		return Payload{}, fmt.Errorf("decode hook payload: %w", err)
	}
	return p, nil
}

func SessionStart(ctx context.Context, st *store.Store, p Payload, window store.Window, w io.Writer) error {
	note := ""
	if window.PID != 0 {
		var err error
		if note, err = repoint(ctx, st, p.SessionID, window); err != nil {
			return err
		}
		if err := keepSubscribedLane(ctx, st, p, window); err != nil {
			return err
		}
	}
	b, ok, err := st.Binding(ctx, p.Key())
	if err != nil {
		return err
	}
	if !ok {
		if note == "" {
			return nil
		}
		return additionalContext(w, "SessionStart", note)
	}
	if b.Lane != "" {
		return deliver(ctx, st, p.Key(), b, "SessionStart", true, note, w)
	}
	var out bytes.Buffer
	out.WriteString(note)
	v, err := inbox.Digest(ctx, st, b.Drive, st.Now().Add(-inbox.DigestWindow))
	if err != nil {
		return err
	}
	v.Write(&out, st.Now(), digestBudget, recordWidth)
	out.WriteString("unseen since this session's cursor (cci tail for more):\n")
	res, err := inbox.Tail(ctx, st, inbox.TailOptions{Filter: store.Filter{Drive: b.Drive, Peer: p.Key().String(), For: "root"}, Cursor: p.Key().String(), Budget: tailBudget, Width: recordWidth, Fold: render.FoldWidth, Defer: true}, &out)
	if err != nil {
		return err
	}
	return acknowledge(ctx, st, w, "SessionStart", out.String(), res.Ack)
}

// repoint moves the session's subscription onto this window and returns the line
// that tells a resumed session it came back, or "" when nothing moved here.
func repoint(ctx context.Context, st *store.Store, session string, window store.Window) (string, error) {
	_, had, err := st.Subscription(ctx, window)
	if err != nil {
		return "", err
	}
	if err := st.Repoint(ctx, session, window); err != nil {
		return "", err
	}
	sub, has, err := st.Subscription(ctx, window)
	if err != nil || had || !has {
		return "", err
	}
	return fmt.Sprintf("cci brought this window's subscription back after the restart: reader %s on drive %s, cursor %s, every filter as it was. "+
		"Run no cci subscribe: it would replace those filters.\n", sub.Reader, sub.Drive, sub.Cursor), nil
}

// keepSubscribedLane binds a root thread with no lane, such as the one a
// /clear starts, to the lane its window's subscription reads, so the window
// keeps the lane cci subscribe --reader claimed for it.
func keepSubscribedLane(ctx context.Context, st *store.Store, p Payload, window store.Window) error {
	if p.AgentID != "" {
		return nil
	}
	sub, ok, err := st.Subscription(ctx, window)
	if err != nil || !ok {
		return err
	}
	b, bound, err := st.Binding(ctx, p.Key())
	if err != nil || bound && b.Lane != "" {
		return err
	}
	moves, err := st.MovesLane(ctx, sub)
	if err != nil || !moves {
		return err
	}
	return st.ClaimLane(ctx, p.Key(), sub.Drive, sub.Reader, time.UnixMilli(window.Started))
}

// PostToolUse marks the thread active and delivers its lane's records, or,
// for a subagent bound to no lane, the records addressed to its peer address,
// so a workflow agent that never subscribes still hears cci post --to
// peer:<session>/<label>. A subagent or teammate the registry does not list as
// live is announced as its own peer first, with window naming the harness
// process it runs in.
func PostToolUse(ctx context.Context, st *store.Store, p Payload, window func() (store.Window, error), w io.Writer) error {
	if p.AgentID != "" {
		if err := AnnounceAgent(ctx, st, p, window); err != nil {
			return err
		}
	}
	if err := st.Touch(ctx, p.Key()); err != nil {
		return err
	}
	b, ok, err := st.Binding(ctx, p.Key())
	switch {
	case err != nil:
		return err
	case ok && b.Lane != "":
		return deliver(ctx, st, p.Key(), b, p.Event, false, "", w)
	case p.AgentID != "":
		return deliver(ctx, st, p.Key(), store.Binding{}, p.Event, false, "", w)
	}
	return nil
}

// AnnounceAgent registers the subagent or teammate p names as its own peer,
// under the name and parent its harness metadata records, unless the registry
// already lists it live in the window the harness runs it in.
func AnnounceAgent(ctx context.Context, st *store.Store, p Payload, window func() (store.Window, error)) error {
	existing, err := st.Peer(ctx, p.Key())
	found := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	agent, err := p.Agent()
	if err != nil {
		return err
	}
	win, err := window()
	if err != nil {
		return err
	}
	if found && existing.PID == win.PID && existing.PIDStarted == win.Started && !existing.Superseded && (existing.AgentName != "" || agent.Name == "") {
		return st.SetParent(ctx, p.Key(), agent.Parent)
	}
	var env map[string]string
	if parent, err := st.Peer(ctx, store.PeerKey{Session: p.SessionID}); err == nil {
		env = parent.Env
	}
	o := peers.Options{Env: env, Session: p.SessionID, Agent: p.AgentID, AgentName: agent.Name, Window: win, Cwd: p.Cwd, Spawned: agent.Spawned}
	if agent.Workflow {
		o.Role = peers.RoleWorkflow
	}
	if _, err := peers.Announce(ctx, st, o); err != nil {
		return err
	}
	return st.SetParent(ctx, p.Key(), agent.Parent)
}

// SubagentStop retires the subagent's peer until its next tool call, unless the
// subagent is the lane its binding names and its turn has not ended on a typed
// record. A helper posting under its parent's lane stops freely.
func SubagentStop(ctx context.Context, st *store.Store, p Payload, w io.Writer) error {
	b, ok, err := st.Binding(ctx, p.Key())
	if err != nil {
		return err
	}
	if ok && b.Lane != "" {
		peer, err := st.Peer(ctx, p.Key())
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil && store.SameLane(peer.AgentName, b.Lane) {
			refused, err := endTurn(ctx, st, p, b, w)
			if err != nil || refused {
				return err
			}
			if err := escalateRefusal(ctx, st, b); err != nil {
				return err
			}
		}
	}
	return st.Retire(ctx, p.Key())
}

func deliver(ctx context.Context, st *store.Store, k store.PeerKey, b store.Binding, event string, relist bool, note string, w io.Writer) error {
	var tail bytes.Buffer
	tail.WriteString("cci records addressed to " + cmp.Or(b.Lane, store.PeerAddress(k.String())) + "; act on each:\n")
	f, err := st.WindowMail(ctx, k, b)
	if err != nil {
		return err
	}
	res, err := inbox.Tail(ctx, st, inbox.TailOptions{Filter: f, Cursor: store.LaneCursor(k), Budget: tailBudget, Compact: true, Reader: b.Lane, Resume: "the next tool call", Open: true, Defer: true}, &tail)
	if err != nil {
		return err
	}
	if res.Printed == 0 && res.More == 0 {
		if relist {
			res.Ack = append(res.Ack, store.CursorMove{Name: openAsksCursor(k), Drive: b.Drive, Seq: -1, Exact: true})
		}
		if note != "" {
			return acknowledge(ctx, st, w, event, note, res.Ack)
		}
		return st.MoveCursors(ctx, res.Ack)
	}
	var out bytes.Buffer
	out.WriteString(note)
	ack := res.Ack
	if b.Lane != "" {
		open, err := owed(ctx, st, b.Drive, b.Lane)
		if err != nil {
			return err
		}
		listed, err := writeOpen(ctx, st, &out, k, b, open, relist)
		if err != nil {
			return err
		}
		ack = append(ack, listed...)
	}
	_, _ = tail.WriteTo(&out)
	return acknowledge(ctx, st, w, event, out.String(), ack)
}

func acknowledge(ctx context.Context, st *store.Store, w io.Writer, event, text string, ack []store.CursorMove) error {
	if err := additionalContext(w, event, text); err != nil {
		return err
	}
	return st.MoveCursors(ctx, ack)
}

func owed(ctx context.Context, st *store.Store, drive, reader string) ([]store.Record, error) {
	open, err := st.OpenTo(ctx, drive, reader, askKinds)
	if err != nil {
		return nil, err
	}
	aliases, err := st.Aliases(ctx, drive, reader)
	if err != nil {
		return nil, err
	}
	names := append([]string{reader}, aliases...)
	return slices.DeleteFunc(open, func(r store.Record) bool {
		first, _, _ := strings.Cut(r.To[0], "@")
		return !slices.ContainsFunc(names, func(name string) bool { return store.SameLane(first, name) }) || store.SameLane(r.Lane, reader) || r.Kind == kinds.Decide && !strings.HasSuffix(strings.TrimSpace(r.Text), "?")
	}), nil
}

func watermark(ctx context.Context, st *store.Store, cursor, drive string) error {
	head, err := st.MaxSeq(ctx)
	if err != nil {
		return err
	}
	return st.SetCursor(ctx, cursor, drive, head)
}

func openAsksCursor(k store.PeerKey) string {
	return "open-asks-" + k.String()
}

func writeOpen(ctx context.Context, st *store.Store, w io.Writer, k store.PeerKey, b store.Binding, open []store.Record, relist bool) ([]store.CursorMove, error) {
	name := openAsksCursor(k)
	shown, listedBefore, err := st.Cursor(ctx, name, b.Drive)
	if err != nil {
		return nil, err
	}
	if relist {
		shown = -1
	}
	sum := fnv.New64a()
	for _, r := range open {
		_, _ = fmt.Fprintf(sum, "%d,", r.Seq)
	}
	listed := int64(sum.Sum64() >> 1)
	if len(open) == 0 {
		listed = 0
	}
	if listed == shown {
		return nil, nil
	}
	ack := []store.CursorMove{{Name: name, Drive: b.Drive, Seq: listed, Exact: true}}
	if len(open) == 0 {
		return ack, nil
	}
	now := st.Now()
	if listedBefore && shown >= 0 {
		newest := open[max(0, len(open)-openShown):]
		ids := make([]string, len(newest))
		for i, r := range newest {
			ids[len(newest)-1-i] = fmt.Sprintf("#%d %s %dm", r.Seq, r.Lane, int(now.Sub(r.At).Minutes()))
		}
		_, _ = fmt.Fprintf(w, "%d open asks addressed to %s, changed since the last list; newest: %s; cci show <seq> prints one, cci digest lists them\n", len(open), b.Lane, strings.Join(ids, ", "))
		return ack, nil
	}
	_, _ = fmt.Fprintf(w, "open asks addressed to %s, newest first; answer with cci post --re <seq> --kind answer, or close with cci post --resolves <seq>:\n", b.Lane)
	for i, r := range slices.Backward(open) {
		if i < len(open)-openShown {
			_, _ = fmt.Fprintf(w, "... %d older open asks\n", i+1)
			break
		}
		age := int(now.Sub(r.At).Minutes())
		r.To = nil
		_, _ = fmt.Fprintf(w, "%s (open %dm)\n", render.Brief(r, now, openWidth), age)
	}
	return ack, nil
}

// Stop holds a lane's thread until its turn can end: first on an overdue ask
// addressed to it, then on a turn with open work and no typed record. A turn
// that ends escalates the refusal the lane never followed with a stored post.
// A store it cannot read lets the turn end with a warning.
func Stop(ctx context.Context, st *store.Store, p Payload, w io.Writer) error {
	if err := holdStop(ctx, st, p, w); err != nil {
		slog.Warn("cci let a turn end on a store it could not read", "session", p.SessionID, "err", err)
	}
	return nil
}

func holdStop(ctx context.Context, st *store.Store, p Payload, w io.Writer) error {
	b, ok, err := st.Binding(ctx, p.Key())
	if err != nil || !ok || b.Lane == "" {
		return err
	}
	nudged, err := nudgeOverdue(ctx, st, p, b, w)
	if err != nil || nudged {
		return err
	}
	refused, err := endTurn(ctx, st, p, b, w)
	if err != nil || refused {
		return err
	}
	return escalateRefusal(ctx, st, b)
}

// nudgeOverdue blocks the stop once per record, about the newest open ask,
// blocker or question-ending decide that names the lane first, came from
// another lane, and has waited past overdue; the next stop passes. A thread's
// first stop only records the head, so older records never block.
func nudgeOverdue(ctx context.Context, st *store.Store, p Payload, b store.Binding, w io.Writer) (bool, error) {
	cursor := "stop-" + p.Key().String()
	nudged, ok, err := st.Cursor(ctx, cursor, b.Drive)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, watermark(ctx, st, cursor, b.Drive)
	}
	open, err := owed(ctx, st, b.Drive, b.Lane)
	if err != nil {
		return false, err
	}
	now := st.Now()
	late := slices.DeleteFunc(open, func(r store.Record) bool { return r.Seq <= nudged || now.Sub(r.At) < overdue })
	if len(late) == 0 {
		return false, nil
	}
	newest := late[len(late)-1]
	reason := fmt.Sprintf("cci #%d %s from %s to %s has waited %dm unanswered: %s. Answer it, then close it with cci post --resolves %d.", newest.Seq, strings.ToUpper(string(newest.Kind)), newest.Lane, b.Lane, int(now.Sub(newest.At).Minutes()), render.Clip(strings.Join(strings.Fields(newest.Text), " "), openWidth), newest.Seq)
	if len(late) > 1 {
		reason += fmt.Sprintf(" %d older overdue asks also wait.", len(late)-1)
	}
	if err := json.NewEncoder(w).Encode(map[string]string{"decision": "block", "reason": reason}); err != nil {
		return false, err
	}
	return true, st.SetCursor(ctx, cursor, b.Drive, newest.Seq)
}

func additionalContext(w io.Writer, event, text string) error {
	return json.NewEncoder(w).Encode(map[string]any{
		"hookSpecificOutput": map[string]string{
			"hookEventName":     event,
			"additionalContext": text,
		},
	})
}

// Prompt records the root's prompt p as the owner record of its drive,
// written at at, so a prompt recorded again later at the same time
// deduplicates against the first.
func Prompt(ctx context.Context, st *store.Store, p Payload, at time.Time) error {
	if err := st.Touch(ctx, p.Key()); err != nil {
		return err
	}
	b, ok, err := st.Binding(ctx, p.Key())
	if err != nil || !ok || !b.Root {
		return err
	}
	prompt := strings.TrimSpace(p.Prompt)
	if prompt == "" || strings.HasPrefix(prompt, "<") || strings.HasPrefix(prompt, "/") {
		return nil
	}
	text, path, err := st.Fit(prompt)
	if err != nil {
		return err
	}
	_, _, err = st.PostAt(ctx, store.Record{
		Drive:  b.Drive,
		Lane:   "owner",
		Kind:   kinds.Owner,
		Text:   text,
		Refs:   store.Refs{Path: path},
		Source: store.HookSource("root"),
	}, 0, at)
	return err
}
