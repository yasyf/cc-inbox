package hook

import (
	"bytes"
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
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
}

// Key is the session thread the hook fired in: the root thread, or the
// subagent or teammate the payload's agent_id names.
func (p Payload) Key() store.PeerKey {
	return store.PeerKey{Session: p.SessionID, Agent: p.AgentID}
}

// Agent describes a subagent from the metadata the harness writes beside the
// session transcript: a teammate's under subagents/, a workflow agent's under
// its workflow's directory there, named by its label. An agent with no
// metadata has no name.
type Agent struct {
	Name     string
	Workflow bool
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
	var meta struct {
		AgentType   string `json:"agentType"`
		Name        string `json:"name"`
		Description string `json:"description"`
	}
	if err := json.Unmarshal(raw, &meta); err != nil {
		return Agent{}, fmt.Errorf("decode %s: %w", path, err)
	}
	if meta.AgentType == workflowAgentType {
		return Agent{Name: meta.Description, Workflow: true}, nil
	}
	return Agent{Name: meta.Name}, nil
}

func Read(r io.Reader) (Payload, error) {
	var p Payload
	if err := json.NewDecoder(r).Decode(&p); err != nil {
		return Payload{}, fmt.Errorf("decode hook payload: %w", err)
	}
	return p, nil
}

func SessionStart(ctx context.Context, st *store.Store, p Payload, window store.Window, w io.Writer) error {
	if window.PID != 0 {
		if err := st.Repoint(ctx, p.SessionID, window); err != nil {
			return err
		}
	}
	b, ok, err := st.Binding(ctx, p.Key())
	if err != nil || !ok {
		return err
	}
	if b.Lane != "" {
		return deliver(ctx, st, p.Key(), b, "SessionStart", w)
	}
	var out bytes.Buffer
	v, err := inbox.Digest(ctx, st, b.Drive, st.Now().Add(-inbox.DigestWindow))
	if err != nil {
		return err
	}
	v.Write(&out, st.Now(), digestBudget, recordWidth)
	out.WriteString("unseen since this session's cursor (cci tail for more):\n")
	if _, err := inbox.Tail(ctx, st, inbox.TailOptions{Filter: store.Filter{Drive: b.Drive, Peer: p.Key().String(), For: "root"}, Cursor: p.Key().String(), Budget: tailBudget, Width: recordWidth}, &out); err != nil {
		return err
	}
	return additionalContext(w, "SessionStart", out.String())
}

// PostToolUse marks the thread active and delivers its lane's records, or,
// for a subagent bound to no lane, the records addressed to its peer address,
// so a workflow agent that never subscribes still hears cci post --to
// peer:<session>/<label>. A subagent or teammate the registry does not list as
// live is announced as its own peer first, with window naming the harness
// process it runs in.
func PostToolUse(ctx context.Context, st *store.Store, p Payload, window func() (store.Window, error), w io.Writer) error {
	if p.AgentID != "" {
		if err := announceAgent(ctx, st, p, window); err != nil {
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
		return deliver(ctx, st, p.Key(), b, p.Event, w)
	case p.AgentID != "":
		return deliver(ctx, st, p.Key(), store.Binding{}, p.Event, w)
	}
	return nil
}

func announceAgent(ctx context.Context, st *store.Store, p Payload, window func() (store.Window, error)) error {
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
		return nil
	}
	o := peers.Options{Session: p.SessionID, Agent: p.AgentID, AgentName: agent.Name, Window: win, Cwd: p.Cwd}
	if agent.Workflow {
		o.Role = peers.RoleWorkflow
	}
	_, err = peers.Announce(ctx, st, o)
	return err
}

// SubagentStop retires the subagent's peer until its next tool call.
func SubagentStop(ctx context.Context, st *store.Store, p Payload) error {
	return st.Retire(ctx, p.Key())
}

func deliver(ctx context.Context, st *store.Store, k store.PeerKey, b store.Binding, event string, w io.Writer) error {
	var tail bytes.Buffer
	tail.WriteString("cci records addressed to " + cmp.Or(b.Lane, store.PeerAddress(k.String())) + "; act on each:\n")
	res, err := inbox.Tail(ctx, st, inbox.TailOptions{Filter: store.Filter{Drive: b.Drive, Peer: k.String(), To: b.Lane}, Cursor: store.LaneCursor(k), Budget: tailBudget, Width: recordWidth, Resume: "the next tool call"}, &tail)
	if err != nil || res.Printed == 0 {
		return err
	}
	var out bytes.Buffer
	if b.Lane != "" {
		open, err := st.OpenTo(ctx, b.Drive, b.Lane, askKinds)
		if err != nil {
			return err
		}
		writeOpen(&out, open, b.Lane, st.Now())
	}
	_, _ = tail.WriteTo(&out)
	return additionalContext(w, event, out.String())
}

func writeOpen(w io.Writer, open []store.Record, reader string, now time.Time) {
	if len(open) == 0 {
		return
	}
	fmt.Fprintf(w, "open asks addressed to %s, newest first; answer with cci post --re <seq> --kind answer, or close with cci post --resolves <seq>:\n", reader)
	for i, r := range slices.Backward(open) {
		if i < len(open)-openShown {
			fmt.Fprintf(w, "... %d older open asks\n", i+1)
			break
		}
		fmt.Fprintf(w, "%s (open %dm)\n", render.Clip(render.Line(r, now), openWidth), int(now.Sub(r.At).Minutes()))
	}
}

// Stop nudges a lane's thread, once per ask, about the newest ask, decide or
// blocker addressed to it that has waited past overdue. It never blocks.
func Stop(ctx context.Context, st *store.Store, p Payload, w io.Writer) error {
	b, ok, err := st.Binding(ctx, p.Key())
	if err != nil || !ok || b.Lane == "" {
		return err
	}
	open, err := st.OpenTo(ctx, b.Drive, b.Lane, askKinds)
	if err != nil {
		return err
	}
	cursor := "stop-" + p.Key().String()
	nudged, _, err := st.Cursor(ctx, cursor, b.Drive)
	if err != nil {
		return err
	}
	now := st.Now()
	late := slices.DeleteFunc(open, func(r store.Record) bool { return r.Seq <= nudged || now.Sub(r.At) < overdue })
	if len(late) == 0 {
		return nil
	}
	newest := late[len(late)-1]
	if err := st.SetCursor(ctx, cursor, b.Drive, newest.Seq); err != nil {
		return err
	}
	text := fmt.Sprintf("cci: this ask to %s has waited %dm unanswered: %s; answer it with cci post --re %d --kind answer, or close it with cci post --resolves %d", b.Lane, int(now.Sub(newest.At).Minutes()), render.Clip(render.Line(newest, now), openWidth), newest.Seq, newest.Seq)
	if len(late) > 1 {
		text += fmt.Sprintf(" (%d older overdue asks also wait)", len(late)-1)
	}
	return additionalContext(w, "Stop", text)
}

func additionalContext(w io.Writer, event, text string) error {
	return json.NewEncoder(w).Encode(map[string]any{
		"hookSpecificOutput": map[string]string{
			"hookEventName":     event,
			"additionalContext": text,
		},
	})
}

func Prompt(ctx context.Context, st *store.Store, p Payload) error {
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
	_, _, err = st.Post(ctx, store.Record{
		Drive:  b.Drive,
		Lane:   "owner",
		Kind:   kinds.Owner,
		Text:   text,
		Refs:   store.Refs{Path: path},
		Source: store.HookSource("root"),
	}, 0)
	return err
}
