package hook

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"

	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/peers"
	"github.com/yasyf/cc-inbox/internal/store"
)

const (
	digestBudget = 2500
	tailBudget   = 1500
	recordWidth  = 400
)

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

// AgentName reads the subagent's name from the metadata the harness writes
// beside the session transcript; an agent with no metadata has no name.
func (p Payload) AgentName() (string, error) {
	path := strings.TrimSuffix(p.TranscriptPath, ".jsonl") + "/subagents/agent-" + p.AgentID + ".meta.json"
	raw, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read agent metadata: %w", err)
	}
	var meta struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(raw, &meta); err != nil {
		return "", fmt.Errorf("decode %s: %w", path, err)
	}
	return meta.Name, nil
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

// PostToolUse marks the thread active and delivers its lane's records. A
// subagent or teammate the registry does not list as live is announced as
// its own peer first, with window naming the harness process it runs in.
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
	if err != nil || !ok || b.Lane == "" {
		return err
	}
	return deliver(ctx, st, p.Key(), b, p.Event, w)
}

func announceAgent(ctx context.Context, st *store.Store, p Payload, window func() (store.Window, error)) error {
	existing, err := st.Peer(ctx, p.Key())
	found := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	name, err := p.AgentName()
	if err != nil {
		return err
	}
	if found && existing.PID != 0 && !existing.Superseded && (existing.AgentName != "" || name == "") {
		return nil
	}
	win, err := window()
	if err != nil {
		return err
	}
	_, err = peers.Announce(ctx, st, peers.Options{Session: p.SessionID, Agent: p.AgentID, AgentName: name, Window: win, Cwd: p.Cwd})
	return err
}

// SubagentStop retires the subagent's peer until its next tool call.
func SubagentStop(ctx context.Context, st *store.Store, p Payload) error {
	return st.Retire(ctx, p.Key())
}

func deliver(ctx context.Context, st *store.Store, k store.PeerKey, b store.Binding, event string, w io.Writer) error {
	var out bytes.Buffer
	out.WriteString("cci records addressed to " + b.Lane + "; act on each:\n")
	res, err := inbox.Tail(ctx, st, inbox.TailOptions{Filter: store.Filter{Drive: b.Drive, Peer: k.String(), To: b.Lane}, Cursor: store.LaneCursor(k), Budget: tailBudget, Width: recordWidth, Resume: "the next tool call"}, &out)
	if err != nil || res.Printed == 0 {
		return err
	}
	return additionalContext(w, event, out.String())
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
