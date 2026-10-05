package hook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
)

const (
	digestBudget = 2500
	tailBudget   = 1500
	recordWidth  = 400
)

type Payload struct {
	SessionID string `json:"session_id"`
	Source    string `json:"source"`
	Prompt    string `json:"prompt"`
	ToolName  string `json:"tool_name"`
}

func Read(r io.Reader) (Payload, error) {
	var p Payload
	if err := json.NewDecoder(r).Decode(&p); err != nil {
		return Payload{}, fmt.Errorf("decode hook payload: %w", err)
	}
	return p, nil
}

func SessionStart(ctx context.Context, st *store.Store, p Payload, w io.Writer) error {
	b, ok, err := st.Binding(ctx, p.SessionID)
	if err != nil || !ok {
		return err
	}
	var out bytes.Buffer
	v, err := inbox.Digest(ctx, st, b.Drive, st.Now().Add(-inbox.DigestWindow))
	if err != nil {
		return err
	}
	v.Write(&out, st.Now(), digestBudget, recordWidth)
	out.WriteString("unseen since this session's cursor (cci tail for more):\n")
	if _, err := inbox.Tail(ctx, st, inbox.TailOptions{Filter: store.Filter{Drive: b.Drive}, Cursor: p.SessionID, Budget: tailBudget, Width: recordWidth}, &out); err != nil {
		return err
	}
	return json.NewEncoder(w).Encode(map[string]any{
		"hookSpecificOutput": map[string]string{
			"hookEventName":     "SessionStart",
			"additionalContext": out.String(),
		},
	})
}

func Prompt(ctx context.Context, st *store.Store, p Payload) error {
	b, ok, err := st.Binding(ctx, p.SessionID)
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
		Source: "hook",
	}, 0)
	return err
}
