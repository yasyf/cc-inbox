package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/yasyf/cc-inbox/internal/hook"
	"github.com/yasyf/cc-inbox/internal/store"
)

// HookHeader is the header every HTTP hook carries: a browser cannot send it
// across origins without a preflight this server refuses, so a web page cannot
// drain a session's mail.
const HookHeader = "X-Cci-Hook"

type hookReply struct {
	w    http.ResponseWriter
	held bytes.Buffer
}

func (r *hookReply) Write(p []byte) (int, error) {
	return r.held.Write(p)
}

// flush hands what is held to Claude Code, so a cursor moves only past mail whose
// response was written.
func (r *hookReply) flush(context.Context) error {
	if r.held.Len() == 0 {
		return nil
	}
	if strings.HasPrefix(r.held.String(), "{") {
		r.w.Header().Set("Content-Type", "application/json")
	} else {
		r.w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	}
	if _, err := r.w.Write(r.held.Bytes()); err != nil {
		return fmt.Errorf("write hook response: %w", err)
	}
	r.held.Reset()
	if err := http.NewResponseController(r.w).Flush(); err != nil {
		return fmt.Errorf("flush hook response: %w", err)
	}
	return nil
}

// HookHandler serves the hooks that only read: a refused connection delays mail
// to the next hook, and the cursor has not moved.
func HookHandler(st *store.Store, home string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/hook/{event}", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(HookHeader) == "" {
			http.Error(w, "missing "+HookHeader, http.StatusForbidden)
			return
		}
		p, err := hook.Read(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		view := *st
		if err := configureStoreOwner(&view, home); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		reply := &hookReply{w: w}
		view.BeforeCursorMove(reply.flush)
		ctx, cancel := context.WithTimeout(r.Context(), hookRunBudget)
		defer cancel()
		switch event := r.PathValue("event"); event {
		case "post-tool-use":
			err = hook.PostToolUse(ctx, &view, p, func() (store.Window, error) { return parentWindow(ctx, &view, p) }, reply)
		case "stop":
			err = hook.Stop(ctx, &view, p, reply)
		case "subagent-stop":
			err = hook.SubagentStop(ctx, &view, p, reply)
		default:
			http.Error(w, "no hook "+event, http.StatusNotFound)
			return
		}
		if err != nil && !errors.Is(err, store.ErrUnconfirmed) && !store.Busy(err) && ctx.Err() == nil {
			slog.Warn("cci daemon: a hook failed", "hook", r.PathValue("event"), "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := reply.flush(ctx); err != nil {
			slog.Debug("cci daemon: a hook's caller went away", "err", err)
		}
	})
	return mux
}

func parentWindow(ctx context.Context, st *store.Store, p hook.Payload) (store.Window, error) {
	parent, err := st.Peer(ctx, store.PeerKey{Session: p.SessionID})
	if err != nil {
		return store.Window{}, nil //nolint:nilerr // a session the registry has not seen yet has no window to name.
	}
	return store.Window{PID: parent.PID, Started: parent.PIDStarted}, nil
}
