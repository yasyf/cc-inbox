package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/yasyf/cc-inbox/internal/hook"
	"github.com/yasyf/cc-inbox/internal/store"
)

// HookHeader is the header every HTTP hook carries: a browser cannot send it
// across origins without a preflight this server refuses, so a web page cannot
// drain a session's mail.
const HookHeader = "X-Cci-Hook"

// HookHomeHeader carries a session's CCI_HOME. The daemon reads one store, so a
// hook from a session on another gets an empty answer and that session's mail
// waits for cci tail.
const HookHomeHeader = "X-Cci-Home"

type hookReply struct {
	w    http.ResponseWriter
	held bytes.Buffer
	sent bool
}

func (r *hookReply) Write(p []byte) (int, error) {
	return r.held.Write(p)
}

// flush hands the whole response to Claude Code, with its length, so a cursor
// moves only past mail in a response that is complete on the socket. A hook writes
// one JSON document before it moves a cursor, so nothing follows it.
func (r *hookReply) flush(context.Context) error {
	if r.held.Len() == 0 || r.sent {
		return nil
	}
	r.sent = true
	r.w.Header().Set("Content-Length", strconv.Itoa(r.held.Len()))
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
		if asked := r.Header.Get(HookHomeHeader); asked != "" && filepath.Clean(asked) != filepath.Clean(home) {
			w.WriteHeader(http.StatusOK)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		captureHook(home, r.PathValue("event"), body)
		p, err := hook.Read(bytes.NewReader(body))
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
		if !httpHook(r.PathValue("event")) {
			http.Error(w, "no hook "+r.PathValue("event"), http.StatusNotFound)
			return
		}
		err = runHookEvent(ctx, &view, r.PathValue("event"), p, reply)
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

func httpHook(event string) bool {
	return event == "post-tool-use" || event == "stop" || event == "subagent-stop"
}

func runHookEvent(ctx context.Context, st *store.Store, event string, p hook.Payload, w io.Writer) error {
	switch event {
	case "post-tool-use":
		return hook.PostToolUse(ctx, st, p, func() (store.Window, error) { return parentWindow(ctx, st, p) }, w)
	case "stop":
		return hook.Stop(ctx, st, p, w)
	default:
		return hook.SubagentStop(ctx, st, p, w)
	}
}

func parentWindow(ctx context.Context, st *store.Store, p hook.Payload) (store.Window, error) {
	parent, err := st.Peer(ctx, store.PeerKey{Session: p.SessionID})
	if err != nil {
		return store.Window{}, nil //nolint:nilerr // a session the registry has not seen yet has no window to name.
	}
	return store.Window{PID: parent.PID, Started: parent.PIDStarted}, nil
}
