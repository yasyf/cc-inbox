package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	mcp "github.com/yasyf/cc-interact/channel"
	"github.com/yasyf/cc-interact/procs"
	"golang.org/x/sys/unix"

	"github.com/yasyf/cc-inbox/internal/channel"
	"github.com/yasyf/cc-inbox/internal/harness"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/version"
)

const (
	// ToolsPIDHeader names the process of the Claude Code window a tools request runs for.
	ToolsPIDHeader = "X-Claude-Pid"
	// ToolsEnvHeader prefixes each header carrying one of the window's environment variables.
	ToolsEnvHeader = "X-Cci-Env-"
	toolsBodyBytes = 4 << 20
)

// ToolsHandler serves cci's tools over MCP Streamable HTTP, one JSON-RPC
// message a POST and no session. A tool call runs for the Claude Code window
// ToolsPIDHeader names: the window's session registry entry gives its session
// and cwd, read on every call since /clear and /resume change the session, and
// the ToolsEnvHeader headers give the rest of the environment a stdio server
// inherits.
func ToolsHandler(daemon context.Context, st *store.Store, home string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if r.Header.Get("Origin") != "" {
			http.Error(w, "cci tools answer no browser", http.StatusForbidden)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, toolsBodyBytes))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var msg struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		var line bytes.Buffer
		if err := errors.Join(json.Unmarshal(body, &msg), json.Compact(&line, body)); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(msg.ID) == 0 {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		ctx := r.Context() //nolint:contextcheck // a tool call lives for its request; the daemon context only marks it hosted.
		if msg.Method == "tools/call" {
			if ctx, err = toolsCaller(daemon, r, st, home); err != nil {
				http.Error(w, err.Error(), http.StatusForbidden)
				return
			}
		}
		line.WriteByte('\n')
		var out bytes.Buffer
		srv := mcp.NewServer(mcp.ServerInfo{Name: "cci", Version: version.String(), Instructions: toolsInstructions}, channelTools())
		if err := srv.Serve(ctx, &line, &out); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		reply, err := rpcReply(out.Bytes(), msg.ID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(reply)
	})
}

func rpcReply(out, id json.RawMessage) ([]byte, error) {
	for line := range bytes.Lines(out) {
		var msg struct {
			ID json.RawMessage `json:"id"`
		}
		if json.Unmarshal(line, &msg) == nil && bytes.Equal(msg.ID, id) {
			return line, nil
		}
	}
	return nil, fmt.Errorf("no reply to request %s", id)
}

func toolsCaller(daemon context.Context, r *http.Request, st *store.Store, home string) (context.Context, error) {
	pid, err := strconv.Atoi(r.Header.Get(ToolsPIDHeader))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ToolsPIDHeader, err)
	}
	env := map[string]string{}
	for name, values := range r.Header {
		if variable, ok := strings.CutPrefix(name, ToolsEnvHeader); ok && len(values) > 0 {
			env[strings.ToUpper(variable)] = values[0]
		}
	}
	if env[homeEnv] != "" || env[unsentEnv] != "" {
		return nil, fmt.Errorf("a window with %s or %s set runs cci tools of its own: the daemon serves only %s", homeEnv, unsentEnv, home)
	}
	ctx := harness.WithConfigDir(r.Context(), env["CLAUDE_CONFIG_DIR"])
	window := procs.FindAncestor(pid, procs.IsClaude)
	session, err := registeredWindow(ctx, window)
	if err != nil {
		return nil, fmt.Errorf("pid %d: %w", pid, err)
	}
	env[sessionEnv] = session.SessionID
	view := *st
	if err := configureStoreOwner(&view, home); err != nil {
		return nil, err
	}
	return channel.Hosted(context.WithValue(ctx, invocationKey{}, &invocation{env: env, cwd: session.Cwd, pid: window, st: &view}), daemon), nil
}

func registeredWindow(ctx context.Context, pid int) (harness.Session, error) {
	if pid == 0 {
		return harness.Session{}, errNoSession
	}
	dir, err := harness.Dir(ctx)
	if err != nil {
		return harness.Session{}, err
	}
	raw, err := os.ReadFile(filepath.Join(dir, strconv.Itoa(pid)+".json")) //nolint:gosec // G304: the path holds only a numeric pid.
	if err != nil {
		return harness.Session{}, fmt.Errorf("no Claude Code session registry entry: %w", err)
	}
	var session harness.Session
	if err := json.Unmarshal(raw, &session); err != nil {
		return harness.Session{}, fmt.Errorf("decode the session registry entry: %w", err)
	}
	if session.PID != pid || session.SessionID == "" || !session.Alive(harness.Procs{}) || unix.Kill(pid, 0) != nil {
		return harness.Session{}, fmt.Errorf("claude pid %d is not a live window of this user", pid)
	}
	return session, nil
}
