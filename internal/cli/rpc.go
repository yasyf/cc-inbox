package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/yasyf/cc-interact/procs"

	"github.com/yasyf/cc-inbox/internal/channel"
	"github.com/yasyf/cc-inbox/internal/harness"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/version"
	"github.com/yasyf/cc-inbox/internal/wire"
)

const (
	replyBufferBytes = 64 << 10
	replyFrameBytes  = 1 << 20
	inputFrames      = 4
	requestPatience  = 5 * time.Second
)

type invocationKey struct{}

// invocation is who a command runs for when the daemon runs it: a client's
// environment, directory and process, in place of the daemon's own.
type invocation struct {
	env            map[string]string
	cwd            string
	pid            int
	stdoutChar     bool
	stderrIsStdout bool
	embedder       bool
	st             *store.Store
}

// WithStore has a command run with ctx use st, in this process's environment, in
// place of opening its own store.
func WithStore(ctx context.Context, st *store.Store) (context.Context, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("resolve cwd: %w", err)
	}
	terminal, shared, err := wire.Outputs(os.Stdout, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("stat stdout: %w", err)
	}
	return context.WithValue(ctx, invocationKey{}, &invocation{
		env: requestEnv(os.Environ()), cwd: cwd, pid: os.Getpid(), stdoutChar: terminal, stderrIsStdout: shared, embedder: true, st: st,
	}), nil
}

func requestEnv(entries []string) map[string]string {
	env := make(map[string]string, len(entries))
	for _, entry := range entries {
		if name, value, ok := strings.Cut(entry, "="); ok {
			env[name] = value
		}
	}
	return env
}

func invoked(ctx context.Context) *invocation {
	inv, _ := ctx.Value(invocationKey{}).(*invocation)
	return inv
}

func getenv(ctx context.Context, name string) string {
	if inv := invoked(ctx); inv != nil {
		return inv.env[name]
	}
	return os.Getenv(name)
}

func getwd(ctx context.Context) (string, error) {
	if inv := invoked(ctx); inv != nil {
		return inv.cwd, nil
	}
	return os.Getwd()
}

func callerPath(ctx context.Context, path string) string {
	if inv := invoked(ctx); inv != nil && !filepath.IsAbs(path) {
		return filepath.Join(inv.cwd, path)
	}
	return path
}

// ServeCommands runs each client's command in this process against st until ctx
// ends. A client of another version or a command the daemon does not run gets a
// fallback frame and runs the command itself.
func ServeCommands(ctx context.Context, ln net.Listener, st *store.Store, home string) error {
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	var running sync.WaitGroup
	defer running.Wait()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("accept a command: %w", err)
		}
		running.Go(func() {
			defer func() { _ = conn.Close() }()
			defer func() {
				if r := recover(); r != nil {
					slog.Error("cci daemon: a command panicked", "panic", r)
					_ = conn.SetWriteDeadline(time.Now().Add(requestPatience))
					_ = wire.WriteFrame(conn, wire.FrameStderr, []byte("cci: the daemon failed this command\n"))
					_ = wire.WriteFrame(conn, wire.FrameExit, wire.ExitPayload(1))
				}
			}()
			if err := serveCommand(ctx, conn, st, home); err != nil && ctx.Err() == nil {
				slog.Debug("cci daemon: a command's client went away", "err", err)
			}
		})
	}
}

func serveCommand(parent context.Context, conn net.Conn, st *store.Store, home string) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	defer context.AfterFunc(ctx, func() { _ = conn.Close() })()
	if err := conn.SetReadDeadline(time.Now().Add(requestPatience)); err != nil {
		return err
	}
	r := bufio.NewReader(conn)
	kind, payload, err := wire.ReadFrame(r)
	if err != nil {
		return err
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		return err
	}
	var request wire.Request
	if err := json.Unmarshal(payload, &request); err != nil || kind != wire.FrameRequest {
		return errors.Join(fmt.Errorf("bad request frame %q", kind), err)
	}
	if !wire.Relayed(request.Argv) || request.Version != version.String() && !wire.Stream(request.Argv) && !request.Embedder {
		return wire.WriteFrame(conn, wire.FrameFallback, nil)
	}
	captureCommand(home, request)
	replies := &replyStream{conn: conn, acks: make(chan struct{}, 1), input: make(chan []byte, inputFrames), gone: ctx.Done(), lines: wire.Stream(request.Argv)}
	go replies.readClient(r, cancel)
	view := *st
	if err := configureStoreOwner(&view, home); err != nil {
		return err
	}
	view.BeforeCursorMove(replies.Flush)
	root := NewRootCmd() //nolint:contextcheck // the tree takes ctx from ExecuteContext below.
	root.SetArgs(request.Argv[1:])
	root.SetOut(replies)
	root.SetErr(replies.stderr())
	root.SetIn(&inputStream{replies: replies})
	env := requestEnv(request.Env)
	err = root.ExecuteContext(channel.Hosted(context.WithValue(harness.WithConfigDir(ctx, env["CLAUDE_CONFIG_DIR"]), invocationKey{}, &invocation{
		env: env, cwd: request.Cwd, pid: request.PID, stdoutChar: request.StdoutChar, stderrIsStdout: request.StderrIsStdout, embedder: request.Embedder, st: &view,
	})))
	if parent.Err() != nil && wire.Stream(request.Argv) && request.Argv[1] != "channel" {
		return parent.Err()
	}
	if flushed := replies.Flush(ctx); flushed != nil {
		return flushed
	}
	code := 0
	if err != nil {
		code = ExitCode(err)
		if werr := replies.writeFrame(wire.FrameStderr, []byte("cci: "+err.Error()+"\n")); werr != nil {
			return werr
		}
	}
	return replies.writeFrame(wire.FrameExit, wire.ExitPayload(code))
}

// ExitCode is the process exit code of a command's error.
func ExitCode(err error) int {
	if down := (*BusDown)(nil); errors.As(err, &down) {
		return BusDownExit
	}
	if held := (*HeldLane)(nil); errors.As(err, &held) {
		return HeldLaneExit
	}
	return 1
}

type replyStream struct {
	conn    net.Conn
	writing sync.Mutex
	mu      sync.Mutex
	buf     []byte
	acks    chan struct{}
	input   chan []byte
	gone    <-chan struct{}
	lines   bool
}

func (s *replyStream) writeFrame(kind byte, payload []byte) error {
	s.writing.Lock()
	defer s.writing.Unlock()
	return wire.WriteFrame(s.conn, kind, payload)
}

func (s *replyStream) readClient(r io.Reader, cancel context.CancelFunc) {
	defer cancel()
	for {
		kind, payload, err := wire.ReadFrame(r)
		if err != nil {
			return
		}
		switch kind {
		case wire.FrameAck:
			select {
			case s.acks <- struct{}{}:
			case <-s.gone:
				return
			}
		case wire.FrameStdin:
			select {
			case s.input <- payload:
			case <-s.gone:
				return
			}
		}
	}
}

// Write holds p until the next flush; a command's output reaches the client
// before any cursor moves past it. A stream flushes at each complete line, so a
// lost connection never splits one.
func (s *replyStream) Write(p []byte) (int, error) {
	s.mu.Lock()
	s.buf = append(s.buf, p...)
	full := len(s.buf) >= replyBufferBytes || s.lines && bytes.HasSuffix(s.buf, []byte{'\n'})
	s.mu.Unlock()
	if full {
		if err := s.Flush(context.Background()); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

// Flush returns once the client has written everything held so far, or ctx ends.
func (s *replyStream) Flush(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for frame := range slices.Chunk(s.buf, replyFrameBytes) {
		if err := s.writeFrame(wire.FrameStdout, frame); err != nil {
			return err
		}
		select {
		case <-s.acks:
		case <-ctx.Done():
			return ctx.Err()
		case <-s.gone:
			return errors.New("the client went away before its output was written")
		}
	}
	s.buf = s.buf[:0]
	return nil
}

func (s *replyStream) stderr() io.Writer {
	return stderrStream{s}
}

type stderrStream struct {
	replies *replyStream
}

func (s stderrStream) Write(p []byte) (int, error) {
	for frame := range slices.Chunk(p, replyFrameBytes) {
		if err := s.replies.writeFrame(wire.FrameStderr, frame); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

type inputStream struct {
	replies *replyStream
	rest    []byte
}

func (s *inputStream) Read(p []byte) (int, error) {
	if len(s.rest) == 0 {
		if err := s.replies.writeFrame(wire.FrameNeedIn, nil); err != nil {
			return 0, err
		}
		select {
		case s.rest = <-s.replies.input:
		case <-s.replies.gone:
			return 0, errors.New("the client went away before its input was read")
		}
		if len(s.rest) == 0 {
			return 0, io.EOF
		}
	}
	n := copy(p, s.rest)
	s.rest = s.rest[n:]
	return n, nil
}

func isClaudeProcess(argv []string) bool {
	if len(argv) == 0 {
		return false
	}
	switch filepath.Base(argv[0]) {
	case "claude":
		return true
	case "node", "bun":
		if len(argv) > 1 {
			script := filepath.Base(argv[1])
			return script == "claude" || script == "claude.js"
		}
	}
	return false
}

func callerPID(ctx context.Context) int {
	if inv := invoked(ctx); inv != nil {
		return inv.pid
	}
	return os.Getpid()
}

func claudePID(ctx context.Context) int {
	if inv := invoked(ctx); inv != nil {
		return procs.FindAncestor(inv.pid, isClaudeProcess)
	}
	return procs.ClaudePID()
}
