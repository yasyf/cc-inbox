package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/yasyf/cc-interact/procs"

	"github.com/yasyf/cc-inbox/internal/channel"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/version"
	"github.com/yasyf/cc-inbox/internal/wire"
)

const replyBufferBytes = 64 << 10

type invocationKey struct{}

// invocation is who a command runs for when the daemon runs it: a client's
// environment, directory and process, in place of the daemon's own.
type invocation struct {
	env        map[string]string
	cwd        string
	pid        int
	stdoutChar bool
	st         *store.Store
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
	r := bufio.NewReader(conn)
	kind, payload, err := wire.ReadFrame(r)
	if err != nil {
		return err
	}
	var request wire.Request
	if err := json.Unmarshal(payload, &request); err != nil || kind != wire.FrameRequest {
		return errors.Join(fmt.Errorf("bad request frame %q", kind), err)
	}
	if !wire.Relayed(request.Argv) || request.Version != version.String() && !wire.Stream(request.Argv) {
		return wire.WriteFrame(conn, wire.FrameFallback, nil)
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	replies := &replyStream{conn: conn, acks: make(chan struct{}, 1), input: make(chan []byte, 1), gone: ctx.Done(), unbuffered: wire.Stream(request.Argv)}
	go replies.readClient(r, cancel)
	view := *st
	if err := configureStoreOwner(&view, home); err != nil {
		return err
	}
	view.BeforeCursorMove(replies.Flush)
	env := make(map[string]string, len(request.Env))
	for _, entry := range request.Env {
		if name, value, ok := strings.Cut(entry, "="); ok {
			env[name] = value
		}
	}
	root := NewRootCmd()
	root.SetArgs(request.Argv[1:])
	root.SetOut(replies)
	root.SetErr(&stderrStream{conn: conn})
	root.SetIn(&inputStream{replies: replies})
	err = root.ExecuteContext(channel.Hosted(context.WithValue(ctx, invocationKey{}, &invocation{
		env: env, cwd: request.Cwd, pid: request.PID, stdoutChar: request.StdoutChar, st: &view,
	})))
	if flushed := replies.Flush(ctx); flushed != nil {
		return flushed
	}
	code := 0
	if err != nil {
		code = ExitCode(err)
		if werr := wire.WriteFrame(conn, wire.FrameStderr, []byte("cci: "+err.Error()+"\n")); werr != nil {
			return werr
		}
	}
	return wire.WriteFrame(conn, wire.FrameExit, wire.ExitPayload(code))
}

// ExitCode is the process exit code of a command's error.
func ExitCode(err error) int {
	if down := (*BusDown)(nil); errors.As(err, &down) {
		return BusDownExit
	}
	return 1
}

type replyStream struct {
	conn  net.Conn
	mu    sync.Mutex
	buf   []byte
	acks  chan struct{}
	input chan []byte
	gone  <-chan struct{}

	unbuffered bool
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
			s.acks <- struct{}{}
		case wire.FrameStdin:
			s.input <- payload
		}
	}
}

// Write holds p until the next flush; a command's output reaches the client
// before any cursor moves past it.
func (s *replyStream) Write(p []byte) (int, error) {
	s.mu.Lock()
	s.buf = append(s.buf, p...)
	full := s.unbuffered || len(s.buf) >= replyBufferBytes
	s.mu.Unlock()
	if full {
		if err := s.Flush(context.Background()); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

// Flush returns once the client has written everything held so far.
func (s *replyStream) Flush(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.buf) == 0 {
		return nil
	}
	if err := wire.WriteFrame(s.conn, wire.FrameStdout, s.buf); err != nil {
		return err
	}
	s.buf = s.buf[:0]
	select {
	case <-s.acks:
		return nil
	case <-s.gone:
		return errors.New("the client went away before its output was written")
	}
}

type stderrStream struct {
	conn net.Conn
}

func (s *stderrStream) Write(p []byte) (int, error) {
	if err := wire.WriteFrame(s.conn, wire.FrameStderr, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

type inputStream struct {
	replies *replyStream
	rest    []byte
}

func (s *inputStream) Read(p []byte) (int, error) {
	if len(s.rest) == 0 {
		if err := wire.WriteFrame(s.replies.conn, wire.FrameNeedIn, nil); err != nil {
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

func claudePID(ctx context.Context) int {
	if inv := invoked(ctx); inv != nil {
		return procs.FindAncestor(inv.pid, isClaudeProcess)
	}
	return procs.ClaudePID()
}
