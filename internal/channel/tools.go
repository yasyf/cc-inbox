package channel

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	mcp "github.com/yasyf/cc-interact/channel"
	"golang.org/x/sys/unix"
)

// ServeTools answers the MCP session on in and out with srv. Serving its own
// stdin, it re-executes a newer install there resumed once every request it
// read is answered, so the install reads each request still unread; a server
// the daemon runs only serves, since the daemon's upgrade moves it.
func ServeTools(ctx context.Context, srv *mcp.Server, in io.Reader, out io.Writer) error {
	stdin, ok := in.(*os.File)
	if !ok {
		return srv.Serve(ctx, in, out)
	}
	requests, relay := io.Pipe()
	p := &pending{out: out}
	go func() { _ = relay.CloseWithError(forward(stdin, relay, p)) }()
	return srv.Serve(ctx, requests, p)
}

func forward(stdin *os.File, relay io.Writer, p *pending) error {
	r := bufio.NewReader(stdin)
	for checked := time.Now(); ; {
		if r.Buffered() == 0 {
			if p.requests.Load() == 0 && time.Since(checked) >= releaseTicks*idle {
				checked = time.Now()
				err := newerRelease()
				var newer upgrade
				if errors.As(err, &newer) {
					err = execve(newer.Path, []string{newer.Path, "mcp", "--" + Resumed}, os.Environ())
				}
				if err != nil {
					slog.Warn("cci mcp", "err", err)
				}
			}
			ready, err := readable(stdin, idle)
			if err != nil {
				return err
			}
			if !ready {
				continue
			}
		}
		line, err := r.ReadBytes('\n')
		if identified(line) {
			p.requests.Add(1)
		}
		if len(line) > 0 {
			if _, err := relay.Write(line); err != nil {
				return err
			}
		}
		if err != nil {
			return err
		}
	}
}

func readable(f *os.File, within time.Duration) (bool, error) {
	conn, err := f.SyscallConn()
	if err != nil {
		return false, err
	}
	var n int
	var polled error
	if err := conn.Control(func(fd uintptr) {
		n, polled = unix.Poll([]unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}, int(within.Milliseconds())) //nolint:gosec // G115: a file descriptor fits in int32.
	}); err != nil {
		return false, err
	}
	if errors.Is(polled, unix.EINTR) {
		return false, nil
	}
	return n > 0, polled
}

type pending struct {
	out      io.Writer
	line     []byte
	requests atomic.Int64
}

func (p *pending) Write(b []byte) (int, error) {
	p.line = append(p.line, b...)
	if !bytes.HasSuffix(p.line, []byte("\n")) {
		return len(b), nil
	}
	line := p.line
	p.line = nil
	if _, err := p.out.Write(line); err != nil {
		return 0, err
	}
	if identified(line) {
		p.requests.Add(-1)
	}
	return len(b), nil
}

func identified(line []byte) bool {
	var msg struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Method  string          `json:"method"`
	}
	return json.Unmarshal(line, &msg) == nil && len(msg.ID) > 0
}
