package daemon

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	offerPrefix     = "handoff-"
	offerSuffix     = ".sock"
	handoffPatience = 2 * time.Second
)

var offers atomic.Int64

// Listen returns the HTTP listener a running daemon or a move in progress
// offers in home, so the connections queued on it are served rather than
// refused, and binds addr only when nothing offers one.
func Listen(home, addr string) (*net.TCPListener, error) {
	ln, ok, err := take(home)
	if err != nil {
		return nil, err
	}
	if ok {
		return ln, nil
	}
	bound, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", addr, err)
	}
	return bound.(*net.TCPListener), nil
}

// Held is a move's hold on the daemon's HTTP listener: while the old daemon
// drains and its successor starts, a client's connection waits on the socket
// instead of being refused.
type Held struct {
	ln      *net.TCPListener
	offered *net.UnixListener
}

// Hold takes the HTTP listener the daemon in home offers and offers it in turn
// until Close, so the daemon that replaces it serves the same socket. It holds
// nothing when no daemon offers one.
func Hold(home string) (*Held, error) {
	ln, ok, err := take(home)
	if err != nil {
		return nil, err
	}
	if !ok {
		return &Held{}, nil
	}
	offered, err := offer(home, ln)
	if err != nil {
		return nil, errors.Join(err, ln.Close())
	}
	return &Held{ln: ln, offered: offered}, nil
}

// Close lets go of the listener; a daemon that took it keeps serving it.
func (h *Held) Close() error {
	if h.ln == nil {
		return nil
	}
	return errors.Join(h.offered.Close(), h.ln.Close())
}

func offerUntil(ctx context.Context, home string, ln *net.TCPListener) {
	offered, err := offer(home, ln)
	if err != nil {
		slog.Warn("cci daemon cannot offer its http listener; the next daemon move refuses connections", "err", err)
		return
	}
	context.AfterFunc(ctx, func() { _ = offered.Close() })
}

func offer(home string, ln *net.TCPListener) (*net.UnixListener, error) {
	raw, err := ln.SyscallConn()
	if err != nil {
		return nil, fmt.Errorf("reach the http listener: %w", err)
	}
	if err := os.MkdirAll(home, 0o700); err != nil {
		return nil, fmt.Errorf("create %s: %w", home, err)
	}
	path := filepath.Join(home, fmt.Sprintf("%s%d-%d%s", offerPrefix, os.Getpid(), offers.Add(1), offerSuffix))
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("clear a dead process's listener offer: %w", err)
	}
	offered, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, fmt.Errorf("offer the http listener: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return nil, errors.Join(fmt.Errorf("lock the listener offer: %w", err), offered.Close())
	}
	go func() {
		for {
			conn, err := offered.AcceptUnix()
			if err != nil {
				if !errors.Is(err, net.ErrClosed) {
					slog.Warn("cci stopped offering its http listener; the next daemon move refuses connections", "err", err)
				}
				return
			}
			var sent error
			if err := raw.Control(func(fd uintptr) {
				_, _, sent = conn.WriteMsgUnix([]byte{0}, syscall.UnixRights(int(fd)), nil)
			}); err != nil {
				sent = err
			}
			if err := errors.Join(sent, conn.Close()); err != nil {
				slog.Debug("cci could not hand over its http listener", "err", err)
			}
		}
	}()
	return offered, nil
}

func take(home string) (*net.TCPListener, bool, error) {
	entries, err := os.ReadDir(home)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("look for a listener offer: %w", err)
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), offerPrefix) || !strings.HasSuffix(entry.Name(), offerSuffix) {
			continue
		}
		path := filepath.Join(home, entry.Name())
		ln, err := takeFrom(path)
		if err == nil {
			return ln, true, nil
		}
		if errors.Is(err, syscall.ECONNREFUSED) {
			_ = os.Remove(path)
		}
		slog.Debug("cci skipped a listener offer it could not take", "path", path, "err", err)
	}
	return nil, false, nil
}

func takeFrom(path string) (*net.TCPListener, error) {
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	if err := conn.SetReadDeadline(time.Now().Add(handoffPatience)); err != nil {
		return nil, err
	}
	oob := make([]byte, syscall.CmsgSpace(4))
	_, oobn, _, _, err := conn.ReadMsgUnix(make([]byte, 1), oob)
	if err != nil {
		return nil, fmt.Errorf("read the offered listener: %w", err)
	}
	msgs, err := syscall.ParseSocketControlMessage(oob[:oobn])
	if err != nil {
		return nil, fmt.Errorf("parse the offered listener: %w", err)
	}
	if len(msgs) != 1 {
		return nil, fmt.Errorf("the offer carried %d control messages, want 1", len(msgs))
	}
	fds, err := syscall.ParseUnixRights(&msgs[0])
	if err != nil {
		return nil, fmt.Errorf("parse the offered listener: %w", err)
	}
	file := os.NewFile(uintptr(fds[0]), path)
	defer func() { _ = file.Close() }()
	ln, err := net.FileListener(file)
	if err != nil {
		return nil, fmt.Errorf("adopt the offered listener: %w", err)
	}
	return ln.(*net.TCPListener), nil
}
