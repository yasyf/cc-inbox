package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/yasyf/cc-inbox/internal/cli"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/version"
	"github.com/yasyf/cc-inbox/internal/wire"
)

const (
	followRedial = time.Second
	followLocal  = 5 * time.Second
)

var (
	errNoDaemon = errors.New("no daemon took this call")
	errLostCall = errors.New("the daemon went away during this call")
)

// daemonRun has the daemon run argv for this process. errNoDaemon means it did
// not start, so the caller runs the call itself against the store; errLostCall
// means the daemon may have run it.
func daemonRun(ctx context.Context, argv []string, stdin io.Reader, stdout, stderr io.Writer) (int, error) {
	home, err := store.Home()
	if err != nil {
		return 0, err
	}
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", filepath.Join(home, wire.SocketName))
	if err != nil {
		return 0, errNoDaemon
	}
	defer func() { _ = conn.Close() }()
	defer context.AfterFunc(ctx, func() { _ = conn.Close() })()
	cwd, err := os.Getwd()
	if err != nil {
		return 0, fmt.Errorf("resolve cwd: %w", err)
	}
	request := wire.Request{
		Version: version.String(), Argv: append([]string{"cci"}, argv...), Env: os.Environ(), Cwd: cwd, PID: os.Getpid(), Embedder: true,
	}
	code, ran, lost := wire.ExchangeOutcome(conn, request, stdin, stdout, stderr)
	switch {
	case !ran && lost:
		return 0, errLostCall
	case !ran:
		return 0, errNoDaemon
	}
	return code, nil
}

// daemonRead is daemonRun for a call that is safe to run again.
func daemonRead(ctx context.Context, argv []string, stdin io.Reader, stdout, stderr io.Writer) (int, error) {
	code, err := daemonRun(ctx, argv, stdin, stdout, stderr)
	if errors.Is(err, errLostCall) {
		return 0, errNoDaemon
	}
	return code, err
}

func daemonError(code int, stderr string) error {
	message := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(stderr), "cci: "))
	switch {
	case code == 0:
		return nil
	case code == cli.BusDownExit:
		return &BusDown{Cause: fmt.Errorf("%w: %s", store.ErrUnconfirmed, message), Path: "the daemon's unsent queue"}
	case message == ErrNoLaneRegistry.Error():
		return ErrNoLaneRegistry
	}
	return errors.New(message)
}

func daemonCall[T any](ctx context.Context, c cli.EmbedCall) (T, error) {
	var answer T
	in, err := json.Marshal(c)
	if err != nil {
		return answer, err
	}
	var stdout, stderr bytes.Buffer
	code, err := daemonRead(ctx, []string{"embed", c.Op}, bytes.NewReader(in), &stdout, &stderr)
	if err != nil {
		return answer, err
	}
	if err := daemonError(code, stderr.String()); err != nil {
		return answer, err
	}
	if err := json.Unmarshal(stdout.Bytes(), &answer); err != nil {
		return answer, fmt.Errorf("the daemon answered %s with %q: %w", c.Op, stdout.String(), err)
	}
	return answer, nil
}

func unwrapped(records []cli.EmbedRecord) []Record {
	if records == nil {
		return nil
	}
	out := make([]Record, len(records))
	for i, r := range records {
		out[i] = r.Unwrapped()
	}
	return out
}

func embedRecords(ctx context.Context, c cli.EmbedCall) ([]Record, error) {
	records, err := daemonCall[[]cli.EmbedRecord](ctx, c)
	if err == nil {
		return unwrapped(records), nil
	}
	if !errors.Is(err, errNoDaemon) {
		return nil, err
	}
	st, err := sharedStore(ctx)
	if err != nil {
		return nil, err
	}
	return cli.EmbedRecordsFrom(ctx, st, c)
}

type lineEmitter struct {
	held []byte
	emit func(Record) error
	err  error
}

func (l *lineEmitter) Write(p []byte) (int, error) {
	l.held = append(l.held, p...)
	for {
		end := bytes.IndexByte(l.held, '\n')
		if end < 0 {
			return len(p), nil
		}
		line := l.held[:end]
		l.held = l.held[end+1:]
		var r cli.EmbedRecord
		if err := json.Unmarshal(line, &r); err != nil {
			l.err = fmt.Errorf("the daemon followed with %q: %w", line, err)
			return 0, l.err
		}
		if err := l.emit(r.Unwrapped()); err != nil {
			l.err = err
			return 0, err
		}
	}
}

func daemonFollow(ctx context.Context, c cli.EmbedCall, emit func(Record) error) error {
	if err := followStart(ctx, &c); err != nil {
		return err
	}
	tracked := func(r Record) error {
		if err := emit(r); err != nil {
			return err
		}
		c.After = r.Seq
		return nil
	}
	for {
		in, err := json.Marshal(c)
		if err != nil {
			return err
		}
		out := &lineEmitter{emit: tracked}
		var stderr bytes.Buffer
		code, err := daemonRead(ctx, []string{"embed", "follow"}, bytes.NewReader(in), out, &stderr)
		switch {
		case ctx.Err() != nil:
			return nil
		case out.err != nil:
			return out.err
		case errors.Is(err, errNoDaemon):
			if err := followStore(ctx, c, tracked); err != nil {
				return err
			}
			continue
		case err != nil:
			return err
		case code != 0 && code != cli.BusDownExit:
			return daemonError(code, stderr.String())
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(followRedial):
		}
	}
}

// followStart fixes where a follow with no After begins, so that a redial cannot
// begin later and skip what arrived between. An empty store begins before any seq.
func followStart(ctx context.Context, c *cli.EmbedCall) error {
	if c.After != 0 {
		return nil
	}
	head, err := daemonCall[int64](ctx, cli.EmbedCall{Op: "head"})
	if errors.Is(err, errNoDaemon) {
		st, openErr := sharedStore(ctx)
		if openErr != nil {
			return openErr
		}
		head, err = st.MaxSeq(ctx)
	}
	if err != nil {
		return err
	}
	c.After = head
	if head == 0 {
		c.After = -1
	}
	return nil
}

// followStore reads the store itself for a moment while no daemon answers, then
// lets the socket be tried again.
func followStore(ctx context.Context, c cli.EmbedCall, emit func(Record) error) error {
	st, err := sharedStore(ctx)
	if err != nil {
		return err
	}
	bounded, cancel := context.WithTimeout(ctx, followLocal)
	defer cancel()
	err = cli.EmbedFollow(bounded, st, c, cli.SelfPeer(ctx).String(), emit)
	if bounded.Err() != nil {
		return nil
	}
	return err
}
