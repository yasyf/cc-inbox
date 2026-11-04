package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/yasyf/cc-inbox/internal/cli"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/version"
	"github.com/yasyf/cc-inbox/internal/wire"
)

const followRedial = time.Second

var errNoDaemon = errors.New("no daemon took this call")

// daemonRun has the daemon run argv for this process. errNoDaemon means it did
// not start, so the caller runs the call itself against the store.
func daemonRun(ctx context.Context, argv []string, stdin io.Reader, stdout, stderr io.Writer) (int, error) {
	home, err := store.Home()
	if err != nil {
		return 0, err
	}
	conn, err := wire.Dial(filepath.Join(home, wire.SocketName))
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
	code, ok := wire.Exchange(conn, request, stdin, stdout, stderr)
	if !ok {
		return 0, errNoDaemon
	}
	return code, nil
}

func daemonError(code int, stderr string) error {
	message := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(stderr), "cci: "))
	switch {
	case code == 0:
		return nil
	case code == cli.BusDownExit:
		return &BusDown{Cause: fmt.Errorf("%w: %s", store.ErrUnconfirmed, message)}
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
	code, err := daemonRun(ctx, []string{"embed", c.Op}, bytes.NewReader(in), &stdout, &stderr)
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
	held  []byte
	after *int64
	emit  func(Record) error
	err   error
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
		*l.after = r.Seq
	}
}

func daemonFollow(ctx context.Context, c cli.EmbedCall, emit func(Record) error) error {
	if c.After == 0 {
		head, err := daemonCall[int64](ctx, cli.EmbedCall{Op: "head"})
		if err != nil {
			return err
		}
		c.After = head
	}
	for {
		in, err := json.Marshal(c)
		if err != nil {
			return err
		}
		out := &lineEmitter{after: &c.After, emit: emit}
		var stderr bytes.Buffer
		code, err := daemonRun(ctx, []string{"embed", "follow"}, bytes.NewReader(in), out, &stderr)
		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case out.err != nil:
			return out.err
		case err != nil && !errors.Is(err, errNoDaemon):
			return err
		case err == nil && code != 0 && code != cli.BusDownExit:
			return daemonError(code, stderr.String())
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(followRedial):
		}
	}
}
