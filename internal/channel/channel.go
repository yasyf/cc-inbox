package channel

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"strconv"
	"time"

	mcp "github.com/yasyf/cc-interact/channel"

	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/version"
)

const (
	Server       = "cci"
	Source       = "plugin:cc-inbox:" + Server
	notifyMethod = "notifications/claude/channel"
)

var (
	idle     = 5 * time.Second
	interval = time.Second
)

const instructions = `This MCP server is cci's channel. It carries coordination records from a long-running drive. After this session runs ` + "`cci subscribe`" + `, every new record for the subscription's reader arrives as a <channel source="` + Source + `" seq="..." kind="..." lane="..."> tag. The tag's body is the record line: ` + "`#<seq> <time> <KIND> <lane> -> <to>: <text> [path]`" + `.

Every tag already passed the subscription's filter, so act on each one as you would a line from ` + "`cci tail`" + `. A record addressed to the reader arrives whatever its kind, and a record addressed to ` + "`main`" + ` is addressed to the reader ` + "`root`" + `: the drive's root answers to both names. Other lanes' broadcasts arrive only for the subscribed kinds. Answer with ` + "`cci post --re <seq>`" + `, close an ask, decide, hold or blocker with ` + "`cci post --resolves <seq>`" + `, and read a record's full body from the path it names. Delivery is at-least-once, so skip any seq you have already handled.

The channel never speaks unsolicited: outside a ` + "`cci subscribe`" + `d session it is silent, and silence needs nothing from you.`

var errStale = errors.New("subscription replaced")

func Serve(ctx context.Context, st *store.Store, window store.Window, in io.Reader, out io.Writer) error {
	srv := mcp.NewServer(mcp.ServerInfo{Name: Server, Version: version.String(), Instructions: instructions}, nil)
	initialized := make(chan struct{})
	requests, relay := io.Pipe()
	go func() { _ = relay.CloseWithError(untilInitialized(in, relay, initialized)) }()
	streamCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-initialized:
			Stream(streamCtx, st, window, srv.Notify)
		case <-streamCtx.Done():
		}
	}()
	err := srv.Serve(ctx, requests, out)
	stop()
	<-done
	return err
}

func untilInitialized(in io.Reader, out io.Writer, initialized chan<- struct{}) error {
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		if initialized != nil {
			var msg struct {
				Method string `json:"method"`
			}
			if json.Unmarshal(sc.Bytes(), &msg) == nil && msg.Method == "notifications/initialized" {
				close(initialized)
				initialized = nil
			}
		}
		if _, err := out.Write(append(sc.Bytes(), '\n')); err != nil {
			return err
		}
	}
	return sc.Err()
}

func Stream(ctx context.Context, st *store.Store, window store.Window, notify func(method string, params any) error) {
	for ctx.Err() == nil {
		sub, ok, err := st.Subscription(ctx, window)
		if err == nil && ok {
			err = follow(ctx, st, sub, notify)
		}
		if err != nil && ctx.Err() == nil {
			slog.Warn("cci channel", "pid", window.PID, "err", err)
		}
		sleep(ctx, idle)
	}
}

func follow(ctx context.Context, st *store.Store, sub store.Subscription, notify func(string, any) error) error {
	watchCtx, stop := context.WithCancel(ctx)
	defer stop()
	changed := make(chan error, 1)
	go func() { changed <- untilChanged(watchCtx, st, sub, stop) }()
	err := inbox.Watch(watchCtx, st, inbox.WatchOptions{Filter: sub.Filter(), Cursor: sub.Cursor, Interval: interval}, func(r store.Record, line string) error {
		now, ok, err := st.Subscription(watchCtx, sub.Window)
		if err != nil {
			return err
		}
		if !ok || !reflect.DeepEqual(now, sub) {
			return errStale
		}
		return notify(notifyMethod, map[string]any{
			"content": line,
			"meta":    map[string]string{"seq": strconv.FormatInt(r.Seq, 10), "kind": string(r.Kind), "lane": r.Lane},
		})
	})
	stop()
	if errors.Is(err, errStale) {
		err = nil
	}
	return errors.Join(err, <-changed)
}

func untilChanged(ctx context.Context, st *store.Store, sub store.Subscription, stop context.CancelFunc) error {
	for sleep(ctx, idle) {
		now, ok, err := st.Subscription(ctx, sub.Window)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			stop()
			return err
		}
		if !ok || !reflect.DeepEqual(now, sub) {
			stop()
			return nil
		}
	}
	return nil
}

func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
