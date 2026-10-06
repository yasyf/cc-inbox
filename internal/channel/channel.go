package channel

import (
	"context"
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

That means records addressed to the reader, plus other lanes' broadcasts of the subscribed kinds. Act on each one as you would a line from ` + "`cci tail`" + `. Answer with ` + "`cci post --re <seq>`" + `, close an ask, decide, hold or blocker with ` + "`cci post --resolves <seq>`" + `, and read a record's full body from the path it names. Delivery is at-least-once, so skip any seq you have already handled.

The channel never speaks unsolicited: outside a ` + "`cci subscribe`" + `d session it is silent, and silence needs nothing from you.`

func Serve(ctx context.Context, st *store.Store, window store.Window, in io.Reader, out io.Writer) error {
	srv := mcp.NewServer(mcp.ServerInfo{Name: Server, Version: version.String(), Instructions: instructions}, nil)
	streamCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		Stream(streamCtx, st, window, srv.Notify)
	}()
	err := srv.Serve(ctx, in, out)
	stop()
	<-done
	return err
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
		return notify(notifyMethod, map[string]any{
			"content": line,
			"meta":    map[string]string{"seq": strconv.FormatInt(r.Seq, 10), "kind": string(r.Kind), "lane": r.Lane},
		})
	})
	stop()
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
