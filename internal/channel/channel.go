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
	"github.com/yasyf/cc-inbox/internal/render"
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

const instructions = `This MCP server is cci's channel. It carries coordination records from a long-running drive. After this session runs ` + "`cci subscribe`" + `, every new record for the subscription's reader arrives as a <channel source="` + Source + `" seq="..." kind="..." lane="..."> tag. The tag's body is the record line without the seq, kind and lane the tag already carries: ` + "`<time> -> <to> <text> [path]`" + `; ` + "`cci show <seq>`" + ` prints the whole record.

Every tag already passed the subscription's filter, so act on each one as you would a line from ` + "`cci tail`" + `. A record addressed to the reader arrives whatever its kind, and a record addressed to ` + "`main`" + ` is addressed to the reader ` + "`root`" + `: the drive's root answers to both names. Other lanes' broadcasts arrive only for the subscribed kinds. A record addressed to ` + "`peer:<this session>`" + ` or ` + "`<reader>@<drive>`" + ` arrives from any drive; reply to its lane with ` + "`cci post --to peer:<its session prefix>`" + `, which ` + "`cci who`" + ` lists. Answer with ` + "`cci post --re <seq>`" + `, close an ask, decide, hold or blocker with ` + "`cci post --resolves <seq>`" + `, and read a record's full body from the path it names. A record this channel delivers is not delivered again by the PostToolUse hook, nor one the hook delivered first by this channel. Delivery is at-least-once, so skip any seq you have already handled.

The channel never speaks unsolicited: outside a ` + "`cci subscribe`" + `d session it is silent, and silence needs nothing from you.`

var errStale = errors.New("subscription replaced")

func Serve(ctx context.Context, st *store.Store, window store.Window, channeled bool, in io.Reader, out io.Writer) error {
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
			Stream(streamCtx, st, window, channeled, srv.Notify)
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

func Stream(ctx context.Context, st *store.Store, window store.Window, channeled bool, notify func(method string, params any) error) {
	for ctx.Err() == nil {
		sub, ok, err := st.Subscription(ctx, window)
		if err == nil && ok {
			err = follow(ctx, st, sub, channeled, notify)
		}
		if err != nil && ctx.Err() == nil {
			slog.Warn("cci channel", "pid", window.PID, "err", err)
		}
		sleep(ctx, idle)
	}
}

func follow(ctx context.Context, st *store.Store, sub store.Subscription, channeled bool, notify func(string, any) error) error {
	mail, shared, err := hookMail(ctx, st, sub, channeled)
	if err != nil {
		return err
	}
	watchCtx, stop := context.WithCancel(ctx)
	defer stop()
	changed := make(chan error, 1)
	go func() { changed <- untilChanged(watchCtx, st, sub, stop) }()
	err = inbox.Watch(watchCtx, st, inbox.WatchOptions{Filter: sub.Filter(), Cursor: sub.Cursor, Interval: interval, Untagged: true}, func(r store.Record, line string) error {
		now, ok, err := st.Subscription(watchCtx, sub.Window)
		if err != nil {
			return err
		}
		if !ok || !reflect.DeepEqual(now, sub) {
			return errStale
		}
		var seen int64
		if shared {
			b, ok, err := st.Binding(watchCtx, mail.key)
			if err != nil {
				return err
			}
			if !ok || b != mail.binding {
				return errStale
			}
			if seen, _, err = st.Cursor(watchCtx, mail.cursor, mail.filter.Drive); err != nil {
				return err
			}
			if r.Seq <= seen {
				delivered, err := mail.has(watchCtx, st, r)
				if err != nil || delivered {
					return err
				}
			}
		}
		meta := map[string]string{"seq": strconv.FormatInt(r.Seq, 10), "kind": string(r.Kind), "lane": r.Lane}
		if r.ID != "" {
			meta["id"] = r.ID
		}
		if err := notify(notifyMethod, map[string]any{"content": render.WithBody(line, r), "meta": meta}); err != nil {
			return err
		}
		if shared && seen >= mail.start {
			return st.SetCursor(watchCtx, mail.cursor, mail.filter.Drive, r.Seq)
		}
		return nil
	})
	stop()
	if errors.Is(err, errStale) {
		err = nil
	}
	return errors.Join(err, <-changed)
}

type laneMail struct {
	key     store.PeerKey
	binding store.Binding
	cursor  string
	filter  store.Filter
	start   int64
}

func hookMail(ctx context.Context, st *store.Store, sub store.Subscription, channeled bool) (laneMail, bool, error) {
	if !channeled {
		return laneMail{}, false, nil
	}
	k := store.PeerKey{Session: sub.Session}
	b, ok, err := st.Binding(ctx, k)
	if err != nil || !ok || b.Lane == "" || b.Drive != sub.Drive || !store.SameLane(b.Lane, sub.Reader) {
		return laneMail{}, false, err
	}
	start, _, err := st.Cursor(ctx, sub.Cursor, sub.Drive)
	if err != nil {
		return laneMail{}, false, err
	}
	return laneMail{key: k, binding: b, cursor: store.LaneCursor(k), filter: store.LaneMail(k, b), start: start}, true, nil
}

func (m laneMail) has(ctx context.Context, st *store.Store, r store.Record) (bool, error) {
	if r.Kind.Opens() && r.Tracked() {
		open, err := st.OpenAmong(ctx, r.Drive, []int64{r.Seq})
		if err != nil || len(open) == 0 {
			return false, err
		}
	}
	f := m.filter
	f.After, f.Before = r.Seq-1, r.Seq+1
	n, err := st.Count(ctx, f)
	return n > 0, err
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
