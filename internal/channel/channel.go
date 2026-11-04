package channel

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
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
	batchKind    = "batch"
	releaseTicks = 12
)

// Resumed is the channel flag a re-executed channel server runs with: Claude
// Code finished the MCP handshake with the binary it replaced.
const Resumed = "resumed"

var (
	idle     = 5 * time.Second
	interval = time.Second

	batchWindow = 5 * time.Minute

	newerInstall = defaultNewerInstall
	execve       = syscall.Exec
)

func defaultNewerInstall() (version.Install, bool, error) {
	roots, err := version.DefaultRoots()
	if err != nil {
		return version.Install{}, false, err
	}
	return roots.NewerThan(version.String())
}

type upgrade struct{ version.Install }

func (u upgrade) Error() string {
	return "cci " + u.Version + " is installed at " + u.Path
}

const instructions = `This MCP server is cci's channel. It carries coordination records from a long-running drive. After this session runs ` + "`cci subscribe`" + `, every new record for the subscription's reader arrives as a <channel source="` + Source + `" seq="..." kind="..." lane="..."> tag. The tag's body is the record line without the seq, kind and lane the tag already carries: ` + "`<time> <text> [path]`" + `; ` + "`cci show <seq>`" + `, or this server's read tool, prints the whole record, its recipients included. A tag of kind ` + "`" + batchKind + "`" + ` carries the subscription's batch kinds together, one record a line as ` + "`<seq> <kind> <lane> <text>`" + `, at most every few minutes and at once ahead of any other record. Its tools post, read, inbox, find, close, who and digest do in one call what cci does in Bash, with no shell quoting.

Every tag already passed the subscription's filter, so act on each one as you would a line from ` + "`cci tail`" + `. A record addressed to the reader arrives when its kind is one of the subscription's addressed kinds, by default every kind except for the reader ` + "`root`" + `, which hears only the kinds a root acts on; ` + "`cci tail`" + ` reads the rest. A record addressed to ` + "`main`" + ` is addressed to the reader ` + "`root`" + `: the drive's root answers to both names. Other lanes' broadcasts arrive only for the subscribed kinds. A record addressed to ` + "`peer:<this session>`" + ` or ` + "`<reader>@<drive>`" + ` arrives from any drive; reply to its lane with ` + "`cci post --lane <reader> --to peer:<its session prefix>`" + `, which ` + "`cci who`" + ` lists. Every post names its writer with ` + "`--lane <reader>`" + `: answer with ` + "`cci post --lane <reader> --re <seq>`" + `, close an ask, decide, hold or blocker with ` + "`cci post --lane <reader> --resolves <seq>`" + `, and read a record's full body from the path it names. The ` + "`post`" + ` tool posts without a Bash call: its args are the arguments ` + "`cci post`" + ` takes, and it posts as this window's thread, as a Bash ` + "`cci post`" + ` here does. A record this channel delivers is not delivered again by the PostToolUse hook, nor one the hook delivered first by this channel. Delivery is at-least-once, so skip any seq you have already handled.

The channel never speaks unsolicited: outside a ` + "`cci subscribe`" + `d session it is silent, and silence needs nothing from you.`

var errStale = errors.New("subscription replaced")

// Serve answers the MCP session on in and out and streams the window's
// subscription once Claude Code reports the session initialized, or at once
// when resumed. When a newer cci is installed, the server re-executes it on
// the same stdio, resumed, so a long-lived window never keeps filtering with
// the release it started on.
type hostedKey struct{}

// Hosted marks ctx as a channel the daemon runs for a client: the daemon's own
// upgrade moves it to the new release, so it never re-executes.
func Hosted(ctx context.Context) context.Context {
	return context.WithValue(ctx, hostedKey{}, true)
}

func hosted(ctx context.Context) bool {
	return ctx.Value(hostedKey{}) != nil
}

func Serve(ctx context.Context, st *store.Store, window store.Window, channeled, resumed bool, in io.Reader, out io.Writer, tools ...mcp.Tool) error {
	srv := mcp.NewServer(mcp.ServerInfo{Name: Server, Version: version.String(), Instructions: instructions, Resumed: resumed}, tools)
	initialized := make(chan struct{})
	watch := initialized
	if resumed {
		close(initialized)
		watch = nil
	}
	requests, relay := io.Pipe()
	go func() { _ = relay.CloseWithError(untilInitialized(in, relay, watch)) }()
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
	for tick := 1; ctx.Err() == nil; tick++ {
		sub, ok, err := st.Subscription(ctx, window)
		switch {
		case err == nil && ok:
			err = follow(ctx, st, sub, channeled, notify)
		case err == nil && tick%releaseTicks == 0 && !hosted(ctx):
			err = newerRelease()
		}
		var newer upgrade
		if errors.As(err, &newer) {
			err = execve(newer.Path, []string{newer.Path, "channel", "--" + Resumed}, os.Environ())
		}
		if err != nil && ctx.Err() == nil {
			slog.Warn("cci channel", "pid", window.PID, "err", err)
		}
		sleep(ctx, idle)
	}
}

type batch struct {
	st     *store.Store
	filter store.Filter
	cursor string
	notify func(string, any) error
	tick   *time.Ticker
	mu     sync.Mutex
}

func (b *batch) run(ctx context.Context, stop context.CancelFunc) error {
	if len(b.filter.Kinds) == 0 {
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-b.tick.C:
			if err := b.flush(ctx); err != nil && ctx.Err() == nil {
				stop()
				return err
			}
		}
	}
}

func (b *batch) flush(ctx context.Context) error {
	if len(b.filter.Kinds) == 0 {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	f := b.filter
	var err error
	if f.After, _, err = b.st.Cursor(ctx, b.cursor, f.Drive); err != nil {
		return err
	}
	records, err := b.st.Query(ctx, f)
	if err != nil || len(records) == 0 {
		return err
	}
	wire := render.Wire{Now: b.st.Now()}
	lines := make([]string, len(records))
	for i, r := range records {
		line := r
		line.To = nil
		lines[i] = render.WithBody(wire.Line(line, 0), r)
	}
	last := records[len(records)-1].Seq
	if err := b.notify(notifyMethod, map[string]any{"content": strings.Join(lines, "\n"), "meta": map[string]string{"seq": strconv.FormatInt(last, 10), "kind": batchKind}}); err != nil {
		return err
	}
	b.tick.Reset(batchWindow)
	return b.st.SetCursor(context.WithoutCancel(ctx), b.cursor, f.Drive, last)
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
	held := &batch{st: st, filter: sub.BatchFilter(), cursor: sub.BatchCursor(), notify: notify, tick: time.NewTicker(batchWindow)}
	defer held.tick.Stop()
	batched := make(chan error, 1)
	go func() { batched <- held.run(watchCtx, stop) }()
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
		if err := held.flush(watchCtx); err != nil {
			if watchCtx.Err() != nil {
				return errStale
			}
			return err
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
	return errors.Join(err, <-changed, <-batched)
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
	var seen store.SubscriptionStamp
	for tick := 1; sleep(ctx, idle); tick++ {
		stamp, err := st.SubscriptionStamp(sub.Window)
		if err == nil && stamp != seen {
			seen = stamp
			var now store.Subscription
			var ok bool
			now, ok, err = st.Subscription(ctx, sub.Window)
			if err == nil && (!ok || !reflect.DeepEqual(now, sub)) {
				stop()
				return nil
			}
		}
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			stop()
			return err
		}
		if tick%releaseTicks == 0 && !hosted(ctx) {
			if err := newerRelease(); err != nil {
				stop()
				return err
			}
		}
	}
	return nil
}

func newerRelease() error {
	newer, found, err := newerInstall()
	if err != nil || !found {
		return err
	}
	return upgrade{newer}
}

func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
