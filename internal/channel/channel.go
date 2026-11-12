package channel

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	mcp "github.com/yasyf/cc-interact/channel"

	"github.com/yasyf/cc-inbox/internal/hook"
	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/version"
)

const (
	Plugin       = "plugin:cc-inbox:"
	Server       = "channel"
	Source       = Plugin + Server
	notifyMethod = "notifications/claude/channel"
	disconnectTo = "communicator"
	batchKind    = "batch"
	skippedKind  = "skipped"
	releaseTicks = 12
	batchBudget  = 3072
	rootWhole    = 2000
)

// Resumed is the channel flag a re-executed channel server runs with: Claude
// Code finished the MCP handshake with the binary it replaced.
const Resumed = "resumed"

// NoTools is the channel flag of the plugin's tool-less channel server, which
// no tool call ever starts; the plugin's cci server serves the tools.
const NoTools = "no-tools"

var (
	idle      = 5 * time.Second
	beatEvery = 15 * time.Second
	interval  = time.Second

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

const instructions = `This MCP server is cci's channel. After this session runs ` + "`cci subscribe`" + `, each record for its reader arrives as a <channel source="` + Source + `" seq kind lane> tag whose body is its compact line under an ` + "`@<time>`" + ` line, ` + "`<seq> <kind> <lane> [^<re>] [x<closed>] [<repo>#<pr>] [t:<topic>] [+] <text>`" + `; act on it as on a ` + "`cci tail`" + ` line. A ` + "`+`" + ` marks text or a body the line leaves out, and the read tool prints the record whole; a record addressed to a root reader carries that rest under its line when it fits 2,000 bytes, so it needs no read. A ` + "`" + batchKind + "`" + ` tag carries several records, one compact line each: the newest per lane and topic, and per pull request for head, within about 3 KB, ending in a ` + "`+N more`" + ` line naming the cci tail that reads the rest. A ` + "`" + skippedKind + "`" + ` tag counts the records addressed to the reader from before this channel started, which the inbox tool reads. Answer a record with the post tool's ` + "`re`" + ` or ` + "`cci post --re <seq>`" + `, and close an ask, decide, hold or blocker with ` + "`resolves`" + `, ` + "`--resolves <seq>`" + ` or the close tool; post and close default ` + "`lane`" + ` to this window's. A record's cursor moves once this session's transcript shows its tag, so until then the PostToolUse hook may deliver it too and a restarted channel repeats it: skip a seq you have already handled. Outside a subscribed session the channel is silent and needs nothing from you.`

var (
	errStale    = errors.New("subscription replaced")
	errReplaced = errors.New("a newer channel server owns the window")
)

type hostedKey struct{}

// Hosted marks ctx as a channel the daemon runs for a client while daemon
// lives: the daemon's own upgrade moves it to the new release, so it never
// re-executes, and a channel the daemon ends that way never disconnected.
func Hosted(ctx, daemon context.Context) context.Context {
	return context.WithValue(ctx, hostedKey{}, daemon)
}

func hosted(ctx context.Context) bool {
	return ctx.Value(hostedKey{}) != nil
}

func stopping(ctx context.Context) bool {
	daemon, ok := ctx.Value(hostedKey{}).(context.Context)
	return ok && daemon.Err() != nil
}

// Serve answers the MCP session on in and out and streams the window's
// subscription once Claude Code reports it initialized, or at once when
// resumed; a newer install re-executes on the same stdio. It returns once a
// server started after proc claims the window. A subscribed channel ending any
// other way than a daemon's stop posts a disconnect to the communicator.
func Serve(ctx context.Context, st *store.Store, window, proc store.Window, channeled, resumed bool, in io.Reader, out io.Writer, tools ...mcp.Tool) error {
	if owner, err := st.ClaimChannel(window, proc); err != nil || owner != proc {
		return err
	}
	var started *int64
	if !resumed {
		head, err := st.MaxSeq(ctx)
		if err != nil {
			return err
		}
		started = &head
	}
	srv := mcp.NewServer(mcp.ServerInfo{Name: "cci", Version: version.String(), Instructions: instructions, Resumed: resumed}, tools)
	initialized := make(chan struct{})
	watch := initialized
	if resumed {
		close(initialized)
		watch = nil
	}
	requests, relay := io.Pipe()
	go func() { _ = relay.CloseWithError(untilInitialized(in, relay, watch)) }()
	streamCtx, stop := context.WithCancel(ctx)
	owns := func() error {
		owner, err := st.ClaimChannel(window, proc)
		if err != nil || owner == proc {
			return err
		}
		slog.Info("cci channel: a newer channel server owns the window", "pid", window.PID, "owner", owner.PID)
		stop()
		_ = relay.Close()
		return errReplaced
	}
	notify := func(method string, params any) error {
		if err := owns(); err != nil {
			return err
		}
		return srv.Notify(method, params)
	}
	again := []string{"channel", "--" + Resumed}
	if len(tools) == 0 {
		again = append(again, "--"+NoTools)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-initialized:
			beating := make(chan struct{})
			go func() {
				defer close(beating)
				beat(streamCtx, st, window, owns)
			}()
			Stream(streamCtx, st, window, channeled, started, again, notify)
			<-beating
		case <-streamCtx.Done():
		}
	}()
	err := srv.Serve(ctx, requests, out)
	stop()
	<-done
	if stopping(ctx) || owns() != nil {
		return err
	}
	return errors.Join(err, disconnected(context.WithoutCancel(ctx), st, window))
}

func disconnected(ctx context.Context, st *store.Store, window store.Window) error {
	sub, ok, err := st.Subscription(ctx, window)
	if err != nil || !ok {
		return err
	}
	_, _, err = st.Post(ctx, store.Record{
		Drive: sub.Drive, Lane: sub.Reader, Kind: kinds.Disconnect, To: []string{disconnectTo},
		Text: fmt.Sprintf("%s's cci channel disconnected: Claude Code closed it in window %d (session %s), so records to %s reach it only through its PostToolUse hook until it reconnects with %s", sub.Reader, window.PID, sub.Session, sub.Reader, hook.ReconnectCommand),
	}, 0)
	return err
}

func reconnected(ctx context.Context, st *store.Store, sub store.Subscription) error {
	last, err := st.Query(ctx, store.Filter{Drive: sub.Drive, Kinds: []kinds.Kind{kinds.Disconnect}, Lanes: []string{sub.Reader}, Descending: true, Limit: 1})
	if err != nil || len(last) == 0 {
		return err
	}
	open, err := st.OpenAmong(ctx, sub.Drive, []int64{last[0].Seq})
	if err != nil || len(open) == 0 {
		return err
	}
	_, _, err = st.Post(ctx, store.Record{
		Drive: sub.Drive, Lane: sub.Reader, Kind: kinds.Recovered, To: []string{disconnectTo}, Resolves: last[0].Seq,
		Text: fmt.Sprintf("%s's cci channel is connected again in window %d (session %s)", sub.Reader, sub.Window.PID, sub.Session),
	}, 0)
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

func beat(ctx context.Context, st *store.Store, window store.Window, owns func() error) {
	for ctx.Err() == nil {
		err := owns()
		if err == nil {
			err = st.BeatChannel(window)
		}
		if err != nil && ctx.Err() == nil {
			slog.Warn("cci channel: beat", "pid", window.PID, "err", err)
		}
		sleep(ctx, beatEvery)
	}
}

// Stream delivers window's subscription through notify until ctx ends, moving
// each cursor only as the session's transcript shows the tags it sent. A new
// channel passes the head it started at as started and begins live; a resumed
// one passes nil and keeps its predecessor's position. A newer install
// re-executes it with again as its arguments.
func Stream(ctx context.Context, st *store.Store, window store.Window, channeled bool, started *int64, again []string, notify func(method string, params any) error) {
	announced := false
	var l *ledger
	for tick := 1; ctx.Err() == nil; tick++ {
		sub, ok, err := st.Subscription(ctx, window)
		switch {
		case err == nil && ok:
			if !announced {
				err = reconnected(ctx, st, sub)
				announced = err == nil
			}
			if err == nil && (l == nil || l.session != sub.Session) {
				l, err = newLedger(ctx, st, sub.Session)
			}
			if err == nil {
				err = l.listen(ctx, func(ctx context.Context) error {
					if started != nil {
						if err := startLive(ctx, st, sub, channeled, *started, l, notify); err != nil {
							return err
						}
						started = nil
					}
					return follow(ctx, st, sub, channeled, l, notify)
				})
			}
		case err == nil && tick%releaseTicks == 0 && !hosted(ctx):
			err = newerRelease()
		}
		var newer upgrade
		if errors.As(err, &newer) {
			err = execve(newer.Path, append([]string{newer.Path}, again...), os.Environ())
		}
		if err != nil && ctx.Err() == nil {
			slog.Warn("cci channel", "pid", window.PID, "err", err)
		}
		sleep(ctx, idle)
	}
}

func startLive(ctx context.Context, st *store.Store, sub store.Subscription, channeled bool, started int64, l *ledger, notify func(string, any) error) error {
	from, err := l.position(ctx, sub.Cursor, sub.Drive)
	if err != nil {
		return err
	}
	head := max(started, sub.Head)
	n, err := st.Count(ctx, store.Filter{Drive: sub.Drive, Addressed: sub.Reader, Peer: sub.Session, Mute: slices.Concat(sub.Mute, sub.Batch), After: from, Before: head + 1})
	if err != nil {
		return err
	}
	since := from
	moves := []store.CursorMove{{Name: sub.Cursor, Drive: sub.Drive, Seq: head}}
	if len(sub.Batch) > 0 {
		held := sub.BatchFilter()
		if held.After, err = l.position(ctx, sub.BatchCursor(), sub.Drive); err != nil {
			return err
		}
		held.Before = head + 1
		pending, err := st.Count(ctx, held)
		if err != nil {
			return err
		}
		n, since = n+pending, min(since, held.After)
		moves = append(moves, store.CursorMove{Name: sub.BatchCursor(), Drive: sub.Drive, Seq: head})
	}
	if channeled {
		if err := st.MoveCursors(ctx, []store.CursorMove{{Name: sub.FloorCursor(), Drive: sub.Drive, Seq: max(from, head), Exact: true}}); err != nil {
			return err
		}
	}
	if n == 0 {
		return l.pass(ctx, moves...)
	}
	text := fmt.Sprintf("%d records addressed to %s, #%d through #%d, came before this channel started and were not replayed; the inbox tool reads them, as does cci tail --reader %s --since %d", n, sub.Reader, since+1, head, sub.Reader, since)
	l.owe(tag{kind: skippedKind, seq: head}, moves...)
	return notify(notifyMethod, map[string]any{"content": text, "meta": map[string]string{"seq": strconv.FormatInt(head, 10), "kind": skippedKind}})
}

type batch struct {
	st     *store.Store
	sent   *ledger
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
	if f.After, err = b.sent.position(ctx, b.cursor, f.Drive); err != nil {
		return err
	}
	records, err := b.st.Query(ctx, f)
	if err != nil || len(records) == 0 {
		return err
	}
	kept := newest(records)
	wire := render.Wire{Now: b.st.Now()}
	var content strings.Builder
	budget := render.NewBudget(&content, batchBudget)
	for i, r := range kept {
		line := r
		line.To = nil
		if !budget.Line(render.WithBody(wire.Line(line, 0), r)) {
			names := make([]string, len(f.Kinds))
			for j, k := range f.Kinds {
				names[j] = string(k)
			}
			budget.Trailer(fmt.Sprintf("+%d more; cci tail --to %s --kind %s --since %d reads them", len(kept)-i, f.Addressed, strings.Join(names, ","), kept[i].Seq-1))
			break
		}
	}
	last := records[len(records)-1].Seq
	b.sent.owe(tag{kind: batchKind, seq: last}, store.CursorMove{Name: b.cursor, Drive: f.Drive, Seq: last})
	if err := b.notify(notifyMethod, map[string]any{"content": strings.TrimSuffix(content.String(), "\n"), "meta": map[string]string{"seq": strconv.FormatInt(last, 10), "kind": batchKind}}); err != nil {
		return err
	}
	b.tick.Reset(batchWindow)
	return nil
}

func newest(records []store.Record) []store.Record {
	seen := map[supersede]bool{}
	var kept []store.Record
	for _, r := range slices.Backward(records) {
		keys := supersedes(r)
		if len(keys) > 0 && !slices.ContainsFunc(keys, func(k supersede) bool { return !seen[k] }) {
			continue
		}
		for _, k := range keys {
			seen[k] = true
		}
		kept = append(kept, r)
	}
	slices.Reverse(kept)
	return kept
}

type supersede struct {
	kind        kinds.Kind
	lane, topic string
	pr          store.PR
}

func supersedes(r store.Record) []supersede {
	if r.Kind == kinds.Head && len(r.Refs.PRs) > 0 {
		keys := make([]supersede, len(r.Refs.PRs))
		for i, pr := range r.Refs.PRs {
			keys[i] = supersede{kind: r.Kind, pr: pr}
		}
		return keys
	}
	if r.Topic == "" {
		return nil
	}
	return []supersede{{kind: r.Kind, lane: r.Lane, topic: r.Topic}}
}

func follow(ctx context.Context, st *store.Store, sub store.Subscription, channeled bool, l *ledger, notify func(string, any) error) error {
	mail, shared, err := hookMail(ctx, st, sub, channeled)
	if err != nil {
		return err
	}
	f := sub.Filter()
	if f.After, err = l.position(ctx, sub.Cursor, sub.Drive); err != nil {
		return err
	}
	watchCtx, stop := context.WithCancel(ctx)
	defer stop()
	changed := make(chan error, 1)
	go func() { changed <- untilChanged(watchCtx, st, sub, stop) }()
	held := &batch{st: st, sent: l, filter: sub.BatchFilter(), cursor: sub.BatchCursor(), notify: notify, tick: time.NewTicker(batchWindow)}
	defer held.tick.Stop()
	batched := make(chan error, 1)
	go func() { batched <- held.run(watchCtx, stop) }()
	err = inbox.Watch(watchCtx, st, inbox.WatchOptions{Filter: f, Interval: interval, Compact: true}, func(r store.Record, line string) error {
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
				if err != nil {
					return err
				}
				if delivered {
					return l.pass(watchCtx, store.CursorMove{Name: sub.Cursor, Drive: sub.Drive, Seq: r.Seq})
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
		wire := render.Wire{Now: st.Now()}
		content, err := tagContent(wire.Headed(r.At, line), r, sub.Reader)
		if err != nil {
			return err
		}
		moves := []store.CursorMove{{Name: sub.Cursor, Drive: sub.Drive, Seq: r.Seq}}
		if shared && seen >= mail.start {
			moves = append(moves, store.CursorMove{Name: mail.cursor, Drive: mail.filter.Drive, Seq: r.Seq})
		}
		l.owe(tag{kind: string(r.Kind), seq: r.Seq}, moves...)
		return notify(notifyMethod, map[string]any{"content": content, "meta": meta})
	})
	stop()
	if errors.Is(err, errStale) {
		err = nil
	}
	return errors.Join(err, <-changed, <-batched)
}

func tagContent(line string, r store.Record, reader string) (string, error) {
	addressed := slices.ContainsFunc(r.To, func(to string) bool {
		name, drive, _ := strings.Cut(to, "@")
		return (drive == "" || drive == r.Drive) && store.SameLane(name, reader)
	})
	if !store.RootName(reader) || !addressed {
		return render.WithBody(line, r), nil
	}
	rest, err := inbox.ShownBody(r)
	if err != nil || len(rest) == 0 || len(rest) > rootWhole {
		return render.WithBody(line, r), err
	}
	return line + "\n" + strings.TrimRight(string(rest), "\n"), nil
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
