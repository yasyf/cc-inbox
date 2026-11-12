package channel

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/shirou/gopsutil/v4/process"
	mcp "github.com/yasyf/cc-interact/channel"

	"github.com/yasyf/cc-inbox/internal/harness"
	"github.com/yasyf/cc-inbox/internal/hook"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
	"github.com/yasyf/cc-inbox/internal/version"
)

var (
	window = store.Window{PID: 4242, Started: 1791277810598}
	proc   = store.Window{PID: 4343, Started: 1791277810612}
)

func init() {
	idle, interval = 10*time.Millisecond, 5*time.Millisecond
}

func newStore(t *testing.T) (*store.Store, *testutil.Clock) {
	t.Helper()
	dir := t.TempDir()
	transcriptOf = func(_ context.Context, session string) (string, error) {
		path := filepath.Join(dir, session+".jsonl")
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o600) //nolint:gosec // G304: the path is under the test's temp dir.
		if err != nil {
			return "", err
		}
		return path, f.Close()
	}
	t.Cleanup(func() { transcriptOf = harness.Transcript })
	return testutil.Store(t)
}

func jsonLine(event map[string]any) string {
	var line strings.Builder
	enc := json.NewEncoder(&line)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(event); err != nil {
		panic(err)
	}
	return strings.TrimSuffix(line.String(), "\n")
}

func userLine(content any, sidechain bool) string {
	return jsonLine(map[string]any{"type": "user", "isSidechain": sidechain, "message": map[string]any{"role": "user", "content": content}})
}

func queuedLine(prompt any) string {
	return jsonLine(map[string]any{"type": "attachment", "isSidechain": false, "attachment": map[string]any{"type": "queued_command", "prompt": prompt, "commandMode": "prompt"}})
}

func tagText(source string, meta map[string]string) string {
	return fmt.Sprintf(`<channel source="%s" seq="%s" kind="%s" lane="%s">`+"\n@now\n%s %s\n</channel>", source, meta["seq"], meta["kind"], meta["lane"], meta["seq"], meta["kind"])
}

func transcribe(ctx context.Context, lines ...string) error {
	path, err := transcriptOf(ctx, "s1")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0) //nolint:gosec // G304: the path is under the test's temp dir.
	if err != nil {
		return err
	}
	for _, line := range lines {
		if _, err := f.WriteString(line + "\n"); err != nil {
			_ = f.Close()
			return err
		}
	}
	return f.Close()
}

func hears(ctx context.Context, params any) error {
	return transcribe(ctx, userLine(tagText(Source, params.(map[string]any)["meta"].(map[string]string)), false))
}

func followHeard(ctx context.Context, st *store.Store, sub store.Subscription, channeled bool, notify func(string, any) error) error {
	l, err := newLedger(ctx, st, sub.Session)
	if err != nil {
		return err
	}
	return l.listen(ctx, func(ctx context.Context) error {
		return follow(ctx, st, sub, channeled, l, func(method string, params any) error {
			if err := hears(ctx, params); err != nil {
				return err
			}
			return notify(method, params)
		})
	})
}

func cursorReaches(t *testing.T, st *store.Store, name string, want int64) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(interval) {
		seq, _, err := st.Cursor(context.Background(), name, "d")
		if err != nil {
			t.Fatal(err)
		}
		if seq >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("cursor %s stands at #%d, never reaching #%d", name, seq, want)
		}
	}
}

type frame struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params struct {
		Content string            `json:"content"`
		Meta    map[string]string `json:"meta"`
	} `json:"params"`
	Result struct {
		Capabilities map[string]any `json:"capabilities"`
		Instructions string         `json:"instructions"`
		Content      []struct {
			Text string `json:"text"`
		} `json:"content"`
	} `json:"result"`
}

type session struct {
	t      *testing.T
	in     *io.PipeWriter
	frames chan frame
	done   chan error
	cancel context.CancelFunc
}

func open(t *testing.T, st *store.Store, p store.Window) *session {
	t.Helper()
	s := start(t, st, window, p, false)
	s.send(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`)
	init := s.next()
	if _, ok := init.Result.Capabilities["experimental"].(map[string]any)["claude/channel"]; !ok || !strings.Contains(init.Result.Instructions, Source) || !strings.Contains(init.Result.Instructions, "--resolves <seq>") {
		t.Fatalf("initialize = %+v", init.Result)
	}
	return s
}

func start(t *testing.T, st *store.Store, w, p store.Window, resumed bool, tools ...mcp.Tool) *session {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	s := &session{t: t, in: inW, frames: make(chan frame, 64), done: make(chan error, 1), cancel: cancel}
	go func() {
		s.done <- Serve(ctx, st, w, p, false, resumed, inR, outW, tools...)
		_ = outW.Close()
	}()
	go func() {
		sc := bufio.NewScanner(outR)
		for sc.Scan() {
			var f frame
			if err := json.Unmarshal(sc.Bytes(), &f); err != nil {
				t.Errorf("frame %q: %v", sc.Text(), err)
				continue
			}
			if f.Method == notifyMethod {
				_ = transcribe(ctx, userLine(tagText(Source, f.Params.Meta), false))
			}
			s.frames <- f
		}
		close(s.frames)
	}()
	return s
}

func (s *session) initialized() {
	s.send(`{"jsonrpc":"2.0","method":"notifications/initialized"}`)
}

func subscribe(t *testing.T, st *store.Store, ks ...kinds.Kind) {
	t.Helper()
	if _, err := st.Subscribe(context.Background(), store.Subscription{Session: "s1", Window: window, Drive: "d", Reader: "root", Kinds: ks, Cursor: "root-channel"}); err != nil {
		t.Fatal(err)
	}
}

func (s *session) send(line string) {
	if _, err := io.WriteString(s.in, line+"\n"); err != nil {
		s.t.Fatal(err)
	}
}

func (s *session) next() frame {
	s.t.Helper()
	select {
	case f := <-s.frames:
		return f
	case <-time.After(5 * time.Second):
		s.t.Fatal("no frame within 5s")
		return frame{}
	}
}

func (s *session) quiet(d time.Duration) {
	s.t.Helper()
	select {
	case f := <-s.frames:
		s.t.Fatalf("unexpected frame %+v", f)
	case <-time.After(d):
	}
}

func (s *session) close() {
	s.t.Helper()
	_ = s.in.Close()
	if err := <-s.done; err != nil {
		s.t.Fatal(err)
	}
	s.cancel()
}

func (s *session) record(want store.Record) {
	s.t.Helper()
	f := s.next()
	seq := fmt.Sprint(want.Seq)
	if f.Method != notifyMethod || f.Params.Meta["seq"] != seq || f.Params.Meta["kind"] != string(want.Kind) || f.Params.Meta["lane"] != want.Lane ||
		!strings.HasPrefix(f.Params.Content, "@"+render.Stamp(want.At, want.At)+"\n"+seq+" "+string(want.Kind)+" ") || !strings.Contains(f.Params.Content, want.Text) {
		s.t.Fatalf("frame = %+v, want record #%d %q", f, want.Seq, want.Text)
	}
}

func (s *session) skipped(n int, through int64) {
	s.t.Helper()
	f := s.next()
	if f.Method != notifyMethod || f.Params.Meta["kind"] != skippedKind || f.Params.Meta["seq"] != fmt.Sprint(through) || !strings.HasPrefix(f.Params.Content, fmt.Sprintf("%d records addressed to ", n)) || !strings.Contains(f.Params.Content, "inbox tool") {
		s.t.Fatalf("frame = %+v, want one tag counting %d skipped records through #%d", f, n, through)
	}
}

func TestChannelDeliversTheSubscriptionAndARestartedOneCountsWhatItMissed(t *testing.T) {
	st, clock := newStore(t)
	ctx := context.Background()
	testutil.Post(t, st, store.Record{Kind: kinds.Ask, Text: "before anyone subscribed", To: []string{"root"}})
	s := open(t, st, proc)
	s.initialized()
	s.quiet(50 * time.Millisecond)
	subscribe(t, st, kinds.Incident)
	time.Sleep(50 * time.Millisecond)
	toMain := testutil.Post(t, st, store.Record{Lane: "owner", Kind: kinds.Owner, Text: "Mark complete: the card", To: []string{"main"}})
	testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "a broadcast of a kind nobody subscribed to"})
	incident := testutil.Post(t, st, store.Record{Lane: "alerts", Kind: kinds.Incident, Topic: "api-5xx", Text: "api 5xx above 2%"})
	testutil.Post(t, st, store.Record{Kind: kinds.Go, Text: "for another lane", To: []string{"desk"}})
	s.record(toMain)
	s.record(incident)
	s.quiet(50 * time.Millisecond)
	s.close()

	missed := testutil.Post(t, st, store.Record{Kind: kinds.Decide, Text: "posted while no channel ran", To: []string{"root"}})
	resumed := open(t, st, proc)
	resumed.quiet(50 * time.Millisecond)
	resumed.initialized()
	resumed.skipped(1, missed.Seq)
	clock.Advance(time.Second)
	subscribe(t, st, kinds.Note)
	time.Sleep(50 * time.Millisecond)
	note := testutil.Post(t, st, store.Record{Lane: "runner", Kind: kinds.Note, Text: "LAUNCHED R1185 cci-interact"})
	resumed.record(note)
	if removed, err := st.Unsubscribe(ctx, window); err != nil || !removed {
		t.Fatalf("Unsubscribe = %v, %v", removed, err)
	}
	time.Sleep(50 * time.Millisecond)
	testutil.Post(t, st, store.Record{Kind: kinds.Ask, Text: "after unsubscribe", To: []string{"root"}})
	resumed.quiet(50 * time.Millisecond)
	resumed.close()
}

func TestChannelDeliversARegisteredSessionsSubscription(t *testing.T) {
	st, clock := newStore(t)
	ctx := context.Background()
	s := open(t, st, proc)
	s.initialized()
	s.quiet(50 * time.Millisecond)
	coordinator := store.Registration{Subscription: store.Subscription{Alias: "release-demo-coordinator", Session: "s1", Window: window, Drive: "d", Reader: "root", Kinds: []kinds.Kind{kinds.Incident}, Cursor: "coordinator"}}
	if _, err := st.Register(ctx, coordinator); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	incident := testutil.Post(t, st, store.Record{Lane: "alerts", Kind: kinds.Incident, Topic: "api-5xx", Text: "api 5xx above 2%"})
	s.record(incident)
	clock.Advance(time.Second)
	coordinator.Kinds = []kinds.Kind{kinds.Note}
	if _, err := st.Register(ctx, coordinator); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	note := testutil.Post(t, st, store.Record{Lane: "runner", Kind: kinds.Note, Text: "LAUNCHED R1185 cci-interact"})
	s.record(note)
	if _, ok, err := st.Unregister(ctx, coordinator.Alias); err != nil || !ok {
		t.Fatalf("Unregister = %v, %v", ok, err)
	}
	time.Sleep(50 * time.Millisecond)
	testutil.Post(t, st, store.Record{Kind: kinds.Ask, Text: "after the alias was removed", To: []string{"root"}})
	s.quiet(50 * time.Millisecond)
	s.close()
}

func TestReplacingTheSubscriptionNeverSkipsANewlySelectedRecord(t *testing.T) {
	st, clock := newStore(t)
	defer func(fast time.Duration) { idle = fast }(idle)
	idle = time.Hour
	ctx := context.Background()
	if err := st.SetCursor(ctx, "root-channel", "d", 0); err != nil {
		t.Fatal(err)
	}
	old, err := st.Subscribe(ctx, store.Subscription{Session: "s1", Window: window, Drive: "d", Reader: "root", Kinds: []kinds.Kind{kinds.Incident}, Cursor: "root-channel"})
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	current, err := st.Subscribe(ctx, store.Subscription{Session: "s1", Window: window, Drive: "d", Reader: "root", Kinds: []kinds.Kind{kinds.Note}, Cursor: "root-channel"})
	if err != nil {
		t.Fatal(err)
	}
	note := testutil.Post(t, st, store.Record{Lane: "runner", Kind: kinds.Note, Text: "LAUNCHED R1185"})
	testutil.Post(t, st, store.Record{Lane: "alerts", Kind: kinds.Incident, Topic: "api-5xx", Text: "api 5xx above 2%"})
	if err := followHeard(ctx, st, old, false, func(string, any) error {
		t.Fatal("the replaced subscription delivered a record")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if seq, _, _ := st.Cursor(ctx, "root-channel", "d"); seq != 0 {
		t.Fatalf("the replaced subscription advanced the cursor to #%d", seq)
	}
	followCtx, stop := context.WithCancel(ctx)
	var got []string
	if err := followHeard(followCtx, st, current, false, func(_ string, params any) error {
		got = append(got, params.(map[string]any)["meta"].(map[string]string)["seq"])
		stop()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != fmt.Sprint(note.Seq) {
		t.Fatalf("delivered %q, want note #%d", got, note.Seq)
	}
}

func TestRecordsPostedRightAfterSubscribingArrive(t *testing.T) {
	st, _ := newStore(t)
	testutil.Post(t, st, store.Record{Kind: kinds.Ask, Text: "before the subscription", To: []string{"root"}})
	s := open(t, st, proc)
	s.initialized()
	subscribe(t, st)
	after := testutil.Post(t, st, store.Record{Kind: kinds.Ask, Text: "before the channel's first poll", To: []string{"root"}})
	s.record(after)
	s.quiet(50 * time.Millisecond)
	s.close()
}

func TestANewChannelStartsLiveWhateverItsCursorMissed(t *testing.T) {
	for _, n := range []int{1, 1000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			st, _ := newStore(t)
			subscribe(t, st)
			var last store.Record
			for i := range n {
				last = testutil.Post(t, st, store.Record{Lane: "desk", Kind: kinds.Ask, To: []string{"root"}, Text: fmt.Sprintf("missed %d;", i)})
			}
			s := open(t, st, proc)
			s.initialized()
			s.skipped(n, last.Seq)
			s.quiet(50 * time.Millisecond)
			attached := testutil.Post(t, st, store.Record{Lane: "desk", Kind: kinds.Ask, To: []string{"root"}, Text: "posted after it attached"})
			s.record(attached)
			s.quiet(50 * time.Millisecond)
			s.close()
		})
	}
}

func TestARefollowInsideOneChannelReplaysNothingAndSkipsNothing(t *testing.T) {
	st, clock := newStore(t)
	subscribe(t, st, kinds.Incident)
	missed := testutil.Post(t, st, store.Record{Lane: "desk", Kind: kinds.Ask, To: []string{"root"}, Text: "missed"})
	s := open(t, st, proc)
	s.initialized()
	s.skipped(1, missed.Seq)
	ask := testutil.Post(t, st, store.Record{Lane: "desk", Kind: kinds.Ask, To: []string{"root"}, Text: "delivered once"})
	s.record(ask)
	note := testutil.Post(t, st, store.Record{Lane: "runner", Kind: kinds.Note, Text: "selected only by the new subscription"})
	s.quiet(50 * time.Millisecond)
	clock.Advance(time.Second)
	subscribe(t, st, kinds.Incident, kinds.Note)
	s.record(note)
	s.quiet(50 * time.Millisecond)
	s.close()
}

func TestAChannelAndTheHookDeliverEachRecordOnce(t *testing.T) {
	st, _ := newStore(t)
	idle, interval = 10*time.Millisecond, 5*time.Millisecond
	ctx := context.Background()
	root := hook.Payload{SessionID: "s1", Event: "PostToolUse"}
	if err := st.BindLane(ctx, store.PeerKey{Session: root.SessionID}, "d", "root", time.Time{}); err != nil {
		t.Fatal(err)
	}
	subscribe(t, st, kinds.Incident)
	sub, _, err := st.Subscription(ctx, window)
	if err != nil {
		t.Fatal(err)
	}
	hookReads := func() string {
		t.Helper()
		var out bytes.Buffer
		if err := hook.PostToolUse(ctx, st, root, func() (store.Window, error) { return window, nil }, &out); err != nil {
			t.Fatal(err)
		}
		return out.String()
	}
	first := testutil.Post(t, st, store.Record{Kind: kinds.Ask, To: []string{"root"}, Text: "the hook reads this first"})
	incident := testutil.Post(t, st, store.Record{Lane: "alerts", Kind: kinds.Incident, Text: "a broadcast only the channel carries"})
	second := testutil.Post(t, st, store.Record{Kind: kinds.Blocker, To: []string{"main"}, Text: "the hook reads this too"})
	settled := testutil.Post(t, st, store.Record{Kind: kinds.Ask, To: []string{"root"}, Text: "settled before the hook read it"})
	testutil.Post(t, st, store.Record{Lane: "lane-b", Kind: kinds.Note, Resolves: settled.Seq, Text: "settled"})
	if got := hookReads(); !strings.Contains(got, first.Text) || !strings.Contains(got, second.Text) || strings.Contains(got, incident.Text) || strings.Contains(got, settled.Text) {
		t.Fatalf("hook delivered %q", got)
	}
	seqs := make(chan string, 8)
	followCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- followHeard(followCtx, st, sub, true, func(_ string, params any) error {
			seqs <- params.(map[string]any)["meta"].(map[string]string)["seq"]
			return nil
		})
	}()
	later := testutil.Post(t, st, store.Record{Kind: kinds.Ask, To: []string{"root"}, Text: "the channel reads this first"})
	for _, want := range []store.Record{incident, settled, later} {
		select {
		case got := <-seqs:
			if got != fmt.Sprint(want.Seq) {
				t.Fatalf("channel delivered #%s, want #%d %q", got, want.Seq, want.Text)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("channel never delivered #%d", want.Seq)
		}
	}
	cursorReaches(t, st, store.LaneCursor(store.PeerKey{Session: root.SessionID}), later.Seq)
	if got := hookReads(); got != "" {
		t.Fatalf("hook redelivered what the channel carried: %q", got)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if len(seqs) != 0 {
		t.Fatalf("channel delivered #%s again", <-seqs)
	}
}

func TestAHookBehindTheChannelsStartLeavesTheChannelWhatItDelivered(t *testing.T) {
	st, _ := newStore(t)
	idle, interval = 10*time.Millisecond, 5*time.Millisecond
	ctx := context.Background()
	root := hook.Payload{SessionID: "s1", Event: "PostToolUse"}
	if err := st.BindLane(ctx, store.PeerKey{Session: root.SessionID}, "d", "root", time.Time{}); err != nil {
		t.Fatal(err)
	}
	hookReads := func() string {
		t.Helper()
		var out bytes.Buffer
		if err := hook.PostToolUse(ctx, st, root, func() (store.Window, error) { return window, nil }, &out); err != nil {
			t.Fatal(err)
		}
		return out.String()
	}
	owed := testutil.Post(t, st, store.Record{Lane: "desk", Kind: kinds.Report, To: []string{"root"}, Text: "posted before the channel started"})
	subscribe(t, st, kinds.Incident)
	started, err := st.MaxSeq(ctx)
	if err != nil {
		t.Fatal(err)
	}
	seqs := make(chan string, 8)
	streamCtx, stop := context.WithCancel(ctx)
	streamed := make(chan struct{})
	go func() {
		defer close(streamed)
		Stream(streamCtx, st, window, true, &started, nil, func(_ string, params any) error {
			if err := hears(streamCtx, params); err != nil {
				return err
			}
			seqs <- params.(map[string]any)["meta"].(map[string]string)["seq"]
			return nil
		})
	}()
	t.Cleanup(func() {
		stop()
		<-streamed
	})
	hookOnly := testutil.Post(t, st, store.Record{Lane: "desk", Kind: kinds.Correction, To: []string{"all"}, Text: "a broadcast the channel does not select"})
	carried := testutil.Post(t, st, store.Record{Lane: "desk", Kind: kinds.Report, To: []string{"root"}, Text: "the channel delivers this"})
	select {
	case got := <-seqs:
		if got != fmt.Sprint(carried.Seq) {
			t.Fatalf("channel delivered #%s, want #%d", got, carried.Seq)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("channel never delivered #%d", carried.Seq)
	}
	cursorReaches(t, st, "root-channel", carried.Seq)
	if got := hookReads(); !strings.Contains(got, owed.Text) || !strings.Contains(got, hookOnly.Text) || strings.Contains(got, carried.Text) {
		t.Fatalf("hook delivered %q, want only the records the channel never carried", got)
	}
	if got := hookReads(); got != "" {
		t.Fatalf("hook redelivered %q", got)
	}
	stop()
	<-streamed
	if len(seqs) != 0 {
		t.Fatalf("channel delivered #%s again", <-seqs)
	}
}

func TestMutedKindsAndUrgentRepeatsReachNeitherTheChannelNorTheHook(t *testing.T) {
	st, _ := newStore(t)
	idle, interval = 10*time.Millisecond, 5*time.Millisecond
	ctx := context.Background()
	root := hook.Payload{SessionID: "s1", Event: "PostToolUse"}
	if err := st.BindLane(ctx, store.PeerKey{Session: root.SessionID}, "d", "root", time.Time{}); err != nil {
		t.Fatal(err)
	}
	sub, err := st.Subscribe(ctx, store.Subscription{Session: root.SessionID, Window: window, Drive: "d", Reader: "root", Mute: []kinds.Kind{kinds.State}, Cursor: "root-channel"})
	if err != nil {
		t.Fatal(err)
	}
	stall := func(minutes int) store.Record {
		return testutil.Post(t, st, store.Record{Lane: "stall-watch", Kind: kinds.Urgent, Topic: "go:1", To: []string{"root"}, Text: fmt.Sprintf("go 1 stalled %d min", minutes)})
	}
	first := stall(10)
	queued := testutil.Post(t, st, store.Record{Lane: "desk-runner", Kind: kinds.State, To: []string{"root"}, Text: "QUEUED go 1"})
	repeat := stall(20)
	var out bytes.Buffer
	if err := hook.PostToolUse(ctx, st, root, func() (store.Window, error) { return window, nil }, &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); !strings.Contains(got, first.Text) || strings.Contains(got, queued.Text) || strings.Contains(got, repeat.Text) {
		t.Fatalf("hook delivered %q", got)
	}
	stall(30)
	ask := testutil.Post(t, st, store.Record{Kind: kinds.Ask, To: []string{"root"}, Text: "merge go 1?"})
	if err := followHeard(ctx, st, sub, true, func(_ string, params any) error {
		if got := params.(map[string]any)["meta"].(map[string]string)["seq"]; got != fmt.Sprint(ask.Seq) {
			t.Fatalf("channel delivered #%s, want only #%d", got, ask.Seq)
		}
		return errStale
	}); err != nil {
		t.Fatal(err)
	}
}

func TestARecordAddressedToTheReaderAtItsDriveArrivesOnceOutsideTheBatch(t *testing.T) {
	st, _ := newStore(t)
	idle, interval, batchWindow = 10*time.Millisecond, 5*time.Millisecond, time.Hour
	ctx := context.Background()
	sub, err := st.Subscribe(ctx, store.Subscription{Session: "s1", Window: window, Drive: "d", Reader: "root", ToKinds: store.DefaultToKinds("root"), Batch: store.DefaultBatch("root"), Cursor: "root-channel"})
	if err != nil {
		t.Fatal(err)
	}
	ask := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Ask, To: []string{"communicator"}, Text: "which layout?"})
	done := testutil.Post(t, st, store.Record{Lane: "builder", Kind: kinds.Done, To: []string{"root@d"}, Text: "journal PR merged"})
	answer := testutil.Post(t, st, store.Record{Lane: "communicator", Kind: kinds.Answer, Re: ask.Seq, To: []string{"root@d"}, Text: "his pick: two databases"})
	owner := testutil.Post(t, st, store.Record{Lane: "communicator", Kind: kinds.Owner, To: []string{"root@d"}, Text: "his ruling: next tree only"})
	tags, stop := following(t, st, sub)
	defer stop()
	batch := nextTag(t, tags)
	if batch.meta["kind"] != batchKind || !strings.Contains(batch.content, done.Text) || strings.Contains(batch.content, answer.Text) || strings.Contains(batch.content, owner.Text) {
		t.Fatalf("batch = %q, want the done alone", batch.content)
	}
	for _, want := range []store.Record{answer, owner} {
		if tag := nextTag(t, tags); tag.meta["seq"] != fmt.Sprint(want.Seq) || tag.meta["kind"] != string(want.Kind) {
			t.Fatalf("tag %+v, want #%d as its own tag", tag.meta, want.Seq)
		}
	}
	noTag(t, tags, 50*time.Millisecond)
}

func TestRootHearsOnlyAddressedKindsItActsOn(t *testing.T) {
	st, _ := newStore(t)
	idle, interval = 10*time.Millisecond, 5*time.Millisecond
	ctx := context.Background()
	root := hook.Payload{SessionID: "s1", Event: "PostToolUse"}
	if err := st.BindLane(ctx, store.PeerKey{Session: root.SessionID}, "d", "root", time.Time{}); err != nil {
		t.Fatal(err)
	}
	sub, err := st.Subscribe(ctx, store.Subscription{Session: root.SessionID, Window: window, Drive: "d", Reader: "root", Kinds: []kinds.Kind{kinds.Decide}, ToKinds: store.DefaultToKinds("root"), Batch: store.DefaultBatch("root"), Cursor: "root-channel"})
	if err != nil {
		t.Fatal(err)
	}
	state := testutil.Post(t, st, store.Record{Lane: "desk-runner", Kind: kinds.State, To: []string{"root"}, Text: "QUEUED go 1"})
	report := testutil.Post(t, st, store.Record{Lane: "ship-lane", Kind: kinds.Report, To: []string{"main"}, Text: "PR opened"})
	var out bytes.Buffer
	if err := hook.PostToolUse(ctx, st, root, func() (store.Window, error) { return window, nil }, &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); strings.Contains(got, state.Text) || strings.Contains(got, report.Text) {
		t.Fatalf("hook delivered %q", got)
	}
	done := testutil.Post(t, st, store.Record{Lane: "ship-lane", Kind: kinds.Done, To: []string{"main"}, Text: "first PR merged"})
	ask := testutil.Post(t, st, store.Record{Lane: "ship-lane", Kind: kinds.Ask, To: []string{"main"}, Text: "merge go 1?"})
	answer := testutil.Post(t, st, store.Record{Lane: "design-lane", Kind: kinds.Answer, Re: ask.Seq, To: []string{"root"}, Text: "schema b"})
	owner := testutil.Post(t, st, store.Record{Lane: "owner", Kind: kinds.Owner, To: []string{"main"}, Text: "ship it"})
	release := testutil.Post(t, st, store.Record{Lane: "release-lane", Kind: kinds.Release, To: []string{"root"}, Text: "11665 LIVE"})
	again := testutil.Post(t, st, store.Record{Lane: "ship-lane", Kind: kinds.Done, To: []string{"main"}, Text: "second PR merged"})
	blocked := testutil.Post(t, st, store.Record{Lane: "mq-lane", Kind: kinds.Blocked, To: []string{"root"}, Text: "waits on the owner's pick"})
	decide := testutil.Post(t, st, store.Record{Lane: "design-lane", Kind: kinds.Decide, Text: "pick a schema?"})
	want := []string{batchKind + fmt.Sprint(again.Seq), "ask" + fmt.Sprint(ask.Seq), "answer" + fmt.Sprint(answer.Seq), "owner" + fmt.Sprint(owner.Seq), "release" + fmt.Sprint(release.Seq), "blocked" + fmt.Sprint(blocked.Seq), "decide" + fmt.Sprint(decide.Seq)}
	var got, batches []string
	if err := followHeard(ctx, st, sub, true, func(_ string, params any) error {
		p := params.(map[string]any)
		meta := p["meta"].(map[string]string)
		got = append(got, meta["kind"]+meta["seq"])
		if meta["kind"] == batchKind {
			batches = append(batches, p["content"].(string))
		}
		if len(got) == len(want) {
			return errStale
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("channel delivered %v, want %v", got, want)
	}
	if len(batches) != 1 || !strings.Contains(batches[0], done.Text) || !strings.Contains(batches[0], again.Text) {
		t.Fatalf("batches = %q, want one carrying both done records", batches)
	}
	out.Reset()
	if err := hook.PostToolUse(ctx, st, root, func() (store.Window, error) { return window, nil }, &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); strings.Contains(got, done.Text) || strings.Contains(got, again.Text) {
		t.Fatalf("hook delivered a batched done: %q", got)
	}
}

func TestAChannelReexecutesANewerInstallResumedOnItsStdio(t *testing.T) {
	for _, subscribed := range []bool{true, false} {
		t.Run(fmt.Sprintf("subscribed=%v", subscribed), func(t *testing.T) {
			st, _ := newStore(t)
			installed := make(chan version.Install, 1)
			argvs := make(chan []string, 8)
			newerInstall = func() (version.Install, bool, error) {
				select {
				case newer := <-installed:
					return newer, true, nil
				default:
					return version.Install{}, false, nil
				}
			}
			execve = func(_ string, argv []string, _ []string) error {
				argvs <- argv
				return nil
			}
			t.Cleanup(func() { newerInstall, execve = defaultNewerInstall, syscall.Exec })
			s := open(t, st, proc)
			s.initialized()
			if subscribed {
				subscribe(t, st)
			}
			time.Sleep(50 * time.Millisecond)

			installed <- version.Install{Path: "/opt/cci/0.46.0/cci", Version: "0.46.0"}

			select {
			case argv := <-argvs:
				if want := []string{"/opt/cci/0.46.0/cci", "channel", "--resumed", "--no-tools"}; !slices.Equal(argv, want) {
					t.Fatalf("exec argv = %q, want %q", argv, want)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the channel never re-executed the newer install")
			}
			s.close()
		})
	}
}

func TestAResumedChannelAnnouncesItsToolsThenStreamsWithoutAnotherHandshake(t *testing.T) {
	st, _ := newStore(t)
	subscribe(t, st)
	handedOff := testutil.Post(t, st, store.Record{Kind: kinds.Ask, Text: "posted while the channel re-executed", To: []string{"root"}})
	s := start(t, st, window, proc, true)
	if f := s.next(); f.Method != "notifications/tools/list_changed" || len(f.ID) != 0 {
		t.Fatalf("first frame = %+v, want notifications/tools/list_changed so Claude Code lists the newer build's tools", f)
	}
	s.record(handedOff)

	ask := testutil.Post(t, st, store.Record{Kind: kinds.Ask, Text: "after the hand-off", To: []string{"root"}})

	s.record(ask)
	s.close()
}

func batched(t *testing.T, st *store.Store, batch ...kinds.Kind) store.Subscription {
	t.Helper()
	sub, err := st.Subscribe(context.Background(), store.Subscription{Session: "s1", Window: window, Drive: "d", Reader: "owner-relay", Batch: batch, Cursor: "relay-channel"})
	if err != nil {
		t.Fatal(err)
	}
	return sub
}

type delivered struct {
	content string
	meta    map[string]string
}

func following(t *testing.T, st *store.Store, sub store.Subscription) (<-chan delivered, func()) {
	t.Helper()
	tags := make(chan delivered, 64)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- followHeard(ctx, st, sub, false, func(_ string, params any) error {
			p := params.(map[string]any)
			tags <- delivered{content: p["content"].(string), meta: p["meta"].(map[string]string)}
			return nil
		})
	}()
	return tags, func() {
		t.Helper()
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

func nextTag(t *testing.T, tags <-chan delivered) delivered {
	t.Helper()
	select {
	case tag := <-tags:
		return tag
	case <-time.After(5 * time.Second):
		t.Fatal("no tag within 5s")
		return delivered{}
	}
}

func noTag(t *testing.T, tags <-chan delivered, d time.Duration) {
	t.Helper()
	select {
	case tag := <-tags:
		t.Fatalf("delivered %+v, want nothing", tag)
	case <-time.After(d):
	}
}

func TestABatchIsOneTag(t *testing.T) {
	for _, n := range []int{1, 20} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			st, _ := newStore(t)
			idle, interval, batchWindow = 10*time.Millisecond, 5*time.Millisecond, 200*time.Millisecond
			sub := batched(t, st, kinds.Report, kinds.Note)
			for i := range n {
				testutil.Post(t, st, store.Record{Lane: "signal", Kind: kinds.Note, To: []string{"owner-relay", "root"}, Text: fmt.Sprintf("look at %d;", i)})
			}
			last := testutil.Post(t, st, store.Record{Lane: "fix-1", Kind: kinds.Report, To: []string{"owner-relay"}, Text: "the fix is live"})
			tags, stop := following(t, st, sub)
			defer stop()
			tag := nextTag(t, tags)
			content := tag.content
			if tag.meta["kind"] != batchKind || tag.meta["seq"] != fmt.Sprint(last.Seq) || !strings.Contains(content, last.Text) {
				t.Fatalf("tag %+v, want one batch through #%d", tag.meta, last.Seq)
			}
			for i := range n {
				if !strings.Contains(content, fmt.Sprintf("look at %d;", i)) {
					t.Fatalf("batch lacks record %d: %q", i, content)
				}
			}
			if strings.Contains(content, ">") || strings.Contains(content, " -> ") {
				t.Fatalf("batch names recipients: %q", content)
			}
			noTag(t, tags, 3*batchWindow)
		})
	}
}

func TestABatchKeepsTheNewestHeadPerPRWithinItsBudgetAndCountsTheRest(t *testing.T) {
	st, _ := newStore(t)
	idle, interval, batchWindow = 10*time.Millisecond, 5*time.Millisecond, 50*time.Millisecond
	sub := batched(t, st, kinds.Head, kinds.Note)
	const prs, pushes, notes = 100, 4, 100
	for push := range pushes {
		for pr := range prs {
			testutil.Post(t, st, store.Record{Lane: "ship-1", Kind: kinds.Head, To: []string{"owner-relay"}, Refs: store.Refs{PRs: []store.PR{{Repo: "o/r", Number: 1000 + pr}}}, Text: fmt.Sprintf("#%d at push-%d-of-%d", 1000+pr, push, 1000+pr)})
		}
	}
	for i := range notes {
		testutil.Post(t, st, store.Record{Lane: "signal", Kind: kinds.Note, To: []string{"owner-relay"}, Text: fmt.Sprintf("look at %d; %s", i, strings.Repeat("detail ", 20))})
	}
	tags, stop := following(t, st, sub)
	defer stop()
	tag := nextTag(t, tags)
	last, err := st.MaxSeq(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if tag.meta["kind"] != batchKind || tag.meta["seq"] != fmt.Sprint(last) || len(tag.content) > batchBudget {
		t.Fatalf("tag %+v of %d bytes, want one batch through #%d within %d bytes", tag.meta, len(tag.content), last, batchBudget)
	}
	for pr := range prs {
		for push := range pushes - 1 {
			if strings.Contains(tag.content, fmt.Sprintf("push-%d-of-%d", push, 1000+pr)) {
				t.Fatalf("batch carries a superseded head of #%d: %q", 1000+pr, tag.content)
			}
		}
	}
	lines := strings.Split(tag.content, "\n")
	more := regexp.MustCompile(`^\+(\d+) more; cci tail --to owner-relay --kind head,note --since (\d+) reads them$`).FindStringSubmatch(lines[len(lines)-1])
	if more == nil {
		t.Fatalf("batch ends in %q, want a +N more line", lines[len(lines)-1])
	}
	shown := 0
	for _, line := range lines {
		if strings.Contains(line, " head ship-1 ") || strings.Contains(line, " note signal ") {
			shown++
		}
	}
	if counted, _ := strconv.Atoi(more[1]); shown == 0 || shown+counted != prs+notes {
		t.Fatalf("batch shows %d and counts %s more, want %d in all", shown, more[1], prs+notes)
	}
	noTag(t, tags, 3*batchWindow)
}

func TestACutSingleTagPointsAtTheReadToolInsteadOfAPath(t *testing.T) {
	st, _ := newStore(t)
	idle, interval = 10*time.Millisecond, 5*time.Millisecond
	text, path, err := st.Fit(strings.Repeat("long report ", 100))
	if err != nil {
		t.Fatal(err)
	}
	sub, err := st.Subscribe(context.Background(), store.Subscription{Session: "s1", Window: window, Drive: "d", Reader: "root", Cursor: "root-channel"})
	if err != nil {
		t.Fatal(err)
	}
	tags, stop := following(t, st, sub)
	defer stop()
	cut := testutil.Post(t, st, store.Record{Lane: "fixer", Kind: kinds.Ask, To: []string{"root"}, Text: text, Refs: store.Refs{Path: path}})
	tag := nextTag(t, tags)
	if tag.meta["seq"] != fmt.Sprint(cut.Seq) || strings.Contains(tag.content, path) || strings.Contains(tag.content, "/") || !strings.Contains(tag.content, fmt.Sprintf("%d ask fixer + long report", cut.Seq)) {
		t.Fatalf("tag = %q, want the compact line with its + marker and no path", tag.content)
	}
}

func TestARootGetsARecordAddressedToItWholeWhenItsRestFits(t *testing.T) {
	st, _ := newStore(t)
	idle, interval = 10*time.Millisecond, 5*time.Millisecond
	fits, fitsPath, err := st.Fit(strings.Repeat("fits ", 200) + "end-of-fit")
	if err != nil {
		t.Fatal(err)
	}
	big, bigPath, err := st.Fit(strings.Repeat("big ", 1000) + "end-of-big")
	if err != nil {
		t.Fatal(err)
	}
	sub, err := st.Subscribe(context.Background(), store.Subscription{Session: "s1", Window: window, Drive: "d", Reader: "root", Cursor: "root-channel"})
	if err != nil {
		t.Fatal(err)
	}
	tags, stop := following(t, st, sub)
	defer stop()
	whole := testutil.Post(t, st, store.Record{Lane: "fixer", Kind: kinds.Ask, To: []string{"root@d"}, Text: fits, Refs: store.Refs{Path: fitsPath}})
	cut := testutil.Post(t, st, store.Record{Lane: "fixer", Kind: kinds.Ask, To: []string{"root"}, Text: big, Refs: store.Refs{Path: bigPath}})
	if tag := nextTag(t, tags); tag.meta["seq"] != fmt.Sprint(whole.Seq) || !strings.HasSuffix(tag.content, "end-of-fit") {
		t.Fatalf("tag = %q, want #%d with the rest of its text under its line", tag.content, whole.Seq)
	}
	if tag := nextTag(t, tags); tag.meta["seq"] != fmt.Sprint(cut.Seq) || strings.Contains(tag.content, "end-of-big") {
		t.Fatalf("tag = %q, want #%d cut, since its rest runs past 2,000 bytes", tag.content, cut.Seq)
	}
}

func TestAnOwnerRecordArrivesAtOnceWithTheBatchAheadOfIt(t *testing.T) {
	st, _ := newStore(t)
	idle, interval, batchWindow = 10*time.Millisecond, 5*time.Millisecond, time.Hour
	sub := batched(t, st, kinds.Report, kinds.Note)
	tags, stop := following(t, st, sub)
	defer stop()
	report := testutil.Post(t, st, store.Record{Lane: "fix-1", Kind: kinds.Report, To: []string{"owner-relay"}, Text: "the fix is live"})
	noTag(t, tags, 50*time.Millisecond)
	tap := testutil.Post(t, st, store.Record{Lane: "board-watch", Kind: kinds.Owner, To: []string{"owner-relay"}, Text: "he tapped Land"})
	if tag := nextTag(t, tags); tag.meta["kind"] != batchKind || !strings.Contains(tag.content, report.Text) {
		t.Fatalf("first tag %+v, want the held report", tag)
	}
	if tag := nextTag(t, tags); tag.meta["seq"] != fmt.Sprint(tap.Seq) || tag.meta["kind"] != string(kinds.Owner) || !strings.Contains(tag.content, tap.Text) {
		t.Fatalf("second tag %+v, want his tap", tag)
	}
	noTag(t, tags, 50*time.Millisecond)
}

func TestABatchHeldAcrossARestartArrivesOnce(t *testing.T) {
	st, _ := newStore(t)
	idle, interval, batchWindow = 10*time.Millisecond, 5*time.Millisecond, time.Hour
	sub := batched(t, st, kinds.Note)
	_, stop := following(t, st, sub)
	note := testutil.Post(t, st, store.Record{Lane: "signal", Kind: kinds.Note, To: []string{"owner-relay"}, Text: "look at 7"})
	time.Sleep(50 * time.Millisecond)
	stop()
	batchWindow = 20 * time.Millisecond
	tags, stop := following(t, st, sub)
	if tag := nextTag(t, tags); !strings.Contains(tag.content, note.Text) {
		t.Fatalf("tag %+v, want the note the first channel held", tag)
	}
	stop()
	tags, stop = following(t, st, sub)
	defer stop()
	noTag(t, tags, 100*time.Millisecond)
}

func TestANewChannelCountsTheBatchItWasHolding(t *testing.T) {
	st, _ := newStore(t)
	batchWindow = time.Hour
	batched(t, st, kinds.Note)
	held := testutil.Post(t, st, store.Record{Lane: "signal", Kind: kinds.Note, To: []string{"owner-relay"}, Text: "held for the batch"})
	tap := testutil.Post(t, st, store.Record{Lane: "board-watch", Kind: kinds.Owner, To: []string{"owner-relay"}, Text: "he tapped Land"})
	if err := st.SetCursor(context.Background(), "relay-channel", "d", tap.Seq); err != nil {
		t.Fatal(err)
	}
	s := open(t, st, proc)
	s.initialized()
	s.skipped(1, tap.Seq)
	again := testutil.Post(t, st, store.Record{Lane: "board-watch", Kind: kinds.Owner, To: []string{"owner-relay"}, Text: "he tapped Land again"})
	if f := s.next(); f.Params.Meta["seq"] != fmt.Sprint(again.Seq) || strings.Contains(f.Params.Content, held.Text) {
		t.Fatalf("frame = %+v, want #%d with no batch replaying the held note ahead of it", f, again.Seq)
	}
	s.close()
}

func TestTheHookLeavesBatchKindsToTheChannelAndLosesNothing(t *testing.T) {
	st, _ := newStore(t)
	idle, interval, batchWindow = 10*time.Millisecond, 5*time.Millisecond, 20*time.Millisecond
	ctx := context.Background()
	relay := hook.Payload{SessionID: "s1", Event: "PostToolUse"}
	if err := st.BindLane(ctx, store.PeerKey{Session: relay.SessionID}, "d", "owner-relay", time.Time{}); err != nil {
		t.Fatal(err)
	}
	sub := batched(t, st, kinds.Note)
	note := testutil.Post(t, st, store.Record{Lane: "signal", Kind: kinds.Note, To: []string{"owner-relay"}, Text: "look at 7"})
	tap := testutil.Post(t, st, store.Record{Lane: "board-watch", Kind: kinds.Owner, To: []string{"owner-relay"}, Text: "he tapped Land"})
	var out bytes.Buffer
	if err := hook.PostToolUse(ctx, st, relay, func() (store.Window, error) { return window, nil }, &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); !strings.Contains(got, tap.Text) || strings.Contains(got, note.Text) {
		t.Fatalf("hook delivered %q", got)
	}
	tags, stop := following(t, st, sub)
	defer stop()
	if tag := nextTag(t, tags); tag.meta["kind"] != batchKind || !strings.Contains(tag.content, note.Text) || strings.Contains(tag.content, tap.Text) {
		t.Fatalf("tag %+v, want only the note", tag)
	}
}

func TestARecipientListNeverReachesARecipientAndOwnRepliesNeverEcho(t *testing.T) {
	st, _ := newStore(t)
	idle, interval, batchWindow = 10*time.Millisecond, 5*time.Millisecond, time.Hour
	ctx := context.Background()
	relay := hook.Payload{SessionID: "s1", Event: "PostToolUse"}
	if err := st.BindLane(ctx, store.PeerKey{Session: relay.SessionID}, "d", "owner-relay", time.Time{}); err != nil {
		t.Fatal(err)
	}
	sub := batched(t, st, kinds.Note)
	everyone := make([]string, 0, 100)
	for i := range 100 {
		everyone = append(everyone, fmt.Sprintf("lane-%d", i))
	}
	mine := testutil.Post(t, st, store.Record{Lane: "owner-relay", Kind: kinds.Ask, To: []string{"meta"}, Text: "order one"})
	testutil.Post(t, st, store.Record{Lane: "owner-relay", Kind: kinds.Ask, Re: mine.Seq, To: []string{"meta"}, Text: "order two"})
	answer := testutil.Post(t, st, store.Record{Lane: "meta", Kind: kinds.Answer, Re: mine.Seq, To: []string{"owner-relay"}, Text: "on it"})
	broadcast := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Ask, To: append([]string{"owner-relay"}, everyone...), Text: "register touched"})
	tags, stop := following(t, st, sub)
	defer stop()
	for _, want := range []store.Record{answer, broadcast} {
		tag := nextTag(t, tags)
		if tag.meta["seq"] != fmt.Sprint(want.Seq) || strings.Contains(tag.content, "lane-") || strings.Contains(tag.content, "lanes") || strings.Contains(tag.content, "->") {
			t.Fatalf("tag %+v, want #%d with no recipients", tag, want.Seq)
		}
	}
	noTag(t, tags, 50*time.Millisecond)
	later := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Ask, To: append([]string{"owner-relay"}, everyone...), Text: "register touched again"})
	if err := st.SetCursor(ctx, store.LaneCursor(store.PeerKey{Session: relay.SessionID}), "d", broadcast.Seq); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := hook.PostToolUse(ctx, st, relay, func() (store.Window, error) { return window, nil }, &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); !strings.Contains(got, later.Text) || strings.Contains(got, "lane-") {
		t.Fatalf("hook delivered %q, want the record with no recipients", got)
	}
}

func TestARunningChannelStampsItsWindow(t *testing.T) {
	st, _ := newStore(t)
	s := start(t, st, window, proc, true)
	defer s.close()
	if f := s.next(); f.Method != "notifications/tools/list_changed" {
		t.Fatalf("first frame = %+v", f)
	}
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if _, ok, err := st.ChannelBeat(window); err != nil {
			t.Fatal(err)
		} else if ok {
			return
		}
	}
	t.Fatal("the channel never stamped its window")
}

func (s *session) exited() {
	s.t.Helper()
	_ = s.in.Close()
	select {
	case err := <-s.done:
		if err != nil {
			s.t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		s.t.Fatal("the channel never exited")
	}
	for f := range s.frames {
		s.t.Fatalf("a replaced channel sent %+v", f)
	}
	s.cancel()
}

func TestOnlyTheNewestChannelOfAReconnectedWindowDeliversABurst(t *testing.T) {
	st, _ := newStore(t)
	subscribe(t, st)
	older := open(t, st, store.Window{PID: 2753, Started: proc.Started - time.Minute.Milliseconds()})
	older.initialized()
	time.Sleep(50 * time.Millisecond)
	newer := open(t, st, proc)
	newer.initialized()
	time.Sleep(50 * time.Millisecond)

	burst := make([]store.Record, 0, 4)
	for i := range 4 {
		burst = append(burst, testutil.Post(t, st, store.Record{Kind: kinds.Ask, Text: fmt.Sprintf("ask %d of 4 sent together", i+1), To: []string{"root"}}))
	}

	for _, r := range burst {
		newer.record(r)
	}
	older.exited()
	newer.close()
}

func TestAnOlderChannelExitsOnceANewerOneStartsAndNeverTakesTheWindowBack(t *testing.T) {
	beatEvery = 10 * time.Millisecond
	t.Cleanup(func() { beatEvery = 15 * time.Second })
	st, _ := newStore(t)
	older := store.Window{PID: 2753, Started: proc.Started - time.Minute.Milliseconds()}
	stale := open(t, st, older)
	stale.initialized()
	newer := open(t, st, proc)
	newer.initialized()

	stale.exited()
	start(t, st, window, older, true).exited()
	newer.close()
}

func TestAServerWithToolsAnOlderPluginConfigStartsForASubagentLeavesTheWindowToTheToollessOne(t *testing.T) {
	st, _ := newStore(t)
	subscribe(t, st)
	toolless := open(t, st, store.Window{PID: 2753, Started: proc.Started - time.Minute.Milliseconds()})
	toolless.initialized()
	time.Sleep(50 * time.Millisecond)
	digest := mcp.Tool{Name: "digest", InputSchema: map[string]any{"type": "object"}, Handler: func(context.Context, json.RawMessage, func(string)) (string, bool) {
		return "cci digest d", false
	}}
	subagent := start(t, st, window, proc, false, digest)
	subagent.send(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`)
	subagent.next()
	subagent.initialized()
	subagent.send(`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"digest","arguments":{}}}`)
	if f := subagent.next(); string(f.ID) != "2" || len(f.Result.Content) != 1 || f.Result.Content[0].Text != "cci digest d" {
		t.Fatalf("tools/call digest = %+v, want the tool's answer", f)
	}
	ask := testutil.Post(t, st, store.Record{Kind: kinds.Ask, Text: "posted after the subagent's tool call", To: []string{"root"}})
	toolless.record(ask)
	subagent.quiet(50 * time.Millisecond)
	subagent.close()
	toolless.close()
	if posted := disconnects(t, st); len(posted) != 1 {
		t.Fatalf("disconnects = %+v, want only the toolless channel's own close", posted)
	}
}

func TestTwoWindowsKeepBothChannelServersPastTwoBeats(t *testing.T) {
	beatEvery = 10 * time.Millisecond
	t.Cleanup(func() { beatEvery = 15 * time.Second })
	st, _ := newStore(t)
	first := start(t, st, window, proc, true)
	second := start(t, st, store.Window{PID: 4244, Started: window.Started}, store.Window{PID: 4345, Started: proc.Started + 1}, true)
	time.Sleep(3 * beatEvery)

	for _, s := range []*session{first, second} {
		select {
		case err := <-s.done:
			t.Fatalf("a channel server exited while another window's server ran: %v", err)
		default:
		}
		s.close()
	}
}

func TestHeardTagReadsOnlyTagsClaudeCodeGaveTheMainThread(t *testing.T) {
	meta := map[string]string{"seq": "671504", "kind": "owner", "lane": "communicator"}
	want := tag{kind: "owner", seq: 671504}
	for _, tt := range []struct {
		name  string
		line  string
		heard bool
	}{
		{"a tag Claude Code delivered at an idle prompt", userLine(tagText(Source, meta), false), true},
		{"a tag queued mid-turn", queuedLine(tagText(Source, meta)), true},
		{"a tag queued mid-turn as text blocks", queuedLine([]map[string]string{{"type": "text", "text": tagText(Source, meta)}}), true},
		{"a tag from the one-server layout", userLine(tagText(Plugin+"cci", meta), false), true},
		{"a subagent's copy", userLine(tagText(Source, meta), true), false},
		{"a tool result quoting a tag", userLine([]map[string]any{{"type": "tool_result", "tool_use_id": "toolu_1", "content": tagText(Source, meta)}}, false), false},
		{"another plugin's channel", userLine(tagText("plugin:cc-slack:cc-slack", meta), false), false},
		{"a prompt quoting a tag mid-sentence", userLine("did "+tagText(Source, meta)+" arrive?", false), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := heardTag([]byte(tt.line + "\n"))
			if ok != tt.heard || (ok && got != want) {
				t.Fatalf("heardTag = %+v, %v; want %+v, %v", got, ok, want, tt.heard)
			}
		})
	}
}

func TestACursorMovesOnlyOnceTheSessionsTranscriptShowsItsTag(t *testing.T) {
	st, _ := newStore(t)
	ctx := context.Background()
	sub, err := st.Subscribe(ctx, store.Subscription{Session: "s1", Window: window, Drive: "d", Reader: "root", Cursor: "root-channel"})
	if err != nil {
		t.Fatal(err)
	}
	before, _, err := st.Cursor(ctx, sub.Cursor, sub.Drive)
	if err != nil {
		t.Fatal(err)
	}
	l, err := newLedger(ctx, st, sub.Session)
	if err != nil {
		t.Fatal(err)
	}
	sent := make(chan map[string]string, 8)
	followCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- l.listen(followCtx, func(ctx context.Context) error {
			return follow(ctx, st, sub, false, l, func(_ string, params any) error {
				sent <- params.(map[string]any)["meta"].(map[string]string)
				return nil
			})
		})
	}()
	t.Cleanup(func() {
		stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	first := testutil.Post(t, st, store.Record{Lane: "desk", Kind: kinds.Ask, To: []string{"root"}, Text: "first"})
	second := testutil.Post(t, st, store.Record{Lane: "desk", Kind: kinds.Report, To: []string{"root"}, Text: "second"})
	tags := make([]map[string]string, 0, 2)
	for _, want := range []store.Record{first, second} {
		select {
		case meta := <-sent:
			if meta["seq"] != fmt.Sprint(want.Seq) {
				t.Fatalf("channel sent #%s, want #%d", meta["seq"], want.Seq)
			}
			tags = append(tags, meta)
		case <-time.After(5 * time.Second):
			t.Fatalf("channel never sent #%d", want.Seq)
		}
	}
	unmoved := func(why string) {
		t.Helper()
		time.Sleep(10 * interval)
		if seq, _, err := st.Cursor(ctx, sub.Cursor, sub.Drive); err != nil || seq != before {
			t.Fatalf("cursor at #%d, %v after %s; want it unmoved at #%d", seq, err, why, before)
		}
	}
	unmoved("sending into a session that never showed the tags")
	if err := transcribe(ctx, userLine(tagText(Source, tags[0]), true), userLine([]map[string]any{{"type": "tool_result", "tool_use_id": "toolu_1", "content": tagText(Source, tags[0])}}, false)); err != nil {
		t.Fatal(err)
	}
	unmoved("a subagent's copy and a tool result")
	if err := transcribe(ctx, userLine(tagText(Source, tags[1]), false)); err != nil {
		t.Fatal(err)
	}
	unmoved("hearing the second tag ahead of the first")
	if err := transcribe(ctx, queuedLine([]map[string]string{{"type": "text", "text": tagText(Source, tags[0])}})); err != nil {
		t.Fatal(err)
	}
	cursorReaches(t, st, sub.Cursor, second.Seq)
}

func disconnects(t *testing.T, st *store.Store) []store.Record {
	t.Helper()
	records, err := st.Query(context.Background(), store.Filter{Drive: "d", Kinds: []kinds.Kind{kinds.Disconnect}})
	if err != nil {
		t.Fatal(err)
	}
	return records
}

func TestAChannelClaudeCodeClosesPostsADisconnectTheNextOneResolves(t *testing.T) {
	st, _ := newStore(t)
	ctx := context.Background()
	subscribe(t, st)
	s := open(t, st, proc)
	s.initialized()
	s.close()
	posted := disconnects(t, st)
	if len(posted) != 1 || !slices.Equal(posted[0].To, []string{"communicator"}) || posted[0].Lane != "root" || !strings.Contains(posted[0].Text, hook.ReconnectCommand) {
		t.Fatalf("disconnects = %+v, want one from root to communicator naming %s", posted, hook.ReconnectCommand)
	}
	again := open(t, st, proc)
	again.initialized()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(interval) {
		still, err := st.OpenAmong(ctx, "d", []int64{posted[0].Seq})
		if err != nil {
			t.Fatal(err)
		}
		if len(still) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the reconnected channel never resolved disconnect #%d", posted[0].Seq)
		}
	}
	recovered, err := st.Query(ctx, store.Filter{Drive: "d", Kinds: []kinds.Kind{kinds.Recovered}})
	if err != nil || len(recovered) != 1 || recovered[0].Resolves != posted[0].Seq || !slices.Equal(recovered[0].To, []string{"communicator"}) {
		t.Fatalf("recovered = %+v, %v; want one to communicator resolving #%d", recovered, err, posted[0].Seq)
	}
	again.close()
}

func TestAReplacedChannelOrOneItsDaemonStopsPostsNoDisconnect(t *testing.T) {
	st, _ := newStore(t)
	subscribe(t, st)
	older := open(t, st, store.Window{PID: 2753, Started: proc.Started - time.Minute.Milliseconds()})
	older.initialized()
	newer := open(t, st, proc)
	newer.initialized()
	older.exited()

	daemon, stopDaemon := context.WithCancel(context.Background())
	inR, inW := io.Pipe()
	served := make(chan error, 1)
	go func() {
		served <- Serve(Hosted(context.Background(), daemon), st, window, store.Window{PID: 4444, Started: proc.Started + 1}, false, true, inR, io.Discard)
	}()
	time.Sleep(50 * time.Millisecond)
	stopDaemon()
	_ = inW.Close()
	if err := <-served; err != nil {
		t.Fatal(err)
	}
	newer.exited()
	if posted := disconnects(t, st); len(posted) != 0 {
		t.Fatalf("disconnects = %+v, want none from a replaced channel or a daemon's stop", posted)
	}
}

func fakeTmux(t *testing.T, up bool) (string, string) {
	t.Helper()
	dir := t.TempDir()
	calls := filepath.Join(dir, "calls")
	script := "#!/bin/sh\necho \"$*\" >> " + calls + "\ncase \"$1\" in\nhas-session) [ -f " + calls + ".up ] ;;\nnew-session) touch " + calls + ".up ;;\nesac\n"
	if err := os.WriteFile(filepath.Join(dir, "tmux"), []byte(script), 0o700); err != nil { //nolint:gosec // G306: the fake tmux runs as a program.
		t.Fatal(err)
	}
	if up {
		if err := os.WriteFile(calls+".up", nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return filepath.Join(dir, "tmux"), calls
}

func tmuxCalls(t *testing.T, calls string) string {
	t.Helper()
	got, err := os.ReadFile(calls) //nolint:gosec // G304: the fake tmux's log under the test's temp dir.
	if err != nil {
		t.Fatal(err)
	}
	return string(got)
}

func register(t *testing.T, st *store.Store, w store.Window, tmux string, notify ...string) store.Registration {
	t.Helper()
	r, err := st.Register(context.Background(), store.Registration{
		Subscription: store.Subscription{Alias: "release-demo-coordinator", Session: "s1", Window: w, Drive: "d", Reader: "root", Cursor: "coordinator"},
		Cwd:          t.TempDir(), Command: "claude", Shell: "/bin/sh", Tmux: tmux, Notify: notify,
	})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestARegisteredSessionWhoseClaudeCodeExitedStartsAgainInTmux(t *testing.T) {
	st, _ := newStore(t)
	tmux, calls := fakeTmux(t, false)
	r := register(t, st, window, tmux, "meta")
	s := open(t, st, proc)
	s.initialized()
	s.close()
	posted := disconnects(t, st)
	if len(posted) != 1 || !slices.Equal(posted[0].To, []string{"meta"}) || posted[0].Text != "root's Claude Code exited in window 4242 (session s1); cci session run starts it again in tmux session release-demo-coordinator" {
		t.Fatalf("disconnects = %+v, want one to meta saying tmux starts it again", posted)
	}
	if got, want := tmuxCalls(t, calls), "has-session -t =release-demo-coordinator\nnew-session -d -s release-demo-coordinator -c "+r.Cwd+" -- /bin/sh -l -c exec cci session run release-demo-coordinator\n"; got != want {
		t.Fatalf("tmux was called with %q, want %q", got, want)
	}
}

func TestARegisteredSessionStillRunningHasItsReconnectTyped(t *testing.T) {
	defer func(settle time.Duration) { exitSettle = settle }(exitSettle)
	exitSettle = 20 * time.Millisecond
	p, err := process.NewProcess(int32(os.Getpid())) //nolint:gosec // G115: OS pids fit in int32.
	if err != nil {
		t.Fatal(err)
	}
	created, err := p.CreateTime()
	if err != nil {
		t.Fatal(err)
	}
	live := store.Window{PID: os.Getpid(), Started: created}
	st, _ := newStore(t)
	tmux, calls := fakeTmux(t, true)
	register(t, st, live, tmux)
	s := start(t, st, live, proc, false)
	s.send(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`)
	s.next()
	s.initialized()
	s.close()
	posted := disconnects(t, st)
	want := fmt.Sprintf("root's cci channel disconnected: Claude Code closed it in window %d (session s1) while it runs on, so cci typed %s into tmux session release-demo-coordinator", live.PID, hook.ReconnectCommand)
	if len(posted) != 1 || !slices.Equal(posted[0].To, []string{"communicator"}) || posted[0].Text != want {
		t.Fatalf("disconnects = %+v, want one to communicator saying %q", posted, want)
	}
	if got, want := tmuxCalls(t, calls), "has-session -t =release-demo-coordinator\nsend-keys -t =release-demo-coordinator: -l "+hook.ReconnectCommand+"\nsend-keys -t =release-demo-coordinator: Enter\n"; got != want {
		t.Fatalf("tmux was called with %q, want %q", got, want)
	}
}
