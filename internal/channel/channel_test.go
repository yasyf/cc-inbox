package channel

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

var window = store.Window{PID: 4242, Started: 1791277810598}

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
	} `json:"result"`
}

type session struct {
	t      *testing.T
	in     *io.PipeWriter
	frames chan frame
	done   chan error
	cancel context.CancelFunc
}

func open(t *testing.T, st *store.Store) *session {
	t.Helper()
	idle, interval = 10*time.Millisecond, 5*time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	s := &session{t: t, in: inW, frames: make(chan frame, 64), done: make(chan error, 1), cancel: cancel}
	go func() {
		s.done <- Serve(ctx, st, window, inR, outW)
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
			s.frames <- f
		}
		close(s.frames)
	}()
	s.send(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`)
	init := s.next()
	if _, ok := init.Result.Capabilities["experimental"].(map[string]any)["claude/channel"]; !ok || !strings.Contains(init.Result.Instructions, Source) || !strings.Contains(init.Result.Instructions, "answers to both names") {
		t.Fatalf("initialize = %+v", init.Result)
	}
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
		!strings.HasPrefix(f.Params.Content, "#"+seq+" ") || !strings.Contains(f.Params.Content, want.Text) {
		s.t.Fatalf("frame = %+v, want record #%d %q", f, want.Seq, want.Text)
	}
}

func TestChannelDeliversTheSubscriptionAndResumesFromItsCursor(t *testing.T) {
	st, clock := testutil.Store(t)
	ctx := context.Background()
	testutil.Post(t, st, store.Record{Kind: kinds.Ask, Text: "before anyone subscribed", To: []string{"root"}})
	s := open(t, st)
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
	resumed := open(t, st)
	resumed.quiet(50 * time.Millisecond)
	resumed.initialized()
	resumed.record(missed)
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

func TestReplacingTheSubscriptionNeverSkipsANewlySelectedRecord(t *testing.T) {
	st, clock := testutil.Store(t)
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
	if err := follow(ctx, st, old, func(string, any) error {
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
	if err := follow(followCtx, st, current, func(_ string, params any) error {
		got = append(got, params.(map[string]any)["content"].(string))
		stop()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !strings.HasPrefix(got[0], fmt.Sprintf("#%d ", note.Seq)) {
		t.Fatalf("delivered %q, want note #%d", got, note.Seq)
	}
}

func TestRecordsPostedRightAfterSubscribingArrive(t *testing.T) {
	st, _ := testutil.Store(t)
	testutil.Post(t, st, store.Record{Kind: kinds.Ask, Text: "before the subscription", To: []string{"root"}})
	subscribe(t, st)
	after := testutil.Post(t, st, store.Record{Kind: kinds.Ask, Text: "before the channel's first poll", To: []string{"root"}})
	s := open(t, st)
	s.initialized()
	s.record(after)
	s.quiet(50 * time.Millisecond)
	s.close()
}
