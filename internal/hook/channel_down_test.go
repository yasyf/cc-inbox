package hook_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/hook"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

type channelFixture struct {
	t       *testing.T
	st      *store.Store
	clock   *testutil.Clock
	window  store.Window
	wired   bool
	reached []string
}

func channelSession(t *testing.T, wired bool) *channelFixture {
	t.Helper()
	st, clock := testutil.Store(t)
	ctx := context.Background()
	window := store.Window{PID: 4242, Started: clock.Now().UnixMilli()}
	if _, err := st.Announce(ctx, store.Peer{Session: "s1", PID: window.PID, PIDStarted: window.Started, Env: map[string]string{"ORCA_TERMINAL_HANDLE": "term_1", "ORCA_CLI_BIN_DIR": "/orca/bin"}}); err != nil {
		t.Fatal(err)
	}
	if err := st.BindLane(ctx, store.PeerKey{Session: "s1"}, "d", "lane-x", clock.Now()); err != nil {
		t.Fatal(err)
	}
	return &channelFixture{t: t, st: st, clock: clock, window: window, wired: wired}
}

func (f *channelFixture) notice() string {
	f.t.Helper()
	note, err := hook.NoticeChannel(context.Background(), f.st, hook.Payload{SessionID: "s1"}, func(int) bool { return f.wired }, func(dir, terminal string) {
		f.reached = append(f.reached, dir+" "+terminal)
	})
	if err != nil {
		f.t.Fatal(err)
	}
	return note
}

func (f *channelFixture) blockers() []store.Record {
	f.t.Helper()
	all, err := f.st.Query(context.Background(), store.Filter{Drive: "d", Kinds: []kinds.Kind{kinds.Blocker}})
	if err != nil {
		f.t.Fatal(err)
	}
	return all
}

func TestAServerThatStopsOpensOneBlockerAndClosesItWhenBack(t *testing.T) {
	f := channelSession(t, true)
	if err := f.st.BeatChannel(f.window); err != nil {
		t.Fatal(err)
	}
	if note := f.notice(); note != "" || len(f.blockers()) != 0 {
		t.Fatalf("a running server: note %q, blockers %v; want neither", note, f.blockers())
	}
	f.clock.Advance(3 * time.Minute)
	note := f.notice()
	got := f.blockers()
	if len(got) != 1 || got[0].Lane != "lane-x" || !slices.Equal(got[0].To, []string{"meta-desk"}) || !strings.Contains(got[0].Text, "/mcp reconnect plugin:cc-inbox:cci") {
		t.Fatalf("blockers after the server stopped = %+v, want one from the lane to meta-desk naming the reconnect", got)
	}
	if !strings.Contains(note, "down") || !slices.Equal(f.reached, []string{"/orca/bin term_1"}) {
		t.Fatalf("note %q, reconnects %v; want the session told and one reconnect started", note, f.reached)
	}
	f.clock.Advance(time.Minute)
	if note := f.notice(); note != "" || len(f.blockers()) != 1 || len(f.reached) != 1 {
		t.Fatalf("the next tool call: note %q, %d blockers, %d reconnects; want no repeat", note, len(f.blockers()), len(f.reached))
	}
	f.clock.Advance(5 * time.Minute)
	if f.notice(); len(f.blockers()) != 1 || len(f.reached) != 2 {
		t.Fatalf("still down after the retry window: %d blockers, %d reconnects; want one blocker, a second reconnect", len(f.blockers()), len(f.reached))
	}
	if err := f.st.BeatChannel(f.window); err != nil {
		t.Fatal(err)
	}
	f.notice()
	records, err := f.st.Query(context.Background(), store.Filter{Drive: "d", Kinds: []kinds.Kind{kinds.Note}})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Resolves != got[0].Seq {
		t.Fatalf("records after the server came back = %+v, want one closing #%d", records, got[0].Seq)
	}
	if _, open, err := f.st.ChannelOutage(f.window); err != nil || open {
		t.Fatalf("outage after the server came back: open %v, err %v", open, err)
	}
}

func TestALaneLaunchedWithoutTheServerIsNeverDown(t *testing.T) {
	f := channelSession(t, false)
	f.clock.Advance(time.Hour)
	if note := f.notice(); note != "" || len(f.blockers()) != 0 || len(f.reached) != 0 {
		t.Fatalf("a strict lane: note %q, blockers %v, reconnects %v; want nothing", note, f.blockers(), f.reached)
	}
}

func TestAHandRunServerInAStrictLaneNeverReadsAsDown(t *testing.T) {
	f := channelSession(t, false)
	if err := f.st.BeatChannel(f.window); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(time.Hour)
	if note := f.notice(); note != "" || len(f.blockers()) != 0 || len(f.reached) != 0 {
		t.Fatalf("a strict lane with a stale beat: note %q, blockers %v, reconnects %v; want nothing", note, f.blockers(), f.reached)
	}
}

func TestAServerThatNeverStartedIsDownOnlyAfterTheGrace(t *testing.T) {
	f := channelSession(t, true)
	f.clock.Advance(time.Minute)
	if f.notice(); len(f.blockers()) != 0 {
		t.Fatalf("inside the startup grace: blockers %v", f.blockers())
	}
	f.clock.Advance(2 * time.Minute)
	if f.notice(); len(f.blockers()) != 1 || !strings.Contains(f.blockers()[0].Text, "never started") {
		t.Fatalf("past the startup grace: blockers %+v, want one saying it never started", f.blockers())
	}
}

func TestASubagentCallDoesNotCheckTheServer(t *testing.T) {
	f := channelSession(t, true)
	f.clock.Advance(time.Hour)
	note, err := hook.NoticeChannel(context.Background(), f.st, hook.Payload{SessionID: "s1", AgentID: "a1"}, func(int) bool { return true }, func(string, string) { t.Error("a subagent started a reconnect") })
	if err != nil || note != "" || len(f.blockers()) != 0 {
		t.Fatalf("note %q, err %v, blockers %v", note, err, f.blockers())
	}
}

type orcaStub struct {
	reads  []string
	wait   error
	sent   []string
	pauses int
}

func (o *orcaStub) run(_ context.Context, args ...string) (string, error) {
	switch args[1] {
	case "wait":
		return "", o.wait
	case "read":
		out := o.reads[0]
		o.reads = o.reads[1:]
		return out, nil
	}
	o.sent = append(o.sent, strings.Join(args[2:], " "))
	return "", nil
}

func TestTypeReconnectWaitsOutADraftThenTypesOnce(t *testing.T) {
	o := &orcaStub{reads: []string{
		`{"result":{"terminal":{"source":"screen","draft":"half a sentence"}}}`,
		`{"result":{"terminal":{"source":"screen","draft":null}}}`,
	}}
	if err := hook.TypeReconnect(context.Background(), o.run, "term_1", func() { o.pauses++ }); err != nil {
		t.Fatal(err)
	}
	if want := []string{"--terminal term_1 --text /mcp reconnect plugin:cc-inbox:cci --enter"}; !slices.Equal(o.sent, want) || o.pauses != 1 {
		t.Fatalf("sent %q after %d pauses, want %q after one", o.sent, o.pauses, want)
	}
}

func TestTypeReconnectGivesUpOnABusyTerminalWithoutTyping(t *testing.T) {
	o := &orcaStub{wait: errors.New("timed out")}
	if err := hook.TypeReconnect(context.Background(), o.run, "term_1", func() { o.pauses++ }); err != nil {
		t.Fatal(err)
	}
	if len(o.sent) != 0 || o.pauses == 0 {
		t.Fatalf("sent %q after %d pauses, want nothing typed", o.sent, o.pauses)
	}
}
