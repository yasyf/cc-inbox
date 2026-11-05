package hook_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/hook"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

const quietLane = "fix-lane-1"

func launch(t *testing.T, st *store.Store, lane, brief string) {
	t.Helper()
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Spawn, Text: "SPAWNED " + lane, Refs: store.Refs{Lanes: []string{lane}}, Fields: store.Fields{Attrs: map[string]string{"brief": brief, "model": "opus"}}})
}

func bindLane(t *testing.T, st *store.Store, lane string) hook.Payload {
	t.Helper()
	if err := st.BindLane(context.Background(), store.PeerKey{Session: "s1"}, "d", lane, time.Time{}); err != nil {
		t.Fatal(err)
	}
	return hook.Payload{SessionID: "s1", Event: "Stop", LastMessage: "CI will finish soon, I will report then"}
}

func stopReason(t *testing.T, st *store.Store, p hook.Payload) string {
	t.Helper()
	var out bytes.Buffer
	if err := hook.Stop(context.Background(), st, p, &out); err != nil {
		t.Fatal(err)
	}
	return decision(t, &out)
}

func decision(t *testing.T, out *bytes.Buffer) string {
	t.Helper()
	if out.Len() == 0 {
		return ""
	}
	var got struct{ Decision, Reason string }
	if err := json.Unmarshal(out.Bytes(), &got); err != nil || got.Decision != "block" {
		t.Fatalf("hook output %q: %v", out.String(), err)
	}
	return got.Reason
}

func blockersToRoot(t *testing.T, st *store.Store) []store.Record {
	t.Helper()
	open, err := st.OpenTo(context.Background(), "d", "root", []kinds.Kind{kinds.Blocker})
	if err != nil {
		t.Fatal(err)
	}
	return open
}

func TestALaunchedLaneCannotEndItsTurnOnAPromise(t *testing.T) {
	st, _ := testutil.Store(t)
	launch(t, st, quietLane, "/briefs/fix-lane-1.md")
	p := bindLane(t, st, quietLane)
	testutil.Post(t, st, store.Record{Lane: quietLane, Kind: kinds.Report, To: []string{"meta-desk-1"}, Text: "pushed the branch"})

	reason := stopReason(t, st, p)
	for _, want := range []string{"--lane fix-lane-1 --kind done", "--kind stopped", "--kind blocked --pr <n>", "--kind blocked --attr until=", "--kind ask --to <who>"} {
		if !strings.Contains(reason, want) {
			t.Fatalf("refusal %q lacks %q", reason, want)
		}
	}
	if got := blockersToRoot(t, st); len(got) != 0 {
		t.Fatalf("the first stop wrote %+v", got)
	}

	if got := stopReason(t, st, p); got != "" {
		t.Fatalf("a second stop with nothing new was refused again: %q", got)
	}
	blockers := blockersToRoot(t, st)
	if len(blockers) != 1 || blockers[0].Lane != quietLane || !strings.Contains(blockers[0].Text, "CI will finish soon") {
		t.Fatalf("the turn ended with %+v; want one blocker carrying its last words", blockers)
	}
	if got := stopReason(t, st, p); got != "" {
		t.Fatalf("a lane on its own blocker was refused: %q", got)
	}
	if got := blockersToRoot(t, st); len(got) != 1 {
		t.Fatalf("stopping again wrote %d blockers", len(got))
	}
}

func TestARecordNewerThanTheRefusalIsRefusedAgain(t *testing.T) {
	st, _ := testutil.Store(t)
	launch(t, st, quietLane, "/briefs/fix-lane-1.md")
	p := bindLane(t, st, quietLane)
	testutil.Post(t, st, store.Record{Lane: quietLane, Kind: kinds.Report, Text: "first"})
	if got := stopReason(t, st, p); got == "" {
		t.Fatal("not refused")
	}
	testutil.Post(t, st, store.Record{Lane: quietLane, Kind: kinds.Report, Text: "second"})
	if got := stopReason(t, st, p); got == "" {
		t.Fatal("a new record that settles nothing passed")
	}
	if got := blockersToRoot(t, st); len(got) != 0 {
		t.Fatalf("a lane still talking got a blocker written for it: %+v", got)
	}
}

func TestATurnEndsOnATypedRecord(t *testing.T) {
	for _, r := range []store.Record{
		{Kind: kinds.Done, Text: "delivered"},
		{Kind: kinds.Stopped, Text: "out of scope"},
		{Kind: kinds.StandDown, To: []string{"root"}, Text: "standing down"},
		{Kind: kinds.Handoff, Text: "handing off"},
		{Kind: kinds.Ask, To: []string{"meta-desk-1"}, Text: "which head?"},
		{Kind: kinds.Blocker, To: []string{"meta-desk-1"}, Text: "no credentials"},
		{Kind: kinds.Blocked, Refs: store.Refs{PRs: []store.PR{{Number: 48099}}}, Text: "lands after 48099"},
	} {
		t.Run(string(r.Kind), func(t *testing.T) {
			st, _ := testutil.Store(t)
			launch(t, st, quietLane, "/briefs/fix-lane-1.md")
			p := bindLane(t, st, quietLane)
			r.Lane = quietLane
			testutil.Post(t, st, r)
			if got := stopReason(t, st, p); got != "" {
				t.Fatalf("refused: %q", got)
			}
		})
	}
}

func TestALandedLaneEndsUnlessItsOwnWorkIsOpen(t *testing.T) {
	pr := func(n int) store.Refs { return store.Refs{PRs: []store.PR{{Number: n}}} }
	for name, tc := range map[string]struct {
		before []store.Record
		held   bool
	}{
		"nothing open":    {before: []store.Record{{Kind: kinds.Opened, Refs: pr(41), Text: "opened #41"}}},
		"another PR open": {before: []store.Record{{Kind: kinds.Opened, Refs: pr(41), Text: "opened #41"}, {Kind: kinds.Opened, Refs: pr(42), Text: "opened #42"}}, held: true},
		"its ask open":    {before: []store.Record{{Kind: kinds.Opened, Refs: pr(41), Text: "opened #41"}, {Kind: kinds.Ask, To: []string{"meta-desk-2"}, Text: "tag it?"}}, held: true},
	} {
		t.Run(name, func(t *testing.T) {
			st, _ := testutil.Store(t)
			launch(t, st, quietLane, "/briefs/fix-lane-1.md")
			p := bindLane(t, st, quietLane)
			for _, r := range tc.before {
				r.Lane = quietLane
				testutil.Post(t, st, r)
			}
			testutil.Post(t, st, store.Record{Lane: quietLane, Kind: kinds.Landed, To: []string{"meta-desk-2"}, Refs: pr(41), Text: "landed #41"})
			if got := stopReason(t, st, p); (got != "") != tc.held {
				t.Fatalf("stop = %q; want held %v", got, tc.held)
			}
		})
	}
}

func TestAStopOnAStoreItCannotReadPasses(t *testing.T) {
	st, _ := testutil.Store(t)
	launch(t, st, quietLane, "/briefs/fix-lane-1.md")
	p := bindLane(t, st, quietLane)
	testutil.Post(t, st, store.Record{Lane: quietLane, Kind: kinds.Report, Text: "working"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out bytes.Buffer
	if err := hook.Stop(ctx, st, p, &out); err != nil || out.Len() != 0 {
		t.Fatalf("stop = %q, %v; want it to pass", out.String(), err)
	}
}

func TestAnAnsweredWaitNoLongerEndsATurn(t *testing.T) {
	st, _ := testutil.Store(t)
	launch(t, st, quietLane, "/briefs/fix-lane-1.md")
	p := bindLane(t, st, quietLane)
	ask := testutil.Post(t, st, store.Record{Lane: quietLane, Kind: kinds.Ask, To: []string{"meta-desk-1"}, Text: "which head?"})
	if got := stopReason(t, st, p); got != "" {
		t.Fatalf("an open ask was refused: %q", got)
	}
	testutil.Post(t, st, store.Record{Lane: "meta-desk-1", Kind: kinds.Answer, To: []string{quietLane}, Re: ask.Seq, Resolves: ask.Seq, Text: "the newest"})
	if got := stopReason(t, st, p); got == "" {
		t.Fatal("a lane whose question was answered stopped on it")
	}
}

func TestARotationHandoffIsNotATurnEnd(t *testing.T) {
	st, _ := testutil.Store(t)
	launch(t, st, quietLane, "/briefs/fix-lane-1.md")
	p := bindLane(t, st, quietLane)
	testutil.Post(t, st, store.Record{Lane: quietLane, Kind: kinds.Report, Text: "working"})
	testutil.Post(t, st, store.Record{Lane: quietLane, Kind: kinds.Handoff, Refs: store.Refs{CCN: "abc1234"}, Text: "handoff before compaction"})
	if got := stopReason(t, st, p); !strings.Contains(got, "REPORT") {
		t.Fatalf("after a rotation = %q; want the report it follows refused", got)
	}
}

func TestAMechanismObligesAnOpenedOrABlocker(t *testing.T) {
	st, _ := testutil.Store(t)
	launch(t, st, "inc-liam-load-fix", "/briefs/fix.md")
	p := bindLane(t, st, "inc-liam-load-fix")
	testutil.Post(t, st, store.Record{Lane: "inc-liam-load-fix", Kind: kinds.Mechanism, Text: "the pool exhausts"})
	if got := stopReason(t, st, p); !strings.Contains(got, "owes the fix") || !strings.Contains(got, "--kind opened --pr <n>") || !strings.Contains(got, "--kind blocker") {
		t.Fatalf("after a mechanism = %q", got)
	}
	testutil.Post(t, st, store.Record{Lane: "inc-liam-load-fix", Kind: kinds.Blocker, To: []string{"root"}, Text: "no quota to fix it"})
	if got := stopReason(t, st, p); got != "" {
		t.Fatalf("after its blocker = %q", got)
	}
}

func TestAThreadWhoseWakeExistsStopsFreely(t *testing.T) {
	t.Run("nothing launched it", func(t *testing.T) {
		st, _ := testutil.Store(t)
		p := bindLane(t, st, "owner-relay")
		testutil.Post(t, st, store.Record{Lane: "owner-relay", Kind: kinds.Report, Text: "told the owner"})
		if got := stopReason(t, st, p); got != "" {
			t.Fatalf("refused: %q", got)
		}
	})
	t.Run("root", func(t *testing.T) {
		st, _ := testutil.Store(t)
		launch(t, st, "root", "/briefs/root.md")
		p := bindLane(t, st, "root")
		if got := stopReason(t, st, p); got != "" {
			t.Fatalf("refused: %q", got)
		}
	})
	t.Run("standing brief", func(t *testing.T) {
		st, _ := testutil.Store(t)
		brief := filepath.Join(t.TempDir(), "desk.md")
		if err := os.WriteFile(brief, []byte("# desk\nStanding: answers every grade\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		launch(t, st, "grade-desk-1", brief)
		p := bindLane(t, st, "grade-desk-1")
		testutil.Post(t, st, store.Record{Lane: "grade-desk-1", Kind: kinds.Report, Text: "answered"})
		if got := stopReason(t, st, p); got != "" {
			t.Fatalf("refused: %q", got)
		}
	})
	t.Run("channel subscription", func(t *testing.T) {
		st, _ := testutil.Store(t)
		launch(t, st, "sub-lane-1", "/briefs/sub.md")
		p := bindLane(t, st, "sub-lane-1")
		if _, err := st.Subscribe(context.Background(), store.Subscription{Session: "s1", Window: store.Window{PID: 100, Started: 1}, Drive: "d", Reader: "sub-lane-1", Cursor: "c"}); err != nil {
			t.Fatal(err)
		}
		if got := stopReason(t, st, p); got != "" {
			t.Fatalf("refused: %q", got)
		}
	})
}

func TestALiveHarnessWaitEndsARootThreadsTurn(t *testing.T) {
	for name, payload := range map[string]string{
		"monitor": `{"background_tasks":[{"id":"m1","type":"monitor","status":"running","description":"ci"}],"session_crons":[]}`,
		"agent":   `{"background_tasks":[{"id":"a1","type":"subagent","status":"running","description":"finder","agent_type":"Explore"}],"session_crons":[]}`,
		"cron":    `{"background_tasks":[],"session_crons":[{"id":"c1","schedule":"15 10 4 11 *","recurring":false,"prompt":"read the build"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			st, _ := testutil.Store(t)
			launch(t, st, quietLane, "/briefs/fix-lane-1.md")
			p := bindLane(t, st, quietLane)
			if err := json.Unmarshal([]byte(payload), &p); err != nil {
				t.Fatal(err)
			}
			testutil.Post(t, st, store.Record{Lane: quietLane, Kind: kinds.Report, To: []string{"meta-desk-1"}, Text: "pushed the branch"})

			for range 2 {
				if reason := stopReason(t, st, p); reason != "" {
					t.Fatalf("a stop on a live wait was refused: %q", reason)
				}
			}
			if open := blockersToRoot(t, st); len(open) != 0 {
				t.Fatalf("a stop on a live wait wrote a blocker: %+v", open)
			}
		})
	}
}

func TestEmptyWaitListsStillHoldATurn(t *testing.T) {
	st, _ := testutil.Store(t)
	launch(t, st, quietLane, "/briefs/fix-lane-1.md")
	p := bindLane(t, st, quietLane)
	if err := json.Unmarshal([]byte(`{"background_tasks":[],"session_crons":[]}`), &p); err != nil {
		t.Fatal(err)
	}
	testutil.Post(t, st, store.Record{Lane: quietLane, Kind: kinds.Report, To: []string{"meta-desk-1"}, Text: "pushed the branch"})

	if reason := stopReason(t, st, p); !strings.Contains(reason, "--kind done") {
		t.Fatalf("stop = %q", reason)
	}
}

func TestASessionsLiveWaitDoesNotReleaseATeammateLane(t *testing.T) {
	ctx := context.Background()
	st, _ := testutil.Store(t)
	launch(t, st, quietLane, "/briefs/fix-lane-1.md")
	lane := hook.Payload{SessionID: "s1", AgentID: "afix-lane-1-0123456789abcdef", Event: "SubagentStop"}
	if err := json.Unmarshal([]byte(`{"background_tasks":[{"id":"m1","type":"monitor","status":"running"}]}`), &lane); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Announce(ctx, store.Peer{Session: lane.SessionID, Agent: lane.AgentID, AgentName: quietLane}); err != nil {
		t.Fatal(err)
	}
	if err := st.BindLane(ctx, lane.Key(), "d", quietLane, time.Time{}); err != nil {
		t.Fatal(err)
	}
	testutil.Post(t, st, store.Record{Lane: quietLane, Kind: kinds.Report, Text: "working"})

	var out bytes.Buffer
	if err := hook.SubagentStop(ctx, st, lane, &out); err != nil {
		t.Fatal(err)
	}
	if got := decision(t, &out); !strings.Contains(got, "--kind done") {
		t.Fatalf("the lane's stop = %q", got)
	}
}

func TestATeammateLaneIsHeldAndItsHelperIsNot(t *testing.T) {
	ctx := context.Background()
	st, _ := testutil.Store(t)
	launch(t, st, quietLane, "/briefs/fix-lane-1.md")
	lane := hook.Payload{SessionID: "s1", AgentID: "afix-lane-1-0123456789abcdef", Event: "SubagentStop"}
	helper := hook.Payload{SessionID: "s1", AgentID: "aship-pr", Event: "SubagentStop"}
	for p, name := range map[*hook.Payload]string{&lane: quietLane, &helper: "ship-pr"} {
		if _, err := st.Announce(ctx, store.Peer{Session: p.SessionID, Agent: p.AgentID, AgentName: name}); err != nil {
			t.Fatal(err)
		}
		if err := st.BindLane(ctx, p.Key(), "d", quietLane, time.Time{}); err != nil {
			t.Fatal(err)
		}
	}
	testutil.Post(t, st, store.Record{Lane: quietLane, Kind: kinds.Report, Text: "working"})

	var out bytes.Buffer
	if err := hook.SubagentStop(ctx, st, lane, &out); err != nil {
		t.Fatal(err)
	}
	if got := decision(t, &out); !strings.Contains(got, "--kind done") {
		t.Fatalf("the lane's stop = %q", got)
	}
	if got, _ := st.Peer(ctx, lane.Key()); got.Superseded {
		t.Fatalf("a refused stop retired the lane: %+v", got)
	}

	out.Reset()
	if err := hook.SubagentStop(ctx, st, helper, &out); err != nil {
		t.Fatal(err)
	}
	if got := decision(t, &out); got != "" {
		t.Fatalf("the helper's stop = %q", got)
	}
	if got, _ := st.Peer(ctx, helper.Key()); !got.Superseded {
		t.Fatalf("the helper stayed live: %+v", got)
	}
}

func TestATurnKilledByAnAPIErrorLeavesABlockerTheSupervisorResumes(t *testing.T) {
	ctx := context.Background()
	st, _ := testutil.Store(t)
	launch(t, st, quietLane, "/briefs/fix-lane-1.md")
	p := bindLane(t, st, quietLane)
	p.Event, p.Error = "StopFailure", "rate_limit"
	if err := hook.StopFailure(ctx, st, p); err != nil {
		t.Fatal(err)
	}
	blockers := blockersToRoot(t, st)
	if len(blockers) != 1 || blockers[0].Fields.Attrs["turn"] != hook.TurnDied || !strings.Contains(blockers[0].Text, "rate_limit") {
		t.Fatalf("blockers = %+v", blockers)
	}
	unlaunched := hook.Payload{SessionID: "s2", Event: "StopFailure", Error: "rate_limit"}
	if err := st.BindLane(ctx, unlaunched.Key(), "d", "owner-relay", time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := hook.StopFailure(ctx, st, unlaunched); err != nil {
		t.Fatal(err)
	}
	if got := blockersToRoot(t, st); len(got) != 1 {
		t.Fatalf("a thread nothing launched left a blocker: %+v", got)
	}
}

func TestLastWordsLeaveACredentialOutOfTheBlocker(t *testing.T) {
	value := "AS" + "IA" + strings.Repeat("A1B2", 4)
	for _, event := range []string{"Stop", "StopFailure"} {
		t.Run(event, func(t *testing.T) {
			st, _ := testutil.Store(t)
			launch(t, st, quietLane, "/briefs/fix-lane-1.md")
			p := bindLane(t, st, quietLane)
			p.Event, p.LastMessage = event, "the key is "+value+" and I stopped"
			testutil.Post(t, st, store.Record{Lane: quietLane, Kind: kinds.Report, Text: "pushed the branch"})
			if event == "Stop" {
				stopReason(t, st, p)
				stopReason(t, st, p)
			} else {
				p.Error = "rate_limit"
				if err := hook.StopFailure(context.Background(), st, p); err != nil {
					t.Fatal(err)
				}
			}
			blockers := blockersToRoot(t, st)
			if len(blockers) != 1 || strings.Contains(blockers[0].Text, value) || !strings.Contains(blockers[0].Text, "an AWS access key id withheld") {
				t.Fatalf("blockers = %+v; want one with the value withheld", blockers)
			}
		})
	}
}
