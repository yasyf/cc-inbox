package store_test

import (
	"context"
	"testing"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func TestOnlyALaunchedLaneIsRegistered(t *testing.T) {
	ctx := context.Background()
	st, _ := testutil.Store(t)
	registered := func(lane string) bool {
		t.Helper()
		ok, err := st.LaneRegistered(ctx, "d", lane)
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	if !registered("first-lane") {
		t.Fatal("a drive that launches nothing through spawn records refused a lane")
	}
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.State, Topic: "spawn:by-state", Text: "SPAWNED by-state /briefs/a.md"})
	if registered("stray-lane") {
		t.Fatal("a lane nothing launched is registered")
	}
	if !registered("by-state") {
		t.Fatal("a lane a SPAWNED state launched is not registered")
	}
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Spawn, Text: "SPAWNED by-spawn", Refs: store.Refs{Lanes: []string{"by-spawn"}}, Fields: store.Fields{Attrs: map[string]string{"brief": "/briefs/b.md", "model": "opus"}}})
	if !registered("by-spawn") {
		t.Fatal("a lane a spawn record launched is not registered")
	}
	testutil.Post(t, st, store.Record{Lane: "stray-lane", Kind: kinds.Blocker, Topic: store.RefusedPostTopic, To: []string{"root"}, Text: "post refused"})
	if registered("stray-lane") {
		t.Fatal("the blocker of a refused post registered the lane it refused")
	}
	testutil.Post(t, st, store.Record{Lane: "old-lane", Kind: kinds.Note, Text: "posted before the rule"})
	if !registered("old-lane") {
		t.Fatal("a lane already on the bus was refused")
	}
}

func TestOpenRefusalsCloseOnTheLanesNextRecord(t *testing.T) {
	ctx := context.Background()
	st, _ := testutil.Store(t)
	refused := testutil.Post(t, st, store.Record{Lane: "fix-lane-1", Kind: kinds.Blocker, Topic: store.RefusedPostTopic, To: []string{"root"}, Text: "post refused"})
	testutil.Post(t, st, store.Record{Lane: "other-lane", Kind: kinds.Blocker, Topic: store.RefusedPostTopic, To: []string{"root"}, Text: "post refused"})
	open, err := st.OpenRefusals(ctx, "d", "fix-lane-1")
	if err != nil || len(open) != 1 || open[0].Seq != refused.Seq {
		t.Fatalf("OpenRefusals = %v, %v; want only the lane's own", open, err)
	}
	testutil.Post(t, st, store.Record{Lane: "fix-lane-1", Kind: kinds.Note, Text: "went through"}.Resolving(open))
	if open, err := st.OpenRefusals(ctx, "d", "fix-lane-1"); err != nil || len(open) != 0 {
		t.Fatalf("after its next record OpenRefusals = %v, %v", open, err)
	}
}

func TestNewestOfLaneSkipsClaimsAndRotations(t *testing.T) {
	ctx := context.Background()
	st, _ := testutil.Store(t)
	report := testutil.Post(t, st, store.Record{Lane: "fix-lane-1", Kind: kinds.Report, Text: "working"})
	testutil.Post(t, st, store.Record{Lane: "fix-lane-1", Kind: kinds.Claim, Text: "claimed", Topic: "x"})
	testutil.Post(t, st, store.Record{Lane: "fix-lane-1", Kind: kinds.Handoff, Refs: store.Refs{CCN: "abc1234"}, Text: "rotated"})
	got, found, err := st.NewestOfLane(ctx, "d", "fix-lane-1")
	if err != nil || !found || got.Seq != report.Seq {
		t.Fatalf("NewestOfLane = %+v, %v, %v; want #%d", got, found, err, report.Seq)
	}
	retiring := testutil.Post(t, st, store.Record{Lane: "fix-lane-1", Kind: kinds.Handoff, Text: "handed off, no doc"})
	if got, _, _ := st.NewestOfLane(ctx, "d", "fix-lane-1"); got.Seq != retiring.Seq {
		t.Fatalf("NewestOfLane = #%d; want the handoff with no doc, #%d", got.Seq, retiring.Seq)
	}
}
