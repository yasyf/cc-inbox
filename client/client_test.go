package client_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/client"
	"github.com/yasyf/cc-inbox/internal/cli"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/relay"
	"github.com/yasyf/cc-inbox/internal/store"
)

const head = "e718a7434fa01ee9835f992a9a647a825350732c"

func grade() client.Draft {
	return client.Draft{
		Drive: "d",
		Lane:  "finder-desk",
		Kind:  "lr.grade",
		Text:  "grade #45821 finder clear",
		ID:    "lr.grade/45821/" + head + "/finder",
		To:    []string{"supervisor"},
		PRs:   []string{"45821"},
		Body:  json.RawMessage(`{"pr":45821,"head":"` + head + `","grader":"finder","verdict":"clear"}`),
	}
}

func post(t *testing.T, d client.Draft) client.Record {
	t.Helper()
	stored, err := client.Post(context.Background(), d)
	if err != nil {
		t.Fatalf("Post(%+v): %v", d, err)
	}
	return stored
}

func TestAPostStoresTheTypedBodyATailReadsBack(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())

	stored := post(t, grade())

	if stored.Seq == 0 || string(stored.Kind) != "lr.grade" || stored.Lane != "finder-desk" {
		t.Fatalf("stored = %+v", stored)
	}
	records, err := client.Tail(context.Background(), client.Query{Drive: "d", Kinds: []string{"lr.grade"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Seq != stored.Seq {
		t.Fatalf("Tail = %+v, want seq %d", records, stored.Seq)
	}
	var body struct {
		PR      int    `json:"pr"`
		Verdict string `json:"verdict"`
	}
	if err := json.Unmarshal(records[0].Body, &body); err != nil {
		t.Fatal(err)
	}
	if body.PR != 45821 || body.Verdict != "clear" {
		t.Fatalf("body = %+v", body)
	}
	if len(records[0].To) != 1 || records[0].To[0] != "supervisor" || len(records[0].Refs.PRs) != 1 || records[0].Refs.PRs[0].Number != 45821 {
		t.Fatalf("record = %+v", records[0])
	}
}

func TestARepostOfAnIDReturnsTheStoredRecord(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())

	first, second := post(t, grade()), post(t, grade())

	if first.Seq != second.Seq {
		t.Fatalf("seqs %d and %d, want one record", first.Seq, second.Seq)
	}
}

func TestAPostRefusedByCciReturnsItsError(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	d := grade()
	d.Kind = "no-such-kind"

	if _, err := client.Post(context.Background(), d); err == nil || !strings.Contains(err.Error(), "unknown kind") {
		t.Fatalf("Post = %v, want unknown kind", err)
	}
}

func TestAForcedPostReachesALaneThatLooksLikeASlipForALiveOne(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	ctx := context.Background()
	st, err := store.Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	live := store.PeerKey{Session: "live-session", Agent: "alane-2"}
	if err := st.BindLane(ctx, live, "d", "lane-2", time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Announce(ctx, store.Peer{Session: live.Session, Agent: live.Agent}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	standDown := client.Draft{Drive: "d", Lane: "supervisor", Kind: "stand-down", Text: "lane-1 stood down", To: []string{"lane-1"}, Successor: "lane-2"}

	if _, err := client.Post(ctx, standDown); err == nil || !strings.Contains(err.Error(), "looks like a slip") {
		t.Fatalf("Post without Force = %v, want the slip refusal", err)
	}
	standDown.Force = true
	if stored := post(t, standDown); len(stored.To) != 1 || stored.To[0] != "lane-1" {
		t.Fatalf("forced stored = %+v", stored)
	}
}

func TestAReplyNamesTheRecordItAnswers(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	ask := post(t, client.Draft{Drive: "d", Lane: "lane-7", Kind: "ask", Text: "which base?", To: []string{"root"}})

	answer := post(t, client.Draft{Drive: "d", Lane: "root", Kind: "answer", Text: "dev", To: []string{"lane-7"}, Re: ask.Seq})

	if answer.Re != ask.Seq {
		t.Fatalf("Re = %d, want %d", answer.Re, ask.Seq)
	}
}

func TestATailReadsPastOnePageFromItsCursor(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	const posted = 503
	var first int64
	for i := range posted {
		stored := post(t, client.Draft{Drive: "d", Lane: "lane-7", Kind: "lr.note", Text: fmt.Sprintf("note %d", i), To: []string{"supervisor"}, Body: json.RawMessage(`{"text":"note"}`)})
		if i == 0 {
			first = stored.Seq
		}
	}

	all, err := client.Tail(context.Background(), client.Query{Drive: "d", Kinds: []string{"lr.note"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != posted {
		t.Fatalf("Tail read %d records, want %d", len(all), posted)
	}
	for i := 1; i < len(all); i++ {
		if all[i].Seq <= all[i-1].Seq {
			t.Fatalf("records out of order at %d: %d after %d", i, all[i].Seq, all[i-1].Seq)
		}
	}
	later, err := client.Tail(context.Background(), client.Query{Drive: "d", After: first, Kinds: []string{"lr.note"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(later) != posted-1 {
		t.Fatalf("Tail after %d read %d records, want %d", first, len(later), posted-1)
	}
}

func TestATailFiltersByKindAndWindow(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	post(t, client.Draft{Drive: "d", Lane: "root", Kind: "note", Text: "a note"})
	post(t, grade())

	records, err := client.Tail(context.Background(), client.Query{Drive: "d", Kinds: []string{"note"}})
	if err != nil || len(records) != 1 || string(records[0].Kind) != "note" {
		t.Fatalf("Tail by kind = %+v, %v", records, err)
	}
	none, err := client.Tail(context.Background(), client.Query{Drive: "d", Since: time.Now().Add(time.Hour)})
	if err != nil || len(none) != 0 {
		t.Fatalf("Tail after the window = %+v, %v", none, err)
	}
}

func TestFollowDeliversEveryKindAddressedToRootAndOnlyTheBroadcastsItNames(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	start := post(t, client.Draft{Drive: "d", Lane: "lane-7", Kind: "note", Text: "before the watch"})
	hold := post(t, client.Draft{Drive: "d", Lane: "owner-relay", Kind: "hold", Text: "hold #48889", PRs: []string{"48889"}, To: []string{"root"}})
	post(t, client.Draft{Drive: "d", Lane: "lane-7", Kind: "note", Text: "a broadcast of a kind root does not follow"})
	post(t, client.Draft{Drive: "d", Lane: "lane-7", Kind: "ask", Text: "addressed to another lane", To: []string{"lane-8"}})
	report := post(t, client.Draft{Drive: "d", Lane: "supervisor", Kind: "report", Text: "hourly waste", To: []string{"root"}})
	service := post(t, client.Draft{Drive: "d", Lane: "supervisor", Kind: "waste.rank", Text: "a kind cci has no name for", To: []string{"root"}})
	incident := post(t, client.Draft{Drive: "d", Lane: "lane-8", Kind: "incident", Text: "api down"})
	want := []int64{hold.Seq, report.Seq, service.Seq, incident.Seq}

	ctx, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	var got []int64
	err := client.Follow(ctx, client.Watch{Drive: "d", Reader: "root", After: start.Seq, Kinds: []string{"incident"}}, func(r client.Record) error {
		got = append(got, r.Seq)
		if len(got) == len(want) {
			stop()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("Follow delivered %v, want %v", got, want)
	}
}

func TestKindsNamesEveryKindALanePostsButNoneOnlyCciWrites(t *testing.T) {
	got := client.Kinds()
	if !slices.Contains(got, "hold") || !slices.Contains(got, "report") || slices.Contains(got, "digest") {
		t.Fatalf("Kinds = %v", got)
	}
}

func TestOpenReadsOnlyTheHoldsNothingHasLifted(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	lifted := post(t, client.Draft{Drive: "d", Lane: "root", Kind: "hold", Text: "hold #46038", PRs: []string{"46038"}})
	held := post(t, client.Draft{Drive: "d", Lane: "root", Kind: "hold", Text: "hold #45821", PRs: []string{"org/repo#45821"}})
	post(t, client.Draft{Drive: "d", Lane: "root", Kind: "lift", Text: "lift #46038", Re: lifted.Seq, PRs: []string{"46038"}})
	post(t, client.Draft{Drive: "d", Lane: "lane-7", Kind: "ask", Text: "an open ask is no hold", To: []string{"root"}})

	open, err := client.Open(context.Background(), "d", "hold")
	if err != nil {
		t.Fatal(err)
	}
	if len(open) != 1 || open[0].Seq != held.Seq || open[0].Refs.PRs[0].Number != 45821 {
		t.Fatalf("Open = %+v, want the one held record", open)
	}
}

func TestATypedKindIsCheckedWithoutStoringAndItsEntryNamesItsFields(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	schema, ok := client.TypedSchema("grade")
	if !ok || strings.Join(schema.Names(), ",") != "pr,head,grader,verdict" || strings.Join(schema.Identity, ",") != "pr,head,grader" {
		t.Fatalf("TypedSchema = %+v, %v", schema, ok)
	}
	if _, ok := client.TypedSchema("evidence"); ok {
		t.Fatal("evidence is no typed kind")
	}
	if kinds := client.TypedKinds(); len(kinds) != 12 || kinds[0] != "head" {
		t.Fatalf("TypedKinds = %v", kinds)
	}
	err := client.CheckBody("grade", json.RawMessage(`{"pr":[1,2],"head":"`+head+`","grader":"finder","verdict":"clear"}`), nil)
	if err == nil || !strings.HasPrefix(err.Error(), "a grade names one PR") {
		t.Fatalf("CheckBody = %v", err)
	}
	records, err := client.Tail(context.Background(), client.Query{Drive: "d"})
	if err != nil || len(records) != 0 {
		t.Fatalf("Tail = %+v, %v", records, err)
	}
}

func TestAnItemPostedToALaneByNameIsStored(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())

	stored := post(t, client.Draft{
		Drive: "d", Lane: "supervisor", Kind: "lr.item", Text: "item 3", To: []string{"lane-7"}, ID: "lr.item/3",
		Body: json.RawMessage(`{"item":3,"action":"verdict","pr":45470,"head":"` + head + `","mover":"lane","deadline":"2026-10-31T06:20:00Z"}`),
	})

	if len(stored.To) != 1 || stored.To[0] != "lane-7" {
		t.Fatalf("stored = %+v", stored)
	}
}

func TestAPostCarriesItsAttrsLaneRefsAndURL(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())

	stored := post(t, client.Draft{Drive: "d", Lane: "supervisor", Kind: "report", Text: "RELAUNCHED a-1 as a-2", To: []string{"root"}, LaneRefs: []string{"a-2"}, Attrs: map[string]string{"audience": "owner"}, URL: "https://example.com/x"})

	if stored.Text != "RELAUNCHED a-1 as a-2" || len(stored.Refs.Lanes) != 1 || stored.Refs.Lanes[0] != "a-2" || stored.Fields.Attrs["audience"] != "owner" || stored.Refs.URL != "https://example.com/x" {
		t.Fatalf("stored = %+v", stored)
	}
}

func TestAStandDownCarriesItsSuccessor(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())

	stored := post(t, client.Draft{Drive: "d", Lane: "supervisor", Kind: "stand-down", Text: "a-1 stood down", To: []string{"a-1"}, Successor: "a-2"})

	if stored.Fields.Attrs["successor"] != "a-2" {
		t.Fatalf("stored = %+v", stored)
	}
}

func TestResolveReachesAStemsNewestLaneAndAStoodDownLanesSuccessor(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	ctx := context.Background()
	post(t, client.Draft{Drive: "d", Lane: "supervisor", Kind: "stand-down", Text: "desk-1 stood down", To: []string{"desk-1"}, Successor: "desk-2"})
	for _, lane := range []string{"desk-2", "desk-3"} {
		post(t, client.Draft{Drive: "d", Lane: lane, Kind: "state", Text: lane + " up"})
	}

	for name, want := range map[string]string{"desk": "desk-3", "desk-2": "desk-2", "desk-1": "desk-2", "other": ""} {
		reach, err := client.Resolve(ctx, "d", name)
		if err != nil || reach.Lane != want {
			t.Fatalf("Resolve(%s) = %+v, %v; want lane %q", name, reach, err, want)
		}
	}
	reach, err := client.Resolve(ctx, "d", "desk")
	if err != nil || !slices.Equal(reach.Holders, []string{"desk-3", "desk-2"}) || reach.Stem != "desk" {
		t.Fatalf("Resolve(desk) = %+v, %v; want desk-3 then desk-2 holding desk", reach, err)
	}
}

func TestGenerationsListsEveryLaneEverRegisteredUnderAStem(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	post(t, client.Draft{Drive: "d", Lane: "supervisor", Kind: "stand-down", Text: "desk-4 stood down", To: []string{"desk-4"}, Successor: "desk-5"})
	for _, lane := range []string{"desk", "desk-tools-1"} {
		post(t, client.Draft{Drive: "d", Lane: lane, Kind: "state", Text: lane + " up"})
	}
	post(t, client.Draft{Drive: "other", Lane: "desk-9", Kind: "state", Text: "desk-9 up"})

	lanes, err := client.Generations(context.Background(), "d", "desk")

	if err != nil || !slices.Equal(lanes, []string{"desk", "desk-4", "desk-5"}) {
		t.Fatalf("Generations(desk) = %v, %v; want desk, the stood-down desk-4 and its successor desk-5", lanes, err)
	}
}

func TestWhoReadsTheSessionsBoundToADriveAndNoOthers(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	ctx := context.Background()
	st, err := cli.OpenStore(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	now := time.Now()
	for session, bound := range map[string][2]string{"aaaa1111": {"d", "desk-1"}, "bbbb2222": {"other", "desk-2"}} {
		if err := st.BindLane(ctx, store.PeerKey{Session: session}, bound[0], bound[1], now); err != nil {
			t.Fatal(err)
		}
		if _, err := st.Announce(ctx, store.Peer{Session: session}); err != nil {
			t.Fatal(err)
		}
	}

	rows, err := client.Who(ctx, "d")
	if err != nil {
		t.Fatal(err)
	}

	if len(rows) != 1 || rows[0].Lane != "desk-1" || rows[0].Session != "aaaa1111" || rows[0].StoodDown {
		t.Fatalf("rows = %+v", rows)
	}
}

func TestASeqFromTailNamesAnotherNodesRecordOnANodeThatIsNoOwner(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	ctx := context.Background()
	if err := relay.SaveConfig(home, relay.Config{Mesh: "machine", URL: "https://relay.invalid", Tier: "service"}); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	row := store.Row{Origin: "bbbbbbbbbbbbbbbb", OriginSeq: 100, Drive: "d", Lane: "lane-a", Kind: kinds.Ask, At: time.Now(), Text: "read a build", To: []string{"desk"}, Source: "post"}
	if _, err := st.Apply(ctx, []store.Row{row}, nil); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	asks, err := client.Tail(ctx, client.Query{Drive: "d", Kinds: []string{"ask"}})
	if err != nil || len(asks) != 1 {
		t.Fatalf("asks = %+v, %v", asks, err)
	}

	answer := post(t, client.Draft{Drive: "d", Lane: "desk", Kind: "answer", Text: "read", To: []string{"lane-a"}, Re: asks[0].Seq, Resolves: asks[0].Seq})

	if answer.Re != asks[0].Seq || answer.Resolves != asks[0].Seq {
		t.Fatalf("answer re %d resolves %d, want %d", answer.Re, answer.Resolves, asks[0].Seq)
	}
	open, err := client.Open(ctx, "d", "ask")
	if err != nil || len(open) != 0 {
		t.Fatalf("open = %+v, %v", open, err)
	}
}

func TestAResolvesClosesItsOpener(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	blocker := post(t, client.Draft{Drive: "d", Lane: "supervisor", Kind: "blocker", Text: "LANDED-NOT-LIVE sand", To: []string{"root"}})

	post(t, client.Draft{Drive: "d", Lane: "supervisor", Kind: "note", Text: "outlived", Resolves: blocker.Seq})

	open, err := client.Open(context.Background(), "d", "blocker")
	if err != nil {
		t.Fatal(err)
	}
	if len(open) != 0 {
		t.Fatalf("open = %+v", open)
	}
}

func TestATailKeepsOnlyTheNamedPullRequestAndLanes(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	wanted := post(t, client.Draft{Drive: "d", Lane: "finder-desk", Kind: "review", Text: "finder: ok " + head, PRs: []string{"org/repo#45821"}})
	post(t, client.Draft{Drive: "d", Lane: "finder-desk", Kind: "review", Text: "finder: ok " + head, PRs: []string{"org/repo#45822"}})
	post(t, client.Draft{Drive: "d", Lane: "another-desk", Kind: "review", Text: "finder: ok " + head, PRs: []string{"org/repo#45821"}})

	records, err := client.Tail(context.Background(), client.Query{Drive: "d", PR: "org/repo#45821", Lanes: []string{"finder-desk"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Seq != wanted.Seq {
		t.Fatalf("Tail = %+v, want only seq %d", records, wanted.Seq)
	}
	if _, err := client.Tail(context.Background(), client.Query{Drive: "d", PR: "not a pull request"}); err == nil {
		t.Fatal("Tail took a PR that does not parse")
	}
}

func TestAmongReadsTheNamedRecordsOfOneDriveOldestFirst(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	first := post(t, client.Draft{Drive: "d", Lane: "root", Kind: "note", Text: "first"})
	post(t, client.Draft{Drive: "d", Lane: "root", Kind: "note", Text: "unnamed"})
	second := post(t, client.Draft{Drive: "d", Lane: "root", Kind: "note", Text: "second"})
	elsewhere := post(t, client.Draft{Drive: "other", Lane: "root", Kind: "note", Text: "elsewhere"})

	records, err := client.Among(context.Background(), "d", []int64{second.Seq, first.Seq, elsewhere.Seq, second.Seq + 1000})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].Seq != first.Seq || records[1].Seq != second.Seq {
		t.Fatalf("Among = %+v, want seqs %d and %d", records, first.Seq, second.Seq)
	}
}

func TestAPostCarriesTheFileHoldingItsWholeBody(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	whole := filepath.Join(t.TempDir(), "record.md")
	if err := os.WriteFile(whole, []byte("finder: ok\n\nthe whole reply\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	stored := post(t, client.Draft{Drive: "d", Lane: "finder-desk", Kind: "review", Text: "finder: ok " + head, Path: whole})

	if stored.Refs.Path != whole {
		t.Fatalf("Refs.Path = %q, want %q", stored.Refs.Path, whole)
	}
}

func TestWholeTextJoinsASpilledTextToItsFileAndNeverClipsOnAFailedRead(t *testing.T) {
	dir := t.TempDir()
	whole := filepath.Join(dir, "whole.txt")
	if err := os.WriteFile(whole, []byte("a long finding that spills past the cap\nand its second line\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name   string
		record client.Record
		want   string
	}{
		{"no file", client.Record{Text: "short"}, "short"},
		{"spilled", client.Record{Text: "a long finding…", Refs: store.Refs{Path: whole}}, "a long finding that spills past the cap\nand its second line\n"},
		{"file missing", client.Record{Text: "short", Refs: store.Refs{Path: filepath.Join(dir, "gone.txt")}}, "short\n" + filepath.Join(dir, "gone.txt") + " is not on this machine"},
	} {
		got, err := client.WholeText(tt.record)
		if err != nil || got != tt.want {
			t.Errorf("%s: WholeText = %q, %v; want %q", tt.name, got, err, tt.want)
		}
	}
	if got, err := client.WholeText(client.Record{Text: "clipped…", Refs: store.Refs{Path: dir}}); err == nil {
		t.Errorf("a path that cannot be read returned %q, nil; want an error", got)
	}
}
