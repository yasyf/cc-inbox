package cli_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/node"
	"github.com/yasyf/cc-inbox/internal/relay"
	"github.com/yasyf/cc-inbox/internal/relay/relaytest"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

type replica struct {
	mac, sprite         *store.Store
	macNode, spriteNode *relay.Node
}

func joinedNode(t *testing.T, srv *relaytest.Server, tier node.Tier) (*store.Store, *relay.Node) {
	t.Helper()
	home := t.TempDir()
	st, err := store.Open(context.Background(), home)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	id, err := node.Load(home)
	if err != nil {
		t.Fatal(err)
	}
	srv.AddMember(id, string(tier))
	n, err := relay.Open(home, st, id, relay.Config{URL: srv.URL, Mesh: relay.DefaultMesh, Tier: string(tier)})
	if err != nil {
		t.Fatal(err)
	}
	return st, n
}

func pinned(t *testing.T, on *store.Store, other *store.Store, tier node.Tier) {
	t.Helper()
	if err := on.PutMember(context.Background(), store.Member{Node: other.Self(), PublicKey: other.Node().PublicKey(), Tier: tier, Via: "relay", By: "test", At: time.Now()}); err != nil {
		t.Fatal(err)
	}
}

func (r replica) push(t *testing.T) {
	t.Helper()
	configs, err := relay.Configs(r.mac.HomeDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.macNode.Push(context.Background(), configs); err != nil {
		t.Fatal(err)
	}
}

func newReplica(t *testing.T, before, after []store.Record) replica {
	t.Helper()
	ctx := context.Background()
	srv := relaytest.NewServer(t)
	mac, macNode := joinedNode(t, srv, node.TierOwner)
	if err := macNode.Join(ctx); err != nil {
		t.Fatal(err)
	}
	r := replica{mac: mac, macNode: macNode}
	for _, rec := range before {
		testutil.Post(t, mac, rec)
	}
	r.push(t)
	sprite, spriteNode := joinedNode(t, srv, node.TierPaired)
	r.sprite, r.spriteNode = sprite, spriteNode
	pinned(t, sprite, mac, node.TierOwner)
	pinned(t, mac, sprite, node.TierPaired)
	if err := spriteNode.Join(ctx); err != nil {
		t.Fatal(err)
	}
	if err := macNode.Share(ctx); err != nil {
		t.Fatal(err)
	}
	for _, rec := range after {
		testutil.Post(t, mac, rec)
	}
	r.push(t)
	if err := spriteNode.RefreshKeys(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := spriteNode.Pull(ctx, 0, nil); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CCI_HOME", sprite.HomeDir())
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	t.Setenv("CODEX_THREAD_ID", "")
	t.Setenv("CLAUDE_LONG_RUNNING_DRIVE", "d")
	return r
}

func held(t *testing.T, st *store.Store) int {
	t.Helper()
	n, err := st.Count(context.Background(), store.Filter{Drive: "d"})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestShowOnAReplicaReadsARecordOlderThanItFromTheHub(t *testing.T) {
	body := filepath.Join(t.TempDir(), "brief.md")
	if err := os.WriteFile(body, []byte("BRIEF HEAD\n"+strings.Repeat("the replica reads the hub ", 30)+"BRIEF END\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	r := newReplica(t,
		[]store.Record{{Lane: "root", Kind: kinds.Decision, Text: "the brief", Refs: store.Refs{Path: body}}, {Lane: "root", Kind: kinds.Decision, Text: "older than the sprite"}},
		[]store.Record{{Lane: "root", Kind: kinds.Decision, Text: "after the join"}})

	out, err := run(t, "show", "#2", "1")
	if err != nil {
		t.Fatal(err)
	}
	second, first := strings.Index(out, "2 decision root older than the sprite"), strings.Index(out, "1 decision root + the brief")
	if second < 0 || first < second || !strings.Contains(out, "BRIEF END") {
		t.Fatalf("show #2 1 on the replica = %q; want the hub's #2, then #1 with its body through BRIEF END", out)
	}
	if n := held(t, r.sprite); n != 1 {
		t.Errorf("replica holds %d records after show, want 1: a record read from the hub is not stored", n)
	}
}

func TestTailOnAReplicaFromAnOlderSeqReadsTheWholeRangeInOrder(t *testing.T) {
	newReplica(t,
		[]store.Record{
			{Lane: "root", Kind: kinds.Decision, Text: "start here"},
			{Lane: "root", Kind: kinds.Decision, Text: "hub one"},
			{Lane: "desk", Kind: kinds.Decision, Text: "hub desk"},
			{Lane: "root", Kind: kinds.Decision, Text: "hub two"},
		},
		[]store.Record{{Lane: "root", Kind: kinds.Decision, Text: "local one"}, {Lane: "root", Kind: kinds.Decision, Text: "local two"}})

	for _, args := range [][]string{{"--oldest"}, {}, {"--json"}} {
		out, err := run(t, append([]string{"tail", "--drive", "d", "--since", "#1", "--budget", "100000"}, args...)...)
		if err != nil {
			t.Fatal(err)
		}
		at := -1
		for _, text := range []string{"hub one", "hub desk", "hub two", "local one", "local two"} {
			i := strings.Index(out, text)
			if i <= at {
				t.Fatalf("tail --since #1 %v on the replica = %q; want hub one, hub desk, hub two, local one, local two in order", args, out)
			}
			at = i
		}
		if strings.Contains(out, "start here") {
			t.Errorf("tail --since #1 %v = %q; want the records after #1, not #1", args, out)
		}
	}
	out, err := run(t, "tail", "--drive", "d", "--since", "#1", "--lane", "root", "--oldest")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out, "hub desk") || !strings.Contains(out, "4 decision root hub two") || !strings.Contains(out, "local two") {
		t.Errorf("tail --since #1 --lane root = %q; want the hub's root records and the replica's, without the desk's", out)
	}
	out, err = run(t, "tail", "--drive", "d", "--since", "#1", "--oldest", "--budget", "1")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "hub one") || strings.Contains(out, "hub desk") || !strings.Contains(out, "4 more; resume with --since #2") {
		t.Errorf("tail --since #1 --oldest --budget 1 = %q; want hub one, then 4 more resuming after #2", out)
	}
}

func TestShowAndTailOnAReplicaKeepTheErrorForANumberNoHubHolds(t *testing.T) {
	newReplica(t, []store.Record{{Lane: "root", Kind: kinds.Decision, Text: "older"}}, []store.Record{{Lane: "root", Kind: kinds.Decision, Text: "newer"}})

	for _, args := range [][]string{{"show", "#99"}, {"tail", "--drive", "d", "--since", "#99"}, {"grep", ".", "--drive", "d", "--since", "#99"}} {
		if _, err := run(t, args...); err == nil || !strings.Contains(err.Error(), "#99: no record has this number") {
			t.Errorf("%v on the replica = %v; want #99: no record has this number", args, err)
		}
	}
}

func routed(t *testing.T, st *store.Store, routes ...string) {
	t.Helper()
	cfg, _, err := relay.LoadConfig(st.HomeDir(), relay.DefaultMesh)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Routes = routes
	if err := relay.SaveConfig(st.HomeDir(), cfg); err != nil {
		t.Fatal(err)
	}
}

func TestBareShowOnAReplicaReadsThroughTheDriveItsRelayCarries(t *testing.T) {
	r := newReplica(t, []store.Record{{Lane: "root", Kind: kinds.Decision, Text: "older than the sprite"}}, []store.Record{{Lane: "root", Kind: kinds.Decision, Text: "newer"}})
	t.Setenv("CLAUDE_LONG_RUNNING_DRIVE", "")
	routed(t, r.sprite, "d")

	if out, err := run(t, "show", "#1"); err != nil || !strings.Contains(out, "older than the sprite") {
		t.Fatalf("bare show #1 on a replica routed to d = %q, %v; want the hub's #1", out, err)
	}
	routed(t, r.sprite, "d", "e")
	if _, err := run(t, "show", "#1"); err == nil || !strings.Contains(err.Error(), "carries drives d, e; pass --drive") {
		t.Errorf("bare show #1 on a replica routed to d and e = %v; want it to name both drives", err)
	}
}

func replies(t *testing.T, st *store.Store) map[string]string {
	t.Helper()
	records, err := st.Query(context.Background(), store.Filter{Drive: "d"})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, rec := range records {
		got[rec.Text] = rec.ReNum
	}
	return got
}

func TestAReplyToARecordOlderThanTheReplicaArrivesWithItsTarget(t *testing.T) {
	r := newReplica(t,
		[]store.Record{{Lane: "root", Kind: kinds.Decide, Text: "ship the relay?"}},
		[]store.Record{{Lane: "root", Kind: kinds.Go, Text: "go", Re: 1}})

	got := replies(t, r.sprite)
	if _, ok := got["ship the relay?"]; !ok || got["go"] != "#1" {
		t.Fatalf("replica holds %v; want the go answering #1 and #1 itself, fetched from the hub", got)
	}
}

var keptLocalThread = []store.Record{
	{Lane: "supervisor", Kind: kinds.Kind("lr.escalation"), Text: "green and ungraded", To: []string{"root"}, Body: []byte(`{"subject":"Forge-AI/monorepo#50699","cause":"green and ungraded"}`)},
	{Lane: "root", Kind: kinds.Decide, Text: "grade #50699 now", Re: 1},
	{Lane: "fidelity-desk", Kind: kinds.Review, Text: "fidelity: ok 72ff491", Topic: "50699", Re: 2},
}

func TestAReplyToARecordTheOwnerKeepsLocalIsStoredWithItsRepliesUnderIt(t *testing.T) {
	r := newReplica(t, nil, keptLocalThread)

	got := replies(t, r.sprite)
	if len(got) != 2 || got["grade #50699 now"] != "" || got["fidelity: ok 72ff491"] != "#2" {
		t.Fatalf("replica holds %v; want the decide answering a record no relay carries, and the review answering #2", got)
	}
}

func TestAReplyToTheEndOfAnOlderThreadOnAKeptLocalRecordArrivesWithTheWholeThread(t *testing.T) {
	r := newReplica(t, keptLocalThread, []store.Record{{Lane: "desk-runner", Kind: kinds.Note, Text: "folded the grade", Re: 3}})

	got := replies(t, r.sprite)
	if len(got) != 3 || got["grade #50699 now"] != "" || got["fidelity: ok 72ff491"] != "#2" || got["folded the grade"] != "#3" {
		t.Fatalf("replica holds %v; want the decide, the review answering #2, and the note answering #3, each fetched from the hub", got)
	}
}

func TestTheNextPullStoresRepliesAnEarlierVersionHeldForARecordTheOwnerKeepsLocal(t *testing.T) {
	ctx := context.Background()
	r := newReplica(t, keptLocalThread, nil)
	rows, err := r.mac.Export(ctx, r.mac.Self(), 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if a, err := r.sprite.Apply(ctx, rows, nil); err != nil || a.Buffered != 2 {
		t.Fatalf("replaying the earlier version's pull = %+v, %v; want the decide and the review held", a, err)
	}

	if _, err := r.spriteNode.Pull(ctx, 0, nil); err != nil {
		t.Fatal(err)
	}
	got := replies(t, r.sprite)
	if len(got) != 2 || got["grade #50699 now"] != "" || got["fidelity: ok 72ff491"] != "#2" {
		t.Fatalf("replica holds %v after a pull that brought nothing new; want the decide, and the review answering #2", got)
	}
}

func TestPostOnAReplicaAnswersARecordOnlyTheHubHoldsByEitherNumber(t *testing.T) {
	r := newReplica(t,
		[]store.Record{{Lane: "root", Kind: kinds.Ask, Text: "first?"}, {Lane: "root", Kind: kinds.Ask, Text: "second?", Re: 1}, {Lane: "root", Kind: kinds.Ask, Text: "third?"}},
		[]store.Record{{Lane: "root", Kind: kinds.Decision, Text: "after the join"}})

	if _, err := run(t, "post", "--lane", "desk", "--kind", "answer", "--re", "#2", "--to", "root", "--text", "by the Mac's number"); err != nil {
		t.Fatalf("post --re #2 on the replica: %v", err)
	}
	if _, err := run(t, "post", "--lane", "desk", "--kind", "answer", "--resolves", "3@"+r.mac.Self()[:8], "--to", "root", "--text", "by its origin"); err != nil {
		t.Fatalf("post --resolves 3@origin on the replica: %v", err)
	}
	records, err := r.sprite.Query(context.Background(), store.Filter{Drive: "d"})
	if err != nil {
		t.Fatal(err)
	}
	re, closes := map[string]string{}, map[string]string{}
	for _, rec := range records {
		re[rec.Text], closes[rec.Text] = rec.ReNum, strings.Join(rec.CloseNums, ",")
	}
	if re["by the Mac's number"] != "#2" || closes["by its origin"] != "#3" || re["second?"] != "#1" {
		t.Fatalf("replica holds replies %v and closes %v; want an answer to #2, one resolving #3, and #2 answering #1, each fetched from the hub", re, closes)
	}
}

func TestGrepOnAReplicaSinceARecordOnlyTheHubHoldsReadsWhatCameAfterIt(t *testing.T) {
	newReplica(t,
		[]store.Record{{Lane: "root", Kind: kinds.Decision, Text: "older than the sprite"}},
		[]store.Record{{Lane: "root", Kind: kinds.Decision, Text: "after the join"}})

	for _, since := range []string{"#1", "1"} {
		out, err := run(t, "grep", ".", "--drive", "d", "--since", since)
		if err != nil {
			t.Fatalf("grep --since %s on the replica: %v", since, err)
		}
		if !strings.Contains(out, "after the join") {
			t.Fatalf("grep --since %s on the replica = %q; want the record the replica holds after the hub's #1", since, out)
		}
	}
}

func TestTailOnAReplicaHoldingOlderHistoryStartsAfterTheSinceRecord(t *testing.T) {
	ctx := context.Background()
	r := newReplica(t,
		[]store.Record{
			{Lane: "root", Kind: kinds.Decision, Text: "seeded history"},
			{Lane: "root", Kind: kinds.Decision, Text: "since here"},
			{Lane: "root", Kind: kinds.Decision, Text: "hub only"},
		},
		[]store.Record{{Lane: "root", Kind: kinds.Decision, Text: "after the join"}})
	seeded, ok, err := r.spriteNode.Find(ctx, "d", store.Number{Seq: 1})
	if err != nil || !ok {
		t.Fatalf("find #1 on the hub = %v, %v", ok, err)
	}
	seeded.At = seeded.At.Add(-48 * time.Hour)
	if _, err := r.sprite.Backfill(ctx, []store.Row{seeded}, nil); err != nil {
		t.Fatal(err)
	}

	out, err := run(t, "tail", "--drive", "d", "--since", "#2", "--oldest", "--budget", "100000")
	if err != nil {
		t.Fatal(err)
	}
	hub, joined := strings.Index(out, "hub only"), strings.Index(out, "after the join")
	if strings.Contains(out, "seeded history") || strings.Contains(out, "since here") || hub < 0 || joined < hub {
		t.Fatalf("tail --since #2 on a replica holding #1 = %q; want hub only, then after the join, and nothing from before #2", out)
	}
}
