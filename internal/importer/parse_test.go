package importer_test

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/importer"
	"github.com/yasyf/cc-inbox/internal/kinds"
)

func TestParseShapes(t *testing.T) {
	f, err := os.Open("testdata/deploy-go.md")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	entries, err := importer.Parse(f, "deploy-go")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		lane  string
		kind  kinds.Kind
		text  string
		pr    int
		clock string
		to    string
	}{
		{"root", kinds.Owner, `owner 10:2x PM, verbatim: "rethink that entirely"`, 0, "22:24", ""},
		{"hook-diet", kinds.Release, "#30425 cc-slack 0.1.1791177647 built+linked", 30425, "22:24", ""},
		{"reco-e2e-walg", kinds.Mechanism, "reco-e2e's postgres still targets the old bucket.", 0, "22:24", ""},
		{"merge-walker", kinds.Applied, "root GO 9:46 item 5 (#30374 K aliases)", 30374, "22:25", ""},
		{"landing-sweep-15", kinds.State, "#30427 (ship-2147-fix): runner queue, first", 30427, "22:18", ""},
		{"applied-from-pulumi", kinds.Opened, "#30431 release: read published versions from Pulumi state", 30431, "22:23", ""},
		{"root", kinds.Go, "R1081 (3:36 PM PT) orca-desk: launch exec-pending-333-fix NOW sol xhigh brief=/x/fix-brief.md", 0, "15:36", "orca-desk"},
		{"hsbc-sanddb-2147-fix", kinds.Report, "msg_27ab68e4bfed hsbc-sanddb-2147-fix: worker_done succeeded dispatch=ctx_65c99a29432f", 0, "22:16", ""},
		{"hsbc-bring-up", kinds.Ready, "18:13Z #29952 green + approved at 264975f3f5", 29952, "18:13Z", ""},
		{"root", kinds.Note, "P65 ALL GREEN does not wait on AIG artifact-storage artifact-storage is an AIG-proof setting, not a deploy target; AIG runs no sanddb pods. Every deploy target row is in.", 0, "12:28Z", ""},
		{"deploy-go", kinds.Note, "a stray line with no shape", 0, "", ""},
	}
	if len(entries) != len(tests) {
		for _, e := range entries {
			t.Logf("%+v", e)
		}
		t.Fatalf("parsed %d entries, want %d", len(entries), len(tests))
	}
	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			e := entries[i]
			if e.Lane != tt.lane || e.Kind != tt.kind || e.Text != tt.text || firstPR(e) != tt.pr {
				t.Errorf("entry = lane %q kind %q pr %d text %q\nwant lane %q kind %q pr %d text %q", e.Lane, e.Kind, firstPR(e), e.Text, tt.lane, tt.kind, tt.pr, tt.text)
			}
			if got := clockString(e.Clock); got != tt.clock {
				t.Errorf("clock = %q, want %q", got, tt.clock)
			}
			if got := fmt.Sprint(e.To); tt.to != "" && got != "["+tt.to+"]" {
				t.Errorf("to = %s, want [%s]", got, tt.to)
			}
		})
	}
}

func firstPR(e importer.Entry) int {
	if len(e.PRs) == 0 {
		return 0
	}
	return e.PRs[0]
}

func TestParseExtractsRefs(t *testing.T) {
	entries, err := importer.Parse(strings.NewReader("APPLIED merge-walker (10:25 PM PT) dns/tnt-usw2-26qmqm1 and api/plat-usw2-prod at 3c5c016fbd, #30374 and #30375, release https://buildkite.com/forge/release/builds/1253 . infra/lib is a path\n"), "deploy-go")
	if err != nil {
		t.Fatal(err)
	}
	e := entries[0]
	if fmt.Sprint(e.Stacks) != "[dns/tnt-usw2-26qmqm1 api/plat-usw2-prod]" || fmt.Sprint(e.PRs) != "[30374 30375]" || fmt.Sprint(e.Builds) != "[https://buildkite.com/forge/release/builds/1253]" {
		t.Fatalf("refs = stacks %v prs %v builds %v", e.Stacks, e.PRs, e.Builds)
	}
}

func clockString(c *importer.Clock) string {
	if c == nil {
		return ""
	}
	s := fmt.Sprintf("%02d:%02d", c.Hour, c.Minute)
	if c.UTC {
		s += "Z"
	}
	return s
}

func TestDateWalksBackAcrossMidnight(t *testing.T) {
	loc := time.Local
	end := time.Date(2026, 10, 4, 0, 30, 0, 0, loc)
	entries := []importer.Entry{
		{Clock: &importer.Clock{Hour: 23, Minute: 50}},
		{},
		{Clock: &importer.Clock{Hour: 0, Minute: 10}},
	}
	got := importer.Date(entries, end)
	want := []time.Time{
		time.Date(2026, 10, 3, 23, 50, 0, 0, loc),
		time.Date(2026, 10, 4, 0, 10, 0, 0, loc),
		time.Date(2026, 10, 4, 0, 10, 0, 0, loc),
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Errorf("Date()[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestParseNamesTheWritingLane(t *testing.T) {
	tests := []struct {
		line, lane string
		kind       kinds.Kind
		clock      string
		text       string
	}{
		{"DONE (1:44 AM PT) tooling-ccx-5: defect 14 fixed. #30533 (landed b5a74c0c) makes stack-enqueue name a parent", "tooling-ccx-5", kinds.Done, "01:44", "defect 14 fixed. #30533 (landed b5a74c0c) makes stack-enqueue name a parent"},
		{"MECHANISM test-slow-41463 (1:45 AM PT) IaC check release-pr-check: #30518's env-spec yaml edits reach all 289 stacks", "test-slow-41463", kinds.Mechanism, "01:45", "IaC check release-pr-check: #30518's env-spec yaml edits reach all 289 stacks"},
		{"OPENED release-simplify-3 (1:42 AM PT) #30541 6fa7fefac1 merge-now release: use stack grants for local state access", "release-simplify-3", kinds.Opened, "01:42", "#30541 6fa7fefac1 merge-now release: use stack grants for local state access"},
		{"READY (9:27 PM PT) tenant-parity-retro-2: #30378 43b022becd J green + approved, landable", "tenant-parity-retro-2", kinds.Ready, "21:27", "#30378 43b022becd J green + approved, landable"},
		{"R (1:06 AM PT) hsbc-routing-revert FIX-LIVE: plat api rollouts restarted 1:00-1:02 AM", "hsbc-routing-revert", kinds.FixLive, "01:06", "R plat api rollouts restarted 1:00-1:02 AM"},
		{"OPENED (9:5x PM PT) platy-ux-promises-3: #30414 release: Fix Platy release messages", "platy-ux-promises-3", kinds.Opened, "21:50", "#30414 release: Fix Platy release messages"},
		{"OWNER (1:33 AM PT) tailscale: live policy file delivered", "root", kinds.Owner, "01:33", "tailscale: live policy file delivered"},
		{"GO (root, 1:33 AM PT) alerts-api-fix: ship #30541", "root", kinds.Go, "01:33", "alerts-api-fix: ship #30541"},
		{"R (1:52 AM PT) hsbc-routing-revert STOOD DOWN: #30525 landed 01f0eb41bc", "hsbc-routing-revert", kinds.Note, "01:52", "R STOOD DOWN: #30525 landed 01f0eb41bc"},
		{"1:43 AM PT tailnet-policy-29377: MECHANISM #29377 row now owns the whole tailnet policy", "tailnet-policy-29377", kinds.Mechanism, "01:43", "#29377 row now owns the whole tailnet policy"},
		{"1:11 AM PT FIX-LIVE alerts-api-0024-fix: root hand promote 12:58", "alerts-api-0024-fix", kinds.FixLive, "01:11", "root hand promote 12:58"},
		{"1:20 AM PT sandsql-handoff-0027 LANDED #30524 c53452e03c45", "sandsql-handoff-0027", kinds.Landed, "01:20", "#30524 c53452e03c45"},
		{"12:28 AM PT EVIDENCE alerts-api-0024-evidence -> alerts-api-0024-fix: #1351 applies passed", "alerts-api-0024-evidence", kinds.Evidence, "00:28", "-> alerts-api-0024-fix: #1351 applies passed"},
		{"OPENED 9:49 PM PT rules-nudge-hook: yasyf/captain-hook#306 general: rules_nudge", "rules-nudge-hook", kinds.Opened, "21:49", "yasyf/captain-hook#306 general: rules_nudge"},
		{"1:33 AM PT OWNER tailscale: live policy file delivered", "root", kinds.Owner, "01:33", "tailscale: live policy file delivered"},
	}
	for _, tt := range tests {
		t.Run(tt.lane, func(t *testing.T) {
			e := parseOne(t, tt.line)
			if e.Lane != tt.lane || e.Kind != tt.kind || e.Text != tt.text || clockString(e.Clock) != tt.clock {
				t.Errorf("entry = lane %q kind %q clock %q text %q\nwant lane %q kind %q clock %q text %q", e.Lane, e.Kind, clockString(e.Clock), e.Text, tt.lane, tt.kind, tt.clock, tt.text)
			}
		})
	}
}

func parseOne(t *testing.T, line string) importer.Entry {
	t.Helper()
	entries, err := importer.Parse(strings.NewReader(line+"\n"), "deploy-go")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("parsed %d entries from %q, want 1", len(entries), line)
	}
	return entries[0]
}

func TestDateKeepsOutOfOrderStampsOnTheirDay(t *testing.T) {
	loc := time.Local
	end := time.Date(2026, 10, 5, 1, 50, 0, 0, loc)
	entries := []importer.Entry{
		{Clock: &importer.Clock{Hour: 1, Minute: 0}},
		{Clock: &importer.Clock{Hour: 1, Minute: 46}},
		{Clock: &importer.Clock{Hour: 1, Minute: 20}},
		{Clock: &importer.Clock{Hour: 1, Minute: 45}},
	}
	got := importer.Date(entries, end)
	for i, want := range []time.Time{
		time.Date(2026, 10, 5, 1, 0, 0, 0, loc),
		time.Date(2026, 10, 5, 1, 46, 0, 0, loc),
		time.Date(2026, 10, 5, 1, 20, 0, 0, loc),
		time.Date(2026, 10, 5, 1, 45, 0, 0, loc),
	} {
		if !got[i].Equal(want) {
			t.Errorf("Date()[%d] = %v, want %v", i, got[i], want)
		}
	}
}

func TestParseFilesRunnerEventsUnderTheRunner(t *testing.T) {
	tests := []struct {
		line, lane, topic string
		kind              kinds.Kind
	}{
		{"01:38 STALE-MAIL stale:msg_d665a83f50f6 alerts-api-0024-fix: msg_d665a83f50f6 unread by a completed dispatch", "runner", "alerts-api-0024-fix", kinds.Note},
		{"01:41 RECLAIM reclaim:ctx_c575b3459b82 runner: 1 settled dispatch(es) still hold their terminal: sandsql-handoff-0027-fix=ctx_c575b3459b82:term_47717eed", "runner", "sandsql-handoff-0027-fix", kinds.Note},
		{"00:34 LAUNCHED R1089 hsbc-routing-revert: dispatch ctx_9ac9b5ebe8a0 terminal term_6ba9da32", "runner", "hsbc-routing-revert", kinds.Note},
		{"01:33 OUTCOME msg_ed92a2d3755c alerts-api-0024-fix: worker_done succeeded dispatch=ctx_699ec468f4a1", "alerts-api-0024-fix", "", kinds.Report},
		{"17:47 QUEUED sprawl-retro: #29607 enqueued via stack-enqueue", "sprawl-retro", "", kinds.Note},
	}
	for _, tt := range tests {
		t.Run(tt.lane+"/"+tt.topic, func(t *testing.T) {
			e := parseOne(t, tt.line)
			if e.Lane != tt.lane || e.Topic != tt.topic || e.Kind != tt.kind {
				t.Errorf("entry = lane %q topic %q kind %q, want lane %q topic %q kind %q", e.Lane, e.Topic, e.Kind, tt.lane, tt.topic, tt.kind)
			}
		})
	}
}

func TestParseMapsEveryWatchedLeadTokenToAKind(t *testing.T) {
	watched := "FIX-LIVE|MECHANISM|NOT-OURS|STOPPED|BLOCKED|REFUSE|FAIL|APPLIED|RELEASE|RECOVERED|READY|HOLD|DECIDE|INCIDENT|STATE|MATRIX|DELETE-LIST|STAND-DOWN|LIFT|SKEW|NOT-LIVE|CORRECTION|URGENT|DONE|HANDOFF|DEFECT|DESIGN|DUPLICATE|SERVING|REPORT|RETRO|POSTED"
	for _, word := range strings.Split(watched, "|") {
		t.Run(word, func(t *testing.T) {
			e := parseOne(t, word+" merge-walker (2:04 AM PT) #30541 receiver")
			want, ok := kinds.Lookup(word)
			if !ok || e.Kind != want || e.Kind == kinds.Note || e.Lane != "merge-walker" {
				t.Errorf("%s imports as kind %q lane %q, want its own kind", word, e.Kind, e.Lane)
			}
			if _, err := kinds.Parse(string(e.Kind)); err != nil {
				t.Errorf("--kind %s: %v", e.Kind, err)
			}
		})
	}
}

func TestDateNeverPlacesAStampAfterTheFileWasWritten(t *testing.T) {
	loc := time.Local
	end := time.Date(2026, 10, 5, 10, 10, 0, 0, loc)
	entries := []importer.Entry{
		{Clock: &importer.Clock{Hour: 22, Minute: 7}},
		{Clock: &importer.Clock{Hour: 10, Minute: 12}},
	}
	got := importer.Date(entries, end)
	for i, want := range []time.Time{
		time.Date(2026, 10, 4, 22, 7, 0, 0, loc),
		time.Date(2026, 10, 5, 10, 12, 0, 0, loc),
	} {
		if !got[i].Equal(want) {
			t.Errorf("Date()[%d] = %v, want %v", i, got[i], want)
		}
	}
}

func TestParseClockResolvesToTheLatestPastInstant(t *testing.T) {
	now := time.Date(2026, 10, 5, 12, 40, 0, 0, time.Local)
	tests := []struct {
		in   string
		want time.Time
	}{
		{"09:00", time.Date(2026, 10, 5, 9, 0, 0, 0, time.Local)},
		{"9:00 AM", time.Date(2026, 10, 5, 9, 0, 0, 0, time.Local)},
		{"12:0x PM", time.Date(2026, 10, 5, 12, 0, 0, 0, time.Local)},
		{"11:30pm", time.Date(2026, 10, 4, 23, 30, 0, 0, time.Local)},
		{"13:15", time.Date(2026, 10, 4, 13, 15, 0, 0, time.Local)},
		{"12:40 PT", now},
	}
	for _, tt := range tests {
		c := importer.ParseClock(tt.in)
		if c == nil {
			t.Errorf("ParseClock(%q) = nil", tt.in)
			continue
		}
		if got := c.Latest(now); !got.Equal(tt.want) {
			t.Errorf("ParseClock(%q).Latest = %v, want %v", tt.in, got, tt.want)
		}
	}
	nowUTC := time.Date(2026, 10, 5, 19, 40, 0, 0, time.UTC)
	if got, want := importer.ParseClock("19:00Z").Latest(nowUTC), time.Date(2026, 10, 5, 19, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("ParseClock(19:00Z).Latest = %v, want %v", got, want)
	}
	for _, in := range []string{"2h", "#123", "25:00", "9:00 tomorrow", "2026-10-05T09:00:00Z"} {
		if c := importer.ParseClock(in); c != nil {
			t.Errorf("ParseClock(%q) = %+v, want nil", in, c)
		}
	}
}
