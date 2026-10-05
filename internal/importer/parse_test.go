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
		{"hsbc-bring-up", kinds.Note, "READY 18:13Z #29952 green + approved at 264975f3f5", 29952, "18:13Z", ""},
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
