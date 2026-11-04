package cli_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/store"
)

func seedPrefixLanes(t *testing.T) {
	t.Helper()
	t.Setenv("CCI_HOME", t.TempDir())
	for _, lane := range []string{"desk-a", "desk-b", "deskb", "other"} {
		for i := range 3 {
			if _, err := run(t, "post", "--drive", "d", "--lane", lane, "--kind", "note", "--text", fmt.Sprintf("%s says %d", lane, i)); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestTailLanePrefixPrintsEachMatchingLanesNewestUnderItsName(t *testing.T) {
	seedPrefixLanes(t)
	out, err := run(t, "tail", "--drive", "d", "--since", "0", "--lane-prefix", "desk-", "--per-lane", "2")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"== desk-a", "desk-a says 1", "desk-a says 2", "== desk-b", "desk-b says 1", "desk-b says 2"} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
	for _, not := range []string{"says 0", "deskb", "other"} {
		if strings.Contains(out, not) {
			t.Errorf("output holds %q:\n%s", not, out)
		}
	}
	if strings.Index(out, "== desk-a") > strings.Index(out, "== desk-b") {
		t.Errorf("lanes out of order:\n%s", out)
	}
}

func TestTailLanePrefixHonoursSinceAndJSON(t *testing.T) {
	seedPrefixLanes(t)
	out, err := run(t, "tail", "--drive", "d", "--since", "8", "--lane-prefix", "desk-", "--lane-prefix", "other", "--per-lane", "1", "--json", "--budget", "100000")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	lanes := make([]string, 0, len(lines))
	for _, line := range lines {
		var record store.Record
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("%q: %v", line, err)
		}
		lanes = append(lanes, record.Lane)
	}
	if got := strings.Join(lanes, " "); got != "other" {
		t.Errorf("lanes past seq 8 = %q, want only other", got)
	}
}

func TestTailLanePrefixWithoutPerLaneReadsEveryRecordOfTheLanes(t *testing.T) {
	seedPrefixLanes(t)
	out, err := run(t, "tail", "--drive", "d", "--since", "0", "--lane-prefix", "desk-", "--budget", "100000")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(out, " says "); got != 6 {
		t.Errorf("records = %d, want six:\n%s", got, out)
	}
}

func TestTailLanePrefixNamesTheLanesTheBudgetCut(t *testing.T) {
	seedPrefixLanes(t)
	out, err := run(t, "tail", "--drive", "d", "--since", "0", "--lane-prefix", "desk", "--per-lane", "1", "--budget", "10")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "desk-a says 2") || !strings.Contains(out, "2 lanes not printed: desk-b deskb; raise --budget") {
		t.Errorf("output names no cut lanes:\n%s", out)
	}
}

func TestTailPerLaneNeedsAPrefix(t *testing.T) {
	seedPrefixLanes(t)
	if _, err := run(t, "tail", "--drive", "d", "--since", "0", "--per-lane", "2"); err == nil || !strings.Contains(err.Error(), "--lane-prefix") {
		t.Fatalf("err = %v, want --per-lane to name --lane-prefix", err)
	}
}
