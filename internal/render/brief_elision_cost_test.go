package render_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
)

var briefElisionCost string

func TestBriefElisionImmutableOutput(t *testing.T) {
	dir := os.Getenv("CCI_BRIEF_ELISION_SNAPSHOTS")
	if dir == "" {
		t.Skip("CI supplies immutable Brief output snapshots")
	}
	host := time.Local
	zone, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Fatal(err)
	}
	time.Local = zone
	t.Cleanup(func() { time.Local = host })
	at := time.Date(2026, 11, 1, 6, 22, 0, 0, time.UTC)
	outputs := map[string][]byte{}
	for textIndex, text := range []string{"", "selected-10000", "ready rollout #42", "READY (rollout), #o/r#42!", "#420 rollout-extra", " α\u00a0β\u3000γ\r\n", " \xffa\t\xc0b \x80 ", "a\x00b c", strings.Repeat("filler ", 32) + "rollout #42", strings.Repeat("x", 32*1024)} {
		for metadata := range 5 {
			r := store.Record{Seq: 10000, Num: "#10000", At: at, Kind: kinds.Report, Lane: "desk", Text: text}
			switch metadata {
			case 1:
				r.Topic = "rollout"
			case 2:
				r.Refs.PRs = []store.PR{{Number: 42}}
			case 3:
				r.Topic = "rollout"
				r.Refs.PRs = []store.PR{{Repo: "o/r", Number: 42}}
			case 4:
				r.To = []string{"root", "api", "desk", "one", "two", "three", "four", "five", "six"}
				r.Refs.Path = "private/path"
			}
			for _, width := range []int{0, 1, 40, 700} {
				name := fmt.Sprintf("text%d/metadata%d/width%d", textIndex, metadata, width)
				outputs[name+"/Brief"] = []byte(render.Brief(r, at, width))
				outputs[name+"/Untagged"] = []byte(render.Untagged(r, at, width))
			}
		}
	}
	path := filepath.Join(dir, "brief-elision-output.json")
	if os.Getenv("CCI_BRIEF_ELISION_WRITE") == "1" {
		raw, err := json.Marshal(outputs)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
		return
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var want map[string][]byte
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	if len(want) != len(outputs) {
		t.Fatalf("Brief output population=%d want=%d", len(outputs), len(want))
	}
	for name, got := range outputs {
		expected, ok := want[name]
		if !ok || !bytes.Equal(got, expected) {
			t.Fatalf("Brief output changed for %s: got=%q want=%q", name, got, expected)
		}
	}
	t.Logf("BRIEF_ELISION_IMMUTABLE_OUTPUT raw_byte_cases=%d", len(outputs))
}

func BenchmarkBriefUnusedElisionAllocations(b *testing.B) {
	at := time.Date(2026, 11, 1, 6, 22, 0, 0, time.UTC)
	r := store.Record{Seq: 10000, Num: "#10000", At: at, Kind: kinds.Report, Lane: "desk", Text: "selected-10000"}
	if got, want := render.Brief(r, at, 0), render.Line(r, at); got != want {
		b.Fatalf("no-reference Brief output=%q want=%q", got, want)
	}
	var line, brief float64
	for range b.N {
		line = testing.AllocsPerRun(100, func() { briefElisionCost = render.Line(r, at) })
		brief = testing.AllocsPerRun(100, func() { briefElisionCost = render.Brief(r, at, 0) })
		if brief > line {
			b.Fatalf("BRIEF_UNUSED_ELISION_ALLOCATION_EXCEEDED brief=%g line=%g", brief, line)
		}
	}
	b.ReportMetric(line, "line-allocs/op")
	b.ReportMetric(brief, "brief-allocs/op")
	b.Logf("BRIEF_UNUSED_ELISION_ALLOCATIONS brief=%g line=%g CPU_gain=UNKNOWN", brief, line)
}

func BenchmarkBriefElision(b *testing.B) {
	at := time.Date(2026, 11, 1, 6, 22, 0, 0, time.UTC)
	for _, references := range []bool{false, true} {
		for _, population := range []int{32, 10000} {
			b.Run(fmt.Sprintf("references=%t/N%d", references, population), func(b *testing.B) {
				records := make([]store.Record, population)
				for i := range records {
					records[i] = store.Record{Seq: int64(i + 2), Num: fmt.Sprintf("#%d", i+2), At: at, Kind: kinds.Report, Lane: "desk-runner", Text: fmt.Sprintf("selected-%d", i+1)}
					if references {
						records[i].Topic = "rollout"
						records[i].Refs.PRs = []store.PR{{Number: 42}}
					}
				}
				b.ReportAllocs()
				for b.Loop() {
					for _, r := range records {
						briefElisionCost = render.Brief(r, at, 700)
					}
				}
			})
		}
	}
}
