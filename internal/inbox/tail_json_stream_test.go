package inbox_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/store"
)

type countedWrites struct {
	bytes.Buffer
	writes int
}

func (c *countedWrites) Write(p []byte) (int, error) {
	c.writes++
	return c.Buffer.Write(p)
}

func TestTailJSONStreamsPagesWithinItsBudget(t *testing.T) {
	const rows = 1200
	st := tailPageAssemblyStore(t, rows)
	f := store.Filter{Drive: "durable-agents-rebuild"}
	tests := []struct {
		name     string
		budget   int
		printed  int
		resumes  bool
		multiple bool
	}{
		{"a budget that holds every row arrives in pieces", 10 << 20, rows, false, true},
		{"a budget that runs out names where to resume", 4000, 0, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out countedWrites
			var notes bytes.Buffer
			res, err := inbox.Tail(t.Context(), st, inbox.TailOptions{Filter: f, JSON: true, Budget: tt.budget, Notes: &notes}, &out)
			if err != nil {
				t.Fatal(err)
			}
			printed := strings.Count(out.String(), "\n")
			if printed != res.Printed || tt.printed > 0 && printed != tt.printed || printed == 0 {
				t.Fatalf("printed %d lines, result %d, want %d", printed, res.Printed, tt.printed)
			}
			if tt.multiple && out.writes < 2 {
				t.Errorf("writes = %d, want the answer in pieces", out.writes)
			}
			if res.Last != int64(148000+printed) || res.More != rows-printed {
				t.Errorf("last = %d, more = %d after %d rows", res.Last, res.More, printed)
			}
			want := ""
			if tt.resumes {
				want = fmt.Sprintf("... %d more; resume with --since %d\n", res.More, res.Last)
			}
			if notes.String() != want {
				t.Errorf("notes = %q, want %q", notes.String(), want)
			}
		})
	}
}
