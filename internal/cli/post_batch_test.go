package cli_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/store"
)

func batchFile(t *testing.T, records ...map[string]any) string {
	t.Helper()
	raw, err := json.Marshal(records)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "batch.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func tailed(t *testing.T) []store.Record {
	t.Helper()
	out, err := run(t, "tail", "--drive", "d", "--since", "0", "--json", "--budget", "100000")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	records := make([]store.Record, 0, len(lines))
	for _, line := range lines {
		var record store.Record
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("%q: %v", line, err)
		}
		records = append(records, record)
	}
	return records
}

func TestPostBatchPostsEachRecordWithTheTextAsWritten(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	said := `it's 'quoted' and "double", no shell in sight`
	path := batchFile(t,
		map[string]any{"kind": "note", "to": []string{"reader"}, "text": said},
		map[string]any{"kind": "report", "lane": "other", "text": "second", "attrs": map[string]string{"step": "two"}},
	)
	if _, err := run(t, "post", "--batch", path, "--lane", "writer", "--drive", "d"); err != nil {
		t.Fatal(err)
	}
	got := tailed(t)
	if len(got) != 2 {
		t.Fatalf("records = %+v, want two", got)
	}
	if got[0].Text != said || got[0].Lane != "writer" || len(got[0].To) != 1 || got[0].To[0] != "reader" {
		t.Errorf("first = %+v; want the text whole, the default lane, the addressee", got[0])
	}
	if got[1].Lane != "other" || got[1].Fields.Attrs["step"] != "two" {
		t.Errorf("second = %+v; want its own lane and attrs", got[1])
	}
}

func TestPostBatchPostsTheRestPastARefusalAndExitsNonzero(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	path := batchFile(t,
		map[string]any{"kind": "note", "text": "before"},
		map[string]any{"kind": "nonsense", "text": "refused"},
		map[string]any{"kind": "note", "text": "after"},
	)
	out, err := run(t, "post", "--batch", path, "--lane", "writer", "--drive", "d")
	if err == nil || !strings.Contains(err.Error(), "1 of 3") {
		t.Fatalf("err = %v, want one of three refused", err)
	}
	if !strings.Contains(out, "--batch record 2:") {
		t.Errorf("output = %q, want the refused position", out)
	}
	got := tailed(t)
	if len(got) != 2 || got[0].Text != "before" || got[1].Text != "after" {
		t.Errorf("records = %+v, want before and after", got)
	}
}

func TestPostBatchRefusals(t *testing.T) {
	for _, tt := range []struct {
		name string
		args func(t *testing.T) []string
		want string
	}{
		{"a flag that belongs in the list", func(t *testing.T) []string {
			return []string{"--batch", batchFile(t, map[string]any{"kind": "note", "text": "x"}), "--kind", "note"}
		}, "--kind belongs in the list"},
		{"text beside the list", func(t *testing.T) []string {
			return []string{"--batch", batchFile(t, map[string]any{"kind": "note", "text": "x"}), "stray"}
		}, "not an argument"},
		{"an empty list", func(t *testing.T) []string { return []string{"--batch", batchFile(t)} }, "the list is empty"},
		{"a nested batch", func(t *testing.T) []string {
			return []string{"--batch", batchFile(t, map[string]any{"kind": "note", "text": "x", "batch": "other.json"})}
		}, "1 of 1"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("CCI_HOME", t.TempDir())
			_, err := run(t, append([]string{"post", "--lane", "writer", "--drive", "d"}, tt.args(t)...)...)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}
