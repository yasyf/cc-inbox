package cli_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/cli"
	"github.com/yasyf/cc-inbox/internal/store"
)

func TestPostPairwisePreservesOrderedPairs(t *testing.T) {
	for _, tt := range []struct {
		name string
		args []string
		want [][2]int64
	}{
		{"ordered", []string{"--pairwise", "--re", "7,2", "--resolves", "9,3"}, [][2]int64{{7, 9}, {2, 3}}},
		{"single", []string{"--pairwise", "--re", "7", "--resolves", "9"}, [][2]int64{{7, 9}}},
		{"default replies", []string{"--re", "7,2", "--resolves", "9"}, [][2]int64{{7, 9}, {2, 9}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("CCI_HOME", t.TempDir())
			out, err := run(t, append([]string{"post", "--json", "--drive", "test-drive", "--lane", "root", "--kind", "note", "--text", "summary"}, tt.args...)...)
			if err != nil {
				t.Fatal(err)
			}
			lines := strings.Split(strings.TrimSpace(out), "\n")
			if len(lines) != len(tt.want) {
				t.Fatalf("post returned %d records, want %d: %q", len(lines), len(tt.want), out)
			}
			for i, line := range lines {
				var got store.Record
				if err := json.Unmarshal([]byte(line), &got); err != nil {
					t.Fatal(err)
				}
				if got.Re != tt.want[i][0] || got.Resolves != tt.want[i][1] || len(got.AlsoResolves) != 0 {
					t.Errorf("record %d = re %d, resolves %d, also %v; want %v", i, got.Re, got.Resolves, got.AlsoResolves, tt.want[i])
				}
			}
		})
	}
}

func TestPostPairwiseIdempotencyIncludesBothTargets(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	args := []string{"post", "--drive", "test-drive", "--lane", "root", "--kind", "note", "--pairwise", "--re", "7,7", "--resolves", "9,8", "--id", "paired", "--text", "summary"}
	if out, err := run(t, args...); err != nil || out != "#1\n#2\n" {
		t.Fatalf("post = %q, %v; want two distinct pairs", out, err)
	}
	if out, err := run(t, args...); err != nil || out != "#1 (duplicate)\n#2 (duplicate)\n" {
		t.Fatalf("repost = %q, %v; want the same two records", out, err)
	}
	if out, err := run(t, "tail", "--drive", "test-drive", "--since", "0", "--json"); err != nil || strings.Count(strings.TrimSpace(out), "\n") != 1 {
		t.Errorf("tail = %q, %v; want exactly two stored records", out, err)
	}
}

func TestPostRejectsInvalidPairwiseBeforeStoreOrStdin(t *testing.T) {
	for _, tt := range []struct {
		name string
		args []string
		want string
	}{
		{"empty", []string{"--pairwise"}, "--pairwise needs equal non-empty"},
		{"no replies", []string{"--pairwise", "--resolves", "1"}, "--pairwise needs equal non-empty"},
		{"no resolves", []string{"--pairwise", "--re", "1"}, "--pairwise needs equal non-empty"},
		{"unequal", []string{"--pairwise", "--re", "1,2", "--resolves", "3"}, "--pairwise needs equal non-empty"},
		{"default refusal", []string{"--re", "1,2", "--resolves", "3,4"}, "not both"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			home := filepath.Join(t.TempDir(), "cci")
			t.Setenv("CCI_HOME", home)
			input := &postInput{}
			var out bytes.Buffer
			root := cli.NewRootCmd()
			root.SetIn(input)
			root.SetOut(&out)
			root.SetErr(&out)
			root.SetArgs(append([]string{"post", "--drive", "test-drive", "--lane", "root", "--kind", "note", "--text", "-"}, tt.args...))
			if err := root.Execute(); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("post = %v; want %q", err, tt.want)
			}
			if input.reads != 0 {
				t.Errorf("stdin read %d times", input.reads)
			}
			if _, err := os.Stat(home); !os.IsNotExist(err) {
				t.Errorf("CCI_HOME exists after rejected post: %v", err)
			}
		})
	}
}

func TestPostHoldDistinguishesExplicitRecordReferences(t *testing.T) {
	for _, tt := range []struct {
		text string
		want string
	}{
		{"wait for record #123", ""},
		{"wait for #123", "add --pr 123"},
		{"record #123 and #123", "add --pr 123"},
		{"record #123 and #456", "add --pr 456"},
	} {
		t.Run(tt.text, func(t *testing.T) {
			t.Setenv("CCI_HOME", t.TempDir())
			out, err := run(t, "post", "--drive", "test-drive", "--lane", "root", "--kind", "hold", "--text", tt.text)
			if tt.want == "" {
				if err != nil || out != "#1\n" {
					t.Errorf("post = %q, %v; want accepted record reference", out, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("post = %q, %v; want %q", out, err, tt.want)
			}
		})
	}
}
