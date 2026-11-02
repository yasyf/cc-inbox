package cli_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/cli"
)

func TestThePostToolPostsWhatCciPostWouldWithoutABashCall(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	t.Setenv("CLAUDE_CODE_SESSION_ID", "session-1")
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "brief.md"), []byte("the brief's first line\nthe rest of it\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	tool := cli.PostTool()
	call := func(args []string, cwd string) (string, bool) {
		raw, err := json.Marshal(map[string]any{"args": args, "cwd": cwd})
		if err != nil {
			t.Fatal(err)
		}
		return tool.Handler(t.Context(), raw, nil)
	}
	if text, isErr := call([]string{"--drive", "d1", "--lane", "desk", "--kind", "note", "--path", "brief.md"}, dir); isErr || text != "#1" {
		t.Fatalf("post tool = %q, error %v; want the new record's number", text, isErr)
	}
	if text, isErr := call([]string{"--drive", "d1", "--lane", "desk", "--kind", "note", "--path", "brief.md"}, dir); isErr || !strings.Contains(text, "#1") {
		t.Fatalf("repeated post tool = %q, error %v; want the duplicate's number", text, isErr)
	}
	out, err := run(t, "grep", "first line", "--drive", "d1")
	if err != nil || strings.Count(out, "the brief's first line") != 1 || !strings.Contains(out, filepath.Join(dir, "brief.md")) {
		t.Fatalf("grep for the tool's post = %q, %v; want one record naming the file under cwd", out, err)
	}
	if text, isErr := call([]string{"--drive", "d1", "--lane", "desk", "--kind", "nonsense", "--text", "x"}, ""); !isErr || !strings.Contains(text, "nonsense") {
		t.Fatalf("post tool with an unknown kind = %q, error %v; want cci post's refusal", text, isErr)
	}
}
