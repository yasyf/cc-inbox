package cli_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/cli"
)

func callTool(t *testing.T, name string, args map[string]any) (string, bool) {
	t.Helper()
	raw, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range cli.ChannelTools() {
		if tool.Name == name {
			return tool.Handler(t.Context(), raw, nil)
		}
	}
	t.Fatalf("no tool %s", name)
	return "", true
}

func mustTool(t *testing.T, name string, args map[string]any) string {
	t.Helper()
	text, isErr := callTool(t, name, args)
	if isErr {
		t.Fatalf("%s(%v) = %q, an error", name, args, text)
	}
	return text
}

func TestThePostToolPostsStructuredRecordsInOneCall(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	got := mustTool(t, "post", map[string]any{
		"drive": "d", "lane": "desk",
		"records": []map[string]any{
			{"kind": "REPORT", "text": "it's done; `quotes` and $vars stay as typed", "to": []string{"root"}, "pr": []any{42}},
			{"kind": "note", "text": "the summary", "body": "the summary\nline two of the body\n"},
		},
	})
	if got != "1 2" {
		t.Fatalf("post records = %q, want both seqs", got)
	}
	out := mustTool(t, "read", map[string]any{"seqs": []any{1, "2"}})
	if !strings.Contains(out, "1 report desk>root #42 it's done; `quotes` and $vars stay as typed") || strings.Count(out, "the summary") != 1 || !strings.Contains(out, "line two of the body") {
		t.Errorf("read 1 2 = %q, want each compact line once and #2's body", out)
	}
	if text, isErr := callTool(t, "post", map[string]any{"drive": "d", "lane": "desk", "kind": "nonsense", "text": "x"}); !isErr || !strings.Contains(text, "nonsense") {
		t.Errorf("post with an unknown kind = %q, %v; want cci's refusal", text, isErr)
	}
}

func TestTheReadToolSplitsItsBudgetAcrossBodies(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	mustTool(t, "post", map[string]any{"drive": "d", "lane": "desk", "kind": "report", "text": "big", "body": strings.Repeat("detail ", 400) + "LAST"})
	out := mustTool(t, "read", map[string]any{"seqs": []any{1}, "budget": 300})
	if strings.Contains(out, "LAST") || !strings.Contains(out, "read seqs [1] budget") {
		t.Errorf("read 1 budget 300 = %q, want the body cut with the read that returns it whole", out)
	}
}

func TestTheCloseToolClosesEverySeqAddressedToTheirWriters(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	mustTool(t, "post", map[string]any{"drive": "d", "lane": "api", "kind": "ask", "text": "which branch?", "to": []string{"root"}})
	mustTool(t, "post", map[string]any{"drive": "d", "lane": "web", "kind": "ask", "text": "which env?", "to": []string{"root"}})
	if got := mustTool(t, "close", map[string]any{"drive": "d", "lane": "root", "seqs": []any{1, 2}, "how": "answer", "text": "main, staging"}); got != "3" {
		t.Fatalf("close = %q, want one record", got)
	}
	out := mustTool(t, "find", map[string]any{"drive": "d", "pattern": "main, staging"})
	if !strings.Contains(out, "3 answer root>api,web x1 x2 main, staging") {
		t.Errorf("find the close = %q, want one answer to both writers closing both asks", out)
	}
}

func TestTheInboxToolReturnsOnlyNewRecordsAndCutsBroadcasts(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	long := strings.Repeat("word ", 60) + "END"
	mustTool(t, "post", map[string]any{"drive": "d", "lane": "root", "kind": "note", "text": long, "to": []string{"api"}})
	mustTool(t, "post", map[string]any{"drive": "d", "lane": "root", "kind": "report", "text": long, "to": []string{"all"}})
	first := mustTool(t, "inbox", map[string]any{"drive": "d", "lane": "api", "kinds": []string{"report"}})
	if strings.Count(first, "END") != 1 || !strings.Contains(first, "1 note root>api") || !strings.Contains(first, "2 report root>all") {
		t.Errorf("first inbox = %q, want the go whole and the broadcast cut", first)
	}
	if again := mustTool(t, "inbox", map[string]any{"drive": "d", "lane": "api"}); again != "none" {
		t.Errorf("second inbox = %q, want nothing new", again)
	}
}

func TestTheFindToolAnswersSeveralPullRequestsInOneCall(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	mustTool(t, "post", map[string]any{"drive": "d", "lane": "desk", "kind": "head", "text": "head one", "pr": []any{"owner/repo#7"}})
	mustTool(t, "post", map[string]any{"drive": "d", "lane": "desk", "kind": "head", "text": "head two", "pr": []any{"owner/repo#8"}})
	out := mustTool(t, "find", map[string]any{"drive": "d", "prs": []any{"owner/repo#7", "owner/repo#8"}})
	seven, eight := strings.Index(out, "pr owner/repo#7\n1 head desk repo#7 head one"), strings.Index(out, "pr owner/repo#8\n2 head desk repo#8 head two")
	if seven < 0 || eight < seven {
		t.Errorf("find prs 7 8 = %q, want each pull request's records under its own header", out)
	}
}
