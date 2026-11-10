package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	mcp "github.com/yasyf/cc-interact/channel"

	"github.com/yasyf/cc-inbox/internal/cli"
	"github.com/yasyf/cc-inbox/internal/peers"
	"github.com/yasyf/cc-inbox/internal/store"
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

func TestThePostToolStampsALeadingLabel(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	stale := time.Now().Add(-2 * time.Hour).Format("3:04 PM")
	seq := mustTool(t, "post", map[string]any{"drive": "d", "lane": "desk", "kind": "report", "text": stale + ": merged"})
	if out := mustTool(t, "read", map[string]any{"seqs": []any{seq}}); strings.Contains(out, stale) || !strings.Contains(out, "merged") {
		t.Errorf("read %s = %q, want cci's own label in place of %s", seq, out, stale)
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
	if strings.Count(first, "END") != 1 || !strings.Contains(first, "1 note root ") || !strings.Contains(first, "2 report root ") {
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
	seven, eight := strings.Index(out, "pr owner/repo#7\n"), strings.Index(out, "pr owner/repo#8\n")
	if seven < 0 || eight < seven || !strings.Contains(out[seven:eight], "1 head desk repo#7 head one") || !strings.Contains(out[eight:], "2 head desk repo#8 head two") {
		t.Errorf("find prs 7 8 = %q, want each pull request's records under its own header", out)
	}
}

func inputSchema(t *testing.T, name string) map[string]any {
	t.Helper()
	for _, tool := range cli.ChannelTools() {
		if tool.Name == name {
			return tool.InputSchema
		}
	}
	t.Fatalf("no tool %s", name)
	return nil
}

func TestThePostToolAdvertisesOnlyTheFieldsLanesPass(t *testing.T) {
	post := inputSchema(t, "post")
	props, _ := post["properties"].(map[string]any)
	if got, want := slices.Sorted(maps.Keys(props)), []string{"attrs", "body", "build", "ccn", "commit", "drive", "head", "id", "kind", "lane", "lane_ref", "outcome", "path", "pr", "re", "records", "resolves", "text", "to", "topic", "url"}; !slices.Equal(got, want) {
		t.Errorf("post fields = %v, want %v: no hidden flag and no field a lane passes only in Bash", got, want)
	}
	records, _ := props["records"].(map[string]any)
	items, _ := records["items"].(map[string]any)
	fields, _ := items["properties"].(map[string]any)
	if got, want := slices.Sorted(maps.Keys(fields)), []string{"body", "kind", "pr", "re", "resolves", "text", "to", "topic"}; !slices.Equal(got, want) {
		t.Errorf("records item fields = %v, want %v", got, want)
	}
	for _, name := range []string{"post", "close"} {
		if required, _ := inputSchema(t, name)["required"].([]string); slices.Contains(required, "lane") {
			t.Errorf("%s requires lane, which defaults to the window's", name)
		}
	}
}

func TestTheToolListStaysUnderItsCeiling(t *testing.T) {
	const ceiling = 6300
	var out bytes.Buffer
	if err := mcp.NewServer(mcp.ServerInfo{Name: "cci"}, cli.ChannelTools()).Serve(t.Context(), strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`+"\n"), &out); err != nil {
		t.Fatal(err)
	}
	var reply struct {
		Result struct {
			Tools json.RawMessage `json:"tools"`
		} `json:"result"`
	}
	if err := json.Unmarshal(out.Bytes(), &reply); err != nil {
		t.Fatal(err)
	}
	if n := len(reply.Result.Tools); n == 0 || n > ceiling {
		t.Errorf("tools/list carries %d bytes of tools, over the %d ceiling: every Claude Code session loads this list, so trim a field or description before raising it", n, ceiling)
	}
}

func TestThePostToolPassesTheFieldsItAdvertises(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	args := map[string]any{"drive": "d", "lane": "desk", "kind": "report", "text": "once", "id": "k1", "lane_ref": []string{"web"}, "attrs": map[string]string{"step": "one"}}
	first := mustTool(t, "post", args)
	if second := mustTool(t, "post", args); second != first+" (duplicate)" {
		t.Errorf("a repost with the same id = %q, want the first record %q marked a duplicate", second, first)
	}
	if out := mustTool(t, "read", map[string]any{"seqs": []any{first}}); !strings.Contains(out, "once") {
		t.Errorf("read %s = %q", first, out)
	}
	for _, tt := range []struct {
		name string
		args map[string]any
		want string
	}{
		{"unknown", map[string]any{"drive": "d", "lane": "desk", "kind": "report", "text": "x", "bogus": "y"}, "post: no field bogus"},
		{"bash only", map[string]any{"drive": "d", "lane": "desk", "kind": "decide", "text": "x", "owned_by": "desk"}, "post: no field owned_by"},
		{"top level only", map[string]any{"drive": "d", "lane": "desk", "records": []map[string]any{{"kind": "note", "text": "x", "attrs": map[string]string{"a": "b"}}}}, "records[0]: post: no field attrs"},
	} {
		if text, isErr := callTool(t, "post", tt.args); !isErr || text != tt.want {
			t.Errorf("%s: post = %q, %v; want %q", tt.name, text, isErr, tt.want)
		}
	}
}

func TestPostAndCloseDefaultTheLaneToTheWindowsBinding(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	t.Setenv("CCI_AGENT_ID", "")
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	ask := mustTool(t, "post", map[string]any{"drive": "d", "lane": "api", "kind": "ask", "text": "which branch?", "to": []string{"desk"}})
	st, err := store.Open(context.Background(), home)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.BindLane(context.Background(), store.PeerKey{Session: "desk-session"}, "d", "desk", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "desk-session")
	note := mustTool(t, "post", map[string]any{"kind": "note", "text": "from the window"})
	answer := mustTool(t, "close", map[string]any{"seqs": []any{ask}, "how": "answer", "text": "main"})
	out := mustTool(t, "find", map[string]any{"drive": "d"})
	if !strings.Contains(out, note+" note desk from the window") || !strings.Contains(out, answer+" answer desk>api x"+ask+" main") {
		t.Errorf("find = %q, want the note and the close both posted as desk", out)
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "stray-session")
	for name, args := range map[string]map[string]any{
		"post":  {"drive": "d", "kind": "note", "text": "from nowhere"},
		"close": {"drive": "d", "seqs": []any{ask}, "how": "answer", "text": "main"},
	} {
		if text, isErr := callTool(t, name, args); !isErr || !strings.Contains(text, "post needs --lane") {
			t.Errorf("%s in an unbound window = %q, %v; want the refusal naming the lane", name, text, isErr)
		}
	}
}

func TestTheWhoToolListsOnlyTheWindowsDrive(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())
	for _, name := range peers.IdentityEnv {
		t.Setenv(name, "")
	}
	t.Setenv("CCI_AGENT_ID", "")
	for _, w := range []struct{ session, drive string }{{"aaaa1111-other", "other"}, {"bbbb2222-mine", "d"}} {
		t.Setenv("CLAUDE_CODE_SESSION_ID", w.session)
		if _, err := run(t, "drive", "use", w.drive, "--root"); err != nil {
			t.Fatal(err)
		}
		if _, err := run(t, "announce"); err != nil {
			t.Fatal(err)
		}
	}
	if out := mustTool(t, "who", map[string]any{}); !strings.Contains(out, "session=bbbb2222") || strings.Contains(out, "session=aaaa1111") {
		t.Errorf("who = %q, want only the session on this window's drive d", out)
	}
	if out := mustTool(t, "who", map[string]any{"drive": "other"}); !strings.Contains(out, "session=aaaa1111") {
		t.Errorf("who drive other = %q, want the session on other", out)
	}
}
