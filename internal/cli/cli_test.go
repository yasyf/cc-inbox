package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/yasyf/cc-inbox/internal/cli"
	"github.com/yasyf/cc-inbox/internal/peers"
	"github.com/yasyf/cc-inbox/internal/store"
)

func run(t *testing.T, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	root := cli.NewRootCmd()
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs(args)
	err := root.Execute()
	return out.String(), err
}

func TestPostDedupeKeepsDistinctCloses(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	t.Setenv("CLAUDE_CODE_SESSION_ID", "session-1")
	if _, err := run(t, "drive", "use", "release-v3", "--root"); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct{ re, want string }{
		{"27805", "#1\n"},
		{"27813", "#2\n"},
		{"27819", "#3\n"},
		{"27819", "#3 (duplicate)\n"},
	} {
		out, err := run(t, "post", "--lane", "root", "--kind", "done", "--re", tt.re, "--text", "Superseded by #27825 (standing).")
		if err != nil || out != tt.want {
			t.Fatalf("post --re %s = %q, %v; want %q", tt.re, out, err, tt.want)
		}
	}
}

func TestPostDedupeKeepsDistinctCommits(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	t.Setenv("CLAUDE_CODE_SESSION_ID", "session-1")
	const (
		first  = "ae92ec4c95769ec03f94a81d8a82bc5d4e7ce8ac"
		second = "ae92ec4c9576c7b232748b7e0aafb970c6c1a9ac"
	)
	for _, tt := range []struct{ commit, want string }{
		{first, "#1\n"},
		{second, "#2\n"},
		{second, "#2 (duplicate)\n"},
	} {
		out, err := run(t, "post", "--drive", "durable-agents-rebuild", "--lane", "p8a-step9", "--kind", "report", "--topic", "42784", "--pr", "42784", "--commit", tt.commit, "--text", "p8a-step9 head green")
		if err != nil || out != tt.want {
			t.Fatalf("post --commit %s = %q, %v; want %q", tt.commit, out, err, tt.want)
		}
	}
	if out, err := run(t, "show", "2", "--json"); err != nil || !strings.Contains(out, `"commit":"`+second+`"`) {
		t.Fatalf("show 2 --json = %q, %v; want fields.commit %s", out, err, second)
	}
}

func TestPostTailDigest(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	t.Setenv("CLAUDE_CODE_SESSION_ID", "session-1")
	if _, err := run(t, "post", "--lane", "l", "--kind", "go", "--text", "x"); err == nil || !strings.Contains(err.Error(), "no drive") {
		t.Fatalf("post without a drive: %v", err)
	}
	if out, err := run(t, "drive", "use", "release-v3", "--root"); err != nil || out != "session session-1 -> release-v3\n" {
		t.Fatalf("drive use = %q, %v", out, err)
	}
	tests := []struct {
		args []string
		want string
	}{
		{[]string{"post", "--lane", "landing-desk", "--kind", "opened", "--pr", "30431", "--text", "release: read published versions"}, "#1\n"},
		{[]string{"post", "--lane", "landing-desk", "--kind", "opened", "--pr", "30431", "--text", "release: read published versions"}, "#1 (duplicate)\n"},
		{[]string{"post", "--lane", "root", "--kind", "hold", "--topic", "#30431", "--pr", "30431", "--to", "landing-desk", "--text", "hold until the walker reports"}, "#2\n"},
	}
	for _, tt := range tests {
		out, err := run(t, tt.args...)
		if err != nil || out != tt.want {
			t.Fatalf("%v = %q, %v; want %q", tt.args, out, err, tt.want)
		}
	}
	out, err := run(t, "tail", "--since", "0")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "OPENED landing-desk release: read published versions pr#30431") || !strings.Contains(out, "HOLD root -> landing-desk [#30431] hold until the walker reports") {
		t.Fatalf("tail = %q", out)
	}
	if out, err = run(t, "tail", "--since", "0", "--to", "landing-desk"); err != nil || strings.Count(out, "\n") != 1 || !strings.Contains(out, "HOLD root") {
		t.Fatalf("tail --to = %q, %v", out, err)
	}
	if out, err = run(t, "digest"); err != nil || !strings.Contains(out, "open holds (1, newest first):") {
		t.Fatalf("digest = %q, %v", out, err)
	}
	if _, err = run(t, "post", "--lane", "l", "--kind", "nope", "--text", "x"); err == nil || !strings.Contains(err.Error(), "unknown kind") {
		t.Fatalf("unknown kind: %v", err)
	}
	if _, err = run(t, "post", "--lane", "root", "--kind", "hold", "--text", "HOLD #39489 until the canary clears"); err == nil || !strings.Contains(err.Error(), "add --pr 39489") {
		t.Fatalf("hold naming an unbound pr: %v", err)
	}
	if out, err = run(t, "grep", "WALKER"); err != nil || !strings.Contains(out, "#2 ") {
		t.Fatalf("grep = %q, %v", out, err)
	}
	if out, err = run(t, "post", "--lane", "evidence-lane", "--kind", "evidence", "--text", "5xx began at "+time.Now().Add(-30*time.Minute).Format("3:04 PM")); err != nil || out != "#3\n" {
		t.Fatalf("post evidence = %q, %v", out, err)
	}
}

func TestTailLimitPrintsTheNewestRecords(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	t.Setenv("CLAUDE_CODE_SESSION_ID", "session-1")
	if _, err := run(t, "drive", "use", "release-v3"); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"first", "second", "third", "fourth"} {
		if _, err := run(t, "post", "--lane", "l", "--kind", "note", "--text", text); err != nil {
			t.Fatal(err)
		}
	}
	for _, args := range [][]string{{"-n", "2", "--since", "0"}, {"--limit", "2", "--since", "0"}, {"-n", "2"}} {
		out, err := run(t, append([]string{"tail"}, args...)...)
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(strings.TrimSpace(out), "\n")
		if len(lines) != 2 || !strings.HasSuffix(lines[0], "third") || !strings.HasSuffix(lines[1], "fourth") {
			t.Fatalf("tail %v = %q, want third then fourth", args, out)
		}
	}
	if out, err := run(t, "tail"); err != nil || out != "" {
		t.Fatalf("tail after -n advanced the cursor = %q, %v", out, err)
	}
}

func TestAFreshSubagentAndAnUnboundSessionPostToTheDriveTheyInherit(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	t.Setenv("CLAUDE_CODE_SESSION_ID", "session-1")
	t.Setenv("CCI_AGENT_ID", "")
	t.Setenv("CLAUDE_LONG_RUNNING_DRIVE", "")
	if _, err := run(t, "drive", "use", "release-v3", "--root"); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CCI_AGENT_ID", "a-fresh-lane")
	if out, err := run(t, "post", "--lane", "fresh-lane", "--kind", "note", "--text", "first post"); err != nil || out != "#1\n" {
		t.Fatalf("subagent post = %q, %v", out, err)
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "session-2")
	t.Setenv("CCI_AGENT_ID", "")
	if _, err := run(t, "post", "--lane", "orca-lane", "--kind", "note", "--text", "x"); err == nil || !strings.Contains(err.Error(), "no drive") {
		t.Fatalf("unbound post without the drive env: %v", err)
	}
	t.Setenv("CLAUDE_LONG_RUNNING_DRIVE", "release-v3")
	if out, err := run(t, "post", "--lane", "orca-lane", "--kind", "note", "--text", "second post"); err != nil || out != "#2\n" {
		t.Fatalf("env post = %q, %v", out, err)
	}
	if out, err := run(t, "tail", "--drive", "release-v3", "--since", "0"); err != nil || !strings.Contains(out, "first post") || !strings.Contains(out, "second post") {
		t.Fatalf("tail = %q, %v", out, err)
	}
}

func TestCodexSessionBindsADrive(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	t.Setenv("CCI_AGENT_ID", "")
	t.Setenv("CODEX_THREAD_ID", "01a13b89-codex")
	if out, err := run(t, "drive", "use", "release-v3"); err != nil || out != "session 01a13b89-codex -> release-v3\n" {
		t.Fatalf("drive use = %q, %v", out, err)
	}
	if out, err := run(t, "post", "--lane", "fix-lane", "--kind", "note", "--text", "shipped the fix"); err != nil || out != "#1\n" {
		t.Fatalf("post = %q, %v", out, err)
	}
	if out, err := run(t, "tail"); err != nil || !strings.Contains(out, "shipped the fix") {
		t.Fatalf("tail = %q, %v", out, err)
	}
	t.Setenv("CODEX_THREAD_ID", "")
	if _, err := run(t, "drive", "use", "release-v3"); err == nil || !strings.Contains(err.Error(), "neither CLAUDE_CODE_SESSION_ID nor CODEX_THREAD_ID is set") {
		t.Fatalf("drive use without a session: %v", err)
	}
}

func TestGrepMatchesWholeRecordLines(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	inbox := filepath.Join(t.TempDir(), "deploy-go.md")
	lines := []string{
		"RELEASE merge-walker-r2 (12:30 AM PT) receiver/core-usw2-auto at 29322a28c8 PASSED",
		"STATE census (2:14 AM PT, cycle 1:54): 234/293 0/0 at 5bc4ece07e; 0 drift",
		"CLAIM merge-walker-r2 (2:04 AM PT) #30541 receiver release",
		"GREEN runner-protocol-idle-tenant (2:40 AM PT) #30568 approved",
	}
	if err := os.WriteFile(inbox, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := run(t, "import", "--drive", "release-v3", inbox); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		args []string
		want []string
	}{
		{[]string{"STATE census"}, []string{"STATE census"}},
		{[]string{"CLAIM merge-walker-r2"}, []string{"CLAIM merge-walker-r2 #30541"}},
		{[]string{"runner-protocol-idle-tenant"}, []string{"runner-protocol-idle-tenant GREEN #30568"}},
		{[]string{"census|GREEN"}, []string{"STATE census", "runner-protocol-idle-tenant GREEN"}},
		{[]string{"-i", "census|green"}, []string{"STATE census", "runner-protocol-idle-tenant GREEN"}},
		{[]string{"-F", "census|GREEN"}, []string{"no records on release-v3 match; store head #4"}},
		{[]string{"234.293"}, []string{"STATE census"}},
		{[]string{"--fixed-strings", "234.293"}, []string{"no records on release-v3 match; store head #4"}},
		{[]string{"-n", "1", "census|GREEN"}, []string{"runner-protocol-idle-tenant GREEN", "... 1 more matches"}},
		{[]string{"--limit", "1", "census|GREEN"}, []string{"runner-protocol-idle-tenant GREEN", "... 1 more matches"}},
		{[]string{"--ignore-case=false", "green"}, []string{"no records on release-v3 match; store head #4"}},
		{[]string{"--ignore-case=false", "GREEN"}, []string{"runner-protocol-idle-tenant GREEN"}},
	}
	for _, tt := range tests {
		out, err := run(t, append([]string{"grep", "--drive", "release-v3"}, tt.args...)...)
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Count(out, "\n"); got != len(tt.want) {
			t.Errorf("grep %v printed %d lines, want %d:\n%s", tt.args, got, len(tt.want), out)
		}
		for _, w := range tt.want {
			if !strings.Contains(out, w) {
				t.Errorf("grep %v = %q, want %q", tt.args, out, w)
			}
		}
	}
}

func TestStateSaysWhenADriveHasNothingToShow(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	if _, err := run(t, "post", "--drive", "release-v3", "--lane", "root", "--kind", "go", "--text", "ship it"); err != nil {
		t.Fatal(err)
	}
	if out, err := run(t, "state", "--drive", "release-v3"); err != nil || out != "no head, contract, or state records on release-v3\n" {
		t.Fatalf("state = %q, %v", out, err)
	}
}

func TestBindingStartsTheSessionCursorAtTheHead(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	t.Setenv("CLAUDE_CODE_SESSION_ID", "root-session")
	for _, text := range []string{"Oct 2 backlog one", "Oct 2 backlog two"} {
		if _, err := run(t, "post", "--drive", "release-v3", "--lane", "merge-walker", "--kind", "state", "--text", text); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := run(t, "tail", "--drive", "release-v3"); err != nil {
		t.Fatal(err)
	}
	if _, err := run(t, "post", "--drive", "release-v3", "--lane", "merge-walker", "--kind", "state", "--text", "read before the bind"); err != nil {
		t.Fatal(err)
	}
	if _, err := run(t, "drive", "use", "release-v3", "--root"); err != nil {
		t.Fatal(err)
	}
	if out, err := run(t, "tail"); err != nil || out != "" {
		t.Fatalf("tail right after the bind = %q, %v; want nothing", out, err)
	}
	if _, err := run(t, "post", "--lane", "root", "--kind", "go", "--text", "after the bind"); err != nil {
		t.Fatal(err)
	}
	if out, err := run(t, "tail"); err != nil || !strings.Contains(out, "after the bind") || strings.Contains(out, "backlog") {
		t.Fatalf("tail after a new record = %q, %v", out, err)
	}
}

func TestGrepSinceTakesAClockTime(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	if _, err := run(t, "post", "--drive", "release-v3", "--lane", "root", "--kind", "go", "--text", "ship it"); err != nil {
		t.Fatal(err)
	}
	soon := time.Now().Add(2 * time.Minute)
	for _, since := range []string{soon.Format("15:04"), soon.Format("3:04 PM")} {
		if out, err := run(t, "grep", "--drive", "release-v3", "--since", since, "ship"); err != nil || !strings.Contains(out, "ship it") {
			t.Errorf("grep --since %q = %q, %v", since, out, err)
		}
	}
	if _, err := run(t, "grep", "--drive", "release-v3", "--since", "9 o'clock", "ship"); err == nil || !strings.Contains(err.Error(), "clock time") {
		t.Errorf("grep --since 9 o'clock: %v", err)
	}
}

func TestPostTakesItsTextAsOneArgument(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	post := []string{"post", "--drive", "release-v3", "--lane", "root", "--kind", "go", "--to", "cci-fix-4"}
	if out, err := run(t, append(post, "land the fix")...); err != nil || out != "#1\n" {
		t.Fatalf("post with a text argument = %q, %v", out, err)
	}
	if out, err := run(t, "tail", "--drive", "release-v3", "--since", "0"); err != nil || !strings.Contains(out, "GO root -> cci-fix-4 land the fix") {
		t.Fatalf("tail = %q, %v", out, err)
	}
	for _, tt := range []struct {
		args []string
		want string
	}{
		{append(post, "land", "the fix"), "one quoted argument or --text; got 2 arguments"},
		{append(post, "--text", "a", "b"), "as an argument or --text, not both"},
		{post, "post needs its text, as one quoted argument, --text, or a --path file"},
	} {
		if _, err := run(t, tt.args...); err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("%v: %v, want %q", tt.args, err, tt.want)
		}
	}
}

func TestPostPathSuppliesTheTextFromItsFirstLine(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	post := []string{"post", "--drive", "release-v3", "--lane", "perf-asks", "--kind", "note"}
	long := strings.Repeat("x", 500)
	dir := t.TempDir()
	summary := filepath.Join(dir, "summary.md")
	if err := os.WriteFile(summary, []byte("  perf findings for api-sql  \n\n"+long+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	wide := filepath.Join(dir, "wide.md")
	if err := os.WriteFile(wide, []byte(long+"\nrest"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		path, want string
	}{
		{summary, "NOTE perf-asks perf findings for api-sql " + summary},
		{wide, "NOTE perf-asks " + strings.Repeat("x", 399) + "… " + wide},
	} {
		if _, err := run(t, append(post, "--path", tt.path)...); err != nil {
			t.Fatalf("post --path %s: %v", tt.path, err)
		}
		if out, err := run(t, "tail", "--drive", "release-v3", "--since", "0"); err != nil || !strings.Contains(out, tt.want) {
			t.Errorf("tail after post --path %s = %q, %v; want %q", tt.path, out, err, tt.want)
		}
	}
	if _, err := run(t, append(post, "--path", filepath.Join(dir, "missing.md"))...); err == nil || !strings.Contains(err.Error(), "--path") {
		t.Errorf("post --path to a missing file: %v", err)
	}
}

func TestPostSpillsTextOver400CharactersToAFile(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	post := []string{"post", "--drive", "release-v3", "--lane", "perf-asks", "--kind", "note"}
	long := "perf findings for api-sql\n" + strings.Repeat("x", 500)
	if _, err := run(t, append(post, "--text", long)...); err != nil {
		t.Fatalf("post --text over 400 characters: %v", err)
	}
	out, err := run(t, "tail", "--drive", "release-v3", "--since", "0", "--json")
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Text string `json:"text"`
		Refs struct {
			Path string `json:"path"`
		} `json:"refs"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &got); err != nil {
		t.Fatalf("tail --json = %q: %v", out, err)
	}
	if want := "perf findings for api-sql " + strings.Repeat("x", 373) + "…"; got.Text != want {
		t.Errorf("spilled record text = %q, want the first 400 characters", got.Text)
	}
	if body, err := os.ReadFile(got.Refs.Path); err != nil || string(body) != long+"\n" {
		t.Errorf("spilled file %q = %q, %v; want the full text", got.Refs.Path, body, err)
	}
	if out, err := run(t, "tail", "--drive", "release-v3", "--since", "0"); err != nil || !strings.Contains(out, got.Text+" "+got.Refs.Path) {
		t.Errorf("tail of the spilled record = %q, %v; want the whole excerpt then the path", out, err)
	}
	if _, err := run(t, append(post, "--topic", "words", "--text", strings.Repeat("words ", 80))...); err != nil {
		t.Fatal(err)
	}
	if out, err := run(t, "tail", "--drive", "release-v3", "--since", "0", "--json", "--topic", "words"); err != nil || !strings.Contains(out, `"text":"`+strings.Repeat("words ", 65)+`words…"`) {
		t.Errorf("spilled prose = %q, %v; want it cut at the last word before 400 characters", out, err)
	}
	if _, err := run(t, append(post, "--text", "perf findings for api-sql\n"+strings.Repeat("y", 500))...); err != nil {
		t.Fatal(err)
	}
	if out, err := run(t, "tail", "--drive", "release-v3", "--since", "0"); err != nil || strings.Count(out, "perf findings for api-sql") != 2 {
		t.Errorf("tail after two spills sharing a first line = %q, %v; want both records", out, err)
	}
	if _, err := run(t, append(post, "--topic", "blank-first", "--text", "\n\n"+strings.Repeat("z", 401))...); err != nil {
		t.Errorf("post of long text after blank lines: %v", err)
	}
	path := filepath.Join(t.TempDir(), "body.md")
	if err := os.WriteFile(path, []byte("summary\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := run(t, append(post, "--text", long, "--path", path)...); err == nil || !strings.Contains(err.Error(), "exceeds 400") {
		t.Errorf("post --text over 400 characters with --path: %v, want the text-length refusal", err)
	}
}

type posted struct {
	Seq  int64     `json:"seq"`
	At   time.Time `json:"at"`
	Text string    `json:"text"`
	Refs struct {
		Path string `json:"path"`
	} `json:"refs"`
}

func (p posted) stamp() string {
	return p.At.In(time.Local).Format("3:04 PM")
}

func postJSON(t *testing.T, args ...string) posted {
	t.Helper()
	out, err := run(t, append([]string{"post", "--json", "--drive", "release-v3", "--lane", "root", "--kind", "note"}, args...)...)
	if err != nil {
		t.Fatalf("post %v: %v", args, err)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	var got posted
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &got); err != nil {
		t.Fatalf("post --json %v = %q: %v", args, out, err)
	}
	return got
}

func TestPostStampsTextWithTheRecordsOwnTime(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	ahead := time.Now().Add(7 * time.Minute)
	for _, tt := range []struct {
		name, text, want string
	}{
		{"twelve-hour clock", "R84 (" + ahead.Format("3:04 PM") + ") orca-desk: relay to api: hold", "R84 ({at}) orca-desk: relay to api: hold"},
		{"clock without a space", "R85 (" + ahead.Format("3:04pm") + ") orca-desk: relay to api: hold", "R85 ({at}) orca-desk: relay to api: hold"},
		{"24-hour clock", "R86 (" + ahead.Format("15:04") + ") orca-desk: relay to api: hold", "R86 ({at}) orca-desk: relay to api: hold"},
		{"fuzzy minute", "R87 (10:5x AM) orca-desk: relay to api: hold", "R87 ({at}) orca-desk: relay to api: hold"},
		{"clock with a zone", "R92 (3:36 PM PT) orca-desk: launch api-fix NOW", "R92 ({at}) orca-desk: launch api-fix NOW"},
		{"lettered key", "R88a (" + ahead.Format("3:04 PM") + ") orca-desk: relay to api: hold", "R88a ({at}) orca-desk: relay to api: hold"},
		{"token in prose", "api landed {at}, live since {at}", "api landed {at}, live since {at}"},
		{"neither", "ship it", "ship it"},
		{"clock in prose", "R89 said the deploy began at 10:52 AM (not 10:45 AM)", "R89 said the deploy began at 10:52 AM (not 10:45 AM)"},
		{"stamp past the start", "relay R90 (10:52 AM) was late", "relay R90 (10:52 AM) was late"},
		{"parenthesis without a clock", "R91 (orca-desk, 10:52 AM) relay to api", "R91 (orca-desk, 10:52 AM) relay to api"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := postJSON(t, tt.text)
			want := strings.ReplaceAll(tt.want, "{at}", got.stamp())
			if got.Text != want {
				t.Fatalf("post %q stored %q, want %q", tt.text, got.Text, want)
			}
			line := got.stamp() + " NOTE root " + want
			if out, err := run(t, "tail", "--drive", "release-v3", "--since", "0"); err != nil || !strings.Contains(out, line) {
				t.Errorf("tail after post %q = %q, %v; want %q", tt.text, out, err, line)
			}
		})
	}
}

func TestPostRefusesALeadingTimeLabelOffTheClockUnlessStamped(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	ahead, before := time.Now().Add(7*time.Minute).Format("3:04 PM"), time.Now().Add(-30*time.Minute).Format("3:04 PM")
	for _, tt := range []struct {
		name, text   string
		stamp        bool
		want, stderr string
		refused      string
	}{
		{name: "leading label ahead of the clock", text: ahead + ": live on dev", refused: ahead},
		{name: "leading label before the clock", text: before + ": live on dev", refused: before},
		{name: "label after a short heading", text: "board " + ahead + ": pulled the row", refused: ahead},
		{name: "label after the lane's name", text: "root " + ahead + " count: 0 ungraded", refused: ahead},
		{name: "stamped label ahead of the clock", text: ahead + ": live on dev", stamp: true, want: "{at}: live on dev"},
		{name: "stamped label after the lane's name", text: "root " + before + " count: 0 ungraded", stamp: true, want: "root {at} count: 0 ungraded"},
		{name: "stamped text without a label", text: "live on dev", stamp: true, want: "{at} live on dev"},
		{name: "time in an opening clause", text: "for his " + ahead + " ask", want: "for his " + ahead + " ask", stderr: "cci: time " + ahead + " in the text is later than the post time {at}; stored as typed"},
		{name: "later time inside the text", text: "live on dev, read " + ahead, want: "live on dev, read " + ahead, stderr: "cci: time " + ahead + " in the text is later than the post time {at}; stored as typed"},
		{name: "time a sentence names since", text: "lane-x: reconciling. Landed since " + before + ": 45899", want: "lane-x: reconciling. Landed since " + before + ": 45899"},
		{name: "stamped text whose sentence names a time since", text: "lane-x: reconciling. Landed since " + before + ": 45899", stamp: true, want: "{at} lane-x: reconciling. Landed since " + before + ": 45899"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			args := []string{"post", "--json", "--drive", "release-v3", "--lane", "root", "--kind", "note", "--text", tt.text}
			if tt.stamp {
				args = append(args, "--stamp")
			}
			out, err := run(t, args...)
			if tt.refused != "" {
				if err == nil || !strings.Contains(err.Error(), "time label "+tt.refused+" is not the post time") || strings.Contains(err.Error(), "\n") {
					t.Fatalf("post %q = %q, %v; want a one-line refusal naming %s", tt.text, out, err, tt.refused)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			lines := strings.Split(strings.TrimSpace(out), "\n")
			var got posted
			if err := json.Unmarshal([]byte(lines[len(lines)-1]), &got); err != nil {
				t.Fatalf("post --json = %q: %v", out, err)
			}
			want, note := strings.ReplaceAll(tt.want, "{at}", got.stamp()), strings.ReplaceAll(tt.stderr, "{at}", got.stamp())
			if stderr := strings.Join(lines[:len(lines)-1], "\n"); got.Text != want || stderr != note {
				t.Errorf("post %q stored %q with stderr %q, want %q with stderr %q", tt.text, got.Text, stderr, want, note)
			}
		})
	}
}

func TestPostHeadAndOpenedTakeTheirPullRequestOrRefuse(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	for _, kind := range []string{"head", "opened"} {
		for _, tt := range []struct {
			name   string
			args   []string
			pr     int
			stderr string
		}{
			{"--pr given", []string{"--pr", "43922", "--text", "lane/branch 0123abc"}, 43922, ""},
			{"one #N in the text", []string{"--text", "#43946 lane/branch 0123abc"}, 43946, "cci: " + kind + " names #43946; recorded as --pr 43946"},
			{"no pull request", []string{"--text", "lane/branch 0123abc"}, 0, ""},
			{"two pull requests", []string{"--text", "#1 and #2 at 0123abc"}, 0, ""},
			{"#0", []string{"--text", "#0 lane/branch 0123abc"}, 0, ""},
		} {
			t.Run(kind+" "+tt.name, func(t *testing.T) {
				out, err := run(t, append([]string{"post", "--json", "--drive", "d", "--lane", "lane-a", "--kind", kind}, tt.args...)...)
				if tt.pr == 0 {
					if err == nil || !strings.Contains(err.Error(), "a "+kind+" record needs --pr") {
						t.Fatalf("post = %q, %v; want a refusal naming --pr", out, err)
					}
					return
				}
				lines := strings.Split(strings.TrimSpace(out), "\n")
				var got store.Record
				if err != nil || json.Unmarshal([]byte(lines[len(lines)-1]), &got) != nil || len(got.Refs.PRs) != 1 || got.Refs.PRs[0].Number != tt.pr {
					t.Fatalf("post = %q, %v; want pr %d", out, err, tt.pr)
				}
				if stderr := strings.Join(lines[:len(lines)-1], "\n"); stderr != tt.stderr {
					t.Errorf("stderr = %q, want %q", stderr, tt.stderr)
				}
			})
		}
	}
}

func TestPostHeadCarriesEveryHeadOfARestack(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	t.Setenv("CLAUDE_CODE_SESSION_ID", "session-1")
	if _, err := run(t, "drive", "use", "release-v3", "--root"); err != nil {
		t.Fatal(err)
	}
	out, err := run(t, "post", "--json", "--lane", "lane-a", "--kind", "head",
		"--head", "45001=a1b2c3d4e5f60718293a4b5c6d7e8f9012345678",
		"--head", "yasyf/cc-inbox#45002=4423264..C3B44A3FEE8EBDCD89A5BBC180D796C3C28B8FD4 restack, range-diff patch-identical (3 commits)",
		"--text", "restacked onto dev")
	var got store.Record
	if err != nil || json.Unmarshal([]byte(strings.TrimSpace(out)), &got) != nil {
		t.Fatalf("post = %q, %v", out, err)
	}
	want := map[string]string{
		"head:45001":                "a1b2c3d4e5f60718293a4b5c6d7e8f9012345678",
		"head:yasyf/cc-inbox#45002": "4423264..c3b44a3fee8ebdcd89a5bbc180d796c3c28b8fd4 restack, range-diff patch-identical (3 commits)",
	}
	prs := []store.PR{{Number: 45001}, {Repo: "yasyf/cc-inbox", Number: 45002}}
	if !reflect.DeepEqual(got.Fields.Attrs, want) || !reflect.DeepEqual(got.Refs.PRs, prs) || got.Fields.Commit != "" {
		t.Fatalf("attrs %v prs %v commit %q; want %v on PRs %v and no record-wide commit", got.Fields.Attrs, got.Refs.PRs, got.Fields.Commit, want, prs)
	}
	if want := "HEAD 2 PRs: #45001 a1b2c3d, #45002 c3b44a3: restacked onto dev"; got.Text != want {
		t.Errorf("text = %q, want %q", got.Text, want)
	}
	for _, pr := range []string{"45001", "yasyf/cc-inbox#45002"} {
		out, err := run(t, "tail", "--since", "0", "--pr", pr)
		if err != nil || strings.Count(out, "\n") != 1 || !strings.HasSuffix(strings.TrimSpace(out), "HEAD 2 PRs: #45001 a1b2c3d, #45002 c3b44a3: restacked onto dev") {
			t.Fatalf("tail --pr %s = %q, %v; want the one head record with no PR refs repeated after its text", pr, out, err)
		}
	}
	out, err = run(t, "post", "--json", "--lane", "lane-a", "--kind", "head", "--head", "45003=0123abc")
	if err != nil || json.Unmarshal([]byte(strings.TrimSpace(out)), &got) != nil || got.Text != "HEAD #45003 0123abc" {
		t.Fatalf("post with no text = %q, %v; want the text HEAD #45003 0123abc", out, err)
	}
}

func TestPostHeadRefusesAMalformedHead(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	for _, tt := range []struct {
		name string
		args []string
		want string
	}{
		{"not a sha", []string{"--kind", "head", "--head", "45001=main"}, `commit "main" is not a 7 to 40 character hex sha`},
		{"was not a sha", []string{"--kind", "head", "--head", "45001=none..0123abc"}, `was "none" is not a 7 to 40 character hex sha`},
		{"no sha", []string{"--kind", "head", "--head", "45001"}, "want <pr>=<sha>"},
		{"no pull request", []string{"--kind", "head", "--head", "x=0123abc"}, "want a number"},
		{"another kind", []string{"--kind", "state", "--head", "45001=0123abc"}, "--head is for a head record, not state"},
		{"with --commit", []string{"--kind", "head", "--commit", "0123abc", "--head", "45001=0123abc"}, "drop --commit"},
		{"one PR twice", []string{"--kind", "head", "--head", "45001=0123abc", "--head", "45001=4567def"}, "--head names 45001 twice"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			out, err := run(t, append([]string{"post", "--drive", "d", "--lane", "lane-a"}, tt.args...)...)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("post = %q, %v; want a refusal containing %q", out, err, tt.want)
			}
		})
	}
}

func TestPostHeadNamingANoteNeedsNoPullRequest(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	out, err := run(t, "post", "--json", "--drive", "d", "--lane", "exp-a", "--kind", "head", "--ccn", "4f2a1b9", "--text", "method note 4f2a1b9")
	var got store.Record
	if err != nil || json.Unmarshal([]byte(strings.TrimSpace(out)), &got) != nil || got.Refs.CCN != "4f2a1b9" || len(got.Refs.PRs) != 0 {
		t.Fatalf("post = %q, %v; want a head naming note 4f2a1b9 and no pull request", out, err)
	}
	if out, err := run(t, "post", "--drive", "d", "--lane", "exp-a", "--kind", "opened", "--ccn", "4f2a1b9", "--text", "method note 4f2a1b9"); err == nil || !strings.Contains(err.Error(), "a opened record needs --pr") {
		t.Fatalf("opened post = %q, %v; want a refusal naming --pr", out, err)
	}
}

func TestPostSpillsTheStampedText(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	typed := time.Now().Add(7 * time.Minute).Format("3:04 PM")
	rest := "\n" + strings.Repeat("x", 500) + "\nlive since "
	got := postJSON(t, "R84 ("+typed+") orca-desk: relay to api"+rest+"{at}")
	first := "R84 (" + got.stamp() + ") orca-desk: relay to api"
	if want := first + " " + strings.Repeat("x", store.MaxText-2-utf8.RuneCountInString(first)) + "…"; got.Text != want {
		t.Errorf("spilled record text = %q, want %q", got.Text, want)
	}
	if body, err := os.ReadFile(got.Refs.Path); err != nil || string(body) != first+rest+got.stamp()+"\n" {
		t.Errorf("spilled file %q = %q, %v; want the stamped text", got.Refs.Path, body, err)
	}
	grown := postJSON(t, strings.Repeat("y", 396)+"{at}")
	if body, err := os.ReadFile(grown.Refs.Path); err != nil || string(body) != strings.Repeat("y", 396)+grown.stamp()+"\n" {
		t.Errorf("text that passes 400 characters once stamped spilled to %q = %q, %v", grown.Refs.Path, body, err)
	}
}

func TestPostPathStoresTheFilesFirstLineUnstamped(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	first := "R84 (" + time.Now().Add(7*time.Minute).Format("3:04 PM") + ") orca-desk: relay to api, sent {at}"
	body := first + "\n\n" + strings.Repeat("x", 500) + "\nlanded {at}\n"
	path := filepath.Join(t.TempDir(), "relay.md")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	got := postJSON(t, "--path", path)
	kept, err := os.ReadFile(path)
	if err != nil || string(kept) != body {
		t.Fatalf("--path file after the post = %q, %v; want it as written", kept, err)
	}
	if line, _, _ := strings.Cut(string(kept), "\n"); got.Text != first || got.Text != line || got.Refs.Path != path {
		t.Errorf("post --path stored %q at %q, want the file's first line %q at %q", got.Text, got.Refs.Path, line, path)
	}
	summary := postJSON(t, "--path", path, "--text", "relay summary, sent {at}")
	if want := "relay summary, sent " + summary.stamp(); summary.Text != want || summary.Refs.Path != path {
		t.Errorf("post --text with --path stored %q at %q, want %q at %q", summary.Text, summary.Refs.Path, want, path)
	}
}

func TestPostReadsItsTextFromStdinWhenItIsADash(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	for _, args := range [][]string{{"-"}, {"--text", "-"}} {
		var out bytes.Buffer
		root := cli.NewRootCmd()
		root.SetIn(strings.NewReader("relay to api landed {at}\n"))
		root.SetOut(&out)
		root.SetErr(&out)
		root.SetArgs(append([]string{"post", "--json", "--drive", "release-v3", "--lane", "root", "--kind", "note"}, args...))
		if err := root.Execute(); err != nil {
			t.Fatalf("post %v: %v", args, err)
		}
		var got posted
		if err := json.Unmarshal(out.Bytes(), &got); err != nil || got.Text != "relay to api landed "+got.stamp() {
			t.Errorf("post %v with a heredoc on stdin stored %q, %v; want the heredoc's text", args, got.Text, err)
		}
	}
}

func TestPostPathFromStdinOrAPipeKeepsTheBody(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	body := "relay summary\n\n" + strings.Repeat("x", 500) + "\n"
	post := []string{"post", "--json", "--drive", "release-v3", "--lane", "root", "--kind", "note"}
	stdin := func(args ...string) (posted, error) {
		var out bytes.Buffer
		root := cli.NewRootCmd()
		root.SetIn(strings.NewReader(body))
		root.SetOut(&out)
		root.SetErr(&out)
		root.SetArgs(append(post, args...))
		if err := root.Execute(); err != nil {
			return posted{}, err
		}
		var got posted
		err := json.Unmarshal(out.Bytes(), &got)
		return got, err
	}
	pipe := func() string {
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = r.Close() })
		if _, err := w.WriteString(body); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return fmt.Sprintf("/dev/fd/%d", r.Fd())
	}
	for _, tt := range []struct {
		name string
		args func() []string
		text string
	}{
		{"-", func() []string { return []string{"--path", "-"} }, "relay summary"},
		{"- with --text", func() []string { return []string{"--path", "-", "--text", "relay"} }, "relay"},
		{"a pipe", func() []string { return []string{"--path", pipe()} }, "relay summary"},
	} {
		got, err := stdin(tt.args()...)
		if err != nil {
			t.Fatalf("post --path %s: %v", tt.name, err)
		}
		kept, err := os.ReadFile(got.Refs.Path)
		if got.Text != tt.text || !strings.HasPrefix(got.Refs.Path, filepath.Join(home, "blobs")) || err != nil || string(kept) != body {
			t.Errorf("post --path %s stored %q at %q holding %q, %v; want %q at a blob holding the body", tt.name, got.Text, got.Refs.Path, kept, err, tt.text)
		}
	}
	if _, err := stdin("--path", os.DevNull, "--text", "relay"); err == nil || !strings.Contains(err.Error(), "empty body") {
		t.Errorf("post --path %s: %v, want the empty-body refusal", os.DevNull, err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "relay.md"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)
	got, err := stdin("--path", "relay.md")
	if err != nil {
		t.Fatalf("post --path relay.md: %v", err)
	}
	want, _ := os.Stat(filepath.Join(dir, "relay.md"))
	if stored, err := os.Stat(got.Refs.Path); !filepath.IsAbs(got.Refs.Path) || err != nil || !os.SameFile(stored, want) {
		t.Errorf("post --path relay.md stored %q, %v; want the file's absolute path", got.Refs.Path, err)
	}
}

func TestPostWithAnIDReturnsTheStoredStampedRecord(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	ahead := time.Now().Add(7 * time.Minute)
	first := postJSON(t, "--id", "relay-84", "R84 ("+ahead.Format("3:04 PM")+") orca-desk: relay to api")
	if want := "R84 (" + first.stamp() + ") orca-desk: relay to api"; first.Text != want {
		t.Fatalf("post --id stored %q, want %q", first.Text, want)
	}
	again := postJSON(t, "--id", "relay-84", "R84 ("+ahead.Add(7*time.Minute).Format("15:04")+") orca-desk: relay to api, retried")
	if again.Seq != first.Seq || again.Text != first.Text {
		t.Errorf("repost with the same --id = #%d %q, want the stored #%d %q", again.Seq, again.Text, first.Seq, first.Text)
	}
}

func TestAnnounceMeWho(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	t.Setenv("CLAUDE_CODE_SESSION_ID", "aaaa1111-2222-3333")
	for _, name := range peers.IdentityEnv {
		t.Setenv(name, "")
	}
	t.Setenv("ORCA_LAUNCH_RUN", "run_7")
	cfg := t.TempDir()
	t.Setenv("CLAUDE_CONFIG_DIR", cfg)
	if err := os.MkdirAll(filepath.Join(cfg, "sessions"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cfg, "sessions", "5.json"), []byte(`{"pid":5,"sessionId":"bbbb4444-5555-6666","cwd":"/w/sole","startedAt":1,"messagingSocketPath":"/tmp/cc-socks/5.sock","name":"sole-51","status":"busy","statusUpdatedAt":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := run(t, "me"); err == nil || !strings.Contains(err.Error(), "not in the registry") {
		t.Fatalf("me before announce = %q, %v", out, err)
	}
	if _, err := run(t, "drive", "use", "release-v3", "--root"); err != nil {
		t.Fatal(err)
	}
	out, err := run(t, "announce", "--desc", "mesh discovery", "--tag", "mesh", "--tag", "cci")
	if err != nil || !strings.HasPrefix(out, "- root drive=release-v3 ") || !strings.Contains(out, " wt=") || !strings.Contains(out, ` "mesh discovery" #mesh #cci session=aaaa1111`) {
		t.Fatalf("announce = %q, %v", out, err)
	}
	me, err := run(t, "me")
	if err != nil || me != out {
		t.Fatalf("me = %q, %v; announce printed %q", me, err, out)
	}
	if spoke, err := run(t, "tail", "--since", "0", "--kind", "peer", "--reader", "root"); err != nil || !strings.HasPrefix(spoke, "#1 ") || !strings.Contains(spoke, " PEER registry - root drive=release-v3 ") {
		t.Fatalf("announce did not broadcast a peer record: %q, %v", spoke, err)
	}
	tests := []struct {
		args []string
		want []string
	}{
		{[]string{"who", "--all"}, []string{"- root drive=release-v3", "sole-51 session cwd=/w/sole gone"}},
		{[]string{"who", "--all", "--tag", "mesh"}, []string{"- root drive=release-v3"}},
		{[]string{"who", "--all", "--name", "sole-51"}, []string{"sole-51 session"}},
		{[]string{"who", "--all", "--session", "bbbb"}, []string{"sole-51 session"}},
		{[]string{"who", "--all", "-q", "discovery"}, []string{"- root drive=release-v3"}},
		{[]string{"who", "--all", "--role", "watcher"}, []string{"no matching peers"}},
	}
	for _, tt := range tests {
		out, err := run(t, tt.args...)
		if err != nil || strings.Count(out, "\n") != len(tt.want) {
			t.Fatalf("%v = %q, %v; want %d lines", tt.args, out, err, len(tt.want))
		}
		for _, w := range tt.want {
			if !strings.Contains(out, w) {
				t.Fatalf("%v = %q; want %q", tt.args, out, w)
			}
		}
	}
	out, err = run(t, "who", "--all", "--json", "--session", "aaaa")
	if err != nil || !strings.Contains(out, `"env":{"ORCA_LAUNCH_RUN":"run_7"}`) || !strings.Contains(out, `"tags":["mesh","cci"]`) {
		t.Fatalf("who --json = %q, %v", out, err)
	}
}

func TestClaimAndWhoByRef(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	t.Setenv("CLAUDE_CODE_SESSION_ID", "aaaa1111-2222-3333")
	t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())
	for _, name := range peers.IdentityEnv {
		t.Setenv(name, "")
	}
	if _, err := run(t, "drive", "use", "release-v3", "--root"); err != nil {
		t.Fatal(err)
	}
	if _, err := run(t, "post", "--lane", "root", "--kind", "opened", "--pr", "34363", "--text", "fix"); err != nil {
		t.Fatal(err)
	}
	if out, err := run(t, "claim"); err == nil || !strings.Contains(err.Error(), "name at least one artifact") {
		t.Fatalf("claim without artifacts = %q, %v", out, err)
	}
	if out, err := run(t, "claim", "--rel", "watches", "--pr", "34370", "--ref", "thread:C1/1.2"); err != nil || out != "watches pr:34370\nwatches thread:C1/1.2\n" {
		t.Fatalf("claim = %q, %v", out, err)
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "cccc3333-never-announced")
	if _, err := run(t, "claim", "--pr", "34370"); err != nil {
		t.Fatal(err)
	}
	if out, err := run(t, "who", "--all", "--pr", "34370", "--session", "cccc"); err != nil || !strings.Contains(out, "session=cccc3333") || !strings.Contains(out, "  owns pr:34370 ") {
		t.Fatalf("claim from an unannounced session is invisible: %q, %v", out, err)
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "aaaa1111-2222-3333")
	tests := []struct {
		args []string
		want []string
	}{
		{[]string{"who", "--all", "--pr", "34363"}, []string{"- root drive=release-v3", "  opened pr:34363 ", " (#1)"}},
		{[]string{"who", "--all", "--pr", "34370"}, []string{"  watches pr:34370 "}},
		{[]string{"who", "--all", "--ref", "thread:C1/1.2", "--rel", "watches"}, []string{"  watches thread:C1/1.2 "}},
		{[]string{"who", "--all", "--pr", "34370", "--rel", "opened"}, []string{"no matching peers"}},
	}
	for _, tt := range tests {
		out, err := run(t, tt.args...)
		if err != nil {
			t.Fatalf("%v: %v", tt.args, err)
		}
		for _, w := range tt.want {
			if !strings.Contains(out, w) {
				t.Fatalf("%v = %q; want %q", tt.args, out, w)
			}
		}
	}
	if _, err := run(t, "who", "--pr", "1", "--build", "2"); err == nil || !strings.Contains(err.Error(), "one of") {
		t.Fatalf("who with two refs: %v", err)
	}
	if out, err := run(t, "unclaim", "--rel", "watches", "--pr", "34370", "--pr", "1"); err != nil || out != "watches pr:34370 removed\nwatches pr:1 no such claim\n" {
		t.Fatalf("unclaim = %q, %v", out, err)
	}
	if out, err := run(t, "who", "--all", "--json", "--pr", "34363"); err != nil || !strings.Contains(out, `"claims":[{"session":"aaaa1111-2222-3333","rel":"opened","kind":"pr","key":"34363"`) {
		t.Fatalf("who --json --pr = %q, %v", out, err)
	}
}

func TestPostToPeerAcrossDrives(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())
	for _, name := range peers.IdentityEnv {
		t.Setenv(name, "")
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "aaaa1111-root-a")
	if _, err := run(t, "drive", "use", "drive-a", "--root"); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "bbbb2222-root-b")
	if _, err := run(t, "drive", "use", "drive-b", "--root"); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "aaaa1111-root-a")
	if _, err := run(t, "post", "--lane", "root", "--kind", "ask", "--to", "peer:zzzz", "--text", "anyone?"); err == nil || !strings.Contains(err.Error(), "no session has that id prefix or name") {
		t.Fatalf("post to unknown peer: %v", err)
	}
	if _, err := run(t, "post", "--lane", "root", "--kind", "go", "--to", "@drive-b", "--text", "x"); err == nil || !strings.Contains(err.Error(), "<lane>@<drive>") {
		t.Fatalf("post to a bare @drive: %v", err)
	}
	if out, err := run(t, "post", "--lane", "root", "--kind", "ask", "--to", "peer:bbbb", "--text", "take #34363?"); err != nil || out != "#1\n" {
		t.Fatalf("post to peer = %q, %v", out, err)
	}
	if out, err := run(t, "post", "--lane", "root", "--kind", "go", "--to", "root@drive-b", "--text", "GO"); err != nil || out != "#2\n" {
		t.Fatalf("post to root@drive = %q, %v", out, err)
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "bbbb2222-root-b")
	out, err := run(t, "tail", "--since", "0", "--reader", "root")
	if err != nil || !strings.Contains(out, "ASK root -> peer:bbbb2222 take #34363?") || !strings.Contains(out, "GO root -> root@drive-b GO") || strings.Count(out, "\n") != 2 {
		t.Fatalf("b tail = %q, %v", out, err)
	}
	if out, err = run(t, "post", "--lane", "root", "--kind", "answer", "--resolves", "1", "--to", "peer:aaaa", "--text", "yes"); err != nil || out != "#3\n" {
		t.Fatalf("answer across drives = %q, %v", out, err)
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "aaaa1111-root-a")
	if out, err = run(t, "tail", "--since", "0"); err != nil || !strings.Contains(out, "ANSWER root -> peer:aaaa1111 yes resolves #1") || !strings.Contains(out, "take #34363? [RESOLVED #3]") {
		t.Fatalf("a tail = %q, %v", out, err)
	}
	if out, err = run(t, "tail", "--since", "0", "--to", "root@drive-b"); err != nil || strings.Count(out, "\n") != 1 || !strings.Contains(out, "GO root -> root@drive-b") {
		t.Fatalf("tail --to root@drive-b = %q, %v", out, err)
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "bbbb2222-root-b")
	if out, err = run(t, "tail", "--since", "0", "--reader", "root", "--to", "api"); err != nil || out != "" {
		t.Fatalf("tail --reader root --to api leaked a root@drive-b record: %q, %v", out, err)
	}
	if out, err = run(t, "state"); err != nil || !strings.Contains(out, "no head, contract, or state") {
		t.Fatalf("state = %q, %v", out, err)
	}
}

func TestPostAddressesALaneInTheDriveItReads(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())
	for _, name := range peers.IdentityEnv {
		t.Setenv(name, "")
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "aaaa1111-root-a")
	if _, err := run(t, "drive", "use", "drive-a", "--root"); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	st, err := store.Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range []struct{ session, drive, lane string }{
		{"bbbb2222-owner", "drive-b", "release-owner-4"},
		{"bbbb2222-desk", "drive-b", "desk"},
		{"aaaa1111-desk", "drive-a", "desk"},
		{"bbbb2222-walker", "drive-b", "walker"},
		{"cccc3333-walker", "drive-c", "walker"},
		{"bbbb2222-root", "drive-b", "root"},
	} {
		k := store.PeerKey{Session: b.session, Agent: "a" + b.lane}
		if err := st.BindLane(ctx, k, b.drive, b.lane, time.Now()); err != nil {
			t.Fatal(err)
		}
		if _, err := st.Announce(ctx, store.Peer{Session: k.Session, Agent: k.Agent}); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := run(t, "post", "--lane", "root", "--kind", "go", "--to", "release-owner,desk", "--text", "RELEASE #45190"); err == nil || err.Error() != "--to release-owner: no lane on drive drive-a has registered as release-owner, which looks like a slip for the live lane release-owner-4; --force addresses release-owner as typed" {
		t.Fatalf("post to a lane nobody holds: %v", err)
	}
	for _, to := range []string{"owner", "all"} {
		if _, err := run(t, "post", "--lane", "root", "--kind", "ask", "--to", to, "--text", "approve?"); err != nil {
			t.Fatalf("post --to %s: %v", to, err)
		}
	}
	out, err := run(t, "post", "--lane", "root", "--kind", "go", "--to", "release-owner-4,desk,root,nobody", "--text", "GO promote")
	if err != nil || out != "cci: --to release-owner-4: release-owner-4 reads drive drive-b, not drive-a; addressed release-owner-4@drive-b\n#3\n" {
		t.Fatalf("post to a lane reading another drive = %q, %v", out, err)
	}
	if _, err := run(t, "post", "--lane", "root", "--kind", "go", "--to", "walker", "--text", "x"); err == nil || !strings.Contains(err.Error(), "read drive-b, drive-c; address one as walker@<drive>") {
		t.Fatalf("post to a lane live in two other drives: %v", err)
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "bbbb2222-owner")
	if out, err := run(t, "tail", "--drive", "drive-b", "--since", "0", "--reader", "release-owner-4"); err != nil || !strings.Contains(out, "GO root -> release-owner-4@drive-b,desk,root,nobody GO promote") || strings.Count(out, "\n") != 1 {
		t.Fatalf("drive-b reader release-owner-4 = %q, %v", out, err)
	}
	if out, err := run(t, "tail", "--drive", "drive-b", "--since", "0", "--reader", "desk"); err != nil || out != "" {
		t.Fatalf("drive-b reader desk got a record addressed to drive-a's desk: %q, %v", out, err)
	}
}

func TestAgentThreadsClaimAndAreAddressedApart(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())
	for _, name := range peers.IdentityEnv {
		t.Setenv(name, "")
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "aaaa1111-2222-3333")
	t.Setenv("CCI_AGENT_ID", "")
	if _, err := run(t, "drive", "use", "mesh", "--root"); err != nil {
		t.Fatal(err)
	}
	if _, err := run(t, "claim", "--pr", "5"); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CCI_AGENT_ID", "aapi-0123")
	if out, err := run(t, "claim", "--rel", "watches", "--pr", "5"); err != nil || out != "watches pr:5\n" {
		t.Fatalf("agent claim = %q, %v", out, err)
	}
	if me, err := run(t, "me"); err != nil || !strings.HasPrefix(me, "- agent ") || !strings.Contains(me, " session=aaaa1111/aapi-0123") {
		t.Fatalf("agent me = %q, %v", me, err)
	}
	out, err := run(t, "who", "--all", "--pr", "5")
	if err != nil || !strings.Contains(out, "session=aaaa1111/aapi-0123\n  watches pr:5 ") || !strings.Contains(out, "session=aaaa1111\n  owns pr:5 ") {
		t.Fatalf("who --pr 5 = %q, %v; want the root and its agent apart", out, err)
	}
	if out, err := run(t, "who", "--all", "--agent", "aapi-0123"); err != nil || strings.Count(out, "\n") != 1 || !strings.Contains(out, "/aapi-0123") {
		t.Fatalf("who --agent = %q, %v", out, err)
	}
	if out, err := run(t, "unclaim", "--rel", "owns", "--pr", "5"); err != nil || out != "owns pr:5 no such claim\n" {
		t.Fatalf("agent unclaimed the root's claim: %q, %v", out, err)
	}
	t.Setenv("CCI_AGENT_ID", "")
	if out, err := run(t, "post", "--lane", "root", "--kind", "ask", "--to", "peer:aaaa/aapi-0123", "--text", "status?"); err != nil || out != "#1\n" {
		t.Fatalf("post to an agent = %q, %v", out, err)
	}
	if _, err := run(t, "post", "--lane", "root", "--kind", "ask", "--to", "peer:aaaa/nobody", "--text", "x"); err == nil || !strings.Contains(err.Error(), "no agent of it") {
		t.Fatalf("post to an unknown agent: %v", err)
	}
	if out, err := run(t, "post", "--lane", "root", "--kind", "ask", "--to", "peer:aaaa", "--text", "root?"); err != nil || out != "#2\n" {
		t.Fatalf("post to the root thread = %q, %v", out, err)
	}
	t.Setenv("CCI_AGENT_ID", "aapi-0123")
	out, err = run(t, "tail", "--drive", "mesh", "--since", "0", "--reader", "api")
	if err != nil || !strings.Contains(out, "ASK root -> peer:aaaa1111/aapi-0123 status?") || strings.Contains(out, "root?") {
		t.Fatalf("agent reader = %q, %v", out, err)
	}
}

func TestPostReachesOwnWorkflowAgentByLabel(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())
	for _, name := range peers.IdentityEnv {
		t.Setenv(name, "")
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "aaaa1111-2222-3333")
	t.Setenv("CCI_AGENT_ID", "")
	if _, err := run(t, "drive", "use", "mesh", "--root"); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	st, err := store.Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	for _, agent := range []string{"aold-0001", "anew-0002"} {
		if _, err := st.Announce(ctx, store.Peer{Session: "aaaa1111-2222-3333", Agent: agent, AgentName: "apply:api", Role: peers.RoleWorkflow}); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Retire(ctx, store.PeerKey{Session: "aaaa1111-2222-3333", Agent: "aold-0001"}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if out, err := run(t, "post", "--lane", "root", "--kind", "go", "--to", "peer:apply:api", "--json", "--text", "ship it"); err != nil || !strings.Contains(out, `"peer:aaaa1111-2222-3333/anew-0002"`) {
		t.Fatalf("post to the live agent labelled apply:api = %q, %v", out, err)
	}
	if _, err := run(t, "post", "--lane", "root", "--kind", "go", "--to", "peer:apply:web", "--text", "x"); err == nil || !strings.Contains(err.Error(), "no agent of this session") {
		t.Fatalf("post to an unknown label: %v", err)
	}
}

func TestPostAndTailTakeQualifiedPRs(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	t.Setenv("CLAUDE_CODE_SESSION_ID", "session-1")
	if _, err := run(t, "drive", "use", "release-v3", "--root"); err != nil {
		t.Fatal(err)
	}
	if out, err := run(t, "post", "--lane", "fde", "--kind", "opened", "--pr", "Forge-FDE/iris-chime#243", "--pr", "https://github.com/Forge-AI/monorepo/pull/35198/files", "--pr", "30431", "--text", "chime fix"); err != nil || out != "#1\n" {
		t.Fatalf("post = %q, %v", out, err)
	}
	out, err := run(t, "tail", "--since", "0")
	if err != nil || !strings.Contains(out, "chime fix pr:Forge-FDE/iris-chime#243 pr:Forge-AI/monorepo#35198 pr#30431") {
		t.Fatalf("tail = %q, %v", out, err)
	}
	for _, pr := range []string{"Forge-FDE/iris-chime#243", "https://github.com/Forge-FDE/iris-chime/pull/243", "30431", "243"} {
		if out, err := run(t, "tail", "--since", "0", "--pr", pr); err != nil || strings.Count(out, "\n") != 1 || !strings.Contains(out, "chime fix") {
			t.Fatalf("tail --pr %s = %q, %v", pr, out, err)
		}
	}
	if out, err := run(t, "tail", "--since", "0", "--pr", "24"); err != nil || strings.Contains(out, "chime fix") {
		t.Fatalf("tail --pr 24 = %q, %v", out, err)
	}
	if _, err := run(t, "post", "--lane", "fde", "--kind", "opened", "--pr", "iris-chime#243", "--text", "x"); err == nil || !strings.Contains(err.Error(), "owner/repo#N") {
		t.Fatalf("post --pr iris-chime#243: %v", err)
	}
}

func TestClaimOpenedPostsAnOpenedRecordOnTheSessionsDrive(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())
	for _, name := range peers.IdentityEnv {
		t.Setenv(name, "")
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "dddd4444-unbound")
	t.Setenv("CCI_AGENT_ID", "")
	if out, err := run(t, "claim", "--rel", "opened", "--pr", "7"); err != nil || out != "opened pr:7\n" {
		t.Fatalf("unbound claim = %q, %v", out, err)
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "aaaa1111-2222-3333")
	if _, err := run(t, "drive", "use", "release-v3", "--root"); err != nil {
		t.Fatal(err)
	}
	if _, err := run(t, "claim", "--rel", "opened", "--pr", "36276"); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CCI_AGENT_ID", "aspeed-0123")
	if _, err := run(t, "claim", "--rel", "opened", "--pr", "36148"); err != nil {
		t.Fatal(err)
	}
	if _, err := run(t, "claim", "--rel", "opened", "--pr", "36148"); err != nil {
		t.Fatal(err)
	}
	if _, err := run(t, "claim", "--rel", "watches", "--pr", "36149"); err != nil {
		t.Fatal(err)
	}
	out, err := run(t, "tail", "--drive", "release-v3", "--since", "0", "--kind", "opened", "--json")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	got := make([]string, 0, len(lines))
	for _, line := range lines {
		var r struct {
			Lane string `json:"lane"`
			Text string `json:"text"`
			Refs struct {
				PRs []int `json:"prs"`
			} `json:"refs"`
		}
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("%q: %v", line, err)
		}
		got = append(got, fmt.Sprintf("%s %s %v", r.Lane, r.Text, r.Refs.PRs))
	}
	want := []string{"root opened #36276 [36276]", "root opened #36148 [36148]"}
	if !slices.Equal(got, want) {
		t.Fatalf("opened records = %q, want %q", got, want)
	}
}

func TestPostBindsACodexSessionsLaneSoClaimOpenedPostsUnderIt(t *testing.T) {
	if os.Getenv("CCI_TEST_CODEX_LANE") == "1" {
		if _, err := run(t, "post", "--drive", "release-v3", "--lane", "inc-unrun-fix-2", "--kind", "mechanism", "--text", "MECHANISM inc-unrun-fix-2: x"); err != nil {
			t.Fatal(err)
		}
		if _, err := run(t, "claim", "--rel", "opened", "--pr", "40359"); err != nil {
			t.Fatal(err)
		}
		return
	}
	home, config := t.TempDir(), t.TempDir()
	codex := exec.Command(os.Args[0], "-test.run=^TestPostBindsACodexSessionsLaneSoClaimOpenedPostsUnderIt$", "codex")
	codex.Env = append(os.Environ(), "CCI_TEST_CODEX_LANE=1", "CCI_HOME="+home, "CLAUDE_CONFIG_DIR="+config, "CLAUDE_CODE_SESSION_ID=", "CCI_AGENT_ID=", "CODEX_THREAD_ID=019a3c3e-codex-thread")
	for _, name := range peers.IdentityEnv {
		if name != "CODEX_THREAD_ID" {
			codex.Env = append(codex.Env, name+"=")
		}
	}
	if out, err := codex.CombinedOutput(); err != nil {
		t.Fatalf("codex session: %v\n%s", err, out)
	}
	t.Setenv("CCI_HOME", home)
	t.Setenv("CLAUDE_CONFIG_DIR", config)
	for _, name := range peers.IdentityEnv {
		t.Setenv(name, "")
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "eeee5555-reader")
	out, err := run(t, "tail", "--drive", "release-v3", "--since", "0", "--kind", "opened", "--json")
	if err != nil {
		t.Fatal(err)
	}
	var r struct {
		Lane string `json:"lane"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &r); err != nil || r.Lane != "inc-unrun-fix-2" || r.Text != "opened #40359" {
		t.Fatalf("opened = %q (%+v), %v", out, r, err)
	}
}

func TestClaimForALaneTiesTheLanesSession(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())
	for _, name := range peers.IdentityEnv {
		t.Setenv(name, "")
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	t.Setenv("CCI_AGENT_ID", "")
	ctx := context.Background()
	st, err := store.Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range []struct{ session, agent, drive, lane string }{
		{"bbbb2222-walker", "awalker", "drive-b", "walker"},
		{"cccc3333-walker", "awalker", "drive-c", "walker"},
		{"bbbb2222-desk", "adesk-1", "drive-b", "desk"},
		{"bbbb2222-desk", "adesk-2", "drive-b", "desk"},
	} {
		k := store.PeerKey{Session: b.session, Agent: b.agent}
		if err := st.BindLane(ctx, k, b.drive, b.lane, time.Now()); err != nil {
			t.Fatal(err)
		}
		if _, err := st.Announce(ctx, store.Peer{Session: k.Session, Agent: k.Agent}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.Announce(ctx, store.Peer{Session: "aaaa1111-root", Agent: "asuccessor-2-0123", AgentName: "successor-2"}); err != nil {
		t.Fatal(err)
	}
	if err := st.Bind(ctx, store.PeerKey{Session: "eeee5555-root"}, "drive-c", true); err != nil {
		t.Fatal(err)
	}
	for _, p := range []store.Peer{{Session: "eeee5555-root"}, {Session: "eeee5555-root", Agent: "aother-1", AgentName: "other"}} {
		if _, err := st.Announce(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if out, err := run(t, "claim", "--for", "successor-2@drive-b", "--pr", "owner/repo#9"); err != nil || out != "owns pr:owner/repo#9\n" {
		t.Fatalf("claim --for a teammate that never posted = %q, %v", out, err)
	}
	if out, err := run(t, "tail", "--drive", "drive-b", "--since", "0"); err != nil || !strings.Contains(out, "CLAIM successor-2 claim: successor-2 owns #owner/repo#9") {
		t.Fatalf("claim record for a teammate that never posted = %q, %v", out, err)
	}
	if out, err := run(t, "who", "--drive", "drive-b", "--lane", "successor-2", "--json"); err != nil || !strings.Contains(out, `"agent":"asuccessor-2-0123"`) {
		t.Fatalf("who --lane after claim --for bound the teammate = %q, %v", out, err)
	}
	if out, err := run(t, "claim", "--for", "walker@drive-b", "--build", "8748", "--rel", "started"); err != nil || out != "started build:8748\n" {
		t.Fatalf("claim --for = %q, %v", out, err)
	}
	out, err := run(t, "who", "--all", "--json", "--build", "8748")
	if err != nil || !strings.Contains(out, `"session":"bbbb2222-walker"`) || strings.Contains(out, "cccc3333-walker") || strings.Count(out, "\n") != 1 {
		t.Fatalf("who --build after claim --for = %q, %v", out, err)
	}
	for _, tt := range []struct{ lane, want string }{
		{"walker", "name the lane as <lane>@<drive>"},
		{"@drive-b", "name the lane as <lane>@<drive>"},
		{"ghost@drive-b", "no live session on this machine is bound as lane ghost in drive drive-b"},
		{"desk@drive-b", "2 live sessions are bound as that lane"},
		{"other@drive-b", "no one live teammate is named other with no lane yet"},
	} {
		if out, err := run(t, "claim", "--for", tt.lane, "--build", "1"); err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Fatalf("claim --for %s = %q, %v; want %q", tt.lane, out, err, tt.want)
		}
	}
	if out, err := run(t, "unclaim", "--for", "walker@drive-b", "--build", "8748", "--rel", "started"); err != nil || out != "started build:8748 removed\n" {
		t.Fatalf("unclaim --for = %q, %v", out, err)
	}
}

func TestSubagentCannotReplaceTheWindowSubscription(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "session-1")
	t.Setenv("CCI_AGENT_ID", "aconversion-0123")
	for _, args := range [][]string{
		{"subscribe", "--drive", "d", "--reader", "conversion", "--kind", "review"},
		{"unsubscribe"},
	} {
		if _, err := run(t, args...); err == nil || !strings.Contains(err.Error(), "a subagent or teammate cannot subscribe") {
			t.Fatalf("%v from a subagent = %v", args, err)
		}
	}
	if _, err := os.Stat(filepath.Join(home, "subscriptions")); !os.IsNotExist(err) {
		t.Fatalf("a subagent touched the subscriptions: %v", err)
	}
}

func TestRepeatedResolvesClosesEverySeq(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	t.Setenv("CLAUDE_CODE_SESSION_ID", "session-1")
	t.Setenv("CCI_AGENT_ID", "")
	if _, err := run(t, "drive", "use", "d", "--root"); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"first?", "second?", "third?"} {
		if _, err := run(t, "post", "--lane", "lane-a", "--kind", "ask", "--to", "root", "--text", text); err != nil {
			t.Fatal(err)
		}
	}
	out, err := run(t, "post", "--lane", "root", "--kind", "note", "--resolves", "1", "--resolves", "2,3,2", "--id", "sweep", "--text", "stale")
	if err != nil || out != "#4\n" {
		t.Fatalf("post with three --resolves = %q, %v; want one record", out, err)
	}
	if out, err := run(t, "post", "--lane", "root", "--kind", "note", "--resolves", "1,2,3", "--id", "sweep", "--text", "stale"); err != nil || out != "#4 (duplicate)\n" {
		t.Fatalf("repost = %q, %v", out, err)
	}
	if out, err := run(t, "tail", "--since", "0", "--kind", "ask"); err != nil || strings.Count(out, "[RESOLVED #4]") != 3 {
		t.Fatalf("tail after closing all three = %q, %v", out, err)
	}
	if out, err := run(t, "tail", "--since", "0", "--kind", "note"); err != nil || strings.Count(out, "\n") != 1 || !strings.Contains(out, "stale resolves #1,#2,#3") {
		t.Fatalf("tail of the closing note = %q, %v; want one line naming all three", out, err)
	}
	if out, err := run(t, "digest"); err != nil || strings.Contains(out, "open asks") {
		t.Fatalf("digest after closing all three = %q, %v; want no open asks", out, err)
	}
	if _, err := run(t, "post", "--lane", "root", "--kind", "answer", "--re", "1", "--re", "2", "--resolves", "1", "--resolves", "3", "--text", "x"); err == nil || !strings.Contains(err.Error(), "not both") {
		t.Fatalf("several --re and --resolves together = %v", err)
	}
}

func TestClaimsPostAsTheOwningLane(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())
	for _, name := range peers.IdentityEnv {
		t.Setenv(name, "")
	}
	ctx := context.Background()
	st, err := store.Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.BindLane(ctx, store.PeerKey{Session: "bbbb2222-lane"}, "drive-a", "web", time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Announce(ctx, store.Peer{Session: "bbbb2222-lane"}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "bbbb2222-lane")
	t.Setenv("CCI_AGENT_ID", "ada0addbf41f1f196")
	if _, err := run(t, "claim", "--rel", "opened", "--pr", "owner/repo#7"); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "aaaa1111-root")
	t.Setenv("CCI_AGENT_ID", "")
	if _, err := run(t, "claim", "--for", "web@drive-a", "--pr", "8"); err != nil {
		t.Fatal(err)
	}
	out, err := run(t, "tail", "--drive", "drive-a", "--since", "0")
	if err != nil || !strings.Contains(out, "OPENED web opened #owner/repo#7") || !strings.Contains(out, "CLAIM web claim: web owns #8") || strings.Contains(out, "ada0addbf41f1f196") {
		t.Fatalf("records of a lane subagent's claims = %q, %v; want both posted as lane web", out, err)
	}
	out, err = run(t, "who", "--all", "--pr", "owner/repo#7")
	if err != nil || !strings.Contains(out, " lane=web ") || !strings.Contains(out, "session=bbbb2222\n  opened pr:owner/repo#7 ") || strings.Contains(out, "ada0addbf41f1f196") {
		t.Fatalf("who --pr owner/repo#7 = %q, %v; want the lane's thread", out, err)
	}
}

func TestShowPrintsRecordsWholeInTheOrderAsked(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	long := strings.Repeat("a long finding about the stack cap ", 20) + "THE END"
	for _, text := range []string{long, "short note"} {
		if _, err := run(t, "post", "--drive", "release-v3", "--lane", "root", "--kind", "note", "--text", text); err != nil {
			t.Fatal(err)
		}
	}

	out, err := run(t, "show", "#2", "1")
	if err != nil {
		t.Fatal(err)
	}
	short, end := strings.Index(out, "#2 "), strings.Index(out, "THE END")
	if short < 0 || end < 0 || short > strings.Index(out, "#1 ") {
		t.Errorf("show #2 1 = %q, want #2 first, then #1 with its text through THE END", out)
	}
	if _, err := run(t, "show", "9"); err == nil || !strings.Contains(err.Error(), "no record #9") {
		t.Errorf("show 9 = %v, want it to name the missing record", err)
	}
}

func TestPostDefaultsOnlyToABoundLaneNeverToRoot(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	t.Setenv("CCI_AGENT_ID", "")
	t.Setenv("CLAUDE_CODE_SESSION_ID", "root-session")
	if _, err := run(t, "drive", "use", "release-v3", "--root"); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(context.Background(), home)
	if err != nil {
		t.Fatal(err)
	}
	for session, lane := range map[string]string{"lane-session": "landing-desk", "lane-root-session": "root"} {
		if err := st.BindLane(context.Background(), store.PeerKey{Session: session}, "release-v3", lane, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	_ = st.Close()
	for _, tt := range []struct{ session, want string }{
		{"root-session", ""},
		{"lane-root-session", ""},
		{"lane-session", "landing-desk"},
		{"unbound-session", ""},
	} {
		t.Setenv("CLAUDE_CODE_SESSION_ID", tt.session)
		out, err := run(t, "post", "--json", "--drive", "release-v3", "--kind", "note", "--text", "posted with no --lane")
		if tt.want == "" {
			if err == nil || !strings.Contains(err.Error(), "post needs --lane") {
				t.Fatalf("%s: post = %q, %v; want a refusal naming --lane", tt.session, out, err)
			}
			continue
		}
		var r struct {
			Lane string `json:"lane"`
		}
		if err != nil || json.Unmarshal([]byte(out), &r) != nil || r.Lane != tt.want {
			t.Fatalf("%s: post = %q, %v; want lane %s", tt.session, out, err, tt.want)
		}
	}
}

func TestALanesOwnReaderTailAdvancesItsHookDelivery(t *testing.T) {
	t.Setenv("CCI_AGENT_ID", "")
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	st, err := store.Open(context.Background(), home)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.BindLane(context.Background(), store.PeerKey{Session: "lane-session"}, "d", "lane-x", time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := st.SetCursor(context.Background(), "lane-x", "d", 0); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()
	t.Setenv("CLAUDE_CODE_SESSION_ID", "root-session")
	if _, err := run(t, "post", "--drive", "d", "--lane", "root", "--kind", "go", "--to", "lane-x", "--text", "take #12"); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "lane-session")
	if out, err := run(t, "tail", "--drive", "d", "--reader", "lane-x", "--cursor", "lane-x"); err != nil || !strings.Contains(out, "take #12") {
		t.Fatalf("tail --reader lane-x = %q, %v", out, err)
	}
	var out bytes.Buffer
	root := cli.NewRootCmd()
	root.SetIn(strings.NewReader(`{"session_id":"lane-session","hook_event_name":"PostToolUse"}`))
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs([]string{"hook", "post-tool-use"})
	if err := root.Execute(); err != nil || out.Len() != 0 {
		t.Fatalf("hook after the lane's own tail = %q, %v; want nothing redelivered", out.String(), err)
	}
}

func TestTailCutsResolvedRecordsUnlessAll(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	t.Setenv("CLAUDE_CODE_SESSION_ID", "session-1")
	if _, err := run(t, "drive", "use", "release-v3", "--root"); err != nil {
		t.Fatal(err)
	}
	question := "Build the single reader anyway? " + strings.Repeat("measured detail ", 15) + "| options: A no new reader; B build it"
	for _, args := range [][]string{
		{"post", "--lane", "lane-a", "--kind", "ask", "--to", "root", "--text", question},
		{"post", "--lane", "lane-a", "--kind", "ask", "--to", "root", "--text", "still open " + question},
		{"post", "--lane", "root", "--kind", "answer", "--re", "1", "--resolves", "1", "--text", "A"},
	} {
		if _, err := run(t, args...); err != nil {
			t.Fatal(err)
		}
	}
	out, err := run(t, "tail", "--since", "0")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 4 || utf8.RuneCountInString(strings.TrimSuffix(lines[0], " [RESOLVED #3]")) != 160 || !strings.HasSuffix(lines[0], "… [RESOLVED #3]") ||
		!strings.HasSuffix(lines[1], "B build it") || lines[3] != "... 1 resolved records cut to 160 characters; cci show <seq> prints one whole, --all prints them whole" {
		t.Fatalf("tail = %q", out)
	}
	if out, err = run(t, "tail", "--since", "0", "--all"); err != nil || strings.Count(out, "B build it") != 2 || strings.Contains(out, "cut to") {
		t.Fatalf("tail --all = %q, %v", out, err)
	}
}

func TestPostReachesALaneThatHasNotStartedYet(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())
	for _, name := range peers.IdentityEnv {
		t.Setenv(name, "")
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "aaaa1111-root")
	t.Setenv("CCI_AGENT_ID", "")
	if _, err := run(t, "drive", "use", "d", "--root"); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	st, err := store.Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	for _, lane := range []string{"gate-fault-5", "n-enum-45410-1"} {
		k := store.PeerKey{Session: "bbbb2222-" + lane, Agent: "a" + lane}
		if err := st.BindLane(ctx, k, "d", lane, time.Now()); err != nil {
			t.Fatal(err)
		}
		if _, err := st.Announce(ctx, store.Peer{Session: k.Session, Agent: k.Agent}); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	for _, to := range []string{"gate-fault-6", "n-enum-45414-1", "fresh-desk"} {
		if out, err := run(t, "post", "--lane", "root", "--kind", "go", "--to", to, "--text", "GO start "+to); err != nil || !strings.HasPrefix(out, "#") {
			t.Fatalf("post --to %s, a lane that has not started = %q, %v", to, out, err)
		}
	}
	for _, to := range []string{"gate-fault-4", "gate-falt-5"} {
		if _, err := run(t, "post", "--lane", "root", "--kind", "go", "--to", to, "--text", "x"); err == nil || !strings.Contains(err.Error(), "slip for the live lane gate-fault-5") {
			t.Fatalf("post --to %s, a slip for gate-fault-5: %v", to, err)
		}
		if _, err := run(t, "post", "--lane", "root", "--kind", "go", "--force", "--to", to, "--text", "x"); err != nil {
			t.Fatalf("post --force --to %s: %v", to, err)
		}
	}
	st, err = store.Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	st.Now = func() time.Time { return time.Now().Add(time.Minute) }
	agent := store.PeerKey{Session: "aaaa1111-root", Agent: "agate-6"}
	if _, err := st.Announce(ctx, store.Peer{Session: agent.Session, Agent: agent.Agent, AgentName: "gate-fault-6"}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CCI_AGENT_ID", agent.Agent)
	if me, err := run(t, "me"); err != nil || !strings.Contains(me, " lane=gate-fault-6 ") {
		t.Fatalf("me for a teammate named gate-fault-6 = %q, %v; want it bound as that lane", me, err)
	}
	if out, err := run(t, "tail", "--reader", "gate-fault-6", "--cursor", store.LaneCursor(agent)); err != nil || !strings.Contains(out, "GO start gate-fault-6") {
		t.Fatalf("gate-fault-6's lane mail = %q, %v; want the go posted before it started", out, err)
	}
	if _, err := run(t, "post", "--kind", "state", "--text", "gate-fault-6 up"); err != nil {
		t.Fatalf("post without --lane from a teammate bound by cci me: %v", err)
	}
	if out, err := run(t, "tail", "--since", "0", "--kind", "state", "--reader", "root", "--json"); err != nil || !strings.Contains(out, `"lane":"gate-fault-6","kind":"state"`) {
		t.Fatalf("state = %q, %v", out, err)
	}
}

func TestStandDownForwardsToTheSuccessor(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())
	for _, name := range peers.IdentityEnv {
		t.Setenv(name, "")
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "aaaa1111-root")
	t.Setenv("CCI_AGENT_ID", "")
	if _, err := run(t, "drive", "use", "d", "--root"); err != nil {
		t.Fatal(err)
	}
	if _, err := run(t, "post", "--lane", "root", "--kind", "go", "--successor", "desk-2", "--to", "desk-1", "--text", "x"); err == nil || !strings.Contains(err.Error(), "--kind stand-down") {
		t.Fatalf("--successor on a go: %v", err)
	}
	for _, args := range [][]string{
		{"--to", "desk-1", "--successor", "desk-2"},
		{"--to", "desk-2", "--successor", "desk-3"},
	} {
		if _, err := run(t, append([]string{"post", "--lane", "root", "--kind", "stand-down", "--text", "stand down"}, args...)...); err != nil {
			t.Fatal(err)
		}
	}
	out, err := run(t, "post", "--lane", "root", "--kind", "go", "--to", "desk-1", "--text", "take #12", "--json")
	if err != nil || !strings.Contains(out, `"to":["desk-1","desk-2","desk-3"]`) {
		t.Fatalf("go to a stood-down lane = %q, %v; want it addressed to every successor", out, err)
	}
	if out, err := run(t, "post", "--lane", "root", "--kind", "stand-down", "--to", "desk-1", "--text", "again", "--json"); err != nil || !strings.Contains(out, `"to":["desk-1"]`) {
		t.Fatalf("a stand-down is forwarded = %q, %v", out, err)
	}
	st, err := store.Open(context.Background(), os.Getenv("CCI_HOME"))
	if err != nil {
		t.Fatal(err)
	}
	desk := store.PeerKey{Session: "bbbb2222-desk", Agent: "adesk-2"}
	if err := st.BindLane(context.Background(), desk, "d", "desk-2", time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Announce(context.Background(), store.Peer{Session: desk.Session, Agent: desk.Agent}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if out, err := run(t, "who", "--all", "--lane", "desk-2"); err != nil || !strings.Contains(out, " lane=desk-2 stood-down successor=desk-3 ") {
		t.Fatalf("who --lane desk-2 = %q, %v; want it marked stood down", out, err)
	}
	if out, err := run(t, "who", "--successor-of", "desk-2"); err != nil || out != "desk-3\n" {
		t.Fatalf("who --successor-of desk-2 = %q, %v", out, err)
	}
	if out, err := run(t, "who", "--successor-of", "desk-1", "--json"); err != nil || out != `{"drive":"d","lane":"desk-1","stood_down":true,"successor":"","chain":[]}`+"\n" {
		t.Fatalf("who --successor-of desk-1 after a stand-down naming none = %q, %v", out, err)
	}
	if out, err := run(t, "who", "--successor-of", "desk-3", "--json"); err != nil || out != `{"drive":"d","lane":"desk-3","stood_down":false,"successor":"","chain":[]}`+"\n" {
		t.Fatalf("who --successor-of desk-3 = %q, %v", out, err)
	}
}

func TestALaneThatRegistersByPostingReadsMailSentBeforeItStarted(t *testing.T) {
	if os.Getenv("CCI_TEST_FRESH_LANE") == "1" {
		if _, err := run(t, "post", "--drive", "d", "--lane", "fresh-lane", "--kind", "state", "--text", "fresh-lane up"); err != nil {
			t.Fatal(err)
		}
		return
	}
	home, config := t.TempDir(), t.TempDir()
	t.Setenv("CCI_HOME", home)
	t.Setenv("CLAUDE_CONFIG_DIR", config)
	for _, name := range peers.IdentityEnv {
		t.Setenv(name, "")
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "aaaa1111-root")
	t.Setenv("CCI_AGENT_ID", "")
	if _, err := run(t, "post", "--drive", "d", "--lane", "root", "--kind", "go", "--to", "fresh-lane", "--text", "GO before fresh-lane started"); err != nil {
		t.Fatal(err)
	}
	lane := exec.Command(os.Args[0], "-test.run=^TestALaneThatRegistersByPostingReadsMailSentBeforeItStarted$", "codex")
	lane.Env = append(os.Environ(), "CCI_TEST_FRESH_LANE=1", "CCI_HOME="+home, "CLAUDE_CONFIG_DIR="+config, "CLAUDE_CODE_SESSION_ID=", "CCI_AGENT_ID=", "CODEX_THREAD_ID=019a3c3e-fresh-lane")
	for _, name := range peers.IdentityEnv {
		if name != "CODEX_THREAD_ID" {
			lane.Env = append(lane.Env, name+"=")
		}
	}
	if out, err := lane.CombinedOutput(); err != nil {
		t.Fatalf("fresh-lane session: %v\n%s", err, out)
	}
	st, err := store.Open(context.Background(), home)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	seq, ok, err := st.Cursor(context.Background(), store.LaneCursor(store.PeerKey{Session: "019a3c3e-fresh-lane"}), "d")
	if err != nil || !ok || seq != 0 {
		t.Fatalf("fresh-lane's lane cursor = %d, %v, %v; want 0, before the go posted to it before it started", seq, ok, err)
	}
}
