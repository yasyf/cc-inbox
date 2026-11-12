package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/yasyf/cc-inbox/internal/cli"
	"github.com/yasyf/cc-inbox/internal/hook"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/pacific"
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
	if !strings.Contains(out, "1 opened landing-desk #30431 release: read published versions") || !strings.Contains(out, "2 hold root>landing-desk #30431 t:#30431 hold until the walker reports") {
		t.Fatalf("tail = %q", out)
	}
	if out, err = run(t, "tail", "--since", "0", "--to", "landing-desk"); err != nil || len(recordLines(out)) != 1 || !strings.Contains(out, "hold root") {
		t.Fatalf("tail --to = %q, %v", out, err)
	}
	if out, err = run(t, "digest"); err != nil || !strings.Contains(out, "open holds (1, newest first):") {
		t.Fatalf("digest = %q, %v", out, err)
	}
	if _, err = run(t, "post", "--lane", "l", "--kind", "nope", "--text", "x"); err == nil || !strings.Contains(err.Error(), "unknown kind") {
		t.Fatalf("unknown kind: %v", err)
	}
	if _, err = run(t, "post", "--lane", "root", "--kind", "hold", "--text", "HOLD #39489 until the canary clears"); err == nil || !strings.Contains(err.Error(), "such as --pr 39489") {
		t.Fatalf("hold naming an unbound pr: %v", err)
	}
	if out, err = run(t, "grep", "WALKER"); err != nil || !strings.Contains(out, "2 hold root") {
		t.Fatalf("grep = %q, %v", out, err)
	}
	if out, err = run(t, "post", "--lane", "evidence-lane", "--kind", "evidence", "--text", "5xx began at "+time.Now().Add(-30*time.Minute).Format("3:04 PM")); err != nil || out != "#3\n" {
		t.Fatalf("post evidence = %q, %v", out, err)
	}
	if out, err = run(t, "post", "--lane", "root", "--kind", "hold", "--pr", "48747", "--text", "#48747 holds until a release carrying #48611 and #48212 is live"); err != nil || !strings.Contains(out, "cci: this hold holds #48747 only; its text also names #48611, #48212, which it does not hold") {
		t.Fatalf("hold naming context prs = %q, %v", out, err)
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
		lines := recordLines(out)
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
		{[]string{"STATE census"}, []string{"state census"}},
		{[]string{"CLAIM merge-walker-r2"}, []string{"claim merge-walker-r2 #30541"}},
		{[]string{"runner-protocol-idle-tenant"}, []string{"runner-protocol-idle-tenant GREEN #30568"}},
		{[]string{"census|GREEN"}, []string{"state census", "runner-protocol-idle-tenant GREEN"}},
		{[]string{"-i", "census|green"}, []string{"state census", "runner-protocol-idle-tenant GREEN"}},
		{[]string{"-F", "census|GREEN"}, []string{"no records on release-v3 match; store head #4"}},
		{[]string{"234.293"}, []string{"state census"}},
		{[]string{"--fixed-strings", "234.293"}, []string{"no records on release-v3 match; store head #4"}},
		{[]string{"-n", "1", "census|GREEN"}, []string{"runner-protocol-idle-tenant GREEN", "... more matches"}},
		{[]string{"--limit", "1", "census|GREEN"}, []string{"runner-protocol-idle-tenant GREEN", "... more matches"}},
		{[]string{"--ignore-case=false", "green"}, []string{"no records on release-v3 match; store head #4"}},
		{[]string{"--ignore-case=false", "GREEN"}, []string{"runner-protocol-idle-tenant GREEN"}},
	}
	for _, tt := range tests {
		out, err := run(t, append([]string{"grep", "--drive", "release-v3"}, tt.args...)...)
		if err != nil {
			t.Fatal(err)
		}
		if got := len(recordLines(out)); got != len(tt.want) {
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

func TestGrepSinceTakesDaysAndWeeks(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	ctx := context.Background()
	st, err := store.Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	ages := []string{"ten", "six", "four"}
	for i, age := range []int{10, 6, 4} {
		if _, _, err := st.PostAt(ctx, store.Record{Drive: "release-v3", Lane: "root", Kind: kinds.Defect, Text: ages[i] + " days old"}, 0, time.Now().AddDate(0, 0, -age)); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		since string
		want  []string
	}{
		{"5d", []string{"four"}},
		{"1w", []string{"four", "six"}},
		{"2w", ages},
	} {
		out, err := run(t, "grep", "--drive", "release-v3", "--since", tt.since, "days old")
		if err != nil {
			t.Fatalf("grep --since %s: %v", tt.since, err)
		}
		for _, age := range ages {
			if got := strings.Contains(out, age+" days old"); got != slices.Contains(tt.want, age) {
				t.Errorf("grep --since %s = %q; want only %v", tt.since, out, tt.want)
			}
		}
	}
	if _, err := run(t, "grep", "--drive", "release-v3", "--since", "5x", "days old"); err == nil || !strings.Contains(err.Error(), "5d, 1w") {
		t.Errorf("grep --since 5x: %v; want a refusal naming the day and week forms", err)
	}
}

func TestPostTakesItsTextAsOneArgument(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	post := []string{"post", "--drive", "release-v3", "--lane", "root", "--kind", "go", "--to", "cci-fix-4"}
	if out, err := run(t, append(post, "land the fix")...); err != nil || out != "cci: --to cci-fix-4: no lane has registered as cci-fix-4 or holds cci-fix; cci-fix-4 reads this once it registers\n#1\n" {
		t.Fatalf("post with a text argument = %q, %v", out, err)
	}
	if out, err := run(t, "tail", "--drive", "release-v3", "--since", "0"); err != nil || !strings.Contains(out, "go root>cci-fix-4 land the fix") {
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
		{summary, "note perf-asks + perf findings for api-sql"},
		{wide, "note perf-asks + " + strings.Repeat("x", 399) + "…"},
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
	if out, err := run(t, "tail", "--drive", "release-v3", "--since", "0"); err != nil || !strings.Contains(out, "+ "+got.Text) {
		t.Errorf("tail of the spilled record = %q, %v; want the body mark then the whole excerpt", out, err)
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
}

type posted struct {
	Seq  int64     `json:"seq"`
	At   time.Time `json:"at"`
	Text string    `json:"text"`
	Refs struct {
		Path string `json:"path"`
	} `json:"refs"`
	Fields struct {
		Attrs map[string]string `json:"attrs"`
	} `json:"fields"`
}

func (p posted) stamp() string {
	return p.At.In(pacific.Zone).Format("3:04 PM")
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
	began, planned := time.Now().Add(-time.Hour).Format("3:04 PM"), time.Now().Add(-70*time.Minute).Format("3:04 PM")
	for _, tt := range []struct {
		name, text, want, shown string
	}{
		{"twelve-hour clock", "R84 (" + ahead.Format("3:04 PM") + ") orca-desk: relay to api: hold", "R84 ({at}) orca-desk: relay to api: hold", "orca-desk: relay to api: hold"},
		{"clock without a space", "R85 (" + ahead.Format("3:04pm") + ") orca-desk: relay to api: hold", "R85 ({at}) orca-desk: relay to api: hold", "orca-desk: relay to api: hold"},
		{"24-hour clock", "R86 (" + ahead.Format("15:04") + ") orca-desk: relay to api: hold", "R86 ({at}) orca-desk: relay to api: hold", "orca-desk: relay to api: hold"},
		{"fuzzy minute", "R87 (10:5x AM) orca-desk: relay to api: hold", "R87 ({at}) orca-desk: relay to api: hold", "orca-desk: relay to api: hold"},
		{"clock with a zone", "R92 (3:36 PM PT) orca-desk: launch api-fix NOW", "R92 ({at}) orca-desk: launch api-fix NOW", "orca-desk: launch api-fix NOW"},
		{"lettered key", "R88a (" + ahead.Format("3:04 PM") + ") orca-desk: relay to api: hold", "R88a ({at}) orca-desk: relay to api: hold", "R88a ({at}) orca-desk: relay to api: hold"},
		{"token in prose", "api landed {at}, live since {at}", "api landed {at}, live since {at}", "api landed {at}, live since {at}"},
		{"neither", "ship it", "ship it", "ship it"},
		{"clock in prose", "R89 said the deploy began at " + began + " (not " + planned + ")", "R89 said the deploy began at " + began + " (not " + planned + ")", "R89 said the deploy began at " + began + " (not " + planned + ")"},
		{"stamp past the start", "relay R90 (" + began + ") was late", "relay R90 (" + began + ") was late", "relay R90 (" + began + ") was late"},
		{"parenthesis without a clock", "R91 (orca-desk, " + began + ") relay to api", "R91 (orca-desk, " + began + ") relay to api", "R91 (orca-desk, " + began + ") relay to api"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := postJSON(t, tt.text)
			want := strings.ReplaceAll(tt.want, "{at}", got.stamp())
			if got.Text != want {
				t.Fatalf("post %q stored %q, want %q", tt.text, got.Text, want)
			}
			line := fmt.Sprintf("%d note root %s", got.Seq, strings.ReplaceAll(tt.shown, "{at}", got.stamp()))
			if out, err := run(t, "tail", "--drive", "release-v3", "--since", "0"); err != nil || !strings.Contains(out, line) || !strings.Contains(out, "@"+got.stamp()+"\n") {
				t.Errorf("tail after post %q = %q, %v; want %q", tt.text, out, err, line)
			}
		})
	}
}

func TestPostStillTakesTheRetiredStampFlag(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	if _, err := run(t, "post", "--drive", "d", "--lane", "root", "--kind", "note", "--stamp", "--text", "live on dev"); err != nil {
		t.Fatalf("post --stamp refused: %v", err)
	}
}

func TestPostNeverRefusesAClockInItsText(t *testing.T) {
	clock := func(d time.Duration) string { return time.Now().Add(d).Format("3:04 PM") }
	label, before := clock(10*time.Minute), clock(-30*time.Minute)
	hint := func(typed string) string {
		return "cci: label " + typed + " became the post time {at}; cci stamps every record, so leave the time out"
	}
	round := " fidelity-desk-19 ending round: holds none; graded nothing since #223185. Share (PR mod 6 = 5) empty 7:31 to " + label + ": no fidelity: reader, FIDELITY-FAILED, CONTRACT-MISSING or FIDELITY-PENDING head waits, claimed or unclaimed."
	for _, tt := range []struct {
		name, lane, kind, text string
		attrs                  []string
		want, stderr           string
	}{
		{name: "leading label off the clock", lane: "fidelity-desk-19", kind: "state", text: label + round, want: "{at}" + round, stderr: hint(label)},
		{name: "label after the lane's name", lane: "root", kind: "note", text: "root " + before + " count: 0 ungraded", want: "root {at} count: 0 ungraded", stderr: hint(before)},
		{name: "deadline that opens the text", lane: "dev-red-d12370de07-fix", kind: "state", text: label + " backup gate: #51584 is still queued", want: label + " backup gate: #51584 is still queued"},
		{name: "past claim 299 minutes ahead", lane: "go-sweep-9", kind: "note", text: "the sweep landed at " + clock(299*time.Minute) + " with no new reds", want: "the sweep landed at " + clock(299*time.Minute) + " with no new reds"},
		{name: "past claim 464 minutes ahead", lane: "supervisor", kind: "note", text: "the relaunch failed at " + clock(464*time.Minute) + ", so go-sweep-9 is still down", want: "the relaunch failed at " + clock(464*time.Minute) + ", so go-sweep-9 is still down"},
		{name: "future deadline as an attr", lane: "root", kind: "note", text: "next state", attrs: []string{"--attr", "due=" + clock(90*time.Minute)}, want: "next state"},
		{name: "no clock", lane: "root", kind: "note", text: "live on dev", want: "live on dev"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("CCI_HOME", t.TempDir())
			out, err := run(t, append([]string{"post", "--json", "--drive", "release-v3", "--lane", tt.lane, "--kind", tt.kind, "--text", tt.text}, tt.attrs...)...)
			if err != nil {
				t.Fatalf("post %q = %v", tt.text, err)
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
			if len(tt.attrs) > 0 && got.Fields.Attrs["due"] != strings.TrimPrefix(tt.attrs[1], "due=") {
				t.Errorf("post %v stored attrs %v, want due as typed", tt.attrs, got.Fields.Attrs)
			}
		})
	}
}

func TestPostAttrKeepsEveryCommaInItsValue(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	tap := "a scope change at 8afb275: HEAD #50894 8afb275 on dev, delta=scope, per his m1064 to m1066: no longer sits on #50177, which he closed"
	refusal := `go refused: "approve, then land" reads as key=value, not one reason`
	got := postJSON(t, "--text", "row", "--attr", "tap_returned="+tap, "--attr", "step=one", "--attr", "step=two", "--attr", "step:1=6:30 PM answer his question, then post", "--attr", "refusal="+refusal)
	want := map[string]string{"tap_returned": tap, "step": "two", "step:1": "6:30 PM answer his question, then post", "refusal": refusal}
	if !reflect.DeepEqual(got.Fields.Attrs, want) {
		t.Errorf("stored attrs %v, want %v", got.Fields.Attrs, want)
	}
	if _, err := run(t, "post", "--drive", "release-v3", "--lane", "root", "--kind", "note", "--text", "row", "--attr", "no-equals"); err == nil {
		t.Error("--attr without = was accepted")
	}
}

func TestPostWithOnlyQuotedWordsStoresThemAsTheTextUnchecked(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	words := "run it at " + time.Now().Add(7*time.Minute).Format("3:04 PM") + ", not {at}"
	out, err := run(t, "post", "--json", "--drive", "release-v3", "--lane", "owner-relay", "--kind", "owner", "--id", "owner:m47", "--quoted", words)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	var got posted
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &got); err != nil {
		t.Fatalf("post --json = %q: %v", out, err)
	}
	if got.Text != words {
		t.Fatalf("post --quoted stored %q, want his words %q unchanged", got.Text, words)
	}
}

func TestPostRefusesAGoWithdrawalWrittenAsProse(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	post := func(args ...string) (string, error) {
		return run(t, append([]string{"post", "--drive", "release-v3", "--lane", "root"}, args...)...)
	}
	for _, text := range []string{"go #46823 at 52dbea3 on green", "go #46906 at 7ac70d2 on green"} {
		if _, err := post("--kind", "go", "--text", text); err != nil {
			t.Fatal(err)
		}
	}
	_, err := post("--kind", "correction", "--text", "9:23 PM WITHDRAW gos #1 #2 (#46823 52dbea3, #46906 7ac70d2) this minute: the runner misread their bracket")
	if err == nil || !strings.Contains(err.Error(), "cci post --kind withdraw --re 1 --re 2 --text") {
		t.Fatalf("prose withdrawal = %v, want a refusal naming cci post --kind withdraw --re 1 --re 2", err)
	}
	if _, err := post("--kind", "withdraw", "--re", "1", "--re", "2", "--text", "withdraw gos #1 #2: the runner misread their bracket"); err != nil {
		t.Fatalf("structured withdrawal = %v, want it posted", err)
	}
	if _, err := post("--kind", "note", "--text", "the go #1 the runner landed was withdrawn"); err != nil {
		t.Fatalf("a note naming a withdrawn go = %v, want it posted", err)
	}
}

func TestPostRefusesAColonWithdrawalAndNamesOnlyTheGoSeqs(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	_, err := run(t, "post", "--drive", "release-v3", "--lane", "root", "--kind", "correction", "--text", "Withdrawn: go #281022 on #46488 and go #281023 on #46503; the runner misread both")
	if err == nil || !strings.Contains(err.Error(), "cci post --kind withdraw --re 281022 --re 281023 --text") {
		t.Fatalf("colon withdrawal = %v, want a refusal naming --re 281022 --re 281023 only", err)
	}
}

func TestPostRefusesAWithdrawWhoseReNamesNoRecordOfItsDrive(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	if _, err := run(t, "post", "--drive", "release-v3", "--lane", "root", "--kind", "go", "--text", "go #47568 at f60fd32 on green"); err != nil {
		t.Fatal(err)
	}
	if _, err := run(t, "post", "--drive", "cci-mesh", "--lane", "p1-data-model", "--kind", "head", "--pr", "12", "--text", "HEAD #12 at abc1234"); err != nil {
		t.Fatal(err)
	}
	withdraw := func(re string) error {
		_, err := run(t, "post", "--drive", "release-v3", "--lane", "root", "--kind", "withdraw", "--re", re, "--text", "withdraw go 1 on 47568 at f60fd32: finder majors are real")
		return err
	}
	for _, re := range []string{"2", "47568"} {
		if err := withdraw(re); err == nil || !strings.Contains(err.Error(), "withdraw --re #"+re+" names no record in drive release-v3") {
			t.Fatalf("withdraw --re %s = %v, want a refusal naming the drive", re, err)
		}
	}
	if err := withdraw("1"); err != nil {
		t.Fatalf("withdraw --re 1 = %v, want it posted", err)
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
		if err != nil || len(recordLines(out)) != 1 || !strings.HasSuffix(strings.TrimSpace(out), "HEAD 2 PRs: #45001 a1b2c3d, #45002 c3b44a3: restacked onto dev") {
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
	if spoke, err := run(t, "tail", "--since", "0", "--kind", "peer", "--reader", "root"); err != nil || !strings.HasPrefix(recordLines(spoke)[0], "1 peer registry - root drive=release-v3 ") {
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
		{[]string{"who", "--since", "1h"}, []string{"- root drive=release-v3"}},
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
	if err != nil || strings.Contains(out, `"env"`) || !strings.Contains(out, `"tags":["mesh","cci"]`) {
		t.Fatalf("who --json = %q, %v; want the tags and no env", out, err)
	}
	if out, err := run(t, "who", "--all", "--json", "--session", "aaaa", "--fields", "env,tags"); err != nil || out != `{"env":{"ORCA_LAUNCH_RUN":"run_7"},"tags":["mesh","cci"]}`+"\n" {
		t.Fatalf("who --json --fields env,tags = %q, %v", out, err)
	}
	if out, err := run(t, "who", "--json", "--fields", "lane,nope"); err == nil || !strings.Contains(err.Error(), "no peer key nope") {
		t.Fatalf("who --fields lane,nope = %q, %v; want the unknown key named", out, err)
	}
	if out, err := run(t, "who", "--since", "#1"); err == nil || !strings.Contains(err.Error(), "sessions have no seq") {
		t.Fatalf("who --since #1 = %q, %v; want a seq refused", out, err)
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
	if err != nil || !strings.Contains(out, "ask root>peer:bbbb2222 take #34363?") || !strings.Contains(out, "go root@drive-a>root@drive-b GO") || len(recordLines(out)) != 2 {
		t.Fatalf("b tail = %q, %v", out, err)
	}
	if out, err = run(t, "post", "--lane", "root", "--kind", "answer", "--resolves", "1", "--to", "peer:aaaa", "--text", "yes"); err != nil || out != "#3\ncci: resolved #1 ask root: take #34363?\n" {
		t.Fatalf("answer across drives = %q, %v", out, err)
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "aaaa1111-root-a")
	if out, err = run(t, "tail", "--since", "0"); err != nil || !strings.Contains(out, "answer root>peer:aaaa1111 x1 yes") || !strings.Contains(out, "take #34363? [RESOLVED #3]") {
		t.Fatalf("a tail = %q, %v", out, err)
	}
	if out, err = run(t, "tail", "--since", "0", "--to", "root@drive-b"); err != nil || len(recordLines(out)) != 1 || !strings.Contains(out, "go root@drive-a>root@drive-b") {
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
	if _, err := run(t, "post", "--lane", "root", "--kind", "go", "--to", "release-owner,desk", "--text", "RELEASE #45190"); err == nil || !strings.HasPrefix(err.Error(), "--to release-owner: no lane on drive drive-a has registered as release-owner, which looks like a slip for the live lane release-owner-4; --force addresses release-owner as typed") {
		t.Fatalf("post to a lane nobody holds: %v", err)
	}
	for _, to := range []string{"owner", "all"} {
		if _, err := run(t, "post", "--lane", "root", "--kind", "ask", "--to", to, "--text", "approve?"); err != nil {
			t.Fatalf("post --to %s: %v", to, err)
		}
	}
	out, err := run(t, "post", "--lane", "root", "--kind", "go", "--to", "release-owner-4,desk,root,nobody", "--text", "GO promote")
	if err != nil || out != "cci: --to release-owner-4: release-owner-4 reads drive drive-b, not drive-a; addressed release-owner-4@drive-b\ncci: --to nobody: no lane holds nobody; the first lane to register as nobody or nobody-<n> reads this\n#3\n" {
		t.Fatalf("post to a lane reading another drive = %q, %v", out, err)
	}
	if _, err := run(t, "post", "--lane", "root", "--kind", "go", "--to", "walker", "--text", "x"); err == nil || !strings.Contains(err.Error(), "read drive-b, drive-c; address one as walker@<drive>") {
		t.Fatalf("post to a lane live in two other drives: %v", err)
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "bbbb2222-owner")
	if out, err := run(t, "tail", "--drive", "drive-b", "--since", "0", "--reader", "release-owner-4"); err != nil || !strings.Contains(out, "go root@drive-a>release-owner-4@drive-b,desk,root,nobody GO promote") || len(recordLines(out)) != 1 {
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
	if err != nil || !strings.Contains(out, "ask root>peer:aaaa1111/aapi-0123 status?") || strings.Contains(out, "root?") {
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
	if err != nil || !strings.Contains(out, "1 opened fde iris-chime#243 monorepo#35198 #30431 chime fix") {
		t.Fatalf("tail = %q, %v", out, err)
	}
	for _, pr := range []string{"Forge-FDE/iris-chime#243", "https://github.com/Forge-FDE/iris-chime/pull/243", "30431", "243"} {
		if out, err := run(t, "tail", "--since", "0", "--pr", pr); err != nil || len(recordLines(out)) != 1 || !strings.Contains(out, "chime fix") {
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
	if err := st.BindLane(ctx, store.PeerKey{Session: "dddd4444-codex"}, "drive-b", "exec-fix", time.Now()); err != nil {
		t.Fatal(err)
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
	if out, err := run(t, "tail", "--drive", "drive-b", "--since", "0"); err != nil || !strings.Contains(out, "claim successor claim: successor-2 owns #owner/repo#9") {
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
	if out, err := run(t, "claim", "--for", "exec-fix@drive-b", "--build", "11733", "--rel", "started"); err != nil || out != "started build:11733\n" {
		t.Fatalf("claim --for a lane whose only session never announced a process, as a codex lane's = %q, %v", out, err)
	}
	if out, err := run(t, "who", "--all", "--json", "--build", "11733"); err != nil || !strings.Contains(out, `"session":"dddd4444-codex"`) {
		t.Fatalf("who --build after claim --for an ended lane = %q, %v", out, err)
	}
	for _, tt := range []struct{ lane, want string }{
		{"walker", "name the lane as <lane>@<drive>"},
		{"@drive-b", "name the lane as <lane>@<drive>"},
		{"ghost@drive-b", "no session on this machine was ever bound as lane ghost in drive drive-b"},
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

func fakeTmux(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	calls := filepath.Join(dir, "calls")
	script := "#!/bin/sh\necho \"$*\" >> " + calls + "\ncase \"$1\" in\nhas-session) [ -f \"" + calls + ".${3#=}\" ] ;;\nnew-session) touch \"" + calls + ".$4\" ;;\nesac\n"
	if err := os.WriteFile(filepath.Join(dir, "tmux"), []byte(script), 0o700); err != nil { //nolint:gosec // G306: the fake tmux runs as a program.
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("SHELL", "/bin/sh")
	return calls
}

func TestSessionRegisterListTypeAndRm(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())
	t.Setenv("CLAUDE_CODE_SESSION_ID", "session-1")
	t.Setenv("CCI_AGENT_ID", "")
	calls := fakeTmux(t)
	if out, err := run(t, "session", "register", "release-demo-coordinator", "--session", "c0ffee01", "--drive", "d", "--reader", "root"); err == nil || !strings.Contains(err.Error(), `"command" not set`) {
		t.Fatalf("session register with no --command = %q, %v", out, err)
	}
	out, err := run(t, "session", "register", "release-demo-coordinator", "--session", "c0ffee01", "--drive", "d", "--reader", "root", "--cursor", "coordinator",
		"--cwd", "/work/release-demo", "--command", "claude -n release-demo-coordinator", "--resume-prompt", "resume as the coordinator", "--notify", "communicator")
	if err != nil || !strings.HasPrefix(out, "release-demo-coordinator registered: session c0ffee01 started in tmux session release-demo-coordinator, which cci session attach release-demo-coordinator shows; drive d as root,") || !strings.HasSuffix(out, "cursor coordinator at #0\n") {
		t.Fatalf("session register = %q, %v", out, err)
	}
	called := func(want string) {
		t.Helper()
		got, err := os.ReadFile(calls) //nolint:gosec // G304: the fake tmux's log under the test's temp dir.
		if err != nil || string(got) != want {
			t.Fatalf("tmux was called with %q, %v; want %q", got, err, want)
		}
	}
	launched := "has-session -t =release-demo-coordinator\nnew-session -d -s release-demo-coordinator -c /work/release-demo -- /bin/sh -l -c exec cci session run release-demo-coordinator\n"
	called(launched)
	if out, err := run(t, "session", "register", "second", "--session", "c0ffee01", "--drive", "d", "--reader", "root", "--command", "claude"); err == nil || !strings.Contains(err.Error(), "session c0ffee01 is registered as release-demo-coordinator") {
		t.Fatalf("a second alias for the session = %q, %v", out, err)
	}
	const listed = "release-demo-coordinator: session c0ffee01 not running, tmux session release-demo-coordinator, drive d as root, cursor coordinator at #0, notify communicator\n" +
		"  cwd /work/release-demo\n  command claude -n release-demo-coordinator\n  resume prompt resume as the coordinator\n"
	if out, err := run(t, "session", "list"); err != nil || out != listed {
		t.Fatalf("session list = %q, %v; want %q", out, err, listed)
	}
	if out, err := run(t, "session", "type", "release-demo-coordinator", hook.ReconnectCommand); err != nil || out != "typed into tmux session release-demo-coordinator: "+hook.ReconnectCommand+"\n" {
		t.Fatalf("session type = %q, %v", out, err)
	}
	launched += "has-session -t =release-demo-coordinator\nsend-keys -t =release-demo-coordinator: -l " + hook.ReconnectCommand + "\nsend-keys -t =release-demo-coordinator: Enter\n"
	called(launched)
	if out, err := run(t, "session", "rm", "release-demo-coordinator"); err != nil || out != "release-demo-coordinator removed: session c0ffee01 keeps no channel subscription, and once Claude Code exits nothing starts it again and tmux session release-demo-coordinator ends\n" {
		t.Fatalf("session rm = %q, %v", out, err)
	}
	if out, err := run(t, "session", "list"); err != nil || out != "no session is registered; cci session register <alias> keeps one\n" {
		t.Fatalf("session list after rm = %q, %v", out, err)
	}
	if out, err := run(t, "session", "rm", "release-demo-coordinator"); err == nil || !strings.Contains(err.Error(), "no session is registered as release-demo-coordinator") {
		t.Fatalf("session rm twice = %q, %v", out, err)
	}
	if out, err := run(t, "session", "register", "release-demo-meta", "--new", "--drive", "d", "--reader", "meta", "--cwd", "/work/release-demo", "--command", "claude -n release-demo-meta"); err != nil || !regexp.MustCompile(`^release-demo-meta registered: session [0-9a-f-]{36} started in tmux session release-demo-meta,`).MatchString(out) {
		t.Fatalf("session register --new = %q, %v", out, err)
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
	const sweptAsks = "cci: resolved #1 ask lane-a: first?\ncci: resolved #2 ask lane-a: second?\ncci: resolved #3 ask lane-a: third?\n"
	out, err := run(t, "post", "--lane", "root", "--kind", "note", "--resolves", "1", "--resolves", "2,3,2", "--id", "sweep", "--text", "stale")
	if err != nil || out != "#4\n"+sweptAsks {
		t.Fatalf("post with three --resolves = %q, %v; want one record", out, err)
	}
	if out, err := run(t, "post", "--lane", "root", "--kind", "note", "--resolves", "1,2,3", "--id", "sweep", "--text", "stale"); err != nil || out != "#4 (duplicate)\n"+sweptAsks {
		t.Fatalf("repost = %q, %v", out, err)
	}
	if out, err := run(t, "tail", "--since", "0", "--kind", "ask"); err != nil || strings.Count(out, "[RESOLVED #4]") != 3 {
		t.Fatalf("tail after closing all three = %q, %v", out, err)
	}
	if out, err := run(t, "tail", "--since", "0", "--kind", "note"); err != nil || len(recordLines(out)) != 1 || !strings.Contains(out, "x1 x2 x3 stale") {
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
	if err != nil || !strings.Contains(out, "opened web opened #owner/repo#7") || !strings.Contains(out, "claim web claim: web owns #8") || strings.Contains(out, "ada0addbf41f1f196") {
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
	short, end := strings.Index(out, "2 note root short note"), strings.Index(out, "THE END")
	if short < 0 || end < 0 || short > strings.Index(out, "1 note root + a long finding") {
		t.Errorf("show #2 1 = %q, want #2 first, then #1 with its text through THE END", out)
	}
	if _, err := run(t, "show", "9"); err == nil || !strings.Contains(err.Error(), "no record #9") {
		t.Errorf("show 9 = %v, want it to name the missing record", err)
	}
}

func TestShowPrintsABodyFilesWords(t *testing.T) {
	for name, tt := range map[string]struct{ body, want string }{
		"the body field":   {`{"body":"WORDS of the report\nsecond line"}`, "WORDS of the report\nsecond line\n"},
		"a bare string":    {`"WORDS of the report"`, "WORDS of the report\n"},
		"a text field":     {`{"text":"WORDS of the report"}`, "WORDS of the report\n"},
		"any other shape":  {`{"rows":[1,2]}`, `{"rows":[1,2]}` + "\n"},
		"the line's words": {`{"text":"a report"}`, "a report\n"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("CCI_HOME", t.TempDir())
			file := filepath.Join(t.TempDir(), "body.json")
			if err := os.WriteFile(file, []byte(tt.body), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := run(t, "post", "--drive", "release-v3", "--lane", "root", "--kind", "report", "--text", "a report", "--body-file", file); err != nil {
				t.Fatal(err)
			}
			out, err := run(t, "show", "1")
			if err != nil {
				t.Fatal(err)
			}
			if !strings.HasSuffix(out, tt.want) {
				t.Errorf("show = %q, want it to end with %q", out, tt.want)
			}
		})
	}
}

func TestShowPrintsEachRecordsTextOnceWithinItsBudget(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	long := "OPENING " + strings.Repeat("a long finding about the stack cap ", 20) + "THE END"
	body := filepath.Join(t.TempDir(), "report.txt")
	if err := os.WriteFile(body, []byte("FIRST LINE of the report\n"+strings.Repeat("detail ", 200)+"LAST WORD\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := run(t, "post", "--drive", "release-v3", "--lane", "root", "--kind", "note", "--text", long); err != nil {
		t.Fatal(err)
	}
	if _, err := run(t, "post", "--drive", "release-v3", "--lane", "root", "--kind", "report", "--path", body); err != nil {
		t.Fatal(err)
	}

	out, err := run(t, "show", "1", "2", "--drive", "release-v3")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(out, "OPENING") != 1 || strings.Count(out, "FIRST LINE") != 1 || !strings.Contains(out, "THE END") || !strings.Contains(out, "LAST WORD") {
		t.Errorf("show 1 2 = %q, want each text printed once and each body through its end", out)
	}
	out, err = run(t, "show", "1", "2", "--budget", "200")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out, "LAST WORD") || !strings.Contains(out, "cci show #2 reads it whole") {
		t.Errorf("show 1 2 --budget 200 = %q, want #2's body cut with the show that reads it whole", out)
	}
	if _, err := run(t, "show", "1", "--drive", "other"); err == nil || !strings.Contains(err.Error(), "#1 is on drive release-v3") {
		t.Errorf("show 1 --drive other = %v, want it to name the record's drive", err)
	}
}

func TestShowBodyPrintsTheWholeTextWithNoHeader(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	long := "OPENING " + strings.Repeat("a long finding about the stack cap ", 20) + "THE END"
	if _, err := run(t, "post", "--drive", "release-v3", "--lane", "root", "--kind", "note", "--text", long); err != nil {
		t.Fatal(err)
	}
	if _, err := run(t, "post", "--drive", "release-v3", "--lane", "root", "--kind", "note", "--text", "short words"); err != nil {
		t.Fatal(err)
	}

	out, err := run(t, "show", "1", "--body")
	if err != nil {
		t.Fatal(err)
	}
	if out != long+"\n" {
		t.Errorf("show 1 --body = %q, want the spilled text whole and nothing else", out)
	}
	out, err = run(t, "show", "1", "2", "--body", "--budget", "0")
	if err != nil {
		t.Fatal(err)
	}
	if out != "#1\n"+long+"\n#2\nshort words\n" {
		t.Errorf("show 1 2 --body = %q, want each text under its seq", out)
	}
}

func TestShowBodySaysWhereABudgetCutIt(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	long := "OPENING " + strings.Repeat("a long finding about the stack cap ", 20) + "THE END"
	if _, err := run(t, "post", "--drive", "release-v3", "--lane", "root", "--kind", "note", "--text", long); err != nil {
		t.Fatal(err)
	}

	out, err := run(t, "show", "1", "--body", "--budget", "100")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out, "THE END") || !strings.Contains(out, "cci show #1 --body --budget 0 reads it whole") {
		t.Errorf("show 1 --body --budget 100 = %q, want a cut that names the read past it", out)
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
	lines := recordLines(out)
	if len(lines) != 4 || utf8.RuneCountInString(strings.TrimSuffix(strings.TrimPrefix(lines[0], "1 ask lane-a>root "), " [RESOLVED #3]")) != 160 || !strings.HasSuffix(lines[0], "… [RESOLVED #3]") ||
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
		out, err := run(t, "post", "--lane", "root", "--kind", "go", "--to", to, "--text", "GO start "+to)
		if lines := strings.Split(strings.TrimSpace(out), "\n"); err != nil || !strings.HasPrefix(lines[len(lines)-1], "#") || !strings.HasPrefix(out, "cci: --to "+to+": no lane has registered as "+to) && !strings.HasPrefix(out, "cci: --to "+to+": no lane holds "+to) {
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

func TestARecordToAnOwnedScriptLaneReachesItsOwner(t *testing.T) {
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
	if _, err := run(t, "post", "--lane", "root", "--kind", "go", "--owned-by", "mq-health-9", "--to", "stall-watch", "--text", "x"); err == nil || !strings.Contains(err.Error(), "--kind decide") {
		t.Fatalf("--owned-by on a go: %v", err)
	}
	out, err := run(t, "post", "--lane", "root", "--kind", "defect", "--to", "stall-watch", "--text", "stale head paged", "--json")
	if err != nil || !strings.Contains(out, `"to":["stall-watch"]`) {
		t.Fatalf("defect to an unowned script lane = %q, %v", out, err)
	}
	for _, owner := range []string{"mq-health-8", "mq-health-9"} {
		if _, err := run(t, "post", "--lane", "root", "--kind", "decide", "--owned-by", owner, "--to", "stall-watch", "--text", "stall-watch is owned by "+owner); err != nil {
			t.Fatal(err)
		}
	}
	out, err = run(t, "post", "--lane", "s5-old-tree-1", "--kind", "defect", "--to", "stall-watch", "--text", "stale head paged", "--json")
	if err != nil || !strings.Contains(out, `"to":["stall-watch","mq-health-9"]`) {
		t.Fatalf("defect to an owned script lane = %q, %v; want its newest owner too", out, err)
	}
	out, err = run(t, "post", "--lane", "root", "--kind", "go", "--to", "stall-watch", "--text", "go #12", "--json")
	if err != nil || !strings.Contains(out, `"to":["stall-watch"]`) {
		t.Fatalf("go to an owned script lane = %q, %v; want only the script lane", out, err)
	}
}

func TestAnOwnerWhoseChainEndedGetsNoCopyAndALiveOwnerStillDoes(t *testing.T) {
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
	for _, args := range [][]string{
		{"--lane", "mq-health-9", "--kind", "decide", "--owned-by", "mq-health-9", "--to", "supervisor", "--text", "supervisor is owned by mq-health-9"},
		{"--lane", "alerts-desk-3", "--kind", "decide", "--owned-by", "alerts-desk-3", "--to", "monitor-watch", "--text", "monitor-watch is owned by alerts-desk-3"},
		{"--lane", "root", "--kind", "stand-down", "--to", "mq-health-9", "--text", "stand down"},
	} {
		if _, err := run(t, append([]string{"post"}, args...)...); err != nil {
			t.Fatal(err)
		}
	}
	out, err := run(t, "post", "--lane", "root", "--kind", "decide", "--to", "supervisor", "--text", "registry request: owner pr-unblock-18", "--json")
	if err != nil || !strings.Contains(out, `"to":["supervisor"]`) {
		t.Fatalf("decide to a lane whose owner stood down with no successor = %q, %v; want the lane alone", out, err)
	}
	out, err = run(t, "post", "--lane", "root", "--kind", "defect", "--to", "monitor-watch", "--text", "stale page", "--json")
	if err != nil || !strings.Contains(out, `"to":["monitor-watch","alerts-desk`) {
		t.Fatalf("defect to a lane whose owner is live = %q, %v; want the owner's copy", out, err)
	}
}

func TestStandDownNamesTheSuccessor(t *testing.T) {
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
	st, err := store.Open(context.Background(), os.Getenv("CCI_HOME"))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.BindLane(context.Background(), store.PeerKey{Session: "cccc3333-desk", Agent: "adesk-1"}, "d", "desk-1", time.Now()); err != nil {
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
	if _, err := run(t, "post", "--lane", "root", "--kind", "go", "--to", "desk-1", "--text", "take #12", "--json"); err == nil || !strings.HasSuffix(err.Error(), "post to its successor desk-3, or to root") {
		t.Fatalf("go to a stood-down lane = %v; want it refused naming its last successor", err)
	}
	out, err := run(t, "post", "--lane", "root", "--kind", "stand-down", "--to", "desk-1", "--text", "again", "--json")
	if err != nil || !strings.Contains(out, `"to":["desk-1"]`) {
		t.Fatalf("a stand-down is forwarded = %q, %v", out, err)
	}
	again := seqOf(t, out)
	if out, err := run(t, "who", "--all", "--lane", "desk-2"); err != nil || !strings.Contains(out, " lane=desk-2 stood-down successor=desk-3 ") {
		t.Fatalf("who --lane desk-2 = %q, %v; want it marked stood down", out, err)
	}
	time.Sleep(10 * time.Millisecond)
	if st, err = store.Open(context.Background(), os.Getenv("CCI_HOME")); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Announce(context.Background(), store.Peer{Session: desk.Session, Agent: desk.Agent}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if out, err := run(t, "who", "--all", "--lane", "desk-2"); err != nil || strings.Contains(out, "stood-down") || !strings.Contains(out, " lane=desk-2 successor=desk-3 ") {
		t.Fatalf("who --lane desk-2 seen after its stand-down = %q, %v; want it live, not stood down", out, err)
	}
	if out, err := run(t, "who", "--successor-of", "desk-2"); err != nil || out != "desk-3\n" {
		t.Fatalf("who --successor-of desk-2 = %q, %v", out, err)
	}
	if out, err := run(t, "who", "--successor-of", "desk-1", "--json"); err != nil || out != `{"drive":"d","lane":"desk-1","stood_down":true,"record":`+again+`,"successor":"","chain":[]}`+"\n" {
		t.Fatalf("who --successor-of desk-1 after a stand-down naming none = %q, %v", out, err)
	}
	if out, err := run(t, "who", "--successor-of", "desk-3", "--json"); err != nil || out != `{"drive":"d","lane":"desk-3","stood_down":false,"successor":"","chain":[]}`+"\n" {
		t.Fatalf("who --successor-of desk-3 = %q, %v", out, err)
	}
}

func TestAStandDownStandsDownOnlyItsFirstToLane(t *testing.T) {
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
	posted := make([]string, 0, 2)
	for _, args := range [][]string{
		{"--to", "board-refresher-5", "--successor", "board-refresher-6"},
		{"--to", "options-1", "--to", "board-refresher-6", "--to", "drive-watchdog"},
	} {
		out, err := run(t, append([]string{"post", "--lane", "root", "--kind", "stand-down", "--text", "stand down", "--json"}, args...)...)
		if err != nil {
			t.Fatal(err)
		}
		posted = append(posted, out)
	}
	handed, copied := seqOf(t, posted[0]), seqOf(t, posted[1])
	for lane, want := range map[string]string{
		"board-refresher-5": `{"drive":"d","lane":"board-refresher-5","stood_down":true,"record":` + handed + `,"successor":"board-refresher-6","chain":["board-refresher-6"]}`,
		"board-refresher-6": `{"drive":"d","lane":"board-refresher-6","stood_down":false,"successor":"","chain":[]}`,
		"options-1":         `{"drive":"d","lane":"options-1","stood_down":true,"record":` + copied + `,"successor":"","chain":[]}`,
		"drive-watchdog":    `{"drive":"d","lane":"drive-watchdog","stood_down":false,"successor":"","chain":[]}`,
	} {
		if out, err := run(t, "who", "--successor-of", lane, "--json"); err != nil || out != want+"\n" {
			t.Errorf("who --successor-of %s = %q, %v; want %s", lane, out, err, want)
		}
	}
}

func seqOf(t *testing.T, posted string) string {
	t.Helper()
	var r struct{ Seq int64 }
	if err := json.Unmarshal([]byte(posted), &r); err != nil || r.Seq == 0 {
		t.Fatalf("posted %q: %v", posted, err)
	}
	return strconv.FormatInt(r.Seq, 10)
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

func TestPostReplyAddressesTheLaneThatWroteTheRecordItAnswers(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	t.Setenv("CLAUDE_CODE_SESSION_ID", "session-1")
	if _, err := run(t, "drive", "use", "release-v3", "--root"); err != nil {
		t.Fatal(err)
	}
	post := func(args ...string) store.Record {
		t.Helper()
		out, err := run(t, append([]string{"post", "--json"}, args...)...)
		lines := strings.Split(strings.TrimSpace(out), "\n")
		var got store.Record
		if err != nil || json.Unmarshal([]byte(lines[len(lines)-1]), &got) != nil {
			t.Fatalf("post %v = %q, %v", args, out, err)
		}
		return got
	}
	ask := post("--lane", "lane-a", "--kind", "ask", "--text", "who owns the relay?")
	peerAsk := post("--lane", "lane-c", "--drive", "peer-drive", "--kind", "ask", "--text", "can root take this?")
	for _, tt := range []struct {
		re   store.Record
		want []string
	}{
		{ask, []string{"lane-a"}},
		{peerAsk, []string{"lane-c@peer-drive"}},
	} {
		if got := post("--lane", "root", "--kind", "answer", "--re", strconv.FormatInt(tt.re.Seq, 10), "--text", "lane-b does"); !reflect.DeepEqual(got.To, tt.want) {
			t.Errorf("reply to #%d is addressed to %v, want %v", tt.re.Seq, got.To, tt.want)
		}
	}
	if got := post("--lane", "lane-a", "--kind", "note", "--re", strconv.FormatInt(ask.Seq, 10), "--text", "still open"); len(got.To) != 0 {
		t.Errorf("a lane's follow-up to its own record is addressed to %v, want no one", got.To)
	}
	out, err := run(t, "post", "--lane", "root", "--kind", "answer", "--re", strconv.FormatInt(peerAsk.Seq, 10), "--to", "main", "--text", "taking it")
	if want := fmt.Sprintf("--to main addresses only root, the lane posting this record; #%d came from lane-c@peer-drive, which a reply with no --to reaches", peerAsk.Seq); err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("post --to its own lane = %q, %v; want the refusal %q", out, err, want)
	}
	if got := post("--lane", "lane-a", "--kind", "note", "--to", "desk-runner,lane-a", "--text", "pushed a new head"); !reflect.DeepEqual(got.To, []string{"desk-runner", "lane-a"}) {
		t.Errorf("a record to another lane and itself is addressed to %v", got.To)
	}
}

func TestAPipedReadStillShowsWhatItsBudgetCutOnStderr(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	t.Setenv("CLAUDE_CODE_SESSION_ID", "session-1")
	if _, err := run(t, "drive", "use", "piped", "--root"); err != nil {
		t.Fatal(err)
	}
	for i := range 40 {
		if _, err := run(t, "post", "--lane", "api", "--kind", "state", "--text", fmt.Sprintf("ASK step %02d %s", i, strings.Repeat("x", 120))); err != nil {
			t.Fatal(err)
		}
	}
	piped := func(args ...string) (string, string) {
		t.Helper()
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		var stderr bytes.Buffer
		root := cli.NewRootCmd()
		root.SetOut(w)
		root.SetErr(&stderr)
		root.SetArgs(args)
		runErr := root.Execute()
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		out, err := io.ReadAll(r)
		if err != nil || runErr != nil {
			t.Fatal(err, runErr)
		}
		return string(out), stderr.String()
	}
	out, stderr := piped("tail", "--drive", "piped", "--since-seq", "0", "--oldest", "--budget", "1000")
	if !strings.Contains(out, "more; resume with --since") || !strings.Contains(stderr, "more; resume with --since") {
		t.Fatalf("a piped tail cut by its budget left stderr %q; stdout:\n%s", stderr, out)
	}
	out, stderr = piped("grep", "--drive", "piped", "-n", "1", "ASK step")
	if !strings.Contains(out, "... more matches") || stderr != "... more matches; narrow with --kind, --lane or --since\n" {
		t.Fatalf("a piped grep cut by its limit left stderr %q; stdout:\n%s", stderr, out)
	}
	whole, quiet := piped("tail", "--drive", "piped", "--since-seq", "0", "--budget", "4000000")
	if quiet != "" || !strings.Contains(whole, "ASK step 39") {
		t.Fatalf("a piped tail that fits wrote stderr %q", quiet)
	}
}

func TestAReadWhoseStderrIsItsStdoutShowsItsCutOnce(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	t.Setenv("CLAUDE_CODE_SESSION_ID", "session-1")
	if _, err := run(t, "drive", "use", "merged", "--root"); err != nil {
		t.Fatal(err)
	}
	for i := range 40 {
		if _, err := run(t, "post", "--lane", "api", "--kind", "state", "--text", fmt.Sprintf("ASK step %02d %s", i, strings.Repeat("x", 120))); err != nil {
			t.Fatal(err)
		}
	}
	merged := func(args ...string) string {
		t.Helper()
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		root := cli.NewRootCmd()
		root.SetOut(w)
		root.SetErr(w)
		root.SetArgs(args)
		runErr := root.Execute()
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		out, err := io.ReadAll(r)
		if err != nil || runErr != nil {
			t.Fatal(err, runErr)
		}
		return string(out)
	}
	if out := merged("tail", "--drive", "merged", "--since-seq", "0", "--oldest", "--budget", "1000"); strings.Count(out, "more; resume with --since") != 1 {
		t.Fatalf("a tail cut by its budget into one file for stdout and stderr printed:\n%s", out)
	}
	if out := merged("grep", "--drive", "merged", "-n", "1", "ASK step"); strings.Count(out, "... more matches; narrow with --kind, --lane or --since") != 1 {
		t.Fatalf("a grep cut by its limit into one file for stdout and stderr printed:\n%s", out)
	}
}

func TestASecondThreadCannotBindALiveLane(t *testing.T) {
	if lane := os.Getenv("CCI_TEST_SECOND_BINDER"); lane != "" {
		out, err := run(t, "post", "--drive", "d", "--lane", lane, "--kind", "state", "--text", lane+" up")
		fmt.Printf("posted=%q err=%v\n", out, err)
		return
	}
	home, config := t.TempDir(), t.TempDir()
	t.Setenv("CCI_HOME", home)
	t.Setenv("CLAUDE_CONFIG_DIR", config)
	ctx := context.Background()
	st, err := store.Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	holder := store.PeerKey{Session: "48c3b7bf-holder", Agent: "ap8a-step7"}
	if _, err := st.Announce(ctx, store.Peer{Session: holder.Session, Agent: holder.Agent, AgentName: "p8a-step7"}); err != nil {
		t.Fatal(err)
	}
	if err := st.BindLane(ctx, holder, "d", "p8a-step7", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	second := func(lane, thread string, env ...string) string {
		t.Helper()
		cmd := exec.Command(os.Args[0], "-test.run=^TestASecondThreadCannotBindALiveLane$", "codex")
		cmd.Env = append(os.Environ(), "CCI_TEST_SECOND_BINDER="+lane, "CCI_HOME="+home, "CLAUDE_CONFIG_DIR="+config, "CLAUDE_CODE_SESSION_ID=", "CCI_AGENT_ID=", "CCI_PARENT_SESSION=", "CODEX_THREAD_ID="+thread)
		for _, name := range peers.IdentityEnv {
			if name != "CODEX_THREAD_ID" {
				cmd.Env = append(cmd.Env, name+"=")
			}
		}
		cmd.Env = append(cmd.Env, env...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s: %v\n%s", thread, err, out)
		}
		return string(out)
	}
	if out := second("p8a-step7", "019a3c3e-handler", "CCI_PARENT_SESSION="+holder.Session); !strings.Contains(out, "err=<nil>") {
		t.Fatalf("a thread its holder's session started posting as p8a-step7 = %s; want it posted with no binding", out)
	}
	if out := second("p8a-step7", "019a3c3e-duplicate"); !strings.Contains(out, "lane p8a-step7 is held by live thread p8a-step7 48c3b7bf-holder/ap8a-step7") || !strings.Contains(out, `posted=""`) {
		t.Fatalf("a second thread binding p8a-step7 = %s; want it refused", out)
	}
	if out := second("p8a-step7", "019a3c3e-duplicate"); !strings.Contains(out, "is held by live thread") {
		t.Fatalf("the duplicate's next post = %s; want it refused again", out)
	}
	if out := second("p8a-step7-2", "019a3c3e-successor"); !strings.Contains(out, "err=<nil>") {
		t.Fatalf("p8a-step7-2 beside a live p8a-step7 = %s; want it bound", out)
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "aaaa1111-root")
	t.Setenv("CCI_AGENT_ID", "")
	for _, name := range peers.IdentityEnv {
		if name != "CLAUDE_CODE_SESSION_ID" {
			t.Setenv(name, "")
		}
	}
	out, err := run(t, "tail", "--drive", "d", "--since", "0", "--kind", "defect", "--kind", "stand-down", "--json")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		var r struct {
			Kind   string   `json:"kind"`
			Lane   string   `json:"lane"`
			To     []string `json:"to"`
			Text   string   `json:"text"`
			Fields struct {
				Attrs map[string]string `json:"attrs"`
			} `json:"fields"`
		}
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("%q: %v", line, err)
		}
		for _, head := range []string{"second binder of p8a-step7 refused", "p8a-step7 stood down for p8a-step7-2, which registered under p8a-step7 while p8a-step7, thread p8a-step7 48c3b7bf-holder/ap8a-step7, was live"} {
			if strings.HasPrefix(r.Text, head) {
				got = append(got, fmt.Sprintf("%s by %s to %s successor %q: %s", r.Kind, r.Lane, strings.Join(r.To, ","), r.Fields.Attrs["successor"], head))
			}
		}
	}
	slices.Sort(got)
	if want := []string{
		`defect by p8a-step7 to root successor "": second binder of p8a-step7 refused`,
		`stand-down by registry to p8a-step7,p8a-step7-2 successor "p8a-step7-2": p8a-step7 stood down for p8a-step7-2, which registered under p8a-step7 while p8a-step7, thread p8a-step7 48c3b7bf-holder/ap8a-step7, was live`,
	}; !slices.Equal(got, want) {
		t.Fatalf("records = %q, want %q: the refusal reaches root, and the successor's registration stands its live predecessor down for it", got, want)
	}
	if out, err := run(t, "who", "--all", "--lane", "p8a-step7"); err != nil || !strings.Contains(out, " stood-down successor=p8a-step7-2 ") {
		t.Fatalf("who --lane p8a-step7 = %s, %v; want it stood down for p8a-step7-2", out, err)
	}
	if out, err := run(t, "who", "--all", "--lane", "p8a-step7", "--json"); err != nil || strings.Contains(out, "019a3c3e-duplicate") || strings.Contains(out, "019a3c3e-handler") {
		t.Fatalf("who --lane p8a-step7 = %s, %v; want the refused thread and the parent's child unbound", out, err)
	}
	if _, err := run(t, "post", "--drive", "d", "--lane", "root", "--kind", "stand-down", "--to", "p8a-step7", "--text", "p8a-step7 stands down"); err != nil {
		t.Fatal(err)
	}
	if out := second("p8a-step7", "019a3c3e-duplicate"); !strings.Contains(out, "err=<nil>") {
		t.Fatalf("a thread binding p8a-step7 after it stood down = %s; want it bound", out)
	}
}

func TestASpawnRecordNamesTheLaneCciWhoListsUntilItBinds(t *testing.T) {
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
	if _, err := run(t, "post", "--lane", "root", "--kind", "spawn", "--text", "SPAWNED p8a-step7-3"); err == nil || !strings.Contains(err.Error(), "kind spawn names one lane, its brief and its model") {
		t.Fatalf("a spawn naming no lane, brief or model: %v", err)
	}
	out, err := run(t, "post", "--lane", "root", "--kind", "spawn", "--lane-ref", "p8a-step7-3", "--attr", "brief=/briefs/p8a-step7-3.md", "--attr", "model=opus", "--attr", "task=1977", "--text", "SPAWNED p8a-step7-3 /briefs/p8a-step7-3.md", "--json")
	if err != nil {
		t.Fatal(err)
	}
	spawn := seqOf(t, out)
	if strings.Contains(out, `"expires_at"`) {
		t.Fatalf("spawn record = %s; want it kept, since the registry reads it as the lane's birth", out)
	}
	if out, err := run(t, "who", "--lane", "p8a-step7-3"); err != nil || !strings.Contains(out, "p8a-step7-3 spawned drive=d lane=p8a-step7-3 by #"+spawn+" ") || !strings.HasSuffix(out, "brief=/briefs/p8a-step7-3.md model=opus task=1977, not bound yet\n") {
		t.Fatalf("who --lane p8a-step7-3 after its spawn = %q, %v", out, err)
	}
	if out, err := run(t, "who", "--lane", "p8a-step7-3", "--json"); err != nil || out != "" {
		t.Fatalf("who --json after a spawn = %q, %v; want peer rows only", out, err)
	}
	st, err := store.Open(context.Background(), os.Getenv("CCI_HOME"))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.BindLane(context.Background(), store.PeerKey{Session: "bbbb2222-lane", Agent: "ap8a-step7-3"}, "d", "p8a-step7-3", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if out, err := run(t, "who", "--all", "--lane", "p8a-step7-3"); err != nil || strings.Contains(out, "not bound yet") {
		t.Fatalf("who --all --lane p8a-step7-3 once bound = %q, %v", out, err)
	}
}

func TestReadsTakeTheFlagsLanesReachFor(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	if _, err := run(t, "post", "--drive", "d", "--lane", "root", "--kind", "note", "--text", "needle "+strings.Repeat("hay ", 50)+"TAIL"); err != nil {
		t.Fatal(err)
	}
	out, err := run(t, "search", "needle", "--drive", "d", "--width", "60")
	if err != nil || !strings.Contains(out, "needle") || strings.Contains(out, "TAIL") {
		t.Errorf("search needle --width 60 = %q, %v; want the match clipped to 60 characters", out, err)
	}
	if out, err := run(t, "tail", "--drive", "d", "--since", "2000-01-01"); err != nil || !strings.Contains(out, "needle") {
		t.Errorf("tail --since 2000-01-01 = %q, %v; want the record posted after that date", out, err)
	}
}

func TestTailMailAndTheHookDeliverARecordOnce(t *testing.T) {
	ctx := context.Background()
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "session-1")
	st, err := store.Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	k := store.PeerKey{Session: "session-1"}
	if err := st.BindLane(ctx, k, "d", "api", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := st.SetCursor(ctx, store.LaneCursor(k), "d", 0); err != nil {
		t.Fatal(err)
	}
	hookReads := func() string {
		t.Helper()
		var out bytes.Buffer
		if err := hook.PostToolUse(ctx, st, hook.Payload{SessionID: "session-1", Event: "PostToolUse"}, func() (store.Window, error) { return store.Window{PID: 1}, nil }, &out); err != nil {
			t.Fatal(err)
		}
		return out.String()
	}
	post := func(text string) {
		t.Helper()
		if _, err := run(t, "post", "--drive", "d", "--lane", "root", "--kind", "go", "--to", "api,web", "--text", text); err != nil {
			t.Fatal(err)
		}
	}
	wait := []string{"tail", "--drive", "d", "--to", "api", "--cursor", "api-wake", "--seed", "api", "--mail"}
	post("the wait reads this")
	out, err := run(t, wait...)
	if err != nil || !strings.Contains(out, "the wait reads this") || strings.Contains(out, "web") {
		t.Fatalf("tail --mail = %q, %v", out, err)
	}
	if got := hookReads(); got != "" {
		t.Fatalf("hook redelivered what the wait printed: %q", got)
	}
	post("the hook reads this")
	if got := hookReads(); !strings.Contains(got, "the hook reads this") || strings.Contains(got, "web") {
		t.Fatalf("hook = %q", got)
	}
	if out, err = run(t, wait...); err != nil || out != "" {
		t.Fatalf("tail --mail redelivered what the hook injected: %q, %v", out, err)
	}
	if out, err = run(t, "tail", "--drive", "d", "--to", "web", "--since", "0", "--mail"); err != nil || !strings.Contains(out, "the wait reads this") {
		t.Fatalf("tail --mail of another lane = %q, %v, want its ordinary read", out, err)
	}
}

func recordLines(out string) []string {
	var lines []string
	for line := range strings.Lines(out) {
		if !strings.HasPrefix(line, "@") {
			lines = append(lines, strings.TrimSuffix(line, "\n"))
		}
	}
	return lines
}

func TestABoundWaitSkipsWhatTheLaneReadThroughItsOwnCursorWhateverItsWakeCursorSays(t *testing.T) {
	ctx := context.Background()
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "session-1")
	st, err := store.Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	k := store.PeerKey{Session: "session-1"}
	if err := st.BindLane(ctx, k, "d", "api", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := st.SetCursor(ctx, store.LaneCursor(k), "d", 0); err != nil {
		t.Fatal(err)
	}
	post := func(text string) {
		t.Helper()
		if _, err := run(t, "post", "--drive", "d", "--lane", "root", "--kind", "go", "--to", "api", "--text", text); err != nil {
			t.Fatal(err)
		}
	}
	post("the lane already read this")
	if out, err := run(t, "tail", "--drive", "d", "--cursor", "api", "--to", "api"); err != nil || !strings.Contains(out, "the lane already read this") {
		t.Fatalf("the lane's own read = %q, %v", out, err)
	}
	if _, err := run(t, "cursor", "set", "api-wake", "0", "--drive", "d"); err != nil {
		t.Fatal(err)
	}
	wait := []string{"tail", "--drive", "d", "--to", "api", "--cursor", "api-wake", "--seed", "api", "--mail"}
	if out, err := run(t, wait...); err != nil || out != "" {
		t.Fatalf("the wait replayed what the lane read: %q, %v", out, err)
	}
	post("this arrived after")
	if out, err := run(t, wait...); err != nil || !strings.Contains(out, "this arrived after") || strings.Contains(out, "the lane already read this") {
		t.Fatalf("the wait after a new record = %q, %v", out, err)
	}
}

func TestAWaitSkipsWhatTheLaneReadThroughTheInboxToolSinceItsLastWait(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	t.Setenv("CLAUDE_CODE_SESSION_ID", "sprite-session")
	post := func(text string) {
		t.Helper()
		if _, err := run(t, "post", "--drive", "d", "--lane", "root", "--kind", "go", "--to", "api", "--text", text); err != nil {
			t.Fatal(err)
		}
	}
	wait := []string{"tail", "--drive", "d", "--to", "api", "--cursor", "api-wake", "--seed", "api", "--mail"}
	post("before the first wait")
	if _, err := run(t, wait...); err != nil {
		t.Fatal(err)
	}
	post("the lane read this through its inbox")
	if got := mustTool(t, "inbox", map[string]any{"lane": "api", "drive": "d"}); !strings.Contains(got, "the lane read this through its inbox") {
		t.Fatalf("inbox = %q", got)
	}
	if out, err := run(t, wait...); err != nil || out != "" {
		t.Fatalf("the next wait replayed what the inbox read: %q, %v", out, err)
	}
	post("the owner's pick")
	if out, err := run(t, wait...); err != nil || !strings.Contains(out, "the owner's pick") || strings.Contains(out, "through its inbox") {
		t.Fatalf("the wait after a new record = %q, %v", out, err)
	}
}

func TestCursorShowPrintsWhereACursorStandsWithoutMovingIt(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	if _, err := run(t, "cursor", "show", "api-wake", "--drive", "d"); err == nil || !strings.Contains(err.Error(), "has not been set") {
		t.Fatalf("cursor show before any set = %v; want it unset", err)
	}
	if _, err := run(t, "cursor", "set", "api-wake", "7", "--drive", "d"); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if out, err := run(t, "cursor", "show", "api-wake", "--drive", "d"); err != nil || out != "cursor api-wake in d = #7\n" {
			t.Fatalf("cursor show = %q, %v; want #7 both times", out, err)
		}
	}
	if _, err := run(t, "cursor", "show", "api-wake", "--drive", "other"); err == nil {
		t.Fatal("cursor show on another drive found the cursor")
	}
}

func TestABoundWaitPrintsAnAskItsAskerAnsweredSomeoneElseOnTheSameTopic(t *testing.T) {
	ctx := context.Background()
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "session-1")
	st, err := store.Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	k := store.PeerKey{Session: "session-1"}
	if err := st.BindLane(ctx, k, "d", "fidelity-shard-5", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := st.SetCursor(ctx, store.LaneCursor(k), "d", 0); err != nil {
		t.Fatal(err)
	}
	for _, post := range [][]string{
		{"--lane", "root", "--kind", "ask", "--to", "fidelity-shard-5", "--topic", "m814", "--text", "what are these four parked on"},
		{"--lane", "root", "--kind", "answer", "--to", "owner-relay", "--topic", "m814", "--text", "item 3 needs no revive"},
		{"--lane", "answer-wakes-grade-2", "--kind", "ask", "--to", "fidelity-shard-5", "--text", "what did you change"},
	} {
		if _, err := run(t, append([]string{"post", "--drive", "d"}, post...)...); err != nil {
			t.Fatal(err)
		}
	}
	out, err := run(t, "tail", "--drive", "d", "--cursor", "fidelity-shard-5-wake", "--seed", "fidelity-shard-5", "--to", "fidelity-shard-5", "--mail")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"what are these four parked on", "what did you change"} {
		if !strings.Contains(out, want) {
			t.Errorf("the wait printed %q, want %q", out, want)
		}
	}
	if strings.Contains(out, "item 3 needs no revive") {
		t.Errorf("the wait printed %q, want no answer addressed to owner-relay", out)
	}
}
