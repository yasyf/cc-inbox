package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

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
	if out, err = run(t, "post", "--lane", "evidence-lane", "--kind", "evidence", "--text", "5xx began at 5:40 PM"); err != nil || out != "#3\n" {
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
	if got.Text != "perf findings for api-sql" {
		t.Errorf("spilled record text = %q, want the first line", got.Text)
	}
	if body, err := os.ReadFile(got.Refs.Path); err != nil || string(body) != long+"\n" {
		t.Errorf("spilled file %q = %q, %v; want the full text", got.Refs.Path, body, err)
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

func TestPostSpillsTheStampedText(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	typed := time.Now().Add(7 * time.Minute).Format("3:04 PM")
	rest := "\n" + strings.Repeat("x", 500) + "\nlive since "
	got := postJSON(t, "R84 ("+typed+") orca-desk: relay to api"+rest+"{at}")
	first := "R84 (" + got.stamp() + ") orca-desk: relay to api"
	if got.Text != first {
		t.Errorf("spilled record text = %q, want %q", got.Text, first)
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
	out, err := run(t, "post", "--lane", "root", "--kind", "go", "--to", "release-owner-4,desk,root,nobody", "--text", "GO promote")
	if err != nil || out != "cci: --to release-owner-4: release-owner-4 reads drive drive-b, not drive-a; addressed release-owner-4@drive-b\n#1\n" {
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
	for _, pr := range []string{"Forge-FDE/iris-chime#243", "https://github.com/Forge-FDE/iris-chime/pull/243", "30431"} {
		if out, err := run(t, "tail", "--since", "0", "--pr", pr); err != nil || strings.Count(out, "\n") != 1 || !strings.Contains(out, "chime fix") {
			t.Fatalf("tail --pr %s = %q, %v", pr, out, err)
		}
	}
	if out, err := run(t, "tail", "--since", "0", "--pr", "243"); err != nil || strings.Contains(out, "chime fix") {
		t.Fatalf("tail --pr 243 = %q, %v", out, err)
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
	want := []string{"root opened #36276 [36276]", "aspeed-0123 opened #36148 [36148]"}
	if !slices.Equal(got, want) {
		t.Fatalf("opened records = %q, want %q", got, want)
	}
}
