package cli_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/cli"
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
		{[]string{"post", "--lane", "root", "--kind", "hold", "--topic", "#30431", "--to", "landing-desk", "--text", "hold until the walker reports"}, "#2\n"},
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
	if out, err = run(t, "grep", "WALKER"); err != nil || !strings.Contains(out, "#2 ") {
		t.Fatalf("grep = %q, %v", out, err)
	}
}

func TestGrepMatchesWholeRecordLinesLiterally(t *testing.T) {
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
		{[]string{"census|GREEN"}, nil},
		{[]string{"--regex", "census|GREEN"}, []string{"STATE census", "runner-protocol-idle-tenant GREEN"}},
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
