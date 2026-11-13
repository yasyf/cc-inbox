package supervisor

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func TestCommandLineStartsANewSessionThenResumesIt(t *testing.T) {
	config := t.TempDir()
	t.Setenv("CLAUDE_CONFIG_DIR", config)
	ctx := context.Background()
	r := store.Registration{Subscription: store.Subscription{Session: "s1"}, Command: "claude -n release-demo-coordinator", ResumePrompt: "resume as the drive's coordinator"}
	if got, err := CommandLine(ctx, r); err != nil || got != `claude -n release-demo-coordinator --session-id s1 -- 'resume as the drive'\''s coordinator'` {
		t.Fatalf("CommandLine before a transcript = %q, %v", got, err)
	}
	projects := filepath.Join(config, "projects", "-work-release-demo")
	if err := os.MkdirAll(projects, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(projects, "s1.jsonl"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	r.ResumePrompt = ""
	if got, err := CommandLine(ctx, r); err != nil || got != "claude -n release-demo-coordinator --resume s1" {
		t.Fatalf("CommandLine once the transcript exists = %q, %v", got, err)
	}
}

func TestLaunchStartsTheTmuxSessionThatCopiesEachAttachingOrcaTerminal(t *testing.T) {
	bin, err := exec.LookPath("tmux")
	if err != nil {
		t.Fatal(err)
	}
	sockets, err := os.MkdirTemp("", "tmux")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockets) })
	t.Setenv("TMUX_TMPDIR", sockets)
	t.Setenv("TMUX", "")
	t.Setenv("TMUX_PANE", "")
	t.Cleanup(func() { _ = exec.Command(bin, "kill-server").Run() })
	shell := filepath.Join(t.TempDir(), "shell")
	if err := os.WriteFile(shell, []byte("#!/bin/sh\nexec sleep 60\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	r := store.Registration{Subscription: store.Subscription{Alias: "release-demo-coordinator"}, Cwd: t.TempDir(), Shell: shell, Tmux: bin}
	if started, err := Launch(ctx, r); err != nil || !started {
		t.Fatalf("Launch = %v, %v; want the tmux session started", started, err)
	}
	out, err := exec.CommandContext(ctx, bin, "show-options", "-t", "=release-demo-coordinator:", "update-environment").CombinedOutput()
	if err != nil || !strings.Contains(string(out), orcaTerminal) {
		t.Fatalf("the session's update-environment = %q, %v; want it to name %s", out, err, orcaTerminal)
	}
}

func TestRunStartsTheSessionAgainUntilItsAliasIsRemoved(t *testing.T) {
	defer func(pause time.Duration) { restartPause = pause }(restartPause)
	restartPause = 10 * time.Millisecond
	config := t.TempDir()
	t.Setenv("CLAUDE_CONFIG_DIR", config)
	t.Setenv("CLAUDECODE", "1")
	t.Setenv("CLAUDE_CODE_SESSION_ID", "the-session-that-started-tmux")
	t.Setenv("ORCA_TERMINAL_HANDLE", "term_that_started_tmux")
	tmux := filepath.Join(t.TempDir(), "tmux")
	if err := os.WriteFile(tmux, []byte("#!/bin/sh\n[ \"$1\" = show-environment ] && echo ORCA_TERMINAL_HANDLE=term_attached\n"), 0o700); err != nil { //nolint:gosec // G306: the fake tmux runs as a program.
		t.Fatal(err)
	}
	projects := filepath.Join(config, "projects", "-work-release-demo")
	if err := os.MkdirAll(projects, 0o700); err != nil {
		t.Fatal(err)
	}
	starts := filepath.Join(t.TempDir(), "starts")
	st, _ := testutil.Store(t)
	ctx := context.Background()
	r := store.Registration{
		Subscription: store.Subscription{Alias: "release-demo-coordinator", Session: "s1", Drive: "d", Reader: "root", Cursor: "coordinator"},
		Cwd:          t.TempDir(), Shell: "/bin/sh", Tmux: tmux, ResumePrompt: "resume as the coordinator",
		Command: "touch " + filepath.Join(projects, "s1.jsonl") + `; echo "${CLAUDECODE:-top-level}" "${CLAUDE_CODE_SESSION_ID:-}" "${ORCA_TERMINAL_HANDLE:-}" >> ` + starts,
	}
	if _, err := st.Register(ctx, r); err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	ran := make(chan error, 1)
	go func() { ran <- Run(ctx, st, r.Alias, strings.NewReader(""), io.Discard, &stderr) }()
	var lines []string
	for deadline := time.Now().Add(5 * time.Second); len(lines) < 2; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("Run started the session %d times in 5s, want 2", len(lines))
		}
		out, _ := os.ReadFile(starts) //nolint:gosec // G304: the command's log under the test's temp dir.
		lines = strings.FieldsFunc(string(out), func(r rune) bool { return r == '\n' })
	}
	if _, ok, err := st.Unregister(ctx, r.Alias); err != nil || !ok {
		t.Fatalf("Unregister = %v, %v", ok, err)
	}
	select {
	case err := <-ran:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run kept starting the session after its alias was removed")
	}
	if lines[0] != "top-level  term_attached --session-id s1 -- resume as the coordinator" || lines[1] != "top-level  term_attached --resume s1 -- resume as the coordinator" {
		t.Fatalf("the session started with %q, want a top-level session in the attached Orca terminal with --session-id first and --resume after", lines)
	}
	if !strings.Contains(stderr.String(), "cci: starting release-demo-coordinator again: touch ") || !strings.HasSuffix(stderr.String(), "cci: release-demo-coordinator is no longer registered, so nothing starts it again\n") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}
