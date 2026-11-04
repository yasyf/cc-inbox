package cli_test

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/cli"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
	"github.com/yasyf/cc-inbox/internal/version"
	"github.com/yasyf/cc-inbox/internal/wire"
)

func commandSocket(t *testing.T, st *store.Store) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "cci")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path := filepath.Join(dir, wire.SocketName)
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- cli.ServeCommands(ctx, ln, st, st.HomeDir()) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	return path
}

type relayed struct {
	code           int
	ran            bool
	stdout, stderr string
}

func relayCommand(t *testing.T, socket string, request wire.Request, stdin string) relayed {
	t.Helper()
	conn, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if request.Version == "" {
		request.Version = version.String()
	}
	var stdout, stderr bytes.Buffer
	code, ran := wire.Exchange(conn, request, strings.NewReader(stdin), &stdout, &stderr)
	return relayed{code, ran, stdout.String(), stderr.String()}
}

func TestDaemonRunsAPostAndATailForItsClient(t *testing.T) {
	st, _ := testutil.Store(t)
	socket := commandSocket(t, st)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "body.txt"), []byte(strings.Repeat("from the client's directory ", 40)), 0o600); err != nil {
		t.Fatal(err)
	}
	env := []string{"CLAUDE_LONG_RUNNING_DRIVE=d", "CLAUDE_CODE_SESSION_ID=client-session"}
	post := relayCommand(t, socket, wire.Request{Argv: []string{"cci", "post", "--lane", "root", "--kind", "note", "--path", "body.txt", "from a client"}, Env: env, Cwd: dir, PID: os.Getpid()}, "")
	if !post.ran || post.code != 0 {
		t.Fatalf("post = %+v", post)
	}
	tail := relayCommand(t, socket, wire.Request{Argv: []string{"cci", "tail", "--json"}, Env: env, Cwd: dir, PID: os.Getpid()}, "")
	if !tail.ran || tail.code != 0 || !strings.Contains(tail.stdout, "from a client") {
		t.Fatalf("tail = %+v", tail)
	}
	again := relayCommand(t, socket, wire.Request{Argv: []string{"cci", "tail", "--json"}, Env: env, Cwd: dir, PID: os.Getpid()}, "")
	if again.code != 0 || strings.Contains(again.stdout, "from a client") {
		t.Fatalf("second tail = %+v, want the cursor past the record", again)
	}
	if _, moved, err := st.Cursor(t.Context(), "client-session", "d"); err != nil || !moved {
		t.Fatalf("cursor moved = %t, %v", moved, err)
	}
}

func TestDaemonReadsAClientsStdin(t *testing.T) {
	st, _ := testutil.Store(t)
	socket := commandSocket(t, st)
	post := relayCommand(t, socket, wire.Request{Argv: []string{"cci", "post", "--drive", "d", "--lane", "root", "--kind", "note", "--text", "-"}, Cwd: t.TempDir(), PID: os.Getpid()}, "piped in\n")
	if post.code != 0 {
		t.Fatalf("post = %+v", post)
	}
	records, err := st.Query(t.Context(), store.Filter{Drive: "d"})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || !strings.Contains(records[0].Text, "piped in") {
		t.Fatalf("records = %+v", records)
	}
}

func TestDaemonLeavesTheCursorWhenTheClientDiesBeforeWriting(t *testing.T) {
	st, _ := testutil.Store(t)
	testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "not delivered"})
	socket := commandSocket(t, st)
	conn, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	code, ran := wire.Exchange(conn, wire.Request{Version: version.String(), Argv: []string{"cci", "tail", "--drive", "d", "--cursor", "c"}, Cwd: t.TempDir(), PID: os.Getpid()}, strings.NewReader(""), failingWriter{}, &bytes.Buffer{})
	if !ran || code != 1 {
		t.Fatalf("Exchange = %d, %t", code, ran)
	}
	_ = conn.Close()
	if seq, moved, err := st.Cursor(t.Context(), "c", "d"); err != nil || moved {
		t.Fatalf("cursor at %d, moved = %t, %v", seq, moved, err)
	}
}

func TestDaemonRunsAHookFromItsPayload(t *testing.T) {
	st, _ := testutil.Store(t)
	socket := commandSocket(t, st)
	hook := relayCommand(t, socket, wire.Request{Argv: []string{"cci", "hook", "post-tool-use"}, Cwd: t.TempDir(), PID: os.Getpid()}, `{"session_id":"a-session","hook_event_name":"PostToolUse"}`)
	if !hook.ran || hook.code != 0 || hook.stderr != "" {
		t.Fatalf("hook = %+v", hook)
	}
}

func TestDaemonSendsOtherVersionsAndOtherCommandsBack(t *testing.T) {
	st, _ := testutil.Store(t)
	socket := commandSocket(t, st)
	for _, request := range []wire.Request{
		{Version: "0.0.0-other", Argv: []string{"cci", "tail"}},
		{Argv: []string{"cci", "channel"}},
		{Argv: []string{"cci", "hook", "session-start"}},
		{Argv: []string{"cci", "post", "--path", "/dev/fd/3"}},
	} {
		if got := relayCommand(t, socket, request, ""); got.ran {
			t.Errorf("%v ran in the daemon: %+v", request.Argv, got)
		}
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, os.ErrClosed }
