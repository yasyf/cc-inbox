package cli_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/cli"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
)

type heldCommandInput struct {
	entered chan struct{}
	release <-chan struct{}
	body    io.Reader
	once    sync.Once
}

func (in *heldCommandInput) Read(p []byte) (int, error) {
	in.once.Do(func() {
		close(in.entered)
		<-in.release
	})
	return in.body.Read(p)
}

func TestCommandInputDoesNotHoldStoreLocks(t *testing.T) {
	for _, tt := range []struct {
		name string
		args []string
		body string
		lane string
		kind kinds.Kind
	}{
		{
			name: "prompt",
			args: []string{"hook", "prompt"},
			body: `{"session_id":"input-session","hook_event_name":"UserPromptSubmit","prompt":"input record"}`,
			lane: "owner",
			kind: kinds.Owner,
		},
		{
			name: "stamped post",
			args: []string{"post", "--drive", "input-drive", "--lane", "input-lane", "--kind", "note", "--stamp", "--text", "-"},
			body: "input record\n",
			lane: "input-lane",
			kind: kinds.Note,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("CCI_HOME", home)
			t.Setenv("CLAUDE_CODE_SESSION_ID", "")
			t.Setenv("CODEX_THREAD_ID", "")
			st, err := store.Open(t.Context(), home)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			peer := store.PeerKey{Session: "input-session"}
			if err := st.Bind(t.Context(), peer, "input-drive", true); err != nil {
				t.Fatal(err)
			}
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			input := &heldCommandInput{entered: make(chan struct{}), release: release, body: strings.NewReader(tt.body)}
			var out bytes.Buffer
			root := cli.NewRootCmd()
			root.SetArgs(tt.args)
			root.SetIn(input)
			root.SetOut(&out)
			root.SetErr(&out)
			done := make(chan error, 1)
			go func() { done <- root.ExecuteContext(t.Context()) }()
			select {
			case <-input.entered:
			case err := <-done:
				t.Fatalf("command returned before input: %v", err)
			case <-time.After(2 * time.Second):
				t.Fatal("command did not reach input")
			}
			lock, err := os.OpenFile(filepath.Join(home, "init.lock"), os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = lock.Close() })
			if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
				t.Fatalf("command held init lock while reading input: %v", err)
			}
			if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_UN); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			if _, _, err := st.Post(ctx, store.Record{Drive: "input-drive", Lane: "concurrent", Kind: kinds.Note, Text: "while input waits", Source: "test"}, 0); err != nil {
				t.Fatalf("command held writer lock while reading input: %v", err)
			}
			unblock()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("command after input = %v; output %q", err, out.String())
				}
			case <-time.After(2 * time.Second):
				t.Fatal("command did not finish after input")
			}
			records, err := st.Query(t.Context(), store.Filter{Drive: "input-drive", Lanes: []string{tt.lane}, Kinds: []kinds.Kind{tt.kind}})
			if err != nil || len(records) != 1 || !strings.HasSuffix(records[0].Text, "input record") {
				t.Fatalf("input record = %+v, %v", records, err)
			}
			if seq, ok, err := st.Cursor(t.Context(), peer.String(), "input-drive"); err != nil || !ok || seq != 0 {
				t.Fatalf("input changed cursor = %d, %t, %v", seq, ok, err)
			}
		})
	}
}
