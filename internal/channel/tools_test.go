package channel

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	"slices"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	mcp "github.com/yasyf/cc-interact/channel"

	"github.com/yasyf/cc-inbox/internal/version"
)

func TestTheToolsServerReexecutesANewerInstallOnlyOnceItsCallIsAnswered(t *testing.T) {
	stdin, client, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	replies, out := io.Pipe()
	started, release := make(chan struct{}), make(chan struct{})
	hold := mcp.Tool{Name: "hold", InputSchema: map[string]any{"type": "object"}, Handler: func(context.Context, json.RawMessage, func(string)) (string, bool) {
		close(started)
		<-release
		return "held", false
	}}
	var installed atomic.Bool
	newerInstall = func() (version.Install, bool, error) {
		return version.Install{Path: "/opt/cci/0.56.0/cci", Version: "0.56.0"}, installed.Load(), nil
	}
	argvs := make(chan []string, 1)
	execve = func(_ string, argv []string, _ []string) error {
		select {
		case argvs <- argv:
		default:
		}
		return nil
	}
	t.Cleanup(func() { newerInstall, execve = defaultNewerInstall, syscall.Exec })
	served := make(chan error, 1)
	go func() {
		served <- ServeTools(context.Background(), mcp.NewServer(mcp.ServerInfo{Name: "cci"}, []mcp.Tool{hold}), stdin, out)
	}()

	if _, err := client.WriteString(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"hold"}}` + "\n"); err != nil {
		t.Fatal(err)
	}
	<-started
	installed.Store(true)
	select {
	case argv := <-argvs:
		t.Fatalf("exec %q while call 1 was unanswered", argv)
	case <-time.After(3 * releaseTicks * idle):
	}

	close(release)
	lines := bufio.NewScanner(replies)
	if !lines.Scan() {
		t.Fatalf("no reply to call 1: %v", lines.Err())
	}
	var reply struct {
		ID     int `json:"id"`
		Result struct {
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		} `json:"result"`
	}
	if err := json.Unmarshal(lines.Bytes(), &reply); err != nil || reply.ID != 1 || len(reply.Result.Content) != 1 || reply.Result.Content[0].Text != "held" {
		t.Fatalf("reply %s (%v), want call 1's result held", lines.Bytes(), err)
	}
	select {
	case argv := <-argvs:
		if want := []string{"/opt/cci/0.56.0/cci", "mcp", "--resumed"}; !slices.Equal(argv, want) {
			t.Fatalf("exec argv = %q, want %q", argv, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the tools server never re-executed the newer install once call 1 was answered")
	}

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-served; err != nil {
		t.Fatal(err)
	}
	if err := stdin.Close(); err != nil {
		t.Fatal(err)
	}
}
