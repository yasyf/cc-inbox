package cli_test

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/cli"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
	"github.com/yasyf/cc-inbox/internal/version"
	"github.com/yasyf/cc-inbox/internal/wire"
)

func TestADaemonRestartLosesAWatchInsteadOfEndingIt(t *testing.T) {
	for range 20 {
		st, _ := testutil.Store(t)
		testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Note, Text: "before the restart"})
		dir, err := os.MkdirTemp("", "cci")
		if err != nil {
			t.Fatal(err)
		}
		ln, err := net.Listen("unix", filepath.Join(dir, wire.SocketName))
		if err != nil {
			t.Fatal(err)
		}
		daemon, restart := context.WithCancel(context.Background())
		served := make(chan error, 1)
		go func() { served <- cli.ServeCommands(daemon, ln, st, st.HomeDir()) }()
		conn, err := net.Dial("unix", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			deadline := time.Now().Add(10 * time.Second)
			for time.Now().Before(deadline) {
				if _, moved, err := st.Cursor(t.Context(), "c", "d"); err == nil && moved {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			restart()
		}()

		var stdout, stderr bytes.Buffer
		request := wire.Request{Version: version.String(), Argv: []string{"cci", "watch", "--drive", "d", "--since", "1h", "--cursor", "c", "--interval", "10ms"}, Cwd: t.TempDir(), PID: os.Getpid()}
		code, ran, lost := wire.ExchangeOutcome(conn, request, bytes.NewReader(nil), &stdout, &stderr)

		if !ran || !lost || code != 0 || stderr.Len() != 0 || !bytes.Contains(stdout.Bytes(), []byte("before the restart")) {
			t.Fatalf("watch cut by a daemon restart = code %d, ran %t, lost %t, stdout %q, stderr %q; want the connection dropped quietly so the client resumes", code, ran, lost, stdout.String(), stderr.String())
		}
		restart()
		if err := <-served; err != nil {
			t.Fatal(err)
		}
		_ = conn.Close()
		_ = os.RemoveAll(dir)
	}
}
