package cli_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/cli"
	"github.com/yasyf/cc-inbox/internal/peers"
)

type cancelOnWrite struct {
	out    bytes.Buffer
	cancel context.CancelFunc
}

func (w *cancelOnWrite) Write(p []byte) (int, error) {
	w.cancel()
	return w.out.Write(p)
}

func TestAWatchCutShortByItsContextFailsInsteadOfExitingZero(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())
	for _, name := range peers.IdentityEnv {
		t.Setenv(name, "")
	}
	if _, err := run(t, "post", "--drive", "d", "--lane", "sv", "--kind", "state", "--text", "word from sv"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	out := &cancelOnWrite{cancel: cancel}
	var stderr bytes.Buffer
	root := cli.NewRootCmd()
	root.SetArgs([]string{"watch", "--drive", "d", "--since", "1h", "--interval", "10ms"})
	root.SetOut(out)
	root.SetErr(&stderr)

	err := root.ExecuteContext(ctx)

	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "watch stopped") || !strings.Contains(out.out.String(), "word from sv") {
		t.Fatalf("watch ended with %v after %q; want a nonzero stop that names itself", err, out.out.String())
	}
}
