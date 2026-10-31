package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/node"
	"github.com/yasyf/cc-inbox/internal/relay"
	"github.com/yasyf/cc-inbox/internal/relay/relaytest"
	"github.com/yasyf/cc-inbox/internal/store"
)

func TestMain(m *testing.M) {
	if os.Getenv("CCI_TEST_AS_CCI") == "1" {
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestSuperviseKeepsAJoinedRelayPushing(t *testing.T) {
	st, n := joinedNode(t)
	post(t, st, "posted with no daemon running")
	sv := startSupervise(t, st.HomeDir())
	waitPushed(t, n, sv)
	sv.stop(t)
}

func TestASecondSuperviseWaitsForTheLabelAndTakesOver(t *testing.T) {
	st, n := joinedNode(t)
	post(t, st, "posted before the first supervisor")
	first := startSupervise(t, st.HomeDir())
	waitPushed(t, n, first)
	second := startSupervise(t, st.HomeDir())
	select {
	case <-second.exited:
		t.Fatalf("a second cci supervise exited while the first held the label: %v\n%s", second.err, logged(t, second.log))
	case <-time.After(5 * time.Second):
	}
	first.stop(t)
	post(t, st, "posted after the first supervisor stopped")
	waitPushed(t, n, second)
	second.stop(t)
}

type supervisor struct {
	cmd    *exec.Cmd
	log    string
	exited chan struct{}
	err    error
}

func joinedNode(t *testing.T) (*store.Store, *relay.Node) {
	t.Helper()
	ctx := context.Background()
	srv := relaytest.NewServer(t)
	home := t.TempDir()
	st, err := store.Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	id, err := node.Load(home)
	if err != nil {
		t.Fatal(err)
	}
	srv.AddMember(id, string(node.TierOwner))
	n, err := relay.Open(home, st, id, relay.Config{URL: srv.URL, Mesh: relay.DefaultMesh, Tier: string(node.TierOwner)})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Join(ctx); err != nil {
		t.Fatal(err)
	}
	return st, n
}

func post(t *testing.T, st *store.Store, text string) {
	t.Helper()
	if _, _, err := st.Post(context.Background(), store.Record{Drive: "d", Lane: "lane-a", Kind: kinds.Decision, Text: text}, 0); err != nil {
		t.Fatal(err)
	}
}

func startSupervise(t *testing.T, home string) *supervisor {
	t.Helper()
	sv := &supervisor{log: filepath.Join(t.TempDir(), "supervise.log"), exited: make(chan struct{})}
	out, err := os.Create(sv.log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = out.Close() })
	sv.cmd = exec.Command(os.Args[0], "supervise")
	sv.cmd.Env = append(os.Environ(), "CCI_TEST_AS_CCI=1", "CCI_HOME="+home)
	sv.cmd.Stdout, sv.cmd.Stderr = out, out
	if err := sv.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		sv.err = sv.cmd.Wait()
		close(sv.exited)
	}()
	t.Cleanup(func() {
		_ = sv.cmd.Process.Signal(syscall.SIGTERM)
		<-sv.exited
	})
	return sv
}

func (sv *supervisor) stop(t *testing.T) {
	t.Helper()
	if err := sv.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	<-sv.exited
	if sv.err != nil {
		t.Fatalf("cci supervise exited %v after SIGTERM\n%s", sv.err, logged(t, sv.log))
	}
}

func waitPushed(t *testing.T, n *relay.Node, sv *supervisor) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		rep, err := n.Status(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if rep.Unpushed == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the supervised daemon never pushed: %d records unpushed\n%s", rep.Unpushed, logged(t, sv.log))
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func logged(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
