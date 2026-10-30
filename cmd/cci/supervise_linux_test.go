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
	if _, _, err := st.Post(ctx, store.Record{Drive: "d", Lane: "lane-a", Kind: kinds.Decision, Text: "posted with no daemon running"}, 0); err != nil {
		t.Fatal(err)
	}

	logPath := filepath.Join(t.TempDir(), "supervise.log")
	out, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = out.Close() })
	cmd := exec.Command(os.Args[0], "supervise")
	cmd.Env = append(os.Environ(), "CCI_TEST_AS_CCI=1", "HOME="+t.TempDir(), "CCI_HOME="+home)
	cmd.Stdout, cmd.Stderr = out, out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		_ = cmd.Wait()
	})

	deadline := time.Now().Add(time.Minute)
	for {
		rep, err := n.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if rep.Unpushed == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the supervised daemon never pushed: %d records unpushed\n%s", rep.Unpushed, logged(t, logPath))
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("cci supervise exited %v after SIGTERM\n%s", err, logged(t, logPath))
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
