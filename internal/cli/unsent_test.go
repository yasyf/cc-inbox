package cli_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/yasyf/cc-inbox/internal/cli"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
)

func TestAPostWhoseStoreReadHitsItsDeadlineIsABusDown(t *testing.T) {
	store := cli.UntilStored(func(*cobra.Command, []string) error {
		return fmt.Errorf("read the refusals of lane-a on d1: %w", context.DeadlineExceeded)
	})
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())

	var down *cli.BusDown
	if err := store(cmd, nil); !errors.As(err, &down) {
		t.Fatalf("a store read past its deadline = %v; want a BusDown", err)
	}
}

func TestAPostTheBusCannotStoreTellsItsSenderAndTheNextPostStoresIt(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "session-1")
	if _, err := run(t, "post", "--drive", "d1", "--lane", "root", "--kind", "note", "--text", "before the outage"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cli.SetPostPatience(500 * time.Millisecond))
	lock, err := os.OpenFile(filepath.Join(home, "init.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lock.Close() })
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}

	refusal := `go refused: "approve, then land" reads as key=value, not one reason`
	_, err = run(t, "post", "--drive", "d1", "--lane", "lane-a", "--kind", "note", "--to", "root", "--text", "the coordinator decision", "--attr", "refusal="+refusal, "--attr", "step=two")
	var down *cli.BusDown
	if !errors.As(err, &down) || down.Path == "" || !strings.Contains(err.Error(), "NOT stored yet") {
		t.Fatalf("a post the bus could not store = %v; want a BusDown naming the kept post", err)
	}
	_, err = run(t, "post", "--lane", "lane-a", "--kind", "note", "--text", "no drive named")
	if !errors.As(err, &down) || down.Path != "" || !strings.Contains(err.Error(), "repost it with --lane and --drive") {
		t.Fatalf("a post naming no --drive = %v; want a BusDown that keeps nothing and says to repost", err)
	}

	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	out, err := run(t, "post", "--drive", "d1", "--lane", "root", "--kind", "note", "--text", "after the outage")
	if err != nil || !strings.Contains(out, "stored 1 post(s) kept while the bus was down") {
		t.Fatalf("the first post after the outage = %q, %v; want it to store the kept post", out, err)
	}
	if kept, _ := filepath.Glob(filepath.Join(home, "unsent", "*")); len(kept) != 0 {
		t.Fatalf("kept posts after the drain = %v; want none", kept)
	}
	out, err = run(t, "grep", "coordinator decision", "--drive", "d1", "--json")
	var kept store.Record
	if err != nil || json.Unmarshal([]byte(strings.TrimSpace(out)), &kept) != nil || kept.Lane != "lane-a" {
		t.Fatalf("grep for the kept post = %q, %v; want lane-a's record", out, err)
	}
	if kept.Fields.Attrs["refusal"] != refusal || kept.Fields.Attrs["step"] != "two" {
		t.Fatalf("the replayed post's attrs = %v; want each attr whole", kept.Fields.Attrs)
	}
}

func TestAPostTheDaemonDidNotConfirmLandsOnceHoweverOftenItIsReplayed(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "session-1")
	if _, err := run(t, "post", "--drive", "d1", "--lane", "root", "--kind", "note", "--text", "opens the store"); err != nil {
		t.Fatal(err)
	}
	kept, err := json.Marshal(store.Write{Records: []store.Record{{Drive: "d1", Lane: "lane-a", Kind: kinds.Note, Text: "may already have landed", Source: "post"}}, At: time.Now().Add(-time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(home, "unsent"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"first drain", "second drain"} {
		if err := os.WriteFile(filepath.Join(home, "unsent", "1.write"), kept, 0o600); err != nil {
			t.Fatal(err)
		}
		out, err := run(t, "post", "--drive", "d1", "--lane", "root", "--kind", "note", "--text", text)
		if err != nil || !strings.Contains(out, "stored 1 post(s) kept while the bus was down") {
			t.Fatalf("the post that drains the kept write = %q, %v", out, err)
		}
	}
	out, err := run(t, "grep", "may already have landed", "--drive", "d1")
	if err != nil || strings.Count(out, "may already have landed") != 1 {
		t.Fatalf("grep for the replayed post = %q, %v; want it stored once", out, err)
	}
}
