package daemon_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"

	"github.com/yasyf/cc-inbox/internal/daemon"
	"github.com/yasyf/cc-inbox/internal/importer"
	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
)

func TestDaemonServesDigestAndHTTP(t *testing.T) {
	home := t.TempDir()
	ctx := context.Background()
	st, err := store.Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.Post(ctx, store.Record{Drive: "d", Lane: "desk", Kind: kinds.Hold, Text: "hold #1", Topic: "#1"}, 0); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()

	d, ln := serve(t, home)
	client, err := daemonkit.Open(d)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(daemon.DigestRequest{Drive: "d", Since: time.Now().Add(-time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	var reply daemonkit.Reply
	deadline := time.Now().Add(10 * time.Second)
	for {
		callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		reply, err = client.Business().Call(callCtx, "digest", body)
		cancel()
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("digest call: %v", err)
	}
	var v inbox.DigestView
	if err := json.Unmarshal(reply.Body, &v); err != nil {
		t.Fatal(err)
	}
	if len(v.Holds) != 1 || v.Holds[0].Text != "hold #1" {
		t.Fatalf("daemon digest = %+v", v)
	}
	resp, err := http.Get("http://" + ln.Addr().String() + "/v1/records?drive=d")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var records []store.Record
	if err := json.NewDecoder(resp.Body).Decode(&records); err != nil || len(records) != 1 || records[0].Status != "open" {
		t.Fatalf("daemon http records = %+v, %v", records, err)
	}
}

func TestDaemonImportsAppendedInboxLines(t *testing.T) {
	home := t.TempDir()
	ctx := context.Background()
	inboxFile := filepath.Join(t.TempDir(), "deploy-go.md")
	if err := os.WriteFile(inboxFile, []byte("GO root (9:00 PM PT) first\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if _, err := importer.Import(ctx, st, inboxFile, "d", ""); err != nil {
		t.Fatal(err)
	}
	serve(t, home)
	f, err := os.OpenFile(inboxFile, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("GO root (9:01 PM PT) second\n"); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	deadline := time.Now().Add(10 * time.Second)
	for {
		n, err := st.Count(ctx, store.Filter{Drive: "d"})
		if err != nil {
			t.Fatal(err)
		}
		if n == 2 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("daemon imported %d records, want 2", n)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func serve(t *testing.T, home string) (daemonkit.Daemon, net.Listener) {
	t.Helper()
	kit, err := os.MkdirTemp("/tmp", "cci-dk")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(kit) })
	t.Setenv("DAEMONKIT_HOME", kit)
	d, err := daemon.Definition()
	if err != nil {
		t.Fatal(err)
	}
	d.Label = daemonkit.Label(fmt.Sprintf("com.yasyf.cc-inbox.test%d.%s", os.Getpid(), filepath.Base(home)))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() {
		_, err := daemonkit.Serve(ctx, d, daemon.New(home, ln).Start)
		served <- err
	}()
	t.Cleanup(func() {
		stop()
		if err := <-served; err != nil {
			t.Errorf("Serve() = %v", err)
		}
	})
	return d, ln
}

func TestDaemonReportsAListenerFailure(t *testing.T) {
	kit, err := os.MkdirTemp("/tmp", "cci-dk")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(kit) })
	t.Setenv("DAEMONKIT_HOME", kit)
	d, err := daemon.Definition()
	if err != nil {
		t.Fatal(err)
	}
	d.Label = daemonkit.Label(fmt.Sprintf("com.yasyf.cc-inbox.test%d.fail", os.Getpid()))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_ = ln.Close()
	rt := daemon.New(t.TempDir(), ln)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := daemonkit.Serve(ctx, d, rt.Start); err != nil {
		t.Fatalf("Serve() = %v", err)
	}
	if err := rt.Err(); err == nil || !strings.Contains(err.Error(), "http listener") {
		t.Fatalf("Err() = %v, want the listener failure", err)
	}
}
