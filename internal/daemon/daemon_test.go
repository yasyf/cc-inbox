package daemon_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
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

	addr, client := serve(t, home)
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
	resp, err := http.Get("http://" + addr + "/v1/records?drive=d")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var records []store.Record
	if err := json.NewDecoder(resp.Body).Decode(&records); err != nil || len(records) != 1 || records[0].Status != "open" {
		t.Fatalf("daemon http records = %+v, %v", records, err)
	}
}

func TestDaemonImportsLinesAppendedToRegisteredInboxes(t *testing.T) {
	home := t.TempDir()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "deploy-go.md")
	if err := os.WriteFile(path, []byte("GO root (9:00 PM PT) one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := importer.Import(ctx, st, path, "d", ""); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()

	addr, _ := serve(t, home)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("GO root (9:01 PM PT) two\n"); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	deadline := time.Now().Add(10 * time.Second)
	for {
		records := getRecords(t, addr)
		if len(records) == 2 && records[1].Text == "two" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("daemon records after append = %+v", records)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func getRecords(t *testing.T, addr string) []store.Record {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/v1/records?drive=d")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var records []store.Record
	if err := json.NewDecoder(resp.Body).Decode(&records); err != nil {
		t.Fatal(err)
	}
	return records
}

func serve(t *testing.T, home string) (string, *daemonkit.Client) {
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
	d.Label = daemonkit.Label(fmt.Sprintf("com.yasyf.cc-inbox.test%d", os.Getpid()))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveCtx, stop := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() {
		_, err := daemonkit.Serve(serveCtx, d, daemon.Start(home, ln, 10*time.Millisecond))
		served <- err
	}()
	t.Cleanup(func() {
		stop()
		if err := <-served; err != nil {
			t.Errorf("Serve() = %v", err)
		}
	})

	client, err := daemonkit.Open(d)
	if err != nil {
		t.Fatal(err)
	}
	return ln.Addr().String(), client
}
