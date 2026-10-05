package daemon_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"

	"github.com/yasyf/cc-inbox/internal/daemon"
	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
)

func TestDaemonServesDigestAndHTTP(t *testing.T) {
	kit, err := os.MkdirTemp("/tmp", "cci-dk")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(kit) })
	t.Setenv("DAEMONKIT_HOME", kit)
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

	d, err := daemon.Definition()
	if err != nil {
		t.Fatal(err)
	}
	d.Label = daemonkit.Label(fmt.Sprintf("com.yasyf.cc-inbox.test%d", os.Getpid()))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveCtx, stop := context.WithCancel(ctx)
	served := make(chan error, 1)
	go func() {
		_, err := daemonkit.Serve(serveCtx, d, daemon.Start(home, ln))
		served <- err
	}()
	defer func() {
		stop()
		if err := <-served; err != nil {
			t.Errorf("Serve() = %v", err)
		}
	}()

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
