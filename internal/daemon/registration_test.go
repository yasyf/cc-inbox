package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/synckit/hostregistry"

	"github.com/yasyf/cc-inbox/internal/mesh"
	"github.com/yasyf/cc-inbox/internal/store"
)

func TestRuntimeStartRefreshesRegistrationWithoutLosingHTTP(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	kit, err := os.MkdirTemp("/tmp", "cci-dk")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(kit) })
	t.Setenv("DAEMONKIT_HOME", kit)
	fixture := t.TempDir()
	if _, err := mesh.WriteManifest(fixture, strings.Repeat("a", 64), time.Second); err != nil {
		t.Fatal(err)
	}
	registeredPath, err := hostregistry.RegisteredPath(mesh.Service)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(registeredPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(registeredPath, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	home := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	st, err := store.Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	schema := st.SchemaFingerprint()
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	rt := New(home, ln)
	var calls atomic.Int32
	invoked := make(chan bool, 1)
	rt.refreshRegistration = func(ctx context.Context, gotHome, gotSchema string) error {
		calls.Add(1)
		invoked <- gotHome == home && gotSchema == schema && ctx.Err() == nil
		return mesh.RefreshRegistration(ctx, fixture, gotSchema)
	}
	var logs registrationLogs
	logger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(logger) })
	d, err := Definition()
	if err != nil {
		t.Fatal(err)
	}
	d.Label = daemonkit.Label(fmt.Sprintf("com.yasyf.cc-inbox.test%d.refresh", os.Getpid()))
	serveCtx, stop := context.WithCancel(ctx)
	served := make(chan error, 1)
	go func() {
		_, err := daemonkit.Serve(serveCtx, d, rt.Start)
		served <- err
	}()
	stopped := sync.OnceValue(func() error {
		stop()
		return <-served
	})
	t.Cleanup(func() {
		if err := stopped(); err != nil {
			t.Errorf("Serve() = %v", err)
		}
	})
	client, err := daemonkit.Open(d)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if !<-invoked {
		t.Fatal("startup refresh did not receive the opened store's home, schema, and live context")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+ln.Addr().String()+"/v1/records?drive=d", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var records []store.Record
	if err := json.NewDecoder(resp.Body).Decode(&records); err != nil || resp.StatusCode != http.StatusOK || len(records) != 0 {
		t.Fatalf("HTTP after refresh failure = %d, %+v, %v", resp.StatusCode, records, err)
	}
	if err := stopped(); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || rt.Err() != nil {
		t.Fatalf("startup refresh calls = %d, runtime error = %v", calls.Load(), rt.Err())
	}
	var warning map[string]any
	decoder := json.NewDecoder(strings.NewReader(logs.String()))
	for decoder.More() {
		var entry map[string]any
		if err := decoder.Decode(&entry); err != nil {
			t.Fatal(err)
		}
		if entry["msg"] == "cci daemon cannot refresh mesh registration" {
			warning = entry
		}
	}
	if warning["level"] != "WARN" || !strings.Contains(fmt.Sprint(warning["err"]), registeredPath) {
		t.Fatalf("refresh warning = %v", warning)
	}
	raw, err := os.ReadFile(registeredPath)
	if err != nil || string(raw) != "{" {
		t.Fatalf("invalid registration changed: %q, %v", raw, err)
	}
}

type registrationLogs struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *registrationLogs) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *registrationLogs) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}
