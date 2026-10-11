package daemon_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/daemon"
	"github.com/yasyf/cc-inbox/internal/version"
)

func TestUpgraderHandsOffToEachNewerInstallOnce(t *testing.T) {
	plugin := t.TempDir()
	calls := filepath.Join(t.TempDir(), "calls")
	install := func(v string) {
		bin := filepath.Join(plugin, v, "bin")
		if err := os.MkdirAll(bin, 0o755); err != nil {
			t.Fatal(err)
		}
		script := "#!/bin/sh\necho " + v + " \"$@\" >> " + calls + "\n"
		if err := os.WriteFile(filepath.Join(bin, "cci"), []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	waitCalls := func(want ...string) {
		t.Helper()
		var got []string
		for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
			data, _ := os.ReadFile(calls)
			got = strings.Fields(strings.ReplaceAll(string(data), " serve", "/serve"))
			if slices.Equal(got, want) {
				return
			}
		}
		t.Fatalf("hand-offs = %q, want %q", got, want)
	}
	install("0.11.0")
	install("0.12.0")
	up := &daemon.Upgrader{Roots: version.Roots{Plugin: plugin}, Running: "0.12.0"}
	if err := up.Check(); err != nil {
		t.Fatal(err)
	}
	install("0.13.0")
	if err := up.Check(); err != nil {
		t.Fatal(err)
	}
	waitCalls("0.13.0/serve")
	if err := up.Check(); err != nil {
		t.Fatal(err)
	}
	install("0.14.0")
	if err := up.Check(); err != nil {
		t.Fatal(err)
	}
	waitCalls("0.13.0/serve", "0.14.0/serve")
}

func TestDevBuildNeverScansForInstalls(t *testing.T) {
	unreadable := filepath.Join(t.TempDir(), "plugin")
	if err := os.WriteFile(unreadable, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	roots := version.Roots{Plugin: unreadable}
	for _, running := range []string{"dev", "v0.12.0-3-gabc1234-dirty"} {
		if err := (&daemon.Upgrader{Roots: roots, Running: running}).Check(); err != nil {
			t.Fatalf("Check() running %s = %v, want no scan", running, err)
		}
	}
	if err := (&daemon.Upgrader{Roots: roots, Running: "0.12.0"}).Check(); err == nil {
		t.Fatal("Check() running a release did not scan")
	}
}
