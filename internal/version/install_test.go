package version

import (
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

func installAt(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
}

func TestNewestRanksPluginAndCaskInstallsBySemver(t *testing.T) {
	plugin := filepath.Join(t.TempDir(), "cc-inbox")
	cask := filepath.Join(t.TempDir(), "cci")
	installAt(t, filepath.Join(plugin, "0.9.3", "bin", "cci"))
	installAt(t, filepath.Join(plugin, "0.12.0", "bin", "cci"))
	installAt(t, filepath.Join(plugin, "0.13.0", "bin", "cci"))
	installAt(t, filepath.Join(plugin, "0.13.0", ".orphaned_at"))
	if err := os.MkdirAll(filepath.Join(plugin, "0.14.0", "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	installAt(t, filepath.Join(cask, "0.11.0", "cci"))
	installAt(t, filepath.Join(cask, "0.12.1", "cci"))
	roots := Roots{Plugin: plugin, Casks: []string{cask, filepath.Join(t.TempDir(), "absent")}}

	got, err := roots.Newest()
	if err != nil {
		t.Fatal(err)
	}
	want := Install{Path: filepath.Join(cask, "0.12.1", "cci"), Version: "0.12.1"}
	if got != want {
		t.Fatalf("Newest() = %+v, want %+v", got, want)
	}

	installAt(t, filepath.Join(plugin, "0.12.10", "bin", "cci"))
	if got, err = roots.Newest(); err != nil || got.Version != "0.12.10" {
		t.Fatalf("Newest() = %+v, %v; want 0.12.10", got, err)
	}
}

func TestNewestFindsNothingWithoutInstalls(t *testing.T) {
	got, err := Roots{Plugin: filepath.Join(t.TempDir(), "absent")}.Newest()
	if err != nil || got != (Install{}) {
		t.Fatalf("Newest() = %+v, %v; want none", got, err)
	}
}

func TestNewerComparesReleasesOnly(t *testing.T) {
	tests := []struct {
		v, than string
		want    bool
	}{
		{"0.13.0", "0.12.0", true},
		{"0.12.10", "0.12.9", true},
		{"0.12.0", "0.12.0", false},
		{"0.11.0", "0.12.0", false},
		{"0.13.0", "", true},
		{"dev", "0.12.0", false},
		{"v0.13.0", "0.12.0", false},
		{"0.13.0-rc.1", "0.12.0", false},
		{"0.13", "0.12.0", false},
	}
	for _, tt := range tests {
		if got := Newer(tt.v, tt.than); got != tt.want {
			t.Errorf("Newer(%q, %q) = %v, want %v", tt.v, tt.than, got, tt.want)
		}
	}
}

func TestUpgradeCommandsCoverEveryInstallOnce(t *testing.T) {
	plugin := filepath.Join(t.TempDir(), "plugins", "cache", "cc-inbox", "cc-inbox")
	installAt(t, filepath.Join(plugin, "0.11.0", "bin", "cci"))
	installAt(t, filepath.Join(plugin, "0.12.0", "bin", "cci"))
	cask := filepath.Join(t.TempDir(), "cci")
	installAt(t, filepath.Join(cask, "0.12.0", "cci"))
	standalone, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	installAt(t, filepath.Join(standalone, "cci"))
	path := strings.Join([]string{
		filepath.Join(plugin, "0.11.0", "bin"),
		standalone,
		t.TempDir(),
		filepath.Join(plugin, "0.12.0", "bin"),
		standalone,
	}, string(os.PathListSeparator))

	got, err := Roots{Plugin: plugin, Casks: []string{cask}}.UpgradeCommands(path)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		pluginUpgrade,
		upgradeCommand(filepath.Join(standalone, "cci"), runtime.GOOS, runtime.GOARCH),
		caskUpgrade,
	}
	if !slices.Equal(got, want) {
		t.Fatalf("UpgradeCommands() = %q, want %q", got, want)
	}
}
