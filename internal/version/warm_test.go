package version

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAHandoffTargetIsStartedOnlyOnceItRanFromAnInstallRoot(t *testing.T) {
	root := t.TempDir()
	plugin := filepath.Join(root, "plugins")
	cask := filepath.Join(root, "Caskroom", "cci")
	roots := Roots{Plugin: plugin, Casks: []string{cask}}
	installed := filepath.Join(cask, "0.50.0", "cci")
	stray := filepath.Join(root, "bin", "cci")
	installAt(t, installed)
	installAt(t, stray)
	if Started(installed) {
		t.Fatal("a fresh cask install reads started before it ran")
	}
	for _, path := range []string{installed, stray} {
		if err := roots.MarkStarted(path); err != nil {
			t.Fatal(err)
		}
	}
	if !Started(installed) || Started(stray) {
		t.Fatalf("started: cask %t, stray %t; want the cask install only", Started(installed), Started(stray))
	}
}

func TestOneCallerWarmsANewInstallUntilTheWarmUpGoesStale(t *testing.T) {
	path := filepath.Join(t.TempDir(), "0.50.0", "cci")
	installAt(t, path)
	now := time.Now()
	for _, tt := range []struct {
		at   time.Time
		want bool
	}{
		{now, true},
		{now.Add(time.Minute), false},
		{now.Add(warmRetry + time.Minute), true},
		{now.Add(warmRetry + 2*time.Minute), false},
	} {
		if got, err := ClaimWarm(path, tt.at); err != nil || got != tt.want {
			t.Fatalf("ClaimWarm at +%s = %t, %v; want %t", tt.at.Sub(now), got, err, tt.want)
		}
	}
	if _, err := os.Stat(path + warmingSuffix); err != nil {
		t.Fatal(err)
	}
}
