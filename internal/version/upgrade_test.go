package version

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestUpgradeCommandNamesTheInvokedInstall(t *testing.T) {
	tests := []struct {
		exe  string
		want string
	}{
		{"/opt/homebrew/Caskroom/cci/0.2.0/cci", "brew upgrade --cask yasyf/tap/cci"},
		{"/Users/y/.daemonkit/cache/41/41f8a5/cci", pluginUpgrade},
		{"/Users/y/.claude/plugins/cache/cc-inbox/cc-inbox/0.12.0/scripts/install-binary.sh", pluginUpgrade},
		{"/Users/y/.local/bin/cci", `d=$(mktemp -d /Users/y/.local/bin/.cci.XXXXXX) && gh release download --repo yasyf/cc-inbox --pattern 'cci_*_darwin_arm64.tar.gz' --output - | tar -xzf - -C "$d" cci && mv -f "$d/cci" /Users/y/.local/bin/cci && rmdir "$d"`},
	}
	for _, tt := range tests {
		if got := upgradeCommand(tt.exe, "darwin", "arm64"); got != tt.want {
			t.Errorf("upgradeCommand(%q) = %q, want %q", tt.exe, got, tt.want)
		}
	}
}

func TestAReleaseThatDoesNotStartLeavesTheCaskAlone(t *testing.T) {
	dir := t.TempDir()
	held := filepath.Join(dir, "held")
	if err := os.WriteFile(held, []byte("#!/bin/sh\nexec sleep 30\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	err := provesStart(context.Background(), held, 300*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "did not start within 300ms") {
		t.Fatalf("provesStart(held) = %v, want a refusal naming the bound", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("provesStart(held) took %s, want it bounded", elapsed)
	}

	starts := filepath.Join(dir, "starts")
	if err := os.WriteFile(starts, []byte("#!/bin/sh\necho 0.47.1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := provesStart(context.Background(), starts, 10*time.Second); err != nil {
		t.Fatalf("provesStart(starts) = %v, want nil", err)
	}
}
