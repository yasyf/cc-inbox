package version

import "testing"

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
