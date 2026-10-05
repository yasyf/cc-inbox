package version

import "testing"

func TestUpgradeCommandNamesTheInvokedInstall(t *testing.T) {
	tests := []struct {
		exe  string
		want string
	}{
		{"/opt/homebrew/Caskroom/cci/0.2.0/cci", "brew upgrade --cask yasyf/tap/cci"},
		{"/Users/y/.daemonkit/cache/41/41f8a5/cci", "claude plugin update cc-inbox@cc-inbox"},
		{"/Users/y/.local/bin/cci", "gh release download --repo yasyf/cc-inbox --pattern 'cci_*_darwin_arm64.tar.gz' --output - | tar -xzf - -C /Users/y/.local/bin cci"},
	}
	for _, tt := range tests {
		if got := upgradeCommand(tt.exe, "darwin", "arm64"); got != tt.want {
			t.Errorf("upgradeCommand(%q) = %q, want %q", tt.exe, got, tt.want)
		}
	}
}
