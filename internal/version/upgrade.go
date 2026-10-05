package version

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

func UpgradeCommand() string {
	exe, err := os.Executable()
	if err != nil {
		panic(fmt.Sprintf("locate the running cci: %v", err))
	}
	if resolved, err := filepath.EvalSymlinks(exe); err == nil {
		exe = resolved
	}
	return upgradeCommand(exe, runtime.GOOS, runtime.GOARCH)
}

func upgradeCommand(exe, goos, goarch string) string {
	switch {
	case strings.Contains(exe, "/Caskroom/cci/"):
		return "brew upgrade --cask yasyf/tap/cci"
	case strings.Contains(exe, "/.daemonkit/cache/"):
		return "claude plugin update cc-inbox@cc-inbox"
	default:
		return fmt.Sprintf("gh release download --repo yasyf/cc-inbox --pattern 'cci_*_%s_%s.tar.gz' --output - | tar -xzf - -C %s cci", goos, goarch, filepath.Dir(exe))
	}
}
