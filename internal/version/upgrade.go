package version

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
)

const (
	pluginUpgrade = "claude plugin marketplace update cc-inbox && claude plugin update cc-inbox@cc-inbox"
	caskUpgrade   = "brew upgrade --cask yasyf/tap/cci"
)

func UpgradeCommand() string {
	exe, err := os.Executable()
	if err != nil {
		panic(fmt.Sprintf("locate the running cci: %v", err))
	}
	return upgradeCommand(resolve(exe), runtime.GOOS, runtime.GOARCH)
}

func (r Roots) UpgradeCommands(path string) ([]string, error) {
	var cmds []string
	add := func(cmd string) {
		if !slices.Contains(cmds, cmd) {
			cmds = append(cmds, cmd)
		}
	}
	for _, exe := range onPath(path) {
		add(upgradeCommand(exe, runtime.GOOS, runtime.GOARCH))
	}
	plugin, casks, err := r.newest()
	if err != nil {
		return nil, err
	}
	if plugin.Path != "" {
		add(pluginUpgrade)
	}
	if casks.Path != "" {
		add(caskUpgrade)
	}
	return cmds, nil
}

func Standalone(path string) []string {
	var standalone []string
	for _, exe := range onPath(path) {
		if cmd := upgradeCommand(exe, runtime.GOOS, runtime.GOARCH); cmd != pluginUpgrade && cmd != caskUpgrade && !slices.Contains(standalone, exe) {
			standalone = append(standalone, exe)
		}
	}
	return standalone
}

func onPath(path string) []string {
	var found []string
	for _, dir := range filepath.SplitList(path) {
		exe := filepath.Join(dir, "cci")
		if info, err := os.Stat(exe); err != nil || info.IsDir() || info.Mode().Perm()&0o111 == 0 {
			continue
		}
		found = append(found, resolve(exe))
	}
	return found
}

func resolve(exe string) string {
	if resolved, err := filepath.EvalSymlinks(exe); err == nil {
		return resolved
	}
	return exe
}

func upgradeCommand(exe, goos, goarch string) string {
	switch {
	case strings.Contains(exe, "/Caskroom/cci/"):
		return caskUpgrade
	case strings.Contains(exe, "/.daemonkit/cache/"), strings.Contains(exe, "/plugins/cache/cc-inbox/"):
		return pluginUpgrade
	default:
		dir := filepath.Dir(exe)
		return fmt.Sprintf(`d=$(mktemp -d %[1]s/.cci.XXXXXX) && gh release download --repo yasyf/cc-inbox --pattern 'cci_*_%[2]s_%[3]s.tar.gz' --output - | tar -xzf - -C "$d" cci && mv -f "$d/cci" %[1]s/cci && rmdir "$d"`, dir, goos, goarch)
	}
}
