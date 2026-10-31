package version

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"
)

const (
	pluginUpgrade = "claude plugin marketplace update cc-inbox && claude plugin update cc-inbox@cc-inbox"
	CaskUpgrade   = "brew upgrade --cask yasyf/tap/cci"
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
		add(CaskUpgrade)
	}
	return cmds, nil
}

func Standalone(path string) []string {
	var standalone []string
	for _, exe := range onPath(path) {
		if cmd := upgradeCommand(exe, runtime.GOOS, runtime.GOARCH); cmd != pluginUpgrade && cmd != CaskUpgrade && !slices.Contains(standalone, exe) {
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
		return CaskUpgrade
	case strings.Contains(exe, "/.daemonkit/cache/"), strings.Contains(exe, "/plugins/cache/cc-inbox/"):
		return pluginUpgrade
	default:
		dir := filepath.Dir(exe)
		return fmt.Sprintf(`d=$(mktemp -d %[1]s/.cci.XXXXXX) && gh release download --repo yasyf/cc-inbox --pattern 'cci_*_%[2]s_%[3]s.tar.gz' --output - | tar -xzf - -C "$d" cci && mv -f "$d/cci" %[1]s/cci && rmdir "$d"`, dir, goos, goarch)
	}
}

// ProveRelease downloads the newest cci release and runs it once, failing when
// it does not start within bound. A Homebrew cask upgrade unlinks the working
// cci before its own first run of the new one, so a release macOS holds at
// launch would leave every caller hanging; proving a copy first leaves the
// cask untouched instead.
func ProveRelease(ctx context.Context, bound time.Duration) error {
	dir, err := os.MkdirTemp("", "cci-release-")
	if err != nil {
		return fmt.Errorf("stage the newest cci release: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	pattern := fmt.Sprintf("cci_*_%s_%s.tar.gz", runtime.GOOS, runtime.GOARCH)
	fetch := exec.CommandContext(ctx, "/bin/sh", "-c", `gh release download --repo yasyf/cc-inbox --pattern "$1" --output - | tar -xzf - -C "$2" cci`, "sh", pattern, dir) //nolint:gosec // G204: fixed script, arguments passed positionally.
	if out, err := fetch.CombinedOutput(); err != nil {
		return fmt.Errorf("download the newest cci release: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return provesStart(ctx, filepath.Join(dir, "cci"), bound)
}

func provesStart(ctx context.Context, path string, bound time.Duration) error {
	bounded, cancel := context.WithTimeout(ctx, bound)
	defer cancel()
	probe := exec.CommandContext(bounded, path, "--version") //nolint:gosec // G204: a release cci just downloaded.
	probe.WaitDelay = time.Second
	err := probe.Run()
	if bounded.Err() != nil && ctx.Err() == nil {
		return fmt.Errorf("the newest cci release did not start within %s, as happens while macOS holds newly downloaded binaries at launch; the cask and its cci link are left as they are, so rerun cci upgrade once a new download starts", bound)
	}
	if err != nil {
		return fmt.Errorf("%s --version: %w", path, err)
	}
	return nil
}
