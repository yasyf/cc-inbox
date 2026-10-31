package version

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"golang.org/x/mod/semver"
)

type Install struct {
	Path    string
	Version string
}

type Roots struct {
	Plugin string
	Casks  []string
}

func DefaultRoots() (Roots, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return Roots{}, fmt.Errorf("resolve home: %w", err)
	}
	return Roots{
		Plugin: filepath.Join(home, ".claude", "plugins", "cache", "cc-inbox", "cc-inbox"),
		Casks:  []string{"/opt/homebrew/Caskroom/cci", "/usr/local/Caskroom/cci"},
	}, nil
}

func (r Roots) Newest() (Install, error) {
	plugin, casks, err := r.newest()
	if err != nil {
		return Install{}, err
	}
	if Newer(casks.Version, plugin.Version) {
		return casks, nil
	}
	return plugin, nil
}

// NewerThan returns the newest cci installed on this machine when it is a
// newer release than running, so a binary that a session's stale plugin
// directory or PATH entry still reaches can hand off to it.
func (r Roots) NewerThan(running string) (Install, bool, error) {
	if !IsRelease(running) {
		return Install{}, false, nil
	}
	newest, err := r.Newest()
	if err != nil {
		return Install{}, false, err
	}
	return newest, Newer(newest.Version, running), nil
}

func (r Roots) newest() (plugin, casks Install, err error) {
	if plugin, err = newestPlugin(r.Plugin); err != nil {
		return Install{}, Install{}, err
	}
	for _, root := range r.Casks {
		cask, err := committedCask(root)
		if err != nil {
			return Install{}, Install{}, err
		}
		if Newer(cask.Version, casks.Version) {
			casks = cask
		}
	}
	return plugin, casks, nil
}

func committedCask(root string) (Install, error) {
	raw, err := os.ReadFile(filepath.Join(root, ".metadata", "INSTALL_RECEIPT.json")) //nolint:gosec // G304: root is a configured Caskroom; the receipt path is fixed.
	if errors.Is(err, fs.ErrNotExist) {
		return Install{}, nil
	}
	if err != nil {
		return Install{}, fmt.Errorf("read cci cask receipt: %w", err)
	}
	var receipt struct {
		Source struct {
			Version string `json:"version"`
		} `json:"source"`
	}
	if err := json.Unmarshal(raw, &receipt); err != nil {
		return Install{}, fmt.Errorf("decode cci cask receipt: %w", err)
	}
	if !IsRelease(receipt.Source.Version) {
		return Install{}, nil
	}
	path := filepath.Join(root, receipt.Source.Version, "cci")
	if info, err := os.Stat(path); err != nil || info.Mode().Perm()&0o111 == 0 {
		return Install{}, nil
	}
	return Install{Path: path, Version: receipt.Source.Version}, nil
}

func newestPlugin(root string) (Install, error) {
	entries, err := os.ReadDir(root)
	if errors.Is(err, fs.ErrNotExist) {
		return Install{}, nil
	}
	if err != nil {
		return Install{}, fmt.Errorf("scan cci installs: %w", err)
	}
	var newest Install
	for _, e := range entries {
		dir := filepath.Join(root, e.Name())
		if !e.IsDir() || !Newer(e.Name(), newest.Version) {
			continue
		}
		if _, err := os.Stat(filepath.Join(dir, ".orphaned_at")); err == nil {
			continue
		}
		path := filepath.Join(dir, "bin", "cci")
		if info, err := os.Stat(path); err != nil || info.Mode().Perm()&0o111 == 0 {
			continue
		}
		newest = Install{Path: path, Version: e.Name()}
	}
	return newest, nil
}

func IsRelease(v string) bool {
	sv := "v" + v
	return semver.IsValid(sv) && semver.Prerelease(sv) == "" && semver.Build(sv) == "" && semver.Canonical(sv) == sv
}

func Newer(v, than string) bool {
	if !IsRelease(v) {
		return false
	}
	return than == "" || semver.Compare("v"+v, "v"+than) > 0
}
