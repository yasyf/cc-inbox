package version

import (
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

func (r Roots) newest() (plugin, casks Install, err error) {
	if plugin, err = newestIn(r.Plugin, filepath.Join("bin", "cci"), true); err != nil {
		return Install{}, Install{}, err
	}
	for _, root := range r.Casks {
		cask, err := newestIn(root, "cci", false)
		if err != nil {
			return Install{}, Install{}, err
		}
		if Newer(cask.Version, casks.Version) {
			casks = cask
		}
	}
	return plugin, casks, nil
}

func newestIn(root, leaf string, skipOrphaned bool) (Install, error) {
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
		if _, err := os.Stat(filepath.Join(dir, ".orphaned_at")); skipOrphaned && err == nil {
			continue
		}
		path := filepath.Join(dir, leaf)
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
