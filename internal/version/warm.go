package version

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	startedSuffix = ".started"
	warmingSuffix = ".warming"
	warmRetry     = 10 * time.Minute
)

// Started reports whether the install at path has run once on this machine,
// so a handoff to it never waits in the hold macOS puts on a binary's first
// launch.
func Started(path string) bool {
	_, err := os.Stat(path + startedSuffix)
	return err == nil
}

// MarkStarted records that the install at path ran, when path is one of the
// installs r holds.
func (r Roots) MarkStarted(path string) error {
	if !r.holds(path) || Started(path) {
		return nil
	}
	if err := os.WriteFile(path+startedSuffix, nil, 0o644); err != nil { //nolint:gosec // G306: a marker beside an install, readable like the install.
		return fmt.Errorf("record that %s started: %w", path, err)
	}
	return nil
}

func (r Roots) holds(path string) bool {
	for _, root := range append([]string{r.Plugin}, r.Casks...) {
		if root != "" && strings.HasPrefix(path, root+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

// ClaimWarm reports whether this caller starts the install at path once to
// warm it: the first caller does, and the next caller once warmRetry passes
// with the install still not started.
func ClaimWarm(path string, now time.Time) (bool, error) {
	marker := path + warmingSuffix
	f, err := os.OpenFile(marker, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644) //nolint:gosec // G302,G304: a marker beside an install cci found.
	if err == nil {
		return true, f.Close()
	}
	if !errors.Is(err, fs.ErrExist) {
		return false, fmt.Errorf("claim the warm-up of %s: %w", path, err)
	}
	info, err := os.Stat(marker)
	if err != nil {
		return false, fmt.Errorf("read the warm-up of %s: %w", path, err)
	}
	if now.Sub(info.ModTime()) < warmRetry {
		return false, nil
	}
	return true, os.Chtimes(marker, now, now)
}
