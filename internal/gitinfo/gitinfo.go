// Package gitinfo names the repository, worktree and branch a directory sits in.
package gitinfo

import (
	"bytes"
	"errors"
	"os/exec"
	"strings"
)

// Info locates cwd within git. Outside a repository every field but Cwd is empty.
type Info struct {
	Cwd      string
	Repo     string
	Worktree string
	Branch   string
	Remote   string
}

// Read runs git for the common dir and top level, the current branch, and
// the origin URL. A detached head leaves Branch empty; no origin leaves Remote empty.
func Read(cwd string) (Info, error) {
	info := Info{Cwd: cwd}
	out, err := git(cwd, "rev-parse", "--path-format=absolute", "--git-common-dir", "--show-toplevel")
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return info, nil
	}
	if err != nil {
		return Info{}, err
	}
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) != 2 {
		return Info{}, errors.New("git rev-parse: expected common dir and top level, got " + strings.Join(lines, " | "))
	}
	info.Repo, info.Worktree = lines[0], lines[1]
	branch, err := git(cwd, "branch", "--show-current")
	if err != nil {
		return Info{}, err
	}
	info.Branch = strings.TrimSpace(branch)
	remote, err := git(cwd, "remote", "get-url", "origin")
	if errors.As(err, &exit) {
		return info, nil
	}
	if err != nil {
		return Info{}, err
	}
	info.Remote = Slug(strings.TrimSpace(remote))
	return info, nil
}

// Slug reduces a git remote URL to owner/name.
func Slug(url string) string {
	s := strings.TrimSuffix(url, ".git")
	if i := strings.LastIndex(s, ":"); i >= 0 && !strings.Contains(s, "://") {
		s = s[i+1:]
	} else if i := strings.Index(s, "://"); i >= 0 {
		s = s[i+3:]
		if j := strings.Index(s, "/"); j >= 0 {
			s = s[j+1:]
		}
	}
	return strings.TrimSuffix(s, "/")
}

func git(cwd string, args ...string) (string, error) {
	cmd := exec.Command("git", append([]string{"-C", cwd}, args...)...) //nolint:gosec // G204: the verbs are fixed; cwd is this session's directory.
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			return "", exit
		}
		return "", err
	}
	return stdout.String(), nil
}
