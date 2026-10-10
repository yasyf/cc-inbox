package gitinfo_test

import (
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/yasyf/cc-inbox/internal/gitinfo"
)

func TestSlug(t *testing.T) {
	tests := []struct{ url, want string }{
		{"git@github.com:yasyf/cc-inbox.git", "yasyf/cc-inbox"},
		{"https://github.com/Forge-AI/monorepo.git", "Forge-AI/monorepo"},
		{"https://github.com/Forge-AI/monorepo", "Forge-AI/monorepo"},
		{"ssh://git@github.com/yasyf/cc-inbox.git", "yasyf/cc-inbox"},
		{"/Users/me/bare.git", "/Users/me/bare"},
	}
	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			if got := gitinfo.Slug(tt.url); got != tt.want {
				t.Errorf("Slug() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRead(t *testing.T) {
	outside := t.TempDir()
	got, err := gitinfo.Read(outside)
	if err != nil || got != (gitinfo.Info{Cwd: outside}) {
		t.Fatalf("Read(outside git) = %+v, %v", got, err)
	}
	repo := t.TempDir()
	for _, args := range [][]string{
		{"init", "-q", "-b", "trunk"},
		{"remote", "add", "origin", "git@github.com:yasyf/cc-inbox.git"},
	} {
		cmd := exec.Command("git", append([]string{"-C", repo}, args...)...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	got, err = gitinfo.Read(repo)
	if err != nil {
		t.Fatal(err)
	}
	real, _ := filepath.EvalSymlinks(repo)
	if got.Cwd != repo || got.Repo != filepath.Join(real, ".git") || got.Worktree != real || got.Branch != "trunk" || got.Remote != "yasyf/cc-inbox" {
		t.Fatalf("Read(repo) = %+v", got)
	}
}
