package cli

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAKeptPostAnotherDrainAlreadyTookIsNotAnError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kept.json")
	if err := os.WriteFile(path, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := dropKept(path); err != nil {
		t.Fatal(err)
	}
	if err := dropKept(path); err != nil {
		t.Fatalf("a second drain removing the same kept post failed its own post: %v", err)
	}
}
