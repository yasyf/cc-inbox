package store

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestIncidentKernelEntryCutIncludesMoreThanOneOldBatch(t *testing.T) {
	watch, err := newIncidentFileWatch()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = watch.Close() })
	folder := t.TempDir()
	var paths []string
	for i := 0; i < 128; i++ {
		path := filepath.Join(folder, fmt.Sprintf("body-%d", i))
		if err := os.WriteFile(path, []byte("initial"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := watch.Register(path); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, path)
	}
	if _, err := watch.Drain(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		if err := os.WriteFile(path, []byte("changed"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	dirty, err := watch.Drain(t.Context())
	if err != nil || len(dirty) != len(paths) {
		t.Fatalf("native entry cut omitted changed files: got=%d want=%d err=%v", len(dirty), len(paths), err)
	}
	for _, path := range paths {
		if !dirty[path] {
			t.Fatalf("native entry cut omitted %s", path)
		}
	}
}
