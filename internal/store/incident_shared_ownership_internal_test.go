package store

import (
	"os"
	"path/filepath"
	"testing"
)

func TestIncidentSharedMissingRebindRetainsSafeUnboundOwnership(t *testing.T) {
	folder := t.TempDir()
	parent := filepath.Join(folder, "parent")
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	watch, err := newIncidentFileWatch()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = watch.Close() })
	paths := []string{filepath.Join(parent, "a"), filepath.Join(parent, "b")}
	for _, path := range paths {
		if err := watch.Register(path); err != nil {
			t.Fatal(err)
		}
	}
	key := incidentWatchKey{Path: parent}
	watch.nodes[key].Stale = true
	if err := os.Remove(parent); err != nil {
		t.Fatal(err)
	}
	if err := watch.Forget(paths[0]); err != nil {
		t.Fatal(err)
	}
	if err := watch.Register(paths[0]); err != nil {
		t.Fatal(err)
	}
	node := watch.nodes[key]
	if node == nil || node.Bound || !node.Stale || !node.Leaves[paths[1]] {
		t.Fatalf("missing shared binding lost ownership: %+v", node)
	}
	if err := watch.Forget(paths[1]); err != nil {
		t.Fatal(err)
	}
	if watch.nodes[key] != nil {
		t.Fatal("retired unbound shared binding remained")
	}
	if err := watch.Register(paths[1]); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	dirty, err := watch.Drain(t.Context())
	if err != nil || !dirty[paths[0]] || !dirty[paths[1]] {
		t.Fatalf("recreated shared ancestor lost routes: dirty=%v error=%v", dirty, err)
	}
}
