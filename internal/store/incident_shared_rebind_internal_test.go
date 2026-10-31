package store

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

func TestIncidentSharedAncestorReplacementRemainsFresh(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "existing", true: "missing"}[missing], func(t *testing.T) {
			folder := t.TempDir()
			parent := filepath.Join(folder, "parent")
			if err := os.Mkdir(parent, 0o700); err != nil {
				t.Fatal(err)
			}
			paths := []string{filepath.Join(parent, "a"), filepath.Join(parent, "b")}
			write := func(values []string) {
				t.Helper()
				for n, path := range paths {
					if err := os.WriteFile(path, []byte("body #"+values[n]), 0o600); err != nil {
						t.Fatal(err)
					}
				}
			}
			if !missing {
				write([]string{"42", "43"})
			}
			st, index, first := incidentPathFixture(t, paths[0])
			second, _, err := st.Post(t.Context(), Record{Drive: "d", Lane: "root", Kind: kinds.Incident, Text: "summary #18", Source: "post", Refs: Refs{Path: paths[1]}}, 0)
			if err != nil {
				t.Fatal(err)
			}
			check := func(values []string) {
				t.Helper()
				got, err := index.Holds(t.Context(), st, "d", []string{"17", "18", "42", "43", "44", "45", "46", "47"})
				if err != nil || len(got) != 2 || got[values[0]].Seq != first.Seq || got[values[1]].Seq != second.Seq {
					t.Fatalf("shared ancestor retained stale membership: want=%v holds=%v error=%v", values, got, err)
				}
			}
			if missing {
				check([]string{"17", "18"})
			} else {
				check([]string{"42", "43"})
			}
			for n, values := range [][]string{{"44", "45"}, {"46", "47"}} {
				retained := filepath.Join(folder, []string{"old", "middle"}[n])
				if err := os.Rename(parent, retained); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(parent, 0o700); err != nil {
					t.Fatal(err)
				}
				if missing {
					check([]string{"17", "18"})
					write(values)
					check(values)
					for _, path := range paths {
						if err := os.Remove(path); err != nil {
							t.Fatal(err)
						}
					}
					check([]string{"17", "18"})
				} else {
					write(values)
					check(values)
				}
			}
			if len(index.watch.rebind) != 0 {
				t.Fatalf("stable shared bindings did not converge: %v", index.watch.rebind)
			}
		})
	}
}

type changedSharedIncidentWatch struct {
	incidentWatchBackend
	path  string
	reads int
}

func (backend *changedSharedIncidentWatch) Info(key incidentWatchKey) incidentWatchInfo {
	info := backend.incidentWatchBackend.Info(key)
	if key.Path == backend.path {
		backend.reads++
		info.Changed = backend.reads == 1
	}
	return info
}

func TestIncidentSharedChangedRegistrationRefreshesOnce(t *testing.T) {
	parent := t.TempDir()
	watch, err := newIncidentFileWatch()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = watch.Close() })
	backend := &changedSharedIncidentWatch{incidentWatchBackend: watch.backend, path: parent}
	watch.backend = backend
	paths := []string{filepath.Join(parent, "a"), filepath.Join(parent, "b")}
	for _, path := range paths {
		if err := watch.Register(path); err != nil {
			t.Fatal(err)
		}
	}
	if backend.reads != 2 {
		t.Fatalf("shared changed registration did not refresh: reads=%d want=2", backend.reads)
	}
	if err := watch.Forget(paths[0]); err != nil {
		t.Fatal(err)
	}
	if err := watch.Register(paths[0]); err != nil {
		t.Fatal(err)
	}
	if backend.reads != 2 || len(watch.rebind) != 0 {
		t.Fatalf("stable shared registration did not converge: reads=%d rebind=%v", backend.reads, watch.rebind)
	}
}
