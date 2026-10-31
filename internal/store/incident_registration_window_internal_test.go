package store

import (
	"os"
	"path/filepath"
	"testing"
)

type gapIncidentRegistration struct {
	incidentWatchBackend
	path        string
	replacement string
	swapped     bool
}

func (backend *gapIncidentRegistration) Add(key incidentWatchKey) error {
	if key.Path != backend.path || backend.swapped {
		return backend.incidentWatchBackend.Add(key)
	}
	return addIncidentWatchAtRegistration(backend.incidentWatchBackend, key, func() error {
		if err := os.Link(backend.path, backend.path+"-retained"); err != nil {
			return err
		}
		if err := os.Rename(backend.replacement, backend.path); err != nil {
			return err
		}
		backend.swapped = true
		return nil
	})
}

func TestIncidentPostOpenRegistrationReplacementRemainsFresh(t *testing.T) {
	folder := t.TempDir()
	bodyPath, replacement := filepath.Join(folder, "body"), filepath.Join(folder, "replacement")
	for _, path := range []string{bodyPath, replacement} {
		if err := os.WriteFile(path, []byte("body #42"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	st, index, record := incidentPathFixture(t, "")
	injected := &gapIncidentRegistration{incidentWatchBackend: index.watch.backend, path: bodyPath, replacement: replacement}
	index.watch.backend = injected
	if _, err := st.db.ExecContext(t.Context(), "UPDATE records SET refs=json_object('path',?) WHERE seq=?", bodyPath, record.Seq); err != nil {
		t.Fatal(err)
	}
	check := func(pr string) {
		t.Helper()
		got, err := index.Holds(t.Context(), st, "d", []string{"17", "42", "43"})
		if err != nil || len(got) != 1 || got[pr].Seq != record.Seq {
			t.Fatalf("post-open registration replacement retained stale hold: want=%s got=%v error=%v", pr, got, err)
		}
	}
	check("42")
	if !injected.swapped {
		t.Fatal("native registration syscall gap was not exercised")
	}
	if err := os.WriteFile(bodyPath, []byte("changed #43"), 0o600); err != nil {
		t.Fatal(err)
	}
	check("43")
	if index.watch.rebind[bodyPath] {
		t.Fatal("stable replacement retained pending registration")
	}
}

func TestIncidentLiveRetiredPendingRegistrationSkipsPathInspection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "body")
	if err := os.WriteFile(path, []byte("body #42"), 0o600); err != nil {
		t.Fatal(err)
	}
	st, index, record := incidentPathFixture(t, path)
	index.watch.rebind[path] = true
	if _, err := st.db.ExecContext(t.Context(), "DELETE FROM records WHERE seq=?", record.Seq); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(path, path); err != nil {
		t.Fatal(err)
	}
	before := index.Stats()
	got, err := index.Holds(t.Context(), st, "d", []string{"17", "42"})
	after := index.Stats()
	if err != nil || len(got) != 0 || after.RouteStats != before.RouteStats || after.BodyStats != before.BodyStats {
		t.Fatalf("live retired pending registration inspected path: holds=%v error=%v stats=%+v/%+v", got, err, before, after)
	}
	if index.watch.paths[path] || index.watch.rebind[path] {
		t.Fatal("retired route remained registered or pending")
	}
}
