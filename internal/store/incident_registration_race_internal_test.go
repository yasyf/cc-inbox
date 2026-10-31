package store

import (
	"os"
	"path/filepath"
	"testing"
)

type swapIncidentRegistration struct {
	incidentWatchBackend
	path        string
	replacement string
	swapped     bool
}

func (backend *swapIncidentRegistration) Add(key incidentWatchKey) error {
	if key.Path == backend.path && !backend.swapped {
		if err := os.Remove(backend.path); err != nil {
			return err
		}
		if err := os.Rename(backend.replacement, backend.path); err != nil {
			return err
		}
		backend.swapped = true
	}
	return backend.incidentWatchBackend.Add(key)
}

func TestIncidentRegistrationUsesBoundObjectType(t *testing.T) {
	for _, name := range []string{"directory_to_file", "file_to_directory_ancestor", "file_to_symlink_ancestor"} {
		t.Run(name, func(t *testing.T) {
			folder := t.TempDir()
			target, replacement := filepath.Join(folder, "target"), filepath.Join(folder, "replacement")
			bodyPath := target
			elsewhere := filepath.Join(folder, "elsewhere")
			if name == "directory_to_file" {
				if err := os.Mkdir(target, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(replacement, []byte("body #42"), 0o600); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.WriteFile(target, []byte("old regular ancestor"), 0o600); err != nil {
					t.Fatal(err)
				}
				bodyPath = filepath.Join(target, "body")
				directory := replacement
				if name == "file_to_symlink_ancestor" {
					directory = filepath.Join(elsewhere, "child")
				}
				if err := os.MkdirAll(directory, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(directory, "body"), []byte("body #42"), 0o600); err != nil {
					t.Fatal(err)
				}
				if name == "file_to_symlink_ancestor" {
					if err := os.Symlink(directory, replacement); err != nil {
						t.Fatal(err)
					}
				}
			}
			st, index, record := incidentPathFixture(t, "")
			injected := &swapIncidentRegistration{incidentWatchBackend: index.watch.backend, path: target, replacement: replacement}
			index.watch.backend = injected
			if _, err := st.db.ExecContext(t.Context(), "UPDATE records SET refs=json_object('path',?) WHERE seq=?", bodyPath, record.Seq); err != nil {
				t.Fatal(err)
			}
			check := func(pr string) {
				t.Helper()
				got, err := index.Holds(t.Context(), st, "d", []string{"17", "42", "43"})
				if err != nil || len(got) != 1 || got[pr].Seq != record.Seq {
					t.Fatalf("watch registration type race retained stale hold: want=%s got=%v error=%v", pr, got, err)
				}
			}
			check("42")
			if !injected.swapped {
				t.Fatal("fixture did not swap during actual native Add")
			}
			if name == "file_to_symlink_ancestor" {
				if err := os.Rename(elsewhere, elsewhere+"-away"); err != nil {
					t.Fatal(err)
				}
				if err := os.MkdirAll(filepath.Join(elsewhere, "child"), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(bodyPath, []byte("changed #43"), 0o600); err != nil {
				t.Fatal(err)
			}
			check("43")
		})
	}
}

func TestIncidentHardlinkRemovalAndRecreationRemainsWatched(t *testing.T) {
	path := filepath.Join(t.TempDir(), "body")
	if err := os.WriteFile(path, []byte("body #42"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(path, path+"-retained"); err != nil {
		t.Fatal(err)
	}
	st, index, record := incidentPathFixture(t, path)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if got, err := index.Holds(t.Context(), st, "d", []string{"17"}); err != nil || got["17"].Seq != record.Seq {
		t.Fatalf("missing hardlink summary=%v/%v", got, err)
	}
	if err := os.WriteFile(path, []byte("recreated #43"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := index.Holds(t.Context(), st, "d", []string{"42", "43"}); err != nil || len(got) != 1 || got["43"].Seq != record.Seq {
		t.Fatalf("hardlink recreation lost watch: %v/%v", got, err)
	}
}
