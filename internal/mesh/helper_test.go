package mesh

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/synckit/codec"
	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/manifest"
)

func TestRefreshRegistrationPreservesEachManifest(t *testing.T) {
	oldSchema := "d63" + strings.Repeat("a", 61)
	schema := "85" + strings.Repeat("b", 62)
	for _, tt := range []struct {
		name             string
		sourceSchema     string
		registeredSchema string
	}{
		{"both stale", oldSchema, oldSchema},
		{"source stale", oldSchema, schema},
		{"registration stale", schema, oldSchema},
		{"already current", schema, schema},
	} {
		t.Run(tt.name, func(t *testing.T) {
			home, registeredPath := registrationHome(t)
			source := Manifest(tt.sourceSchema, 12*time.Second)
			source.Binary = "source-cci"
			source.Brew = "source/tap/cci"
			source.Helper = &manifest.HelperSpec{Command: "source-helper", SessionType: manifest.SessionTypeAqua}
			registered := Manifest(tt.registeredSchema, 21*time.Second)
			registered.Binary = "/opt/cci/bin/registered-cci"
			registered.Brew = "registered/tap/cci"
			registered.Service.Kind = "spawned"
			registered.Helper = &manifest.HelperSpec{Command: "registered-helper", SessionType: manifest.SessionTypeBackground}
			writeRegistrationFixture(t, ManifestPath(home), source)
			writeRegistrationFixture(t, registeredPath, registered)
			sourceFile, err := os.Open(ManifestPath(home))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = sourceFile.Close() }()
			registeredFile, err := os.Open(registeredPath)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = registeredFile.Close() }()
			if err := RefreshRegistration(context.Background(), home, schema); err != nil {
				t.Fatal(err)
			}
			for _, copy := range []struct {
				path string
				file *os.File
				old  manifest.Manifest
			}{
				{ManifestPath(home), sourceFile, source},
				{registeredPath, registeredFile, registered},
			} {
				got, err := manifest.Load(copy.path)
				if err != nil {
					t.Fatal(err)
				}
				want := copy.old
				want.Service.SchemaFingerprint = schema
				if !reflect.DeepEqual(*got, want) {
					t.Fatalf("manifest %s = %+v, want %+v", copy.path, *got, want)
				}
				before, err := copy.file.Stat()
				if err != nil {
					t.Fatal(err)
				}
				after, err := os.Stat(copy.path)
				if err != nil {
					t.Fatal(err)
				}
				changed := copy.old.Service.SchemaFingerprint != schema
				if os.SameFile(before, after) == changed {
					t.Fatalf("manifest %s inode changed = %v, want %v", copy.path, !os.SameFile(before, after), changed)
				}
				if after.Mode().Perm() != 0o600 {
					t.Fatalf("manifest %s mode = %o, want 600", copy.path, after.Mode().Perm())
				}
				oldRaw, err := io.ReadAll(copy.file)
				if err != nil {
					t.Fatal(err)
				}
				var old manifest.Manifest
				if err := json.Unmarshal(oldRaw, &old); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(old, copy.old) {
					t.Fatalf("old inode %s was modified: %+v", copy.path, old)
				}
			}
		})
	}
}

func TestRefreshRegistrationRequiresBothFiles(t *testing.T) {
	for _, present := range []string{"neither", "source", "registered"} {
		t.Run(present, func(t *testing.T) {
			home, registeredPath := registrationHome(t)
			paths := []string{ManifestPath(home), registeredPath}
			if present != "neither" {
				path := paths[0]
				if present == "registered" {
					path = paths[1]
				}
				if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte("invalid but disabled"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			before := snapshotRegistration(t, paths)
			if err := RefreshRegistration(context.Background(), home, strings.Repeat("a", 64)); err != nil {
				t.Fatal(err)
			}
			if after := snapshotRegistration(t, paths); !reflect.DeepEqual(after, before) {
				t.Fatalf("disabled registration changed: %v, want %v", after, before)
			}
			for _, path := range paths {
				if _, exists := before[path]; !exists {
					if _, err := os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("created disabled manifest directory %s: %v", path, err)
					}
				}
			}
		})
	}
}

func TestRefreshRegistrationRejectsInvalidManifestsBeforeWriting(t *testing.T) {
	for _, invalid := range []string{"source JSON", "registered JSON", "source name", "registered name", "new schema"} {
		t.Run(invalid, func(t *testing.T) {
			home, registeredPath := registrationHome(t)
			source := Manifest(strings.Repeat("a", 64), time.Second)
			registered := Manifest(strings.Repeat("b", 64), 2*time.Second)
			if invalid == "source name" {
				source.Name = "other"
			}
			if invalid == "registered name" {
				registered.Name = "other"
			}
			writeRegistrationFixture(t, ManifestPath(home), source)
			writeRegistrationFixture(t, registeredPath, registered)
			if invalid == "source JSON" || invalid == "registered JSON" {
				path := ManifestPath(home)
				if invalid == "registered JSON" {
					path = registeredPath
				}
				if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			paths := []string{ManifestPath(home), registeredPath, filepath.Join(filepath.Dir(registeredPath), "other.json")}
			before := snapshotRegistration(t, paths)
			schema := strings.Repeat("c", 64)
			if invalid == "new schema" {
				schema = "invalid"
			}
			if err := RefreshRegistration(context.Background(), home, schema); err == nil {
				t.Fatal("invalid manifest refresh succeeded")
			}
			if after := snapshotRegistration(t, paths); !reflect.DeepEqual(after, before) {
				t.Fatalf("invalid registration changed: %v, want %v", after, before)
			}
		})
	}
}

func TestRefreshRegistrationPublicationFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires filesystem permission enforcement")
	}
	for _, failing := range []string{"source", "registered"} {
		t.Run(failing, func(t *testing.T) {
			home, registeredPath := registrationHome(t)
			oldSchema, schema := strings.Repeat("a", 64), strings.Repeat("b", 64)
			writeRegistrationFixture(t, ManifestPath(home), Manifest(oldSchema, time.Second))
			writeRegistrationFixture(t, registeredPath, Manifest(oldSchema, 2*time.Second))
			path := ManifestPath(home)
			if failing == "registered" {
				path = registeredPath
			}
			dir := filepath.Dir(path)
			if err := os.Chmod(dir, 0o500); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := os.Chmod(dir, 0o700); err != nil {
					t.Error(err)
				}
			})
			if err := RefreshRegistration(context.Background(), home, schema); !errors.Is(err, os.ErrPermission) {
				t.Fatalf("publication error = %v, want permission failure", err)
			}
			registered, err := manifest.Load(registeredPath)
			if err != nil {
				t.Fatal(err)
			}
			if registered.Service.SchemaFingerprint != oldSchema {
				t.Fatalf("failed publication changed registration: %+v", registered)
			}
			source, err := manifest.Load(ManifestPath(home))
			if err != nil {
				t.Fatal(err)
			}
			want := oldSchema
			if failing == "registered" {
				want = schema
			}
			if source.Service.SchemaFingerprint != want {
				t.Fatalf("source after publication failure = %s, want %s", source.Service.SchemaFingerprint, want)
			}
		})
	}
}

func TestWriteManifestPublishesAtomically(t *testing.T) {
	home := t.TempDir()
	path, err := WriteManifest(home, strings.Repeat("a", 64), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	old, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = old.Close() }()
	before, err := io.ReadAll(old)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := WriteManifest(home, strings.Repeat("b", 64), 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := old.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	after, err := io.ReadAll(old)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("WriteManifest modified the previously open inode")
	}
	got, err := manifest.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Service.SchemaFingerprint != strings.Repeat("b", 64) || got.Watch.Debounce != codec.Duration(2*time.Second) {
		t.Fatalf("published manifest = %+v", got)
	}
}

func registrationHome(t *testing.T) (string, string) {
	t.Helper()
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("DAEMONKIT_HOME", t.TempDir())
	path, err := hostregistry.RegisteredPath(Service)
	if err != nil {
		t.Fatal(err)
	}
	return t.TempDir(), path
}

func writeRegistrationFixture(t *testing.T, path string, m manifest.Manifest) {
	t.Helper()
	if err := writeManifest(path, m); err != nil {
		t.Fatal(err)
	}
}

func snapshotRegistration(t *testing.T, paths []string) map[string]string {
	t.Helper()
	result := make(map[string]string)
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		result[path] = string(raw)
	}
	return result
}
