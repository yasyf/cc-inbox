package cli

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/manifest"

	"github.com/yasyf/cc-inbox/internal/mesh"
)

func TestWriteMeshManifestKeepsTheRegisteredDebounce(t *testing.T) {
	schema, registeredSchema := strings.Repeat("a", 64), strings.Repeat("b", 64)
	tests := []struct {
		name       string
		registered string
		want       time.Duration
		wantErr    string
	}{
		{
			name:       "registration keeps its debounce",
			registered: `{"name":"cci","binary":"cci","watch":{"debounce":"12s"},"service":{"kind":"resident","schema_fingerprint":"` + registeredSchema + `"}}`,
			want:       12 * time.Second,
		},
		{name: "no registration gets the default", want: 5 * time.Second},
		{name: "malformed registration fails", registered: `{"name":"cci",`, wantErr: "keep the registered debounce: decode manifest"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("XDG_CONFIG_HOME", t.TempDir())
			home := t.TempDir()
			if tt.registered != "" {
				registeredPath, err := hostregistry.RegisteredPath(mesh.Service)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.MkdirAll(filepath.Dir(registeredPath), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(registeredPath, []byte(tt.registered), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			path, err := writeMeshManifest(home, schema)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("writeMeshManifest() error = %v, want %q", err, tt.wantErr)
				}
				if _, err := os.Stat(mesh.ManifestPath(home)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("staged manifest after a failed write: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if path != mesh.ManifestPath(home) {
				t.Fatalf("writeMeshManifest() = %q, want %q", path, mesh.ManifestPath(home))
			}
			got, err := manifest.Load(path)
			if err != nil {
				t.Fatal(err)
			}
			if debounce := time.Duration(got.Watch.Debounce); debounce != tt.want || got.Service.SchemaFingerprint != schema {
				t.Fatalf("staged debounce %s, schema %s; want %s, %s", debounce, got.Service.SchemaFingerprint, tt.want, schema)
			}
		})
	}
}
