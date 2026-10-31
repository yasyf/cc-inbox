package version

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func commitCask(t *testing.T, root, version string) {
	t.Helper()
	dir := filepath.Join(root, ".metadata")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(map[string]any{"source": map[string]string{"version": version}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "INSTALL_RECEIPT.json")
	if err := os.WriteFile(path+".tmp", raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		t.Fatal(err)
	}
}

func TestCaskHandoffWaitsForCommittedReceipt(t *testing.T) {
	root := t.TempDir()
	plugin := filepath.Join(root, "plugins")
	cask := filepath.Join(root, "Caskroom", "cci")
	installAt(t, filepath.Join(plugin, "0.46.3", "bin", "cci"))
	installAt(t, filepath.Join(cask, "0.46.4", "cci"))
	commitCask(t, cask, "0.46.4")
	roots := Roots{Plugin: plugin, Casks: []string{cask}}
	assertHandoff := func(want string) {
		t.Helper()
		got, ok, err := roots.NewerThan("0.46.3")
		if err != nil || !ok || got.Version != want || got.Path != filepath.Join(cask, want, "cci") {
			t.Fatalf("handoff = %+v, %t, %v; want committed %s", got, ok, err, want)
		}
	}
	assertHandoff("0.46.4")
	installAt(t, filepath.Join(cask, "0.46.6", "cci"))
	installAt(t, filepath.Join(cask, ".metadata", "0.46.6", "20261031113323.753", "Casks", "cci.rb"))
	got, ok, err := roots.NewerThan("0.46.3")
	if err != nil || !ok || got.Version != "0.46.4" {
		t.Fatalf("staged cask selected before committed receipt: %+v, %t, %v", got, ok, err)
	}
	assertHandoff("0.46.4")
	commitCask(t, cask, "0.46.6")
	assertHandoff("0.46.6")
	if got, ok, err := roots.NewerThan("0.46.6"); err != nil || ok || got.Version != "0.46.6" {
		t.Fatalf("equal-version handoff = %+v, %t, %v", got, ok, err)
	}
	if got, ok, err := roots.NewerThan("0.46.7"); err != nil || ok || got.Version != "0.46.6" {
		t.Fatalf("downgrade handoff = %+v, %t, %v", got, ok, err)
	}
	installAt(t, filepath.Join(plugin, "0.46.7", "bin", "cci"))
	if got, ok, err := roots.NewerThan("0.46.3"); err != nil || !ok || got.Version != "0.46.7" {
		t.Fatalf("newer plugin handoff = %+v, %t, %v", got, ok, err)
	}
	installAt(t, filepath.Join(plugin, "0.46.7", ".orphaned_at"))
	assertHandoff("0.46.6")
}

func TestCaskWithoutReceiptDoesNotHideEligiblePlugin(t *testing.T) {
	root := t.TempDir()
	plugin := filepath.Join(root, "plugins")
	cask := filepath.Join(root, "Caskroom", "cci")
	installAt(t, filepath.Join(plugin, "0.46.3", "bin", "cci"))
	installAt(t, filepath.Join(cask, "0.46.6", "cci"))
	roots := Roots{Plugin: plugin, Casks: []string{cask}}
	got, ok, err := roots.NewerThan("0.46.2")
	if err != nil || !ok || got.Version != "0.46.3" || got.Path != filepath.Join(plugin, "0.46.3", "bin", "cci") {
		t.Fatalf("plugin with staged cask = %+v, %t, %v", got, ok, err)
	}
}

func TestCaskReadinessUsesReceiptVersionInEachPrefix(t *testing.T) {
	root := t.TempDir()
	arm := filepath.Join(root, "arm", "Caskroom", "cci")
	intel := filepath.Join(root, "intel", "Caskroom", "cci")
	for _, cask := range []string{arm, intel} {
		installAt(t, filepath.Join(cask, "0.46.6", "cci"))
		installAt(t, filepath.Join(cask, "0.46.9", "cci"))
	}
	commitCask(t, arm, "0.46.6")
	commitCask(t, intel, "0.46.9")
	got, err := (Roots{Casks: []string{arm, intel}}).Newest()
	if err != nil || got.Version != "0.46.9" || got.Path != filepath.Join(intel, "0.46.9", "cci") {
		t.Fatalf("multiple committed prefixes = %+v, %v", got, err)
	}
}

func TestCaskReadinessRejectsInvalidOrUnusableReceipt(t *testing.T) {
	for _, tt := range []struct {
		name    string
		receipt string
		wantErr string
	}{
		{"invalid json", `{`, "decode cci cask receipt"},
		{"invalid version", `{"source":{"version":"../0.46.6"}}`, ""},
		{"missing version", `{"source":{}}`, ""},
		{"unavailable version", `{"source":{"version":"0.46.5"}}`, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cask := t.TempDir()
			installAt(t, filepath.Join(cask, "0.46.6", "cci"))
			path := filepath.Join(cask, ".metadata", "INSTALL_RECEIPT.json")
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte(tt.receipt), 0o644); err != nil {
				t.Fatal(err)
			}
			got, err := (Roots{Casks: []string{cask}}).Newest()
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("invalid receipt = %+v, %v", got, err)
				}
			} else if err != nil || got != (Install{}) {
				t.Fatalf("unusable receipt = %+v, %v", got, err)
			}
		})
	}
}
