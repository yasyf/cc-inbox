package relay

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/yasyf/cc-inbox/e2e"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
)

const DefaultMesh = "default"

var meshName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,39}$`)

type Config struct {
	URL      string   `json:"url"`
	Mesh     string   `json:"mesh"`
	Tier     string   `json:"tier"`
	RelayPub []byte   `json:"relay_pub"`
	Routes   []string `json:"routes,omitempty"`
	Tap      string   `json:"tap,omitempty"`

	Delegation *e2e.Delegation `json:"delegation,omitempty"`
}

func (c Config) Carries(drive string) bool {
	for _, r := range c.Routes {
		if prefix, ok := strings.CutSuffix(r, "*"); ok && strings.HasPrefix(drive, prefix) || r == drive {
			return true
		}
	}
	return false
}

func root(home string) string { return filepath.Join(home, "relay") }

func dir(home, mesh string) string { return filepath.Join(root(home), mesh) }

func configPath(home, mesh string) string { return filepath.Join(dir(home, mesh), "config.json") }

func keyringPath(home, mesh string) string { return filepath.Join(dir(home, mesh), "keyring.json") }

func LoadConfig(home, mesh string) (Config, bool, error) {
	var c Config
	ok, err := readJSON(configPath(home, mesh), &c)
	return c, ok, err
}

func Configs(home string) ([]Config, error) {
	entries, err := os.ReadDir(root(home))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []Config
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		c, ok, err := LoadConfig(home, e.Name())
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, c)
		}
	}
	return out, nil
}

func SaveConfig(home string, c Config) error {
	if !meshName.MatchString(c.Mesh) {
		return fmt.Errorf("relay: invalid mesh name %q", c.Mesh)
	}
	return writeJSON(configPath(home, c.Mesh), c)
}

func Forget(home, mesh string) error {
	if err := os.RemoveAll(dir(home, mesh)); err != nil {
		return fmt.Errorf("forget relay mesh %s: %w", mesh, err)
	}
	return nil
}

type Keyring struct {
	Wraps []e2e.Wrap `json:"wraps"`

	mu   sync.Mutex
	open map[uint64]e2e.GroupKey
}

func LoadKeyring(home, mesh string) (*Keyring, error) {
	k := &Keyring{}
	if _, err := readJSON(keyringPath(home, mesh), k); err != nil {
		return nil, err
	}
	return k, nil
}

func (k *Keyring) Save(home, mesh string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	return writeJSON(keyringPath(home, mesh), k)
}

func (k *Keyring) Add(w e2e.Wrap) bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	for _, have := range k.Wraps {
		if have.Mesh == w.Mesh && have.Epoch == w.Epoch && have.To == w.To {
			return false
		}
	}
	k.Wraps = append(k.Wraps, w)
	return true
}

func (k *Keyring) DropFrom(node string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.Wraps = slices.DeleteFunc(k.Wraps, func(w e2e.Wrap) bool { return w.From == node })
	k.open = nil
}

func (k *Keyring) Latest(mesh string) uint64 {
	k.mu.Lock()
	defer k.mu.Unlock()
	var e uint64
	for _, w := range k.Wraps {
		if w.Mesh == mesh {
			e = max(e, w.Epoch)
		}
	}
	return e
}

func (k *Keyring) Keys(mesh string, id e2e.Identity, members map[string]e2e.Member) (map[uint64]e2e.GroupKey, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.open == nil {
		k.open = map[uint64]e2e.GroupKey{}
	}
	var errs []error
	for _, w := range k.Wraps {
		if w.Mesh != mesh || w.To != id.ID() {
			continue
		}
		if _, ok := k.open[w.Epoch]; ok {
			continue
		}
		g, err := e2e.UnwrapKey(w, mesh, id, members)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		k.open[w.Epoch] = g
	}
	out := make(map[uint64]e2e.GroupKey, len(k.open))
	for e, g := range k.open {
		out[e] = g
	}
	return out, errors.Join(errs...)
}

func (k *Keyring) Current(id e2e.Identity, mesh string, members map[string]e2e.Member) (e2e.GroupKey, error) {
	keys, err := k.Keys(mesh, id, members)
	var best e2e.GroupKey
	for e, g := range keys {
		if e > best.Epoch {
			best = g
		}
	}
	if best.Epoch == 0 {
		return e2e.GroupKey{}, errors.Join(fmt.Errorf("%w: mesh %s holds no usable wrap", e2e.ErrNoGroupKey, mesh), err)
	}
	return best, nil
}

func readJSON(path string, v any) (bool, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := json.Unmarshal(b, v); err != nil {
		return false, fmt.Errorf("decode %s: %w", path, err)
	}
	return true, nil
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}
