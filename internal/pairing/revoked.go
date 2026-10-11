package pairing

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

func revokedPath(home, mesh string) string {
	return filepath.Join(home, "relay", mesh, "revoked.json")
}

func loadRevoked(home, mesh string) (map[string]time.Time, error) {
	raw, err := os.ReadFile(revokedPath(home, mesh))
	if errors.Is(err, fs.ErrNotExist) {
		return map[string]time.Time{}, nil
	}
	if err != nil {
		return nil, err
	}
	out := map[string]time.Time{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("parse %s: %w", revokedPath(home, mesh), err)
	}
	return out, nil
}

func revoke(home, mesh, id string, at time.Time) error {
	revoked, err := loadRevoked(home, mesh)
	if err != nil {
		return err
	}
	if at.Before(revoked[id]) {
		return nil
	}
	revoked[id] = at
	raw, err := json.Marshal(revoked)
	if err != nil {
		return err
	}
	path := revokedPath(home, mesh)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
