package hook

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/yasyf/cc-inbox/internal/node"
	"github.com/yasyf/cc-inbox/internal/store"
)

const (
	promptsDir  = "prompts"
	refusedExt  = ".refused"
	promptsGlob = "*.json"
)

type keptPrompt struct {
	Origin  string          `json:"origin"`
	At      time.Time       `json:"at"`
	Payload json.RawMessage `json:"payload"`
}

// KeepPrompt keeps a prompt hook's payload, received at at, under home for
// DrainPrompts to record once the store answers, so a prompt the store could
// not take in the hook's budget is recorded late instead of lost.
func KeepPrompt(home string, payload []byte, at time.Time) error {
	n, err := node.Load(home)
	if err != nil {
		return fmt.Errorf("keep prompt: %w", err)
	}
	body, err := json.Marshal(keptPrompt{Origin: n.ID(), At: at, Payload: payload})
	if err != nil {
		return fmt.Errorf("keep prompt: %w", err)
	}
	dir := filepath.Join(home, promptsDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("keep prompt: %w", err)
	}
	path := filepath.Join(dir, fmt.Sprintf("%d-%d.json", at.UnixNano(), os.Getpid()))
	if err := os.WriteFile(path+".tmp", body, 0o600); err != nil {
		return fmt.Errorf("keep prompt: %w", err)
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		return fmt.Errorf("keep prompt: %w", err)
	}
	return nil
}

// DrainPrompts records every prompt KeepPrompt kept for st's home and node,
// oldest first, at the time its hook received it, and stops at the first one
// the store cannot take yet. A prompt recorded twice, by two drains or after a
// write the daemon did not confirm, deduplicates against the first; one Prompt
// refuses is set aside as .refused, and one another node kept waits for that
// node.
func DrainPrompts(ctx context.Context, st *store.Store) error {
	kept, err := filepath.Glob(filepath.Join(st.HomeDir(), promptsDir, promptsGlob))
	if err != nil {
		return err
	}
	slices.Sort(kept)
	for _, path := range kept {
		raw, err := os.ReadFile(path) //nolint:gosec // G304: a prompt KeepPrompt kept under this store's home.
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		var k keptPrompt
		err = json.Unmarshal(raw, &k)
		if err == nil && k.Origin != st.Self() {
			continue
		}
		if err == nil {
			var p Payload
			if p, err = Read(bytes.NewReader(k.Payload)); err == nil {
				err = Prompt(ctx, st, p, k.At)
			}
		}
		switch {
		case err == nil:
		case store.Busy(err), errors.Is(err, store.ErrUnconfirmed), errors.Is(err, store.ErrNeedsInit), ctx.Err() != nil:
			return err
		default:
			slog.Warn("cci set aside a kept prompt it cannot record", "path", path, "err", err)
			if err := os.Rename(path, path+refusedExt); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
			continue
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return nil
}
