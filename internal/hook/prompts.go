package hook

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/yasyf/cc-inbox/internal/store"
)

const (
	promptsDir  = "prompts"
	refusedExt  = ".refused"
	promptsGlob = "*.json"
)

// KeepPrompt keeps a prompt hook's payload under home for DrainPrompts to
// record once the store answers, so a prompt the store could not take in the
// hook's budget is recorded late instead of lost.
func KeepPrompt(home string, payload []byte) error {
	dir := filepath.Join(home, promptsDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("keep prompt: %w", err)
	}
	path := filepath.Join(dir, fmt.Sprintf("%d-%d.json", time.Now().UnixNano(), os.Getpid()))
	if err := os.WriteFile(path+".tmp", payload, 0o600); err != nil {
		return fmt.Errorf("keep prompt: %w", err)
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		return fmt.Errorf("keep prompt: %w", err)
	}
	return nil
}

// DrainPrompts records every prompt KeepPrompt kept for st's home, oldest
// first, and stops at the first one the store cannot take yet. A prompt
// recorded twice, by two drains or by a drain after a write the daemon did
// not confirm, deduplicates against the first; a payload Prompt refuses is
// set aside as .refused.
func DrainPrompts(ctx context.Context, st *store.Store) error {
	kept, err := filepath.Glob(filepath.Join(st.HomeDir(), promptsDir, promptsGlob))
	if err != nil {
		return err
	}
	slices.Sort(kept)
	for _, path := range kept {
		raw, err := os.ReadFile(path) //nolint:gosec // G304: a payload KeepPrompt wrote under this store's home.
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		p, err := Read(bytes.NewReader(raw))
		if err == nil {
			err = Prompt(ctx, st, p)
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
