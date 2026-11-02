package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestBoardWatchRefusesAmbiguousMissingParentBeforeFanout(t *testing.T) {
	for _, population := range []int{32, 10000} {
		t.Run(fmt.Sprint(population), func(t *testing.T) {
			ctx := t.Context()
			watch, err := newIncidentFileWatch()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = watch.Close() }()
			folder := t.TempDir()
			for i := range population {
				if err := watch.Register(filepath.Join(folder, fmt.Sprintf("missing-%d", i))); err != nil {
					t.Fatal(err)
				}
			}
			warm, err := watch.DrainBoard(ctx)
			if err != nil || len(warm) != 0 {
				t.Fatalf("warm cut=%v, %v", warm, err)
			}
			stats, names := watch.stats, watch.missingNames
			if err := os.WriteFile(filepath.Join(folder, "unrelated"), []byte("unrelated"), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err = watch.DrainBoard(ctx)
			if !errors.Is(err, errBoardWatchIncomplete) {
				t.Fatalf("ambiguous parent cut=%v", err)
			}
			if watch.stats != stats || watch.missingNames != names || len(watch.rebind) != 0 {
				t.Fatalf("ambiguous event expanded missing routes: stats=%d/%d names=%d/%d rebind=%d", stats, watch.stats, names, watch.missingNames, len(watch.rebind))
			}
		})
	}
}
