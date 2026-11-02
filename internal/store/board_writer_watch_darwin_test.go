package store

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func invalidateBoardNativeBackend(t *testing.T, watch *incidentFileWatch) {
	t.Helper()
	queue := watch.backend.(*incidentKqueue)
	if err := queue.Close(); err != nil {
		t.Fatal(err)
	}
	queue.fd = -1
	queue.files = map[incidentWatchKey]int{}
}

func TestBoardDarwinMissingParentRefusesBeforeFanout(t *testing.T) {
	boardContractEnabled(t)
	for _, population := range []int{32, 10000, 100000} {
		t.Run(fmt.Sprint(population), func(t *testing.T) {
			watch, err := newIncidentFileWatch()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = watch.Close() }()
			folder := t.TempDir()
			for i := 0; i < population; i++ {
				if err := watch.Register(filepath.Join(folder, fmt.Sprintf("missing-%d", i))); err != nil {
					t.Fatal(err)
				}
			}
			beforeStats, beforeNames := watch.stats, watch.missingNames
			if err := os.WriteFile(filepath.Join(folder, "unrelated"), []byte("ordinary write"), 0o600); err != nil {
				t.Fatal(err)
			}
			dirty, err := watch.DrainBoard(t.Context())
			if err == nil || !strings.Contains(err.Error(), "unnamed parent event") || len(dirty) != 0 || watch.stats != beforeStats || watch.missingNames != beforeNames {
				t.Fatalf("missing parent ranged history: dirty=%d error=%v route_stats=%d name_checks=%d", len(dirty), err, watch.stats-beforeStats, watch.missingNames-beforeNames)
			}
			t.Logf("BOARD_DARWIN_MISSING_REFUSAL population=%d route_stat_attempts=%d missing_name_checks=%d", population, watch.stats-beforeStats, watch.missingNames-beforeNames)
		})
	}
}
