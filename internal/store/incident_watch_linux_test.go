package store

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestIncidentNamedEventsDoNotRangeMissingSiblings(t *testing.T) {
	for _, population := range []int{32, 10000} {
		t.Run(fmt.Sprint(population), func(t *testing.T) {
			watch, err := newIncidentFileWatch()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = watch.Close() })
			folder := t.TempDir()
			for i := 0; i < population; i++ {
				if err := watch.Register(filepath.Join(folder, fmt.Sprintf("missing-%d", i))); err != nil {
					t.Fatal(err)
				}
			}
			beforeStats, beforeNames := watch.stats, watch.missingNames
			if err := os.WriteFile(filepath.Join(folder, "unrelated"), []byte("data"), 0o600); err != nil {
				t.Fatal(err)
			}
			dirty, err := watch.Drain(t.Context())
			if err != nil || len(dirty) != 0 || watch.stats != beforeStats || watch.missingNames-beforeNames > 8 {
				t.Fatalf("named event work: dirty=%v error=%v stats=%d names=%d", dirty, err, watch.stats-beforeStats, watch.missingNames-beforeNames)
			}
			raw := watch.backend.(*incidentInotify)
			if raw.lastCutBytes <= 0 || raw.lastReadBytes != raw.lastCutBytes {
				t.Fatalf("entry byte cut incomplete: cut=%d read=%d", raw.lastCutBytes, raw.lastReadBytes)
			}
			t.Logf("missing_names=%d actual_named_map_lookups=%d body_route_stats=0 kernel_prefix_bytes=%d native_read_calls=%d", population, watch.missingNames-beforeNames, raw.lastCutBytes, raw.lastReads)
		})
	}
}

func TestIncidentAutomaticRemovalDoesNotRetainConsumedTombstones(t *testing.T) {
	watch, err := newIncidentFileWatch()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = watch.Close() })
	path := filepath.Join(t.TempDir(), "body")
	if err := os.WriteFile(path, []byte("initial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := watch.Register(path); err != nil {
		t.Fatal(err)
	}
	raw := watch.backend.(*incidentInotify)
	for i := 0; i < 100; i++ {
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if _, err := watch.Drain(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("recreated"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := watch.Drain(t.Context()); err != nil {
			t.Fatal(err)
		}
		if len(raw.removed) > len(raw.files)+2 {
			t.Fatalf("historical watch tombstones retained: cycle=%d tombstones=%d live=%d", i, len(raw.removed), len(raw.files))
		}
	}
	if err := watch.Forget(path); err != nil {
		t.Fatal(err)
	}
	if _, err := watch.Drain(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(raw.removed) != 0 || len(raw.dead) != 0 || len(raw.files) != 0 {
		t.Fatalf("retired kernel state remains: removed=%d dead=%d live=%d", len(raw.removed), len(raw.dead), len(raw.files))
	}
}
