package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type cancelAfterIncidentDrain struct {
	incidentWatchBackend
	cancel   context.CancelFunc
	consumed int
}

func (backend *cancelAfterIncidentDrain) Drain(ctx context.Context) ([]incidentWatchEvent, error) {
	events, err := backend.incidentWatchBackend.Drain(ctx)
	backend.consumed += len(events)
	backend.cancel()
	return events, err
}

func TestIncidentCancelledConsumerRetainsConsumedKernelEvents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "body")
	if err := os.WriteFile(path, []byte("body #42"), 0o600); err != nil {
		t.Fatal(err)
	}
	st, index, record := incidentPathFixture(t, path)
	if err := os.WriteFile(path, []byte("changed #43"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	raw := index.watch.backend
	wrapped := &cancelAfterIncidentDrain{incidentWatchBackend: raw, cancel: cancel}
	index.watch.backend = wrapped
	if _, err := index.Holds(ctx, st, "d", []string{"42", "43"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled query=%v", err)
	}
	if wrapped.consumed == 0 {
		t.Fatal("cancellation fixture did not consume actual native events")
	}
	index.watch.backend = raw
	got, err := index.Holds(t.Context(), st, "d", []string{"42", "43"})
	if err != nil || len(got) != 1 || got["43"].Seq != record.Seq {
		t.Fatalf("cancelled kernel prefix lost or authority poisoned: %v, %v", got, err)
	}
}
