package store

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

func TestIncidentSharedInodeIsParsedOnceAndOldVersionsAreCollected(t *testing.T) {
	ctx := t.Context()
	st := openMembershipStore(ctx, t, t.TempDir())
	folder := t.TempDir()
	first, second := filepath.Join(folder, "first"), filepath.Join(folder, "second")
	if err := os.WriteFile(first, []byte("body #42"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(first, second); err != nil {
		t.Fatal(err)
	}
	var latest Record
	for _, path := range []string{first, second} {
		var err error
		latest, _, err = st.Post(ctx, Record{Drive: "d", Lane: "root", Kind: kinds.Incident, Text: "summary", Source: "post", Refs: Refs{Path: path}}, 0)
		if err != nil {
			t.Fatal(err)
		}
	}
	report, err := st.ProvisionIncidentScope(ctx, "d")
	if err != nil || report.Statistics.BodyOpens != 1 {
		t.Fatalf("shared provision=%+v, %v", report, err)
	}
	index, err := OpenIncidentIndex(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = index.Close() })
	before := index.Stats()
	if err := os.WriteFile(first, []byte("changed #43"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := index.Holds(ctx, st, "d", []string{"42", "43"})
	after := index.Stats()
	if err != nil || len(got) != 1 || got["43"].Seq != latest.Seq || after.BodyOpens-before.BodyOpens != 1 {
		t.Fatalf("shared update=%v/%v stats=%+v/%+v", got, err, before, after)
	}
	var versions int
	if err := st.db.QueryRowContext(ctx, "SELECT count(*) FROM incident_body_versions").Scan(&versions); err != nil {
		t.Fatal(err)
	}
	if versions != 1 {
		t.Fatalf("unreferenced body versions retained: %d", versions)
	}
}

func TestIncidentUnrelatedParentEventDoesNotOpenHealthyBodies(t *testing.T) {
	const population = 32
	ctx := t.Context()
	st := openMembershipStore(ctx, t, t.TempDir())
	folder := t.TempDir()
	for i := 0; i < population; i++ {
		path := filepath.Join(folder, fmt.Sprintf("body-%d", i))
		if err := os.WriteFile(path, []byte(fmt.Sprintf("body #%d", 90000+i)), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := st.Post(ctx, Record{Drive: "d", Lane: "root", Kind: kinds.Incident, Text: "summary", Source: "post", Refs: Refs{Path: path}}, 0); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := st.Post(ctx, Record{Drive: "d", Lane: "root", Kind: kinds.Incident, Text: "missing summary", Source: "post", Refs: Refs{Path: filepath.Join(folder, "missing")}}, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ProvisionIncidentScope(ctx, "d"); err != nil {
		t.Fatal(err)
	}
	index, err := OpenIncidentIndex(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = index.Close() })
	if _, err := index.Holds(ctx, st, "d", []string{"90000"}); err != nil {
		t.Fatal(err)
	}
	before := index.Stats()
	if err := os.WriteFile(filepath.Join(folder, "unregistered-sibling"), []byte("unrelated"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := index.Holds(ctx, st, "d", []string{"90000"})
	after := index.Stats()
	if err != nil || len(got) != 1 {
		t.Fatalf("after parent event=%v, %v", got, err)
	}
	if after.BodyOpens != before.BodyOpens || after.BodyBytes != before.BodyBytes || after.BodyStats-before.BodyStats > population+1 || after.RouteStats-before.RouteStats > before.RouteStats {
		t.Fatalf("unrelated parent work before=%+v after=%+v", before, after)
	}
	t.Logf("registered_healthy=%d registered_missing=1 unrelated_parent_event_body_stats=%d route_stats=%d body_opens=%d body_bytes=%d", population, after.BodyStats-before.BodyStats, after.RouteStats-before.RouteStats, after.BodyOpens-before.BodyOpens, after.BodyBytes-before.BodyBytes)
}
