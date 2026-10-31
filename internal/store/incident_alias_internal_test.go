package store

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

func incidentPathFixture(t *testing.T, path string) (*Store, *IncidentIndex, Record) {
	t.Helper()
	ctx := t.Context()
	st := openMembershipStore(ctx, t, t.TempDir())
	record, _, err := st.Post(ctx, Record{Drive: "d", Lane: "root", Kind: kinds.Incident, Text: "summary #17", Source: "post", Refs: Refs{Path: path}}, 0)
	if err != nil {
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
	return st, index, record
}

func TestIncidentWatchPreservesSymlinkDotDotRoutes(t *testing.T) {
	folder := t.TempDir()
	for _, directory := range []string{"a", "elsewhere/child", "alternate/child"} {
		if err := os.MkdirAll(filepath.Join(folder, directory), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	actual := filepath.Join(folder, "elsewhere", "hold.md")
	if err := os.WriteFile(actual, []byte("body #42"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(folder, "a", "hold.md"), []byte("wrong #999"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(folder, "a", "link")
	if err := os.Symlink(filepath.Join(folder, "elsewhere", "child"), link); err != nil {
		t.Fatal(err)
	}
	st, index, record := incidentPathFixture(t, link+"/../hold.md")
	check := func(pr string) {
		t.Helper()
		got, err := index.Holds(t.Context(), st, "d", []string{"17", "42", "43", "44", "45", "999"})
		if err != nil || len(got) != 1 || got[pr].Seq != record.Seq {
			t.Fatalf("native watch stale after write: want=%s got=%v error=%v", pr, got, err)
		}
	}
	check("42")
	if err := os.WriteFile(actual, []byte("changed #43"), 0o600); err != nil {
		t.Fatal(err)
	}
	check("43")
	if err := os.WriteFile(filepath.Join(folder, "alternate", "hold.md"), []byte("replacement #44"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(folder, "alternate", "child"), link); err != nil {
		t.Fatal(err)
	}
	check("44")
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(folder, "missing", "child"), link); err != nil {
		t.Fatal(err)
	}
	check("17")
	if err := os.MkdirAll(filepath.Join(folder, "missing", "child"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(folder, "missing", "hold.md"), []byte("created #45"), 0o600); err != nil {
		t.Fatal(err)
	}
	check("45")
}

func TestIncidentDirtyBodyCheckpointSurvivesFailedPublicationAndReopen(t *testing.T) {
	ctx := context.Background()
	st, index, record := incidentPathFixture(t, "")
	path := filepath.Join(t.TempDir(), "new-body")
	if err := os.WriteFile(path, []byte("body #43"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, "UPDATE records SET refs=json_object('path',?),text='summary #44' WHERE seq=?", path, record.Seq); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, `CREATE TRIGGER incident_fault BEFORE INSERT ON incident_body_versions BEGIN SELECT RAISE(ABORT,'injected body publication failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := index.Holds(ctx, st, "d", []string{"43"}); err == nil || !strings.Contains(err.Error(), "injected body publication failure") {
		t.Fatalf("publication fault=%v", err)
	}
	if err := index.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, "DROP TRIGGER incident_fault"); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenIncidentIndex(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	got, err := reopened.Holds(ctx, st, "d", []string{"17", "43", "44"})
	if err != nil || len(got) != 1 || got["43"].Seq != record.Seq {
		t.Fatalf("checkpoint lost after reopen: %v, %v", got, err)
	}
}

func TestIncidentColdOwnerDiscardsRetiredPathBeforeFileAccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "body")
	if err := os.WriteFile(path, []byte("body #42"), 0o600); err != nil {
		t.Fatal(err)
	}
	st, index, record := incidentPathFixture(t, path)
	if _, err := st.db.ExecContext(t.Context(), "DELETE FROM records WHERE seq=?", record.Seq); err != nil {
		t.Fatal(err)
	}
	if err := index.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("body", path); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenIncidentIndex(t.Context(), st)
	if err != nil {
		t.Fatalf("retired canonical body was inspected: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	got, err := reopened.Holds(t.Context(), st, "d", []string{"42"})
	if err != nil || len(got) != 0 {
		t.Fatalf("retired record returned: %v, %v", got, err)
	}
	if stats := reopened.Stats(); stats.BodyStats != 0 || stats.Watches != 0 {
		t.Fatalf("retired path work=%+v", stats)
	}
}
