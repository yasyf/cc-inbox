package store

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestIncidentProjectionRejectsConcurrentProjectorsBeforePublication(t *testing.T) {
	ctx := t.Context()
	st, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	record, _, err := st.Post(ctx, Record{Drive: "d", Lane: "root", Kind: "incident", Text: "summary #42", Source: "post"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.ProvisionIncidentScope(ctx, "d"); err != nil {
		t.Fatal(err)
	}
	first, err := OpenIncidentIndex(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })
	other, err := Open(ctx, st.HomeDir())
	if err != nil {
		t.Fatalf("ordinaryStoreOpen blocked by owner: %v", err)
	}
	t.Cleanup(func() { _ = other.Close() })
	second, err := OpenIncidentIndex(ctx, other)
	if err == nil {
		defer func() { _ = second.Close() }()
		if _, err := st.db.ExecContext(ctx, "UPDATE records SET text='summary #43' WHERE seq=?", record.Seq); err != nil {
			t.Fatal(err)
		}
		stale, err := st.captureIncidentChanges(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.db.ExecContext(ctx, "UPDATE records SET text='summary #44' WHERE seq=?", record.Seq); err != nil {
			t.Fatal(err)
		}
		if got, err := second.Holds(ctx, other, "d", []string{"44"}); err != nil || got["44"].Seq != record.Seq {
			t.Fatalf("newer projector did not publish: %v/%v", got, err)
		}
		if _, err := st.publishIncidentChanges(ctx, stale); err != nil {
			t.Fatal(err)
		}
		got, err := second.Holds(ctx, other, "d", []string{"43", "44"})
		if err != nil || got["43"].Seq != record.Seq || len(got) != 1 {
			t.Fatalf("old race schedule not reproduced: %v/%v", got, err)
		}
		t.Fatal("concurrent incident projector accepted; reversed publication permanently regressed membership")
	}
	var unavailable *IncidentError
	if !errors.As(err, &unavailable) || unavailable.Code != "projection_unavailable" || !strings.Contains(err.Error(), "projection owner") {
		t.Fatalf("second projector error=%v", err)
	}
	if _, _, err := other.Post(ctx, Record{Drive: "d", Lane: "root", Kind: "incident", Text: "ordinary writer #45", Source: "post"}, 0); err != nil {
		t.Fatalf("ordinarywriter blocked by owner: %v", err)
	}
	if _, err := other.ProvisionIncidentScope(ctx, "another"); err == nil {
		t.Fatal("concurrent explicitprovision accepted")
	}
	var scopes int
	if err := st.db.QueryRowContext(ctx, "SELECT count(*) FROM incident_scopes WHERE drive='another'").Scan(&scopes); err != nil || scopes != 0 {
		t.Fatalf("owner failure mutated admission: %d/%v", scopes, err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := other.ProvisionIncidentScope(ctx, "another"); err != nil {
		t.Fatalf("owner not released afterClose: %v", err)
	}
	next, err := OpenIncidentIndex(ctx, other)
	if err != nil {
		t.Fatal(err)
	}
	if err := next.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestIncidentOpenNeverRecreatesMissingOwnerLock(t *testing.T) {
	ctx := t.Context()
	st, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	path := filepath.Join(st.HomeDir(), "incident-owner.lock")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenIncidentIndex(ctx, st); err == nil {
		t.Fatal("missing physical ownerlock accepted")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("runtimeOpen recreated ownerlock: %v", err)
	}
}
