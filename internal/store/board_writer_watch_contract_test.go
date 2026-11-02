package store

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

func boardContractEnabled(t *testing.T) {
	t.Helper()
	if os.Getenv("CCI_BOARD_WRITER_WATCH_CONTRACT") != "1" {
		t.Skip("set CCI_BOARD_WRITER_WATCH_CONTRACT=1 for private writer/watch controls")
	}
}

func TestBoardOldWriterMovesDeletesAndCurrentBodyRetention(t *testing.T) {
	boardContractEnabled(t)
	st, index, spec, _ := boardFixture(t)
	first, second := filepath.Join(spec.CWD, "first"), filepath.Join(spec.CWD, "second")
	for path, body := range map[string]string{first: "old #123", second: "new #123"} {
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	ruling, _, err := st.Post(t.Context(), Record{Drive: "d", Lane: "root", Kind: kinds.Go, Text: "short #123", Refs: Refs{Path: first}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	id, err := index.Provision(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	check := func(count int, body, source string) {
		t.Helper()
		err := index.Read(t.Context(), st, spec, BoardSelection{PRs: []string{"123"}}, func(_ *BoardSnapshot, result *BoardResult) error {
			if result.Rulings.Error != nil || len(result.Rulings.Records) != count {
				t.Fatalf("rulings=%+v expected=%d", result.Rulings, count)
			}
			if count != 0 {
				record := result.Rulings.Records[0]
				if string(result.Rulings.Bodies[record.Seq]) != body || record.Source != source {
					t.Fatalf("body/source=%q/%s", result.Rulings.Bodies[record.Seq], record.Source)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	check(1, "old #123", ruling.Source)
	if _, err := st.db.ExecContext(t.Context(), "UPDATE records SET drive='other' WHERE seq=?", ruling.Seq); err != nil {
		t.Fatal(err)
	}
	check(0, "", "")
	var retained int
	if err := st.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM board_query_bodies WHERE scope=?", id).Scan(&retained); err != nil || retained != 0 {
		t.Fatalf("moved-out body retained=%d error=%v", retained, err)
	}
	if _, err := st.db.ExecContext(t.Context(), "UPDATE records SET drive='d',source='import:owned-control',refs=json_object('path',?) WHERE seq=?", second, ruling.Seq); err != nil {
		t.Fatal(err)
	}
	check(1, "new #123", "import:owned-control")
	if _, err := st.db.ExecContext(t.Context(), "DELETE FROM records WHERE seq=?", ruling.Seq); err != nil {
		t.Fatal(err)
	}
	check(0, "", "")
	if err := st.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM board_query_bodies WHERE scope=?", id).Scan(&retained); err != nil || retained != 0 {
		t.Fatalf("deleted body retained=%d error=%v", retained, err)
	}
}

func TestBoardAtomicRenameRejectsMixedCutThenRebinds(t *testing.T) {
	boardContractEnabled(t)
	st, index, spec, _ := boardFixture(t)
	path := filepath.Join(spec.CWD, "body")
	if err := os.WriteFile(path, []byte("before #123"), 0o600); err != nil {
		t.Fatal(err)
	}
	ruling, _, err := st.Post(t.Context(), Record{Drive: "d", Lane: "root", Kind: kinds.Go, Text: "short #123", Refs: Refs{Path: path}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := index.Provision(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	called := false
	err = index.Read(t.Context(), st, spec, BoardSelection{PRs: []string{"123"}}, func(_ *BoardSnapshot, result *BoardResult) error {
		called = true
		if string(result.Rulings.Bodies[ruling.Seq]) != "before #123" {
			t.Fatal("initial body was not captured")
		}
		temporary := path + ".replacement"
		if err := os.WriteFile(temporary, []byte("after #123"), 0o600); err != nil {
			return err
		}
		if err := os.Rename(temporary, path); err != nil {
			return err
		}
		_, _, err := st.Post(t.Context(), Record{Drive: "d", Lane: "root", Kind: kinds.Note, Text: "later canonical commit"}, 0)
		return err
	})
	if !called || err == nil {
		t.Fatalf("mixed cut accepted: callback=%v error=%v", called, err)
	}
	err = index.Read(t.Context(), st, spec, BoardSelection{PRs: []string{"123"}}, func(_ *BoardSnapshot, result *BoardResult) error {
		if string(result.Rulings.Bodies[ruling.Seq]) != "after #123" {
			t.Fatal("replacement did not rebind to current bytes")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBoardMissingPathCreationIsFreshOrVisibleIncomplete(t *testing.T) {
	boardContractEnabled(t)
	st, index, spec, _ := boardFixture(t)
	path := filepath.Join(spec.CWD, "missing-body")
	ruling, _, err := st.Post(t.Context(), Record{Drive: "d", Lane: "root", Kind: kinds.Go, Text: "fallback #123", Refs: Refs{Path: path}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := index.Provision(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("created #123"), 0o600); err != nil {
		t.Fatal(err)
	}
	called := false
	read := func() error {
		return index.Read(t.Context(), st, spec, BoardSelection{PRs: []string{"123"}}, func(_ *BoardSnapshot, result *BoardResult) error {
			called = true
			if string(result.Rulings.Bodies[ruling.Seq]) != "created #123" {
				t.Fatal("missing-path creation returned stale fallback")
			}
			return nil
		})
	}
	err = read()
	if runtime.GOOS == "darwin" {
		if called || err == nil || !strings.Contains(err.Error(), "unnamed parent event") {
			t.Fatalf("ambiguous missing event was not visible incomplete: callback=%v error=%v", called, err)
		}
		if _, err := index.Provision(t.Context(), spec); err != nil {
			t.Fatal(err)
		}
		if err := read(); err != nil {
			t.Fatal(err)
		}
	} else if !called || err != nil {
		t.Fatalf("named missing event did not refresh: callback=%v error=%v", called, err)
	}
}

func TestBoardLostRegistrationNeverReturnsSuccess(t *testing.T) {
	boardContractEnabled(t)
	st, index, spec, _ := boardFixture(t)
	path := filepath.Join(spec.CWD, "body")
	if err := os.WriteFile(path, []byte("current #123"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.Post(t.Context(), Record{Drive: "d", Lane: "root", Kind: kinds.Go, Text: "short #123", Refs: Refs{Path: path}}, 0); err != nil {
		t.Fatal(err)
	}
	id, err := index.Provision(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	invalidateBoardNativeBackend(t, index.scopes[id].watch)
	called := false
	err = index.Read(t.Context(), st, spec, BoardSelection{PRs: []string{"123"}}, func(*BoardSnapshot, *BoardResult) error { called = true; return nil })
	if called || err == nil {
		t.Fatalf("lost native authority accepted: callback=%v error=%v", called, err)
	}
}

func TestBoardNativeRegistrationFailureIsVisible(t *testing.T) {
	boardContractEnabled(t)
	watch, err := newIncidentFileWatch()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = watch.Close() }()
	path := filepath.Join(t.TempDir(), "body")
	if err := os.WriteFile(path, []byte("current"), 0o600); err != nil {
		t.Fatal(err)
	}
	invalidateBoardNativeBackend(t, watch)
	if err := watch.Register(path); err == nil {
		t.Fatal("registration on the disposed native queue succeeded")
	}
}

func TestBoardBodyMovesDeletesNeverRetainStaleBytes(t *testing.T) {
	boardContractEnabled(t)
	st, index, spec, _ := boardFixture(t)
	original, moved := filepath.Join(spec.CWD, "original"), filepath.Join(spec.CWD, "moved")
	if err := os.WriteFile(original, []byte("current #123"), 0o600); err != nil {
		t.Fatal(err)
	}
	ruling, _, err := st.Post(t.Context(), Record{Drive: "d", Lane: "root", Kind: kinds.Go, Text: "fallback #123", Refs: Refs{Path: original}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := index.Provision(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	read := func(want []byte) {
		t.Helper()
		var actual []byte
		run := func() error {
			return index.Read(t.Context(), st, spec, BoardSelection{PRs: []string{"123"}}, func(_ *BoardSnapshot, result *BoardResult) error {
				actual = result.Rulings.Bodies[ruling.Seq]
				return nil
			})
		}
		err := run()
		if err != nil {
			if runtime.GOOS != "darwin" || !strings.Contains(err.Error(), "unnamed parent event") {
				t.Fatal(err)
			}
			if _, err := index.Provision(t.Context(), spec); err != nil {
				t.Fatal(err)
			}
			if err := run(); err != nil {
				t.Fatal(err)
			}
		}
		if string(actual) != string(want) {
			t.Fatalf("successful read retained stale bytes: got=%q want=%q", actual, want)
		}
	}
	if err := os.Rename(original, moved); err != nil {
		t.Fatal(err)
	}
	read(nil)
	if _, err := st.db.ExecContext(t.Context(), "UPDATE records SET refs=json_object('path',?) WHERE seq=?", moved, ruling.Seq); err != nil {
		t.Fatal(err)
	}
	read([]byte("current #123"))
	if err := os.Remove(moved); err != nil {
		t.Fatal(err)
	}
	read(nil)
}
