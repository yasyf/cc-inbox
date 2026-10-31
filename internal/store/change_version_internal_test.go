package store

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

func TestChangeVersionIndependentReadOnlyConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	st, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	st.db.SetMaxOpenConns(1)
	observer, err := st.OpenChangeVersion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = observer.Close() }()
	before, err := observer.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if again, err := observer.Read(ctx); err != nil || again != before {
		t.Fatalf("unchanged version = %d, %v; want %d", again, err, before)
	}
	if _, _, err := st.Post(ctx, Record{Drive: "d", Lane: "root", Kind: kinds.Note, Text: "committed"}, 0); err != nil {
		t.Fatal(err)
	}
	after, err := observer.Read(ctx)
	if err != nil || after == before {
		t.Fatalf("committed version = %d, %v; previous %d", after, err, before)
	}
	if _, err := observer.connection.ExecContext(ctx, "INSERT INTO cursors(name,drive,seq) VALUES('forbidden','d',1)"); err == nil {
		t.Fatal("read-only observer accepted a write")
	}
	var cursors int
	if err := st.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cursors").Scan(&cursors); err != nil || cursors != 0 {
		t.Fatalf("observer wrote cursors: %d, %v", cursors, err)
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, err := observer.Read(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read = %v", err)
	}
}

func TestChangeVersionCommitDuringReadSnapshot(t *testing.T) {
	ctx := t.Context()
	st, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	for i := range 2 {
		if _, _, err := st.Post(ctx, Record{Drive: "d", Lane: "root", Kind: kinds.Note, Text: fmt.Sprint(i)}, 0); err != nil {
			t.Fatal(err)
		}
	}
	observer, err := st.OpenChangeVersion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = observer.Close() }()
	before, err := observer.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := st.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	rows, err := reader.QueryContext(ctx, "SELECT seq FROM records ORDER BY seq")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		t.Fatalf("read snapshot is empty: %v", rows.Err())
	}
	if _, _, err := st.Post(ctx, Record{Drive: "d", Lane: "root", Kind: kinds.Hold, Text: "late committed hold"}, 0); err != nil {
		t.Fatal(err)
	}
	seen := 1
	for rows.Next() {
		seen++
	}
	if err := rows.Err(); err != nil || seen != 2 {
		t.Fatalf("snapshot rows = %d, %v; want 2", seen, err)
	}
	if after, err := observer.Read(ctx); err != nil || after == before {
		t.Fatalf("missed concurrent commit: before=%d after=%d err=%v", before, after, err)
	}
}

func TestChangeVersionCloseReleasesPinnedConnection(t *testing.T) {
	st, err := Open(t.Context(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	observer, err := st.OpenChangeVersion(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := observer.Close(); err != nil {
		t.Fatal(err)
	}
	if n := observer.database.Stats().OpenConnections; n != 0 {
		t.Fatalf("observer retained %d connections", n)
	}
}

func TestChangeVersionReservedHomePreservesReadOnlyAuthority(t *testing.T) {
	for _, name := range []string{"store#fragment", "store?query", "store%23encoded"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			decoded, err := url.PathUnescape(name)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(filepath.Join(root, decoded), 0o700); err != nil {
				t.Fatal(err)
			}
			st, err := Open(t.Context(), filepath.Join(root, name))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = st.Close() }()
			observer, err := st.OpenChangeVersion(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = observer.Close() }()
			var original, observed string
			if err := st.db.QueryRowContext(t.Context(), "SELECT file FROM pragma_database_list WHERE name='main'").Scan(&original); err != nil {
				t.Fatal(err)
			}
			if err := observer.connection.QueryRowContext(t.Context(), "SELECT file FROM pragma_database_list WHERE name='main'").Scan(&observed); err != nil {
				t.Fatal(err)
			}
			if original != observed {
				t.Fatalf("observer source %q differs from Store source %q", observed, original)
			}
			if _, err := observer.connection.ExecContext(t.Context(), "INSERT INTO cursors(name,drive,seq) VALUES('forbidden','d',1)"); err == nil {
				t.Fatal("reserved-character source opened writable")
			}
		})
	}
}
