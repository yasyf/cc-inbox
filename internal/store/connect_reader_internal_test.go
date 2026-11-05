package store

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

func postedStore(t *testing.T) (*Store, string, int64) {
	t.Helper()
	home := t.TempDir()
	st, err := Open(context.Background(), home)
	if err != nil {
		t.Fatal(err)
	}
	r, _, err := st.Post(context.Background(), Record{Drive: "d", Lane: "root", Kind: kinds.Note, Text: "committed"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	return st, home, r.Seq
}

func logSize(t *testing.T, home string) int64 {
	t.Helper()
	info, err := os.Stat(filepath.Join(home, DBFile+"-wal"))
	if err != nil {
		t.Fatalf("the writer's log is gone: %v", err)
	}
	return info.Size()
}

func TestAReaderClosingLastLeavesTheLogToTheWriter(t *testing.T) {
	ctx := context.Background()
	st, home, seq := postedStore(t)
	reader, err := Connect(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := reader.MaxSeq(ctx); err != nil || got != seq {
		t.Fatalf("MaxSeq() = %d, %v, want %d", got, err, seq)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	before := logSize(t, home)
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if after := logSize(t, home); after != before || after == 0 {
		t.Fatalf("a reader closing last moved the log from %d to %d bytes: it checkpointed under the exclusive lock", before, after)
	}
}

func TestConnectReadsAStoreWithNoLogOrIndexFiles(t *testing.T) {
	ctx := context.Background()
	st, home, seq := postedStore(t)
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if err := os.Remove(filepath.Join(home, DBFile+suffix)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			t.Fatal(err)
		}
	}
	reader, err := Connect(ctx, home)
	if err != nil {
		t.Fatalf("connect with no -wal or -shm and no writer = %v", err)
	}
	defer func() { _ = reader.Close() }()
	if got, err := reader.MaxSeq(ctx); err != nil || got != seq {
		t.Fatalf("MaxSeq() = %d, %v, want %d", got, err, seq)
	}
}

func TestReadersBehindAHeldWriteReturnAndNeverWrite(t *testing.T) {
	for _, n := range []int{1, 1000} {
		ctx := context.Background()
		st, home, seq := postedStore(t)
		tx, err := st.rw.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		before := logSize(t, home)
		for i := range n {
			readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			reader, err := Connect(readCtx, home)
			if err != nil {
				cancel()
				t.Fatalf("N = %d: reader %d behind a held write = %v", n, i, err)
			}
			got, err := reader.MaxSeq(readCtx)
			cancel()
			if err != nil || got != seq {
				t.Fatalf("N = %d: reader %d MaxSeq() = %d, %v, want %d", n, i, got, err, seq)
			}
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if after := logSize(t, home); after != before {
			t.Fatalf("N = %d: readers moved the log from %d to %d bytes", n, before, after)
		}
		_ = tx.Rollback()
		_ = st.Close()
	}
}
