package store

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

const (
	walWriteLockProbe = "CCI_WAL_WRITE_LOCK_PROBE"
	walWriteLockByte  = 120
)

func TestAWriteTransactionKeepsItsWALWriteLockAcrossBuildReadModel(t *testing.T) {
	if shm := os.Getenv(walWriteLockProbe); shm != "" {
		fmt.Println(walWriteLockHolder(t, shm))
		os.Exit(0)
	}
	st, _ := openStore(t)
	ctx := t.Context()
	selectedPost(t, st, Record{Lane: "root", Kind: kinds.Note, Text: "before the write"})
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	holder := func(when string) {
		t.Helper()
		probe := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+t.Name()+"$") //nolint:gosec // G204: this test binary, probing the lock.
		probe.Env = append(os.Environ(), walWriteLockProbe+"="+filepath.Join(st.home, DBFile)+"-shm")
		out, err := probe.Output()
		if err != nil {
			t.Fatalf("probe the WAL write lock %s: %v", when, err)
		}
		if pid, err := strconv.Atoi(strings.TrimSpace(string(out))); err != nil || pid != os.Getpid() {
			t.Fatalf("the WAL write lock's holder %s = %q; want this process, %d, whose write transaction is open", when, out, os.Getpid())
		}
	}
	holder("before the build")
	model, err := st.BuildReadModel(ctx, func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	holder("after BuildReadModel")
	if err := model.Close(); err != nil {
		t.Fatal(err)
	}
	holder("after the model's Close")
}

func walWriteLockHolder(t *testing.T, shm string) int32 {
	file, err := os.OpenFile(shm, os.O_RDWR, 0) //nolint:gosec // G304: the store's own WAL index, named by the test.
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	lock := syscall.Flock_t{Type: syscall.F_WRLCK, Whence: io.SeekStart, Start: walWriteLockByte, Len: 1}
	if err := syscall.FcntlFlock(file.Fd(), syscall.F_GETLK, &lock); err != nil {
		t.Fatal(err)
	}
	if lock.Type == syscall.F_UNLCK {
		return 0
	}
	return lock.Pid
}
