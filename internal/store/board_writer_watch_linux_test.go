package store

import (
	"encoding/binary"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func invalidateBoardNativeBackend(t *testing.T, watch *incidentFileWatch) {
	t.Helper()
	queue := watch.backend.(*incidentInotify)
	if err := queue.Close(); err != nil {
		t.Fatal(err)
	}
	queue.fd = -1
	queue.files = map[incidentWatchKey]int{}
}

func TestBoardLinuxOverflowWireNeverReturnsSuccess(t *testing.T) {
	boardContractEnabled(t)
	st, index, spec, _ := boardFixture(t)
	id, err := index.Provision(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.scopes[id].watch.Close(); err != nil {
		t.Fatal(err)
	}
	var pipes [2]int
	if err := unix.Pipe2(pipes[:], unix.O_NONBLOCK|unix.O_CLOEXEC); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(pipes[1]) }()
	queue := &incidentInotify{fd: pipes[0]}
	index.scopes[id].watch = &incidentFileWatch{backend: queue, nodes: map[incidentWatchKey]*incidentWatchNode{}, paths: map[string]bool{}, routes: map[string]map[incidentWatchKey]map[string]bool{}, rebind: map[string]bool{}}
	wire := make([]byte, unix.SizeofInotifyEvent)
	binary.NativeEndian.PutUint32(wire, ^uint32(0))
	binary.NativeEndian.PutUint32(wire[4:], unix.IN_Q_OVERFLOW)
	if count, err := unix.Write(pipes[1], wire); err != nil || count != len(wire) {
		t.Fatalf("overflow wire write=%d error=%v", count, err)
	}
	called := false
	err = index.Read(t.Context(), st, spec, BoardSelection{}, func(*BoardSnapshot, *BoardResult) error { called = true; return nil })
	if called || err == nil || !strings.Contains(err.Error(), "authority lost") {
		t.Fatalf("overflow decoder returned success: callback=%v error=%v", called, err)
	}
	if queue.lastCutBytes != len(wire) || queue.lastReadBytes != len(wire) || queue.lastReads != 1 {
		t.Fatalf("real overflow wire was not consumed: cut=%d read=%d calls=%d", queue.lastCutBytes, queue.lastReadBytes, queue.lastReads)
	}
}
