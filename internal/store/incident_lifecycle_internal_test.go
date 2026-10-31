package store

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestIncidentCloseReleasesEveryNativeDescriptor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "body")
	if err := os.WriteFile(path, []byte("body #42"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, index, _ := incidentPathFixture(t, path)
	descriptors := append(incidentNativeDescriptors(index.watch.backend), int(index.owner.Fd()))
	if len(descriptors) == 0 {
		t.Fatal("fixture opened no native descriptors")
	}
	for _, fd := range descriptors {
		if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); err != nil {
			t.Fatalf("native descriptor%d was not live: %v", fd, err)
		}
	}
	if err := index.Close(); err != nil {
		t.Fatal(err)
	}
	for _, fd := range descriptors {
		if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
			t.Fatalf("native descriptor%d retained afterClose: %v", fd, err)
		}
	}
	if err := index.Close(); err != nil {
		t.Fatalf("repeatedClose=%v", err)
	}
}
