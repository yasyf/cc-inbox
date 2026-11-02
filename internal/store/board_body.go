package store

import (
	"io"
	"os"
	"syscall"
)

func readBoardBody(path string, regular bool) ([]byte, bool, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0) //nolint:gosec // G304: record body references intentionally support arbitrary paths; nonblocking open prevents a replacement FIFO from blocking reconciliation.
	if err != nil {
		return nil, regular, err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return nil, regular, err
	}
	regular = regular && info.Mode().IsRegular()
	if !info.Mode().IsRegular() && !info.IsDir() {
		return nil, false, &os.PathError{Op: "read", Path: path, Err: syscall.EINVAL}
	}
	body, err := io.ReadAll(file)
	return body, regular, err
}
