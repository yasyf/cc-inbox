package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
)

type incidentIdentity struct {
	Device uint64
	Inode uint64
	Size int64
	Modified int64
	Changed int64
	Mode os.FileMode
}

type incidentBody struct {
	Identity string
	Words map[string]bool
	Valid bool
}

func incidentFileIdentity(info os.FileInfo) string {
	stat := info.Sys().(*syscall.Stat_t)
	value := incidentIdentity{Device: uint64(stat.Dev), Inode: stat.Ino, Size: info.Size(), Modified: info.ModTime().UnixNano(), Changed: incidentChangeTime(stat), Mode: info.Mode()}
	data, _ := json.Marshal(value)
	return string(data)
}

type incidentCountingReader struct {
	reader io.Reader
	stats *IncidentIndexStats
}

func (r incidentCountingReader) Read(buffer []byte) (int, error) {
	n, err := r.reader.Read(buffer)
	r.stats.BodyBytes += uint64(n)
	return n, err
}

func readIncidentBody(ctx context.Context, path string, stats *IncidentIndexStats) (incidentBody, error) {
	stats.BodyStats++
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
		return incidentBody{Identity: "missing"}, nil
	}
	if err != nil {
		return incidentBody{}, fmt.Errorf("stat incident body %s: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return incidentBody{Identity: incidentFileIdentity(info)}, nil
	}
	stats.BodyOpens++
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return incidentBody{}, fmt.Errorf("open incident body %s: %w", path, err)
	}
	defer func() { _ = file.Close() }()
	stats.BodyStats++
	info, err = file.Stat()
	if err != nil {
		return incidentBody{}, fmt.Errorf("stat incident body %s: %w", path, err)
	}
	identity := incidentFileIdentity(info)
	if !info.Mode().IsRegular() {
		return incidentBody{Identity: identity}, nil
	}
	words, valid, err := incidentWords(ctx, incidentCountingReader{reader: file, stats: stats})
	if err != nil {
		return incidentBody{}, fmt.Errorf("read incident body %s: %w", path, err)
	}
	stats.BodyStats++
	after, err := file.Stat()
	if err != nil {
		return incidentBody{}, fmt.Errorf("stat incident body %s after read: %w", path, err)
	}
	if identity != incidentFileIdentity(after) {
		return incidentBody{}, fmt.Errorf("incident body %s changed during read", path)
	}
	return incidentBody{Identity: identity, Words: words, Valid: valid}, nil
}
