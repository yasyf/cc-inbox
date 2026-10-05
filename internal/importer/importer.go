package importer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/yasyf/cc-inbox/internal/store"
)

const ParserVersion = 3

type Result struct {
	Path     string `json:"path"`
	Entries  int    `json:"entries"`
	Inserted int    `json:"inserted"`
	Reparsed int    `json:"reparsed"`
	Offset   int64  `json:"offset"`
}

func Import(ctx context.Context, st *store.Store, path, drive, lane string) (Result, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return Result{}, fmt.Errorf("resolve %s: %w", path, err)
	}
	src, known, err := st.Source(ctx, abs)
	if err != nil {
		return Result{}, err
	}
	if !known {
		src = store.Source{Path: abs, Drive: drive, Lane: lane}
	}
	if drive != "" {
		src.Drive = drive
	}
	if lane != "" {
		src.Lane = lane
	}
	if src.Lane == "" {
		src.Lane = DefaultLane(abs)
	}
	if src.Drive == "" {
		return Result{}, store.ErrNoDrive
	}
	f, err := os.Open(filepath.Clean(abs))
	if err != nil {
		return Result{}, fmt.Errorf("open %s: %w", abs, err)
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return Result{}, fmt.Errorf("stat %s: %w", abs, err)
	}
	inode := info.Sys().(*syscall.Stat_t).Ino
	if inode != src.Inode || info.Size() < src.Offset {
		src.Offset = 0
	}
	src.Inode = inode
	reparsed, split := 0, 0
	stale := src.Parser != ParserVersion
	if stale && src.Offset > 0 {
		prefix := make([]byte, src.Offset)
		if _, err := io.ReadFull(f, prefix); err != nil {
			return Result{}, fmt.Errorf("read %s: %w", abs, err)
		}
		_, items, err := ingestions(st, src, abs, prefix, info.ModTime())
		if err != nil {
			return Result{}, err
		}
		if reparsed, split, err = st.Reparse(ctx, items, false); err != nil {
			return Result{}, err
		}
	}
	src.Parser = ParserVersion
	if _, err := f.Seek(src.Offset, io.SeekStart); err != nil {
		return Result{}, fmt.Errorf("seek %s: %w", abs, err)
	}
	chunk, err := io.ReadAll(f)
	if err != nil {
		return Result{}, fmt.Errorf("read %s: %w", abs, err)
	}
	chunk = chunk[:bytes.LastIndexByte(chunk, '\n')+1]
	entries, items, err := ingestions(st, src, abs, chunk, info.ModTime())
	if err != nil {
		return Result{}, err
	}
	var inserted int
	if stale {
		var updated int
		updated, inserted, err = st.Reparse(ctx, items, true)
		reparsed += updated
	} else {
		inserted, err = st.IngestAll(ctx, items)
	}
	if err != nil {
		return Result{}, err
	}
	src.Offset += int64(len(chunk))
	if err := st.SaveSource(ctx, src); err != nil {
		return Result{}, err
	}
	return Result{Path: abs, Entries: len(entries), Inserted: split + inserted, Reparsed: reparsed, Offset: src.Offset}, nil
}

func ingestions(st *store.Store, src store.Source, abs string, chunk []byte, end time.Time) ([]Entry, []store.Ingestion, error) {
	entries, err := Parse(bytes.NewReader(chunk), src.Lane)
	if err != nil {
		return nil, nil, fmt.Errorf("parse %s: %w", abs, err)
	}
	times := Date(entries, end)
	items := make([]store.Ingestion, 0, len(entries))
	for i, e := range entries {
		r := store.Record{
			Drive:  src.Drive,
			Lane:   e.Lane,
			Topic:  e.Topic,
			Kind:   e.Kind,
			At:     times[i].UTC(),
			Text:   e.Text,
			To:     e.To,
			Refs:   store.Refs{PRs: e.PRs, Builds: e.Builds, Stacks: e.Stacks},
			Source: "import:" + abs,
		}
		if r.Text, r.Refs.Path, err = st.Fit(e.Text); err != nil {
			return nil, nil, err
		}
		items = append(items, store.Ingestion{Record: r, LineHash: lineHash(src.Drive, e.Start), Line: e.Start})
	}
	return entries, items, nil
}

func Refresh(ctx context.Context, st *store.Store) ([]Result, error) {
	sources, err := st.Sources(ctx)
	if err != nil {
		return nil, err
	}
	known := map[string]bool{}
	for _, src := range sources {
		known[src.Path] = true
	}
	var out []Result
	for _, src := range sources {
		archives, err := filepath.Glob(src.Path + ".archive/*.md")
		if err != nil {
			return nil, fmt.Errorf("list archives of %s: %w", src.Path, err)
		}
		for _, archive := range archives {
			if known[archive] {
				continue
			}
			known[archive] = true
			res, err := Import(ctx, st, archive, src.Drive, src.Lane)
			if err != nil {
				return nil, err
			}
			out = append(out, res)
		}
		info, err := os.Stat(src.Path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("stat %s: %w", src.Path, err)
		}
		if info.Size() == src.Offset && info.Sys().(*syscall.Stat_t).Ino == src.Inode && src.Parser == ParserVersion {
			continue
		}
		res, err := Import(ctx, st, src.Path, "", "")
		if err != nil {
			return nil, err
		}
		out = append(out, res)
	}
	return out, nil
}

func DefaultLane(path string) string {
	if dir := filepath.Base(filepath.Dir(path)); strings.HasSuffix(dir, ".archive") {
		path = strings.TrimSuffix(dir, ".archive")
	}
	base := filepath.Base(path)
	return strings.TrimSuffix(base, filepath.Ext(base))
}

func lineHash(drive, line string) string {
	sum := sha256.Sum256([]byte(drive + "\x00" + strings.TrimSpace(line)))
	return hex.EncodeToString(sum[:])
}
