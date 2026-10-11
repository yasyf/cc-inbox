package mesh

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/yasyf/synckit/syncservice"
)

const keptRevisions = 512

type revision struct {
	Rev      uint64 `json:"rev"`
	Seq      int64  `json:"seq"`
	Presence string `json:"presence"`
}

type revisionLog struct {
	path    string
	entries []revision
}

func openRevisions(home string) (*revisionLog, error) {
	l := &revisionLog{path: filepath.Join(home, "mesh", "revisions.json")}
	raw, err := os.ReadFile(l.path)
	if errors.Is(err, os.ErrNotExist) {
		return l, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", l.path, err)
	}
	if err := json.Unmarshal(raw, &l.entries); err != nil {
		return nil, fmt.Errorf("decode %s: %w", l.path, err)
	}
	return l, nil
}

func (l *revisionLog) head() (revision, bool) {
	if len(l.entries) == 0 {
		return revision{}, false
	}
	return l.entries[len(l.entries)-1], true
}

func (l *revisionLog) at(rev uint64) (revision, bool) {
	i := slices.IndexFunc(l.entries, func(r revision) bool { return r.Rev == rev })
	if i < 0 {
		return revision{}, false
	}
	return l.entries[i], true
}

func (l *revisionLog) acked(rev syncservice.Revision) (int64, error) {
	value, err := rev.Uint64()
	if err != nil {
		return 0, err
	}
	entry, _ := l.at(value)
	return entry.Seq, nil
}

func AckedSeq(home string, rev syncservice.Revision) (int64, error) {
	log, err := openRevisions(home)
	if err != nil {
		return 0, err
	}
	return log.acked(rev)
}

func (l *revisionLog) advance(seq int64, presence string, now time.Time) (revision, error) {
	head, ok := l.head()
	if ok && head.Seq == seq && head.Presence == presence {
		return head, nil
	}
	next := revision{Rev: max(head.Rev+1, uint64(now.UnixMilli())), Seq: seq, Presence: presence}
	l.entries = append(l.entries, next)
	if len(l.entries) > keptRevisions {
		l.entries = l.entries[len(l.entries)-keptRevisions:]
	}
	raw, err := json.Marshal(l.entries)
	if err != nil {
		return revision{}, err
	}
	if err := os.MkdirAll(filepath.Dir(l.path), 0o700); err != nil {
		return revision{}, err
	}
	tmp := l.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return revision{}, fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, l.path); err != nil {
		return revision{}, fmt.Errorf("install %s: %w", l.path, err)
	}
	return next, nil
}
