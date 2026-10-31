package store

import (
	"context"
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

type incidentKqueue struct {
	fd     int
	files  map[incidentWatchKey]int
	keys   map[int]incidentWatchKey
	info   map[incidentWatchKey]incidentWatchInfo
	events []unix.Kevent_t
}

func newIncidentWatchBackend() (incidentWatchBackend, error) {
	fd, err := unix.Kqueue()
	if err != nil {
		return nil, err
	}
	unix.CloseOnExec(fd)
	return &incidentKqueue{fd: fd, files: map[incidentWatchKey]int{}, keys: map[int]incidentWatchKey{}, info: map[incidentWatchKey]incidentWatchInfo{}}, nil
}

func (q *incidentKqueue) Add(key incidentWatchKey) error {
	return q.addWithRegistration(key, func(change unix.Kevent_t) error {
		_, err := unix.Kevent(q.fd, []unix.Kevent_t{change}, nil, nil)
		return err
	})
}

func (q *incidentKqueue) addWithRegistration(key incidentWatchKey, register func(unix.Kevent_t) error) error {
	flags := unix.O_EVTONLY | unix.O_CLOEXEC
	if !key.Follow {
		flags |= unix.O_SYMLINK
	}
	fd, err := unix.Open(key.Path, flags, 0)
	if err != nil {
		return fmt.Errorf("watch incident body route %s: %w", key.Path, err)
	}
	var metadata unix.Stat_t
	if err := unix.Fstat(fd, &metadata); err != nil {
		_ = unix.Close(fd)
		return err
	}
	info := incidentWatchInfo{Device: uint64(metadata.Dev), Inode: metadata.Ino, Kind: uint32(metadata.Mode) & unix.S_IFMT, Directory: metadata.Mode&unix.S_IFMT == unix.S_IFDIR, Symlink: metadata.Mode&unix.S_IFMT == unix.S_IFLNK}
	change := unix.Kevent_t{Ident: uint64(fd), Filter: unix.EVFILT_VNODE, Flags: unix.EV_ADD | unix.EV_CLEAR, Fflags: unix.NOTE_WRITE | unix.NOTE_EXTEND | unix.NOTE_ATTRIB | unix.NOTE_LINK | unix.NOTE_RENAME | unix.NOTE_DELETE | unix.NOTE_REVOKE}
	if err := register(change); err != nil {
		_ = unix.Close(fd)
		return err
	}
	q.files[key], q.keys[fd] = fd, key
	info.Changed = incidentWatchPathChanged(key, info)
	q.info[key] = info
	q.resizeEvents()
	return nil
}

func (q *incidentKqueue) Info(key incidentWatchKey) incidentWatchInfo { return q.info[key] }

func (q *incidentKqueue) Remove(key incidentWatchKey) error {
	fd, ok := q.files[key]
	if !ok {
		return nil
	}
	delete(q.files, key)
	delete(q.info, key)
	delete(q.keys, fd)
	q.resizeEvents()
	return unix.Close(fd)
}

func (q *incidentKqueue) resizeEvents() {
	n := len(q.keys)
	if n == 0 {
		q.events = nil
		return
	}
	if cap(q.events) < n || n < cap(q.events)/4 {
		q.events = make([]unix.Kevent_t, n*2)
	}
}

func (q *incidentKqueue) Drain(ctx context.Context) ([]incidentWatchEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(q.keys) == 0 {
		return nil, nil
	}
	events := q.events[:len(q.keys)]
	n, err := unix.Kevent(q.fd, nil, events, &unix.Timespec{})
	if errors.Is(err, unix.EINTR) {
		return nil, fmt.Errorf("incident watch drain interrupted: %w", err)
	}
	if err != nil {
		return nil, err
	}
	out := make([]incidentWatchEvent, 0, n)
	for _, event := range events[:n] {
		if event.Flags&unix.EV_ERROR != 0 || event.Fflags&unix.NOTE_REVOKE != 0 {
			return nil, fmt.Errorf("incident watch authority lost: flags=%d data=%d", event.Fflags, event.Data)
		}
		key, ok := q.keys[int(event.Ident)]
		if !ok {
			return nil, fmt.Errorf("unknown incident watch descriptor %d", event.Ident)
		}
		out = append(out, incidentWatchEvent{Keys: map[incidentWatchKey]bool{key: true}, Invalidated: event.Fflags&(unix.NOTE_RENAME|unix.NOTE_DELETE|unix.NOTE_ATTRIB) != 0 || event.Fflags&unix.NOTE_LINK != 0 && !q.info[key].Directory})
	}
	return out, nil
}

func (q *incidentKqueue) Close() error {
	var result error
	for _, fd := range q.files {
		result = errors.Join(result, unix.Close(fd))
	}
	return errors.Join(result, unix.Close(q.fd))
}
