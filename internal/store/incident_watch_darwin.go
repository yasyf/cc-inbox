package store

import (
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

type incidentKqueue struct {
	fd int
	files map[incidentWatchKey]int
	keys map[int]incidentWatchKey
}

func newIncidentWatchBackend() (incidentWatchBackend, error) {
	fd, err := unix.Kqueue()
	if err != nil {
		return nil, err
	}
	unix.CloseOnExec(fd)
	return &incidentKqueue{fd: fd, files: map[incidentWatchKey]int{}, keys: map[int]incidentWatchKey{}}, nil
}

func (q *incidentKqueue) Add(key incidentWatchKey) error {
	flags := unix.O_EVTONLY | unix.O_CLOEXEC
	if !key.Follow {
		flags |= unix.O_SYMLINK
	}
	fd, err := unix.Open(key.Path, flags, 0)
	if err != nil {
		return fmt.Errorf("watch incident body route %s: %w", key.Path, err)
	}
	change := unix.Kevent_t{Ident: uint64(fd), Filter: unix.EVFILT_VNODE, Flags: unix.EV_ADD | unix.EV_CLEAR, Fflags: unix.NOTE_WRITE | unix.NOTE_EXTEND | unix.NOTE_ATTRIB | unix.NOTE_LINK | unix.NOTE_RENAME | unix.NOTE_DELETE | unix.NOTE_REVOKE}
	if _, err := unix.Kevent(q.fd, []unix.Kevent_t{change}, nil, nil); err != nil {
		_ = unix.Close(fd)
		return err
	}
	q.files[key], q.keys[fd] = fd, key
	return nil
}

func (q *incidentKqueue) Remove(key incidentWatchKey) error {
	fd, ok := q.files[key]
	if !ok {
		return nil
	}
	delete(q.files, key)
	delete(q.keys, fd)
	return unix.Close(fd)
}

func (q *incidentKqueue) Drain() ([]incidentWatchEvent, error) {
	if len(q.keys) == 0 {
		return nil, nil
	}
	var events [64]unix.Kevent_t
	var out []incidentWatchEvent
	for {
		n, err := unix.Kevent(q.fd, nil, events[:], &unix.Timespec{})
		if errors.Is(err, unix.EINTR) {
			return nil, fmt.Errorf("incident watch drain interrupted: %w", err)
		}
		if err != nil {
			return nil, err
		}
		for _, event := range events[:n] {
			if event.Flags&unix.EV_ERROR != 0 || event.Fflags&unix.NOTE_REVOKE != 0 {
				return nil, fmt.Errorf("incident watch authority lost: flags=%d data=%d", event.Fflags, event.Data)
			}
			key, ok := q.keys[int(event.Ident)]
			if !ok {
				return nil, fmt.Errorf("unknown incident watch descriptor %d", event.Ident)
			}
			out = append(out, incidentWatchEvent{Key: key, Invalidated: event.Fflags&(unix.NOTE_RENAME|unix.NOTE_DELETE|unix.NOTE_ATTRIB) != 0})
		}
		if n < len(events) {
			return out, nil
		}
	}
}

func (q *incidentKqueue) Close() error {
	var result error
	for _, fd := range q.files {
		result = errors.Join(result, unix.Close(fd))
	}
	return errors.Join(result, unix.Close(q.fd))
}
