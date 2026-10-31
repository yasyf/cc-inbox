package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/sys/unix"
)

type incidentInotify struct {
	fd int
	files map[incidentWatchKey]int
	keys map[int]map[incidentWatchKey]bool
	removed map[int]bool
	dead map[int]bool
}

func newIncidentWatchBackend() (incidentWatchBackend, error) {
	fd, err := unix.InotifyInit1(unix.IN_NONBLOCK | unix.IN_CLOEXEC)
	if err != nil {
		return nil, err
	}
	return &incidentInotify{fd: fd, files: map[incidentWatchKey]int{}, keys: map[int]map[incidentWatchKey]bool{}, removed: map[int]bool{}, dead: map[int]bool{}}, nil
}

func (q *incidentInotify) Add(key incidentWatchKey) error {
	mask := uint32(unix.IN_MODIFY | unix.IN_ATTRIB | unix.IN_CLOSE_WRITE | unix.IN_CREATE | unix.IN_DELETE | unix.IN_MOVED_FROM | unix.IN_MOVED_TO | unix.IN_MOVE_SELF | unix.IN_DELETE_SELF | unix.IN_MASK_ADD)
	if !key.Follow {
		mask |= unix.IN_DONT_FOLLOW
	}
	wd, err := unix.InotifyAddWatch(q.fd, key.Path, mask)
	if err != nil {
		return fmt.Errorf("watch incident body route %s: %w", key.Path, err)
	}
	if q.keys[wd] == nil {
		q.keys[wd] = map[incidentWatchKey]bool{}
	}
	q.files[key], q.keys[wd][key] = wd, true
	delete(q.removed, wd)
	delete(q.dead, wd)
	return nil
}

func (q *incidentInotify) Remove(key incidentWatchKey) error {
	wd, ok := q.files[key]
	if !ok {
		return nil
	}
	delete(q.files, key)
	delete(q.keys[wd], key)
	if len(q.keys[wd]) != 0 {
		return nil
	}
	if _, err := unix.InotifyRmWatch(q.fd, uint32(wd)); err != nil && !(q.dead[wd] && errors.Is(err, unix.EINVAL)) {
		return err
	}
	delete(q.keys, wd)
	delete(q.dead, wd)
	q.removed[wd] = true
	return nil
}

func (q *incidentInotify) Drain() ([]incidentWatchEvent, error) {
	var buffer [16 * 1024]byte
	var out []incidentWatchEvent
	for {
		n, err := unix.Read(q.fd, buffer[:])
		if errors.Is(err, unix.EAGAIN) {
			return out, nil
		}
		if err != nil {
			return nil, fmt.Errorf("drain incident watch: %w", err)
		}
		if n == 0 {
			return nil, errors.New("incident watch closed during drain")
		}
		for offset := 0; offset < n; {
			if n-offset < unix.SizeofInotifyEvent {
				return nil, errors.New("incomplete incident watch event")
			}
			data := buffer[offset:n]
			wd := int(int32(binary.NativeEndian.Uint32(data)))
			mask := binary.NativeEndian.Uint32(data[4:])
			length := int(binary.NativeEndian.Uint32(data[12:]))
			if length > n-offset-unix.SizeofInotifyEvent {
				return nil, errors.New("incomplete incident watch event name")
			}
			name := strings.TrimRight(string(data[unix.SizeofInotifyEvent:unix.SizeofInotifyEvent+length]), "\x00")
			offset += unix.SizeofInotifyEvent + length
			if mask&(unix.IN_Q_OVERFLOW|unix.IN_UNMOUNT) != 0 {
				return nil, fmt.Errorf("incident watch authority lost: mask=%d", mask)
			}
			if q.removed[wd] {
				if mask&unix.IN_IGNORED != 0 {
					delete(q.removed, wd)
				}
				continue
			}
			keys := q.keys[wd]
			if keys == nil {
				return nil, fmt.Errorf("unknown incident watch descriptor %d", wd)
			}
			if mask&unix.IN_IGNORED != 0 {
				q.dead[wd] = true
			}
			for key := range keys {
				out = append(out, incidentWatchEvent{Key: key, Name: name, Invalidated: mask&(unix.IN_MOVE_SELF|unix.IN_DELETE_SELF|unix.IN_IGNORED) != 0 || name == "" && mask&unix.IN_ATTRIB != 0})
			}
		}
	}
}

func (q *incidentInotify) Close() error {
	return unix.Close(q.fd)
}
