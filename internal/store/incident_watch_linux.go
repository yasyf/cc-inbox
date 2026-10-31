package store

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/sys/unix"
)

type incidentInotify struct {
	fd            int
	files         map[incidentWatchKey]int
	keys          map[int]map[incidentWatchKey]bool
	info          map[incidentWatchKey]incidentWatchInfo
	removed       map[int]bool
	dead          map[int]bool
	lastCutBytes  int
	lastReadBytes int
	lastReads     int
}

func newIncidentWatchBackend() (incidentWatchBackend, error) {
	fd, err := unix.InotifyInit1(unix.IN_NONBLOCK | unix.IN_CLOEXEC)
	if err != nil {
		return nil, err
	}
	return &incidentInotify{fd: fd, files: map[incidentWatchKey]int{}, keys: map[int]map[incidentWatchKey]bool{}, removed: map[int]bool{}, dead: map[int]bool{}, info: map[incidentWatchKey]incidentWatchInfo{}}, nil
}

func (q *incidentInotify) Add(key incidentWatchKey) error {
	mask := uint32(unix.IN_MODIFY | unix.IN_ATTRIB | unix.IN_CLOSE_WRITE | unix.IN_CREATE | unix.IN_DELETE | unix.IN_MOVED_FROM | unix.IN_MOVED_TO | unix.IN_MOVE_SELF | unix.IN_DELETE_SELF | unix.IN_MASK_ADD)
	flags := unix.O_PATH | unix.O_CLOEXEC
	if !key.Follow {
		flags |= unix.O_NOFOLLOW
	}
	pathFD, err := unix.Open(key.Path, flags, 0)
	if err != nil {
		return fmt.Errorf("watch incident body route %s: %w", key.Path, err)
	}
	defer func() { _ = unix.Close(pathFD) }()
	var metadata unix.Stat_t
	if err := unix.Fstat(pathFD, &metadata); err != nil {
		return err
	}
	info := incidentWatchInfo{Directory: metadata.Mode&unix.S_IFMT == unix.S_IFDIR, Symlink: metadata.Mode&unix.S_IFMT == unix.S_IFLNK}
	wd, err := unix.InotifyAddWatch(q.fd, fmt.Sprintf("/proc/self/fd/%d", pathFD), mask)
	if err != nil {
		return fmt.Errorf("watch incident body route %s: %w", key.Path, err)
	}
	if q.removed[wd] || q.dead[wd] {
		return fmt.Errorf("incident watch descriptor %d reused before retirement completed", wd)
	}
	if q.keys[wd] == nil {
		q.keys[wd] = map[incidentWatchKey]bool{}
	}
	q.files[key], q.keys[wd][key] = wd, true
	q.info[key] = info
	delete(q.removed, wd)
	delete(q.dead, wd)
	return nil
}

func (q *incidentInotify) Info(key incidentWatchKey) incidentWatchInfo { return q.info[key] }

func (q *incidentInotify) Remove(key incidentWatchKey) error {
	wd, ok := q.files[key]
	if !ok {
		return nil
	}
	delete(q.files, key)
	delete(q.info, key)
	delete(q.keys[wd], key)
	if len(q.keys[wd]) != 0 {
		return nil
	}
	if _, err := unix.InotifyRmWatch(q.fd, uint32(wd)); err != nil && !errors.Is(err, unix.EINVAL) { //nolint:gosec // G115: wd is the nonnegative signed32 descriptor returned by InotifyAddWatch.
		return err
	}
	ignoredRead := q.dead[wd]
	delete(q.keys, wd)
	delete(q.dead, wd)
	if !ignoredRead {
		q.removed[wd] = true
	}
	return nil
}

func (q *incidentInotify) Drain(ctx context.Context) ([]incidentWatchEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	remaining, err := unix.IoctlGetInt(q.fd, unix.TIOCINQ)
	if err != nil {
		return nil, err
	}
	if remaining < 0 {
		return nil, errors.New("negative incident watch entry prefix")
	}
	q.lastCutBytes, q.lastReadBytes, q.lastReads = remaining, 0, 0
	var buffer [16 * 1024]byte
	var out []incidentWatchEvent
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		n, err := unix.Read(q.fd, buffer[:min(len(buffer), remaining)])
		if err != nil {
			return out, fmt.Errorf("drain incident watch entry prefix: %w", err)
		}
		if n == 0 {
			return out, errors.New("incident watch closed during drain")
		}
		remaining -= n
		q.lastReadBytes += n
		q.lastReads++
		for offset := 0; offset < n; {
			if n-offset < unix.SizeofInotifyEvent {
				return nil, errors.New("incomplete incident watch event")
			}
			data := buffer[offset:n]
			wd := int(int32(binary.NativeEndian.Uint32(data))) //nolint:gosec // G115: the kernel wire field is signed32; negative one identifies queue overflow.
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
			out = append(out, incidentWatchEvent{Keys: keys, Name: name, Invalidated: mask&(unix.IN_MOVE_SELF|unix.IN_DELETE_SELF|unix.IN_IGNORED) != 0 || name == "" && mask&unix.IN_ATTRIB != 0})
		}
	}
	return out, nil
}

func (q *incidentInotify) Close() error {
	return unix.Close(q.fd)
}
