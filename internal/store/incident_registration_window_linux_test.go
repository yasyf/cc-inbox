package store

import "golang.org/x/sys/unix"

func addIncidentWatchAtRegistration(backend incidentWatchBackend, key incidentWatchKey, change func() error) error {
	native := backend.(*incidentInotify)
	return native.addWithRegistration(key, func(fd int, path string, mask uint32) (int, error) {
		if err := change(); err != nil {
			return 0, err
		}
		return unix.InotifyAddWatch(fd, path, mask)
	})
}
