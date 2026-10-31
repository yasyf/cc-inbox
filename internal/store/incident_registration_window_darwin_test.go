package store

import "golang.org/x/sys/unix"

func addIncidentWatchAtRegistration(backend incidentWatchBackend, key incidentWatchKey, change func() error) error {
	native := backend.(*incidentKqueue)
	return native.addWithRegistration(key, func(event unix.Kevent_t) error {
		if err := change(); err != nil {
			return err
		}
		_, err := unix.Kevent(native.fd, []unix.Kevent_t{event}, nil, nil)
		return err
	})
}
