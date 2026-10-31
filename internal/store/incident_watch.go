package store

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

type incidentWatchKey struct {
	Path string
	Follow bool
}

type incidentWatchEvent struct {
	Key incidentWatchKey
	Name string
	Invalidated bool
}

type incidentWatchBackend interface {
	Add(incidentWatchKey) error
	Remove(incidentWatchKey) error
	Drain() ([]incidentWatchEvent, error)
	Close() error
}

type incidentWatchNode struct {
	Directory bool
	Leaves map[string]bool
	Missing map[string]map[string]bool
}

type incidentFileWatch struct {
	backend incidentWatchBackend
	nodes map[incidentWatchKey]*incidentWatchNode
	paths map[string]bool
	routes map[string]map[incidentWatchKey]map[string]bool
	stats uint64
}

func newIncidentFileWatch() (*incidentFileWatch, error) {
	backend, err := newIncidentWatchBackend()
	if err != nil {
		return nil, err
	}
	return &incidentFileWatch{backend: backend, nodes: map[incidentWatchKey]*incidentWatchNode{}, paths: map[string]bool{}, routes: map[string]map[incidentWatchKey]map[string]bool{}}, nil
}

func (w *incidentFileWatch) addNode(key incidentWatchKey, directory bool, leaf string) error {
	node := w.nodes[key]
	if node == nil {
		if err := w.backend.Add(key); err != nil {
			return err
		}
		node = &incidentWatchNode{Directory: directory, Leaves: map[string]bool{}, Missing: map[string]map[string]bool{}}
		w.nodes[key] = node
	}
	node.Leaves[leaf] = true
	if w.routes[leaf][key] == nil {
		w.routes[leaf][key] = map[string]bool{}
	}
	return nil
}

func (w *incidentFileWatch) Register(path string) error {
	if w.paths[path] {
		return nil
	}
	w.routes[path] = map[incidentWatchKey]map[string]bool{}
	if err := w.registerRoute(filepath.Clean(path), path, map[string]bool{}); err != nil {
		return err
	}
	w.paths[path] = true
	return nil
}

func (w *incidentFileWatch) registerRoute(route, path string, seen map[string]bool) error {
	if seen[route] {
		return syscall.ELOOP
	}
	seen[route] = true
	defer delete(seen, route)
	parts := strings.Split(strings.TrimPrefix(route, string(filepath.Separator)), string(filepath.Separator))
	parent := incidentWatchKey{Path: string(filepath.Separator), Follow: true}
	if err := w.addNode(parent, true, path); err != nil {
		return err
	}
	for i, name := range parts {
		current := filepath.Join(parent.Path, name)
		w.stats++
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
			missing := w.nodes[parent].Missing[name]
			if missing == nil {
				missing = map[string]bool{}
				w.nodes[parent].Missing[name] = missing
			}
			missing[path] = true
			w.routes[path][parent][name] = true
			return nil
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			if err := w.addNode(incidentWatchKey{Path: current}, false, path); err != nil {
				return err
			}
			target, err := os.Readlink(current)
			if err != nil {
				return err
			}
			if !filepath.IsAbs(target) {
				target = filepath.Join(parent.Path, target)
			}
			if err := w.registerRoute(filepath.Clean(target), path, seen); err != nil {
				return err
			}
			w.stats++
			info, err = os.Stat(current)
			if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
				return nil
			}
			if err != nil {
				return err
			}
		}
		key := incidentWatchKey{Path: current, Follow: true}
		if err := w.addNode(key, info.IsDir(), path); err != nil {
			return err
		}
		if !info.IsDir() && i != len(parts)-1 {
			return nil
		}
		parent = key
	}
	return nil
}

func (w *incidentFileWatch) Forget(path string) error {
	delete(w.paths, path)
	for key, missingNames := range w.routes[path] {
		node := w.nodes[key]
		delete(node.Leaves, path)
		for name := range missingNames {
			leaves := node.Missing[name]
			delete(leaves, path)
			if len(leaves) == 0 {
				delete(node.Missing, name)
			}
		}
		if len(node.Leaves) == 0 {
			if err := w.backend.Remove(key); err != nil {
				return err
			}
			delete(w.nodes, key)
		}
	}
	delete(w.routes, path)
	return nil
}

func (w *incidentFileWatch) Drain() (map[string]bool, error) {
	events, err := w.backend.Drain()
	if err != nil {
		return nil, err
	}
	dirty := map[string]bool{}
	rebind := map[string]bool{}
	for _, event := range events {
		node := w.nodes[event.Key]
		if node == nil {
			continue
		}
		if event.Invalidated || !node.Directory {
			for leaf := range node.Leaves {
				dirty[leaf] = true
				if event.Invalidated || !event.Key.Follow {
					rebind[leaf] = true
				}
			}
			continue
		}
		for name, leaves := range node.Missing {
			if event.Name != "" && event.Name != name {
				continue
			}
			for leaf := range leaves {
				dirty[leaf], rebind[leaf] = true, true
			}
		}
	}
	for path := range rebind {
		if err := w.Forget(path); err != nil {
			return nil, err
		}
		if err := w.Register(path); err != nil {
			return nil, err
		}
	}
	return dirty, nil
}

func (w *incidentFileWatch) Close() error {
	return w.backend.Close()
}
