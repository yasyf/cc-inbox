package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

type incidentWatchKey struct {
	Path   string
	Follow bool
}

type incidentWatchEvent struct {
	Keys        map[incidentWatchKey]bool
	Name        string
	Invalidated bool
}

type incidentWatchInfo struct {
	Device    uint64
	Inode     uint64
	Kind      uint32
	Changed   bool
	Directory bool
	Symlink   bool
}

type incidentWatchBackend interface {
	Add(incidentWatchKey) error
	Info(incidentWatchKey) incidentWatchInfo
	Remove(incidentWatchKey) error
	Drain(context.Context) ([]incidentWatchEvent, error)
	Close() error
}

type incidentWatchNode struct {
	Changed   bool
	Directory bool
	Symlink   bool
	Leaves    map[string]bool
	Missing   map[string]map[string]bool
}

type incidentFileWatch struct {
	backend      incidentWatchBackend
	nodes        map[incidentWatchKey]*incidentWatchNode
	paths        map[string]bool
	routes       map[string]map[incidentWatchKey]map[string]bool
	stats        uint64
	missingNames uint64
	pending      []incidentWatchEvent
	rebind       map[string]bool
}

func newIncidentFileWatch() (*incidentFileWatch, error) {
	backend, err := newIncidentWatchBackend()
	if err != nil {
		return nil, err
	}
	return &incidentFileWatch{backend: backend, nodes: map[incidentWatchKey]*incidentWatchNode{}, paths: map[string]bool{}, routes: map[string]map[incidentWatchKey]map[string]bool{}, rebind: map[string]bool{}}, nil
}

func (w *incidentFileWatch) addNode(key incidentWatchKey, leaf string) (*incidentWatchNode, error) {
	node := w.nodes[key]
	if node == nil {
		w.stats++
		if err := w.backend.Add(key); err != nil {
			return nil, err
		}
		info := w.backend.Info(key)
		node = &incidentWatchNode{Changed: info.Changed, Directory: info.Directory, Symlink: info.Symlink, Leaves: map[string]bool{}, Missing: map[string]map[string]bool{}}
		w.nodes[key] = node
	}
	node.Leaves[leaf] = true
	if w.routes[leaf][key] == nil {
		w.routes[leaf][key] = map[string]bool{}
	}
	if node.Changed {
		w.rebind[leaf] = true
	}
	return node, nil
}

func (w *incidentFileWatch) Register(path string) error {
	if w.paths[path] {
		return nil
	}
	w.routes[path] = map[incidentWatchKey]map[string]bool{}
	if err := w.registerRoute(path, path, map[string]bool{}); err != nil {
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
	parent := incidentWatchKey{Path: string(filepath.Separator)}
	if _, err := w.addNode(parent, path); err != nil {
		return err
	}
	for i, name := range parts {
		current := parent.Path + string(filepath.Separator) + name
		if parent.Path == string(filepath.Separator) {
			current = parent.Path + name
		}
		key := incidentWatchKey{Path: current}
		node, err := w.addNode(key, path)
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
		if node.Changed {
			w.rebind[path] = true
			return nil
		}
		if node.Symlink {
			w.stats++
			_, followErr := os.Stat(current)
			if followErr != nil && !errors.Is(followErr, os.ErrNotExist) && !errors.Is(followErr, syscall.ENOTDIR) {
				return followErr
			}
			target, err := os.Readlink(current)
			if err != nil {
				return err
			}
			if !filepath.IsAbs(target) {
				target = parent.Path + string(filepath.Separator) + target
			}
			if err := w.registerRoute(target, path, seen); err != nil {
				return err
			}
			if followErr != nil {
				return nil
			}
			key = incidentWatchKey{Path: current, Follow: true}
			node, err = w.addNode(key, path)
			if err != nil {
				return err
			}
			if node.Changed {
				return nil
			}
		}
		if !node.Directory && i != len(parts)-1 {
			return nil
		}
		parent = key
	}
	return nil
}

func (w *incidentFileWatch) Forget(path string) error {
	delete(w.rebind, path)
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

func incidentWatchPathChanged(key incidentWatchKey, bound incidentWatchInfo) bool {
	info, err := os.Lstat(key.Path)
	if key.Follow {
		info, err = os.Stat(key.Path)
	}
	if err != nil {
		return true
	}
	stat := info.Sys().(*syscall.Stat_t)
	return uint64(stat.Dev) != bound.Device || stat.Ino != bound.Inode || uint32(stat.Mode)&syscall.S_IFMT != bound.Kind
}

func (w *incidentFileWatch) Drain(ctx context.Context) (map[string]bool, error) {
	dirty := map[string]bool{}
	for path := range w.rebind {
		if err := ctx.Err(); err != nil {
			return dirty, err
		}
		dirty[path] = true
	}
	if err := w.collect(ctx, dirty); err != nil {
		return dirty, err
	}
	events, err := w.backend.Drain(ctx)
	w.pending = events
	if err != nil {
		return dirty, err
	}
	return dirty, w.collect(ctx, dirty)
}

func (w *incidentFileWatch) collect(ctx context.Context, dirty map[string]bool) error {
	for len(w.pending) != 0 {
		event := w.pending[0]
		for key := range event.Keys {
			if err := ctx.Err(); err != nil {
				return err
			}
			node := w.nodes[key]
			if node == nil {
				continue
			}
			if event.Invalidated || !node.Directory {
				if err := w.mark(ctx, node.Leaves, event.Invalidated || node.Symlink, dirty); err != nil {
					return err
				}
				continue
			}
			if len(node.Missing) == 0 {
				continue
			}
			if event.Name != "" {
				w.missingNames++
				if err := w.mark(ctx, node.Missing[event.Name], true, dirty); err != nil {
					return err
				}
				continue
			}
			for _, leaves := range node.Missing {
				w.missingNames++
				if err := w.mark(ctx, leaves, true, dirty); err != nil {
					return err
				}
			}
		}
		w.pending = w.pending[1:]
	}
	w.pending = nil
	return ctx.Err()
}

func (w *incidentFileWatch) mark(ctx context.Context, leaves map[string]bool, rebind bool, dirty map[string]bool) error {
	for leaf := range leaves {
		if err := ctx.Err(); err != nil {
			return err
		}
		dirty[leaf] = true
		if rebind {
			w.rebind[leaf] = true
		}
	}
	return nil
}

func (w *incidentFileWatch) Close() error {
	return w.backend.Close()
}
