package mesh

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/synckit/codec"
	"github.com/yasyf/synckit/helperruntime"
	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"

	"github.com/yasyf/cc-inbox/internal/store"
)

const (
	DefaultDebounce   = 5 * time.Second
	helperReadyWithin = 10 * time.Second
)

var HelperArgs = []string{"mesh", "helper"}

func Manifest(schema string, debounce time.Duration) manifest.Manifest {
	return manifest.Manifest{
		Name:    Service,
		Binary:  Service,
		Watch:   manifest.WatchSpec{Debounce: codec.Duration(debounce)},
		Service: manifest.ServiceSpec{Kind: "resident", SchemaFingerprint: schema},
	}
}

func ManifestPath(home string) string {
	return filepath.Join(home, "mesh", Service+".json")
}

func WriteManifest(home, schema string, debounce time.Duration) (string, error) {
	m := Manifest(schema, debounce)
	if err := m.Validate(); err != nil {
		return "", err
	}
	path := ManifestPath(home)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", fmt.Errorf("create %s: %w", filepath.Dir(path), err)
	}
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, append(raw, '\n'), 0o600); err != nil {
		return "", fmt.Errorf("write %s: %w", path, err)
	}
	return path, nil
}

func Enabled(home string) (bool, error) {
	_, err := os.Stat(ManifestPath(home))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

type helperProduct struct{ st *store.Store }

func (helperProduct) Drain(context.Context) error   { return nil }
func (p helperProduct) Close(context.Context) error { return p.st.Close() }

func RunHelper(ctx context.Context, home string) error {
	dispatcher := rpc.NewDispatcher()
	runtime, err := helperruntime.New(helperruntime.Config{
		App:        helperruntime.App{Name: Service},
		Dispatcher: dispatcher,
		Prepare: func(c daemonkit.Ctx) (helperruntime.Product, error) {
			st, err := store.Open(c.Context, home)
			if err != nil {
				return nil, err
			}
			syncservice.RegisterConsumer(dispatcher, NewConsumer(st, home))
			return helperProduct{st: st}, nil
		},
	})
	if err != nil {
		return err
	}
	return runtime.Run(ctx)
}

func StartHelper(ctx context.Context, scope daemonkit.Ctx) (*daemonkit.Child, error) {
	executable, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("resolve cci executable: %w", err)
	}
	spawnCtx, cancel := context.WithTimeout(ctx, helperReadyWithin)
	defer cancel()
	child, err := scope.Spawn(spawnCtx, daemonkit.Cmd{
		Path: executable, Args: HelperArgs, Session: true,
	}, daemonkit.ChannelNone, nil)
	if err != nil {
		return nil, fmt.Errorf("start mesh helper: %w", err)
	}
	stop := func(cause error) (*daemonkit.Child, error) {
		stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), helperReadyWithin)
		defer cancel()
		_, stopErr := child.Stop(stopCtx)
		return nil, errors.Join(cause, stopErr)
	}
	spec, err := helperruntime.Spec(Service, daemonkit.Program{}, 0)
	if err != nil {
		return stop(err)
	}
	client, err := daemonkit.Open(spec)
	if err != nil {
		return stop(err)
	}
	ready := make(chan error, 1)
	go func() {
		_, err := client.WaitReady(spawnCtx)
		ready <- err
	}()
	select {
	case err := <-ready:
		if err != nil {
			return stop(fmt.Errorf("await mesh helper readiness: %w", err))
		}
	case exit := <-child.Done():
		return stop(fmt.Errorf("mesh helper exited before readiness: code=%d signal=%v", exit.Code, exit.Signal))
	}
	return child, nil
}
