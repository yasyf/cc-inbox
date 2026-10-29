package daemon

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/yasyf/daemonkit"
	daemonversion "github.com/yasyf/daemonkit/version"

	"github.com/yasyf/cc-inbox/internal/importer"
	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/mesh"
	"github.com/yasyf/cc-inbox/internal/node"
	"github.com/yasyf/cc-inbox/internal/pairing"
	"github.com/yasyf/cc-inbox/internal/relay"
	"github.com/yasyf/cc-inbox/internal/server"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/version"
)

const (
	Label          daemonkit.Label  = "com.yasyf.cc-inbox"
	Schema         daemonkit.Schema = "cci.v1"
	Addr                            = "127.0.0.1:7377"
	ImportInterval                  = time.Second
)

func Definition() (daemonkit.Daemon, error) {
	program, err := daemonkit.Stable(daemonversion.Running(version.String()))
	if err != nil {
		return daemonkit.Daemon{}, fmt.Errorf("resolve cci program: %w", err)
	}
	return daemonkit.Daemon{
		Label:   Label,
		Program: program,
		Args:    []string{"daemon", "run"},
		Schemas: []daemonkit.Schema{Schema},

		Restart:  daemonkit.RestartOnFailure,
		Shutdown: daemonkit.Grace(10 * time.Second),
	}, nil
}

type Product struct {
	st       *store.Store
	http     *http.Server
	done     chan struct{}
	stopSync context.CancelFunc
	synced   chan struct{}
}

type Runtime struct {
	home   string
	ln     net.Listener
	mu     sync.Mutex
	failed error
}

func New(home string, ln net.Listener) *Runtime {
	return &Runtime{home: home, ln: ln}
}

func (r *Runtime) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.failed
}

func (r *Runtime) fail(c daemonkit.Ctx, err error) {
	r.mu.Lock()
	r.failed = cmp.Or(r.failed, err)
	r.mu.Unlock()
	c.Stop(err)
}

func (r *Runtime) Start(c daemonkit.Ctx) (daemonkit.Product, error) {
	roots, err := version.DefaultRoots()
	if err != nil {
		return nil, err
	}
	up := &Upgrader{Roots: roots, Running: version.String()}
	if err := up.Check(); err != nil {
		slog.Warn("cci daemon cannot scan for upgrades", "err", err)
	}
	st, err := store.Open(c.Context, r.home)
	if err != nil {
		return nil, err
	}
	p := &Product{
		st:     st,
		http:   &http.Server{Handler: server.New(st, time.Second).Handler(), ReadHeaderTimeout: 5 * time.Second},
		done:   make(chan struct{}),
		synced: make(chan struct{}),
	}
	id, err := node.Load(r.home)
	if err != nil {
		return nil, errors.Join(err, st.Close())
	}
	var syncCtx context.Context
	syncCtx, p.stopSync = context.WithCancel(context.Background())
	var loops sync.WaitGroup
	loops.Go(func() {
		if err := p.syncImports(syncCtx); err != nil {
			r.fail(c, err)
		}
	})
	loops.Go(func() {
		if err := superviseMesh(syncCtx, c, r.home); err != nil {
			r.fail(c, err)
		}
	})
	loops.Go(func() {
		loop := relay.NewLoop(r.home, st, id)
		loop.OnControl(pairing.Handler(r.home, st, id))
		loop.Run(syncCtx)
	})
	loops.Go(func() { up.Follow(syncCtx, UpgradeInterval) })
	go func() {
		loops.Wait()
		close(p.synced)
	}()
	go func() {
		err := p.http.Serve(r.ln)
		if !errors.Is(err, http.ErrServerClosed) {
			r.fail(c, fmt.Errorf("http listener: %w", err))
		}
		close(p.done)
	}()
	return p, nil
}

func (p *Product) syncImports(ctx context.Context) error {
	tick := time.NewTicker(ImportInterval)
	defer tick.Stop()
	for {
		if _, err := importer.Refresh(ctx, p.st); err != nil && ctx.Err() == nil {
			return fmt.Errorf("refresh imports: %w", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
	}
}

func superviseMesh(ctx context.Context, c daemonkit.Ctx, home string) error {
	tick := time.NewTicker(ImportInterval)
	defer tick.Stop()
	var exited <-chan daemonkit.Exit
	for {
		if exited == nil {
			enabled, err := mesh.Enabled(home)
			if err != nil {
				return err
			}
			if enabled {
				child, err := mesh.StartHelper(ctx, c)
				if err != nil && ctx.Err() == nil {
					return err
				}
				if child != nil {
					exited = child.Done()
				}
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case exit := <-exited:
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("mesh helper exited: code=%d signal=%v", exit.Code, exit.Signal)
		case <-tick.C:
		}
	}
}

type DigestRequest struct {
	Drive string    `json:"drive"`
	Since time.Time `json:"since"`
}

func (p *Product) Handle(ctx context.Context, req daemonkit.Request) (daemonkit.Reply, error) {
	switch req.Op {
	case "digest":
		var in DigestRequest
		if err := json.Unmarshal(req.Body, &in); err != nil {
			return daemonkit.Reply{}, fmt.Errorf("decode digest request: %w", err)
		}
		v, err := inbox.Digest(ctx, p.st, in.Drive, in.Since)
		if err != nil {
			return daemonkit.Reply{}, err
		}
		body, err := json.Marshal(v)
		if err != nil {
			return daemonkit.Reply{}, fmt.Errorf("encode digest: %w", err)
		}
		return daemonkit.Reply{Body: body}, nil
	case "addr":
		return daemonkit.Reply{Body: []byte(Addr)}, nil
	}
	return daemonkit.Reply{}, fmt.Errorf("unknown op %q", req.Op)
}

func (p *Product) Drain(b daemonkit.Budget) error {
	p.stopSync()
	graceful, cancelGraceful := b.Share("http", 0.5).Context(context.Background())
	defer cancelGraceful()
	if err := p.http.Shutdown(graceful); errors.Is(err, context.DeadlineExceeded) {
		if err := p.http.Close(); err != nil {
			return fmt.Errorf("close lingering http streams: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("drain http: %w", err)
	}
	<-p.done
	ctx, cancel := b.Context(context.Background())
	defer cancel()
	select {
	case <-p.synced:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("drain imports: %w", ctx.Err())
	}
}

func (p *Product) Close(daemonkit.Budget) error {
	return p.st.Close()
}
