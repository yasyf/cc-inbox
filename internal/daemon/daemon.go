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
	"net/http/pprof"
	"sync"
	"time"

	"github.com/yasyf/daemonkit"
	daemonversion "github.com/yasyf/daemonkit/version"

	"github.com/yasyf/cc-inbox/internal/hook"
	"github.com/yasyf/cc-inbox/internal/importer"
	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/kinds"
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
	// Sessions is how many client sessions, and requests in flight, the
	// daemon takes at once: every cci process that writes holds one.
	Sessions = 256
	// SessionIdle frees a quiet session's slot, so a long-lived client such as
	// a window's channel server never holds one between its writes.
	SessionIdle = time.Minute
	// MaintenanceDrive holds the records the daemon writes about the store
	// itself, such as a migration that held every client open.
	MaintenanceDrive = "cci"
	maintainRetry    = time.Minute
	collectInterval  = 10 * time.Minute
	collectPause     = time.Second
	sweepInterval    = 24 * time.Hour
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

		Restart:     daemonkit.RestartOnFailure,
		Shutdown:    daemonkit.Grace(10 * time.Second),
		Idle:        daemonkit.Grace(SessionIdle),
		Concurrency: Sessions,
	}, nil
}

type Product struct {
	home     string
	st       *store.Store
	writes   *queue
	reads    ReadAPI
	http     *http.Server
	done     chan struct{}
	stopSync context.CancelFunc
	synced   chan struct{}
}

type Runtime struct {
	home                string
	ln                  net.Listener
	mu                  sync.Mutex
	failed              error
	refreshRegistration func(context.Context, string, string) error
	reads               func(context.Context, *store.Store) (ReadAPI, error)
}

type ReadAPI interface {
	http.Handler
	Close() error
}

func New(home string, ln net.Listener, reads func(context.Context, *store.Store) (ReadAPI, error)) *Runtime {
	return &Runtime{home: home, ln: ln, refreshRegistration: mesh.RefreshRegistration, reads: reads}
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
	if m, ok := st.Migrated(); ok {
		recordMigration(c.Context, st, m)
	}
	if err := r.refreshRegistration(c.Context, r.home, st.SchemaFingerprint()); err != nil {
		slog.Warn("cci daemon cannot refresh mesh registration", "err", err)
	}
	reads, err := r.reads(c.Context, st)
	if err != nil {
		return nil, errors.Join(err, st.Close())
	}
	handler := http.NewServeMux()
	handler.Handle("/v1/tail", reads)
	handler.Handle("/v1/incident-holds", reads)
	handler.HandleFunc("/debug/pprof/", pprof.Index)
	handler.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	handler.HandleFunc("/debug/pprof/profile", pprof.Profile)
	handler.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	handler.HandleFunc("/debug/pprof/trace", pprof.Trace)
	feed := server.New(st, time.Second)
	handler.Handle("/", feed.Handler())
	p := &Product{
		home:   r.home,
		st:     st,
		reads:  reads,
		http:   &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second},
		done:   make(chan struct{}),
		synced: make(chan struct{}),
	}
	id, err := node.Load(r.home)
	if err != nil {
		return nil, errors.Join(err, reads.Close(), st.Close())
	}
	if p.writes, err = newQueue(st); err != nil {
		return nil, errors.Join(err, reads.Close(), st.Close())
	}
	p.writes.posted = feed.Kick
	p.http.RegisterOnShutdown(feed.Close)
	st.WriteThrough(p.writes)
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
	loops.Go(func() { maintainIndexes(syncCtx, r.home) })
	loops.Go(func() { collect(syncCtx, st) })
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
		if err := hook.DrainPrompts(ctx, p.st); err != nil && ctx.Err() == nil {
			slog.Warn("cci daemon could not record a kept prompt yet", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
	}
}

func collect(ctx context.Context, st *store.Store) {
	pause := func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(collectPause):
			return nil
		}
	}
	var swept time.Time
	for {
		if err := pause(ctx); err != nil {
			return
		}
		res, err := st.Collect(ctx, pause)
		if err != nil && ctx.Err() == nil {
			slog.Warn("cci daemon could not collect the store", "err", err)
		}
		if res.Archived > 0 || res.Sessions > 0 || res.Vacuumed || len(res.Skipped) > 0 {
			slog.Info("cci daemon collected the store", "archived", res.Archived, "sessions", res.Sessions, "claims", res.Claims, "vacuumed", res.Vacuumed, "freed_pages", res.Freed, "skipped", res.Skipped)
		}
		if time.Since(swept) >= sweepInterval {
			swept = time.Now()
			blobs, archives, err := st.SweepFiles(ctx)
			if err != nil && ctx.Err() == nil {
				slog.Warn("cci daemon could not sweep the store's files", "err", err)
			}
			if blobs > 0 || archives > 0 {
				slog.Info("cci daemon swept the store's files", "blobs", blobs, "archives", archives)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(collectInterval):
		}
	}
}

func maintainIndexes(ctx context.Context, home string) {
	for {
		_, err := store.MaintainIndexes(ctx, home)
		if err == nil || ctx.Err() != nil {
			return
		}
		slog.Warn("cci daemon could not maintain the store's indexes yet", "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(maintainRetry):
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
	case OpWrite:
		return p.write(ctx, req.Body)
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
	case <-ctx.Done():
		return fmt.Errorf("drain imports: %w", ctx.Err())
	}
	return p.writes.close(ctx)
}

func recordMigration(ctx context.Context, st *store.Store, m store.Migration) {
	text := fmt.Sprintf("migrated the store from schema %d to %d in %s, holding every client open behind init.lock meanwhile", m.From, m.To, m.Took.Round(time.Millisecond))
	if m.From == m.To {
		text = fmt.Sprintf("built the store's query indexes in %s, holding every client open behind init.lock meanwhile", m.Took.Round(time.Millisecond))
	}
	slog.Info("cci daemon migrated the store", "from", m.From, "to", m.To, "took", m.Took)
	if _, _, err := st.Post(ctx, store.Record{Drive: MaintenanceDrive, Lane: "daemon", Kind: kinds.Note, Text: text, Source: "daemon"}, 0); err != nil {
		slog.Warn("cci daemon could not record its migration", "err", err)
	}
}

func (p *Product) Close(daemonkit.Budget) error {
	return errors.Join(p.reads.Close(), p.st.Close())
}
