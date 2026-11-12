package daemon

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"github.com/yasyf/daemonkit"
	daemonversion "github.com/yasyf/daemonkit/version"

	"github.com/yasyf/cc-inbox/internal/harness"
	"github.com/yasyf/cc-inbox/internal/hook"
	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/node"
	"github.com/yasyf/cc-inbox/internal/pairing"
	"github.com/yasyf/cc-inbox/internal/relay"
	"github.com/yasyf/cc-inbox/internal/server"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/supervisor"
	"github.com/yasyf/cc-inbox/internal/version"
	"github.com/yasyf/cc-inbox/internal/wire"
)

const (
	Label           daemonkit.Label  = "com.yasyf.cc-inbox"
	Schema          daemonkit.Schema = "cci.v1"
	Addr                             = "127.0.0.1:7377"
	readModelFollow                  = time.Second
	readModelPace                    = 5 * time.Millisecond
	schedulerProcs                   = 4
	// Sessions is how many client sessions, and requests in flight, the
	// daemon takes at once: every cci process that writes holds one.
	Sessions = 256
	// SessionIdle frees a quiet session's slot, so a long-lived client such as
	// a window's channel server never holds one between its writes.
	SessionIdle = time.Minute
	// MaintenanceDrive holds the records the daemon writes about the store
	// itself, such as a migration that held every client open.
	MaintenanceDrive    = "cci"
	maintainRetry       = time.Minute
	promptDrainInterval = 5 * time.Second
	collectInterval     = 24 * time.Hour
	collectSettle       = time.Hour
	trigramPause        = 200 * time.Millisecond
	collectPause        = time.Second
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

		Restart:     daemonkit.RestartAlways,
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
	home     string
	ln       *net.TCPListener
	mu       sync.Mutex
	failed   error
	reads    func(context.Context, *store.Store) (ReadAPI, error)
	commands func(context.Context, net.Listener, *store.Store, string) error
}

// ServeCommands has the daemon run clients' commands over $CCI_HOME/cci.sock.
func (r *Runtime) ServeCommands(serve func(context.Context, net.Listener, *store.Store, string) error) {
	r.commands = serve
}

type ReadAPI interface {
	http.Handler
	Close() error
}

func New(home string, ln *net.TCPListener, reads func(context.Context, *store.Store) (ReadAPI, error)) *Runtime {
	return &Runtime{home: home, ln: ln, reads: reads}
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
	runtime.GOMAXPROCS(min(schedulerProcs, runtime.GOMAXPROCS(0)))
	offerUntil(c.Context, r.home, r.ln)
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
	reads, err := r.reads(c.Context, st)
	if err != nil {
		return nil, errors.Join(err, st.Close())
	}
	handler := http.NewServeMux()
	handler.Handle("/v1/tail", reads)
	handler.Handle("/v1/hook/", reads)
	handler.Handle("/v1/incident-holds", reads)
	handler.HandleFunc("/debug/pprof/", pprof.Index)
	handler.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	handler.HandleFunc("/debug/pprof/profile", pprof.Profile)
	handler.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	handler.HandleFunc("/debug/pprof/trace", pprof.Trace)
	feed := server.New(st, time.Second)
	st.FollowThrough(feed)
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
		p.drainPrompts(syncCtx)
	})
	loops.Go(func() {
		loop := relay.NewLoop(r.home, st, id)
		loop.OnControl(pairing.Handler(r.home, st, id))
		loop.Run(syncCtx)
	})
	loops.Go(func() { up.Follow(syncCtx, UpgradeInterval) })
	loops.Go(func() { maintainIndexes(syncCtx, r.home) })
	loops.Go(func() { collect(syncCtx, st) })
	loops.Go(func() { indexTrigrams(syncCtx, st) })
	loops.Go(func() { keepReadModel(syncCtx, st, r.home) })
	loops.Go(func() {
		if err := supervisor.Keep(syncCtx, st); err != nil && syncCtx.Err() == nil {
			slog.Warn("cci daemon could not start every registered session's tmux session", "err", err)
		}
	})
	if r.commands != nil {
		loops.Go(func() {
			if err := r.serveCommands(syncCtx, st); err != nil && syncCtx.Err() == nil {
				slog.Warn("cci daemon stopped running clients' commands; they run themselves", "err", err)
			}
		})
	}
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

func (p *Product) drainPrompts(ctx context.Context) {
	tick := time.NewTicker(promptDrainInterval)
	defer tick.Stop()
	for {
		if err := hook.DrainPrompts(ctx, p.st); err != nil && ctx.Err() == nil {
			slog.Warn("cci daemon could not record a kept prompt yet", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (r *Runtime) serveCommands(ctx context.Context, st *store.Store) error {
	path := filepath.Join(r.home, wire.SocketName)
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("clear the command socket: %w", err)
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return fmt.Errorf("listen on the command socket: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return errors.Join(fmt.Errorf("lock the command socket: %w", err), ln.Close())
	}
	return r.commands(ctx, ln, st, r.home)
}

const (
	readModelRetry    = 5 * time.Minute
	readModelSessions = time.Second
	readModelProcs    = 1
	readModelBuildGC  = 25
)

func buildReadModel(ctx context.Context, st *store.Store, pace func(context.Context) error) (*store.ReadModel, error) {
	defer debug.SetGCPercent(debug.SetGCPercent(readModelBuildGC))
	return st.BuildReadModel(ctx, pace)
}

func keepReadModel(ctx context.Context, st *store.Store, home string) {
	var model *store.ReadModel
	drop := func() {
		if model == nil {
			return
		}
		st.ServeReadModel(nil)
		if err := model.Close(); err != nil && ctx.Err() == nil {
			slog.Warn("cci daemon cannot close the read model", "err", err)
		}
		model = nil
	}
	defer drop()
	tick := time.NewTicker(readModelFollow)
	defer tick.Stop()
	var buildAfter time.Time
	pace := func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(readModelPace):
			return nil
		}
	}
	for {
		on := store.ReadModelOn(home)
		if on {
			harness.KeepSessions(readModelSessions)
			relay.KeepConfigs(readModelSessions)
			runtime.GOMAXPROCS(min(readModelProcs, runtime.NumCPU()))
		} else {
			harness.KeepSessions(0)
			relay.KeepConfigs(0)
			runtime.GOMAXPROCS(min(schedulerProcs, runtime.NumCPU()))
		}
		if model != nil && (!on || model.Stale()) {
			drop()
		}
		switch {
		case on && model == nil && time.Now().After(buildAfter):
			built, err := buildReadModel(ctx, st, pace)
			if err != nil {
				if ctx.Err() == nil {
					slog.Warn("cci daemon cannot build the read model; reads stay on the store", "err", err)
				}
				buildAfter = time.Now().Add(readModelRetry)
				break
			}
			model = built
			st.ServeReadModel(model)
		case model != nil && model.Full() && time.Now().After(buildAfter):
			built, err := buildReadModel(ctx, st, pace)
			if err != nil {
				if ctx.Err() == nil {
					slog.Warn("cci daemon cannot rebuild the full read model on a newer window; the full one keeps serving", "err", err)
				}
				buildAfter = time.Now().Add(readModelRetry)
				break
			}
			full := model
			model = built
			st.ServeReadModel(model)
			if err := full.Close(); err != nil && ctx.Err() == nil {
				slog.Warn("cci daemon cannot close the full read model", "err", err)
			}
		case model != nil:
			if err := model.CatchUp(ctx); err != nil && !model.Stale() && ctx.Err() == nil {
				slog.Warn("cci daemon cannot catch the read model up", "err", err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func indexTrigrams(ctx context.Context, st *store.Store) {
	err := st.IndexTrigrams(ctx, func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(trigramPause):
			return nil
		}
	})
	if err != nil && ctx.Err() == nil {
		slog.Warn("cci daemon could not build grep's index; grep still scans", "err", err)
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
	select {
	case <-ctx.Done():
		return
	case <-time.After(collectSettle):
	}
	for {
		res, err := st.Collect(ctx, pause)
		if err != nil && ctx.Err() == nil {
			slog.Warn("cci daemon could not collect the store", "err", err)
		}
		if res.Archived > 0 || res.Sessions > 0 || res.Vacuumed || len(res.Skipped) > 0 {
			slog.Info("cci daemon collected the store", "archived", res.Archived, "sessions", res.Sessions, "claims", res.Claims, "vacuumed", res.Vacuumed, "freed_pages", res.Freed, "skipped", res.Skipped)
		}
		blobs, archives, err := st.SweepFiles(ctx)
		if err != nil && ctx.Err() == nil {
			slog.Warn("cci daemon could not sweep the store's files", "err", err)
		}
		if blobs > 0 || archives > 0 {
			slog.Info("cci daemon swept the store's files", "blobs", blobs, "archives", archives)
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
