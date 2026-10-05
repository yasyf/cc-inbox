package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/yasyf/daemonkit"

	"github.com/yasyf/cc-inbox/internal/importer"
	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/server"
	"github.com/yasyf/cc-inbox/internal/store"
)

const (
	Label          daemonkit.Label  = "com.yasyf.cc-inbox"
	Schema         daemonkit.Schema = "cci.v1"
	Addr                            = "127.0.0.1:7377"
	ImportInterval                  = time.Second
)

func Definition() (daemonkit.Daemon, error) {
	program, err := daemonkit.Stable()
	if err != nil {
		return daemonkit.Daemon{}, fmt.Errorf("resolve cci program: %w", err)
	}
	return daemonkit.Daemon{
		Label:    Label,
		Program:  program,
		Args:     []string{"daemon", "run"},
		Schemas:  []daemonkit.Schema{Schema},
		Trust:    daemonkit.Trust{Serving: daemonkit.ServingSameUser()},
		Restart:  daemonkit.RestartOnFailure,
		Shutdown: daemonkit.Grace(10 * time.Second),
	}, nil
}

type Product struct {
	st       *store.Store
	http     *http.Server
	done     chan error
	stopSync context.CancelFunc
	synced   chan struct{}
}

func Start(home string, ln net.Listener) daemonkit.Start {
	return func(c daemonkit.Ctx) (daemonkit.Product, error) {
		st, err := store.Open(c.Context, home)
		if err != nil {
			return nil, err
		}
		p := &Product{
			st:     st,
			http:   &http.Server{Handler: server.New(st, time.Second).Handler(), ReadHeaderTimeout: 5 * time.Second},
			done:   make(chan error, 1),
			synced: make(chan struct{}),
		}
		var syncCtx context.Context
		syncCtx, p.stopSync = context.WithCancel(context.Background())
		go func() {
			defer close(p.synced)
			if err := p.syncImports(syncCtx); err != nil {
				c.Stop(err)
			}
		}()
		go func() {
			err := p.http.Serve(ln)
			if !errors.Is(err, http.ErrServerClosed) {
				c.Stop(fmt.Errorf("http listener: %w", err))
			}
			p.done <- nil
		}()
		return p, nil
	}
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
	<-p.synced
	ctx, cancel := b.Context(context.Background())
	defer cancel()
	if err := p.http.Shutdown(ctx); err != nil {
		return fmt.Errorf("drain http: %w", err)
	}
	<-p.done
	return nil
}

func (p *Product) Close(daemonkit.Budget) error {
	return p.st.Close()
}
