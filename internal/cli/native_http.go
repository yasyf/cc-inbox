package cli

import (
	"context"
	"net/http"

	"github.com/yasyf/cc-inbox/internal/store"
)

type nativeHTTP struct {
	http.Handler
	incident *IncidentHTTP
}

func NativeReadHandler(ctx context.Context, st *store.Store, home string) (*nativeHTTP, error) {
	tail, err := TailHandler(st, home)
	if err != nil {
		return nil, err
	}
	incident, err := IncidentHoldsHandler(ctx, st)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/v1/tail", tail)
	mux.Handle("/v1/incident-holds", incident)
	return &nativeHTTP{Handler: mux, incident: incident}, nil
}

func (h *nativeHTTP) Close() error { return h.incident.Close() }
