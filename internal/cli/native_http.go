package cli

import (
	"context"
	"errors"
	"net/http"

	"github.com/yasyf/cc-inbox/internal/store"
)

type nativeHTTP struct {
	http.Handler
	incident *IncidentHTTP
	board    *BoardHTTP
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
	board, err := BoardRecordsHandler(st, home)
	if err != nil {
		_ = incident.Close()
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/v1/tail", tail)
	mux.Handle("/v1/incident-holds", incident)
	mux.Handle("/v1/board-records", board)
	mux.Handle("/v1/board-provision", board)
	mux.Handle("/v1/hook/", HookHandler(st, home))
	return &nativeHTTP{Handler: mux, incident: incident, board: board}, nil
}

func (h *nativeHTTP) Close() error { return errors.Join(h.incident.Close(), h.board.Close()) }
