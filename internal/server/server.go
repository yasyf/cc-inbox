package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
)

const (
	defaultLimit = 100
	maxLimit     = 500
)

type Server struct {
	st       *store.Store
	interval time.Duration
}

func New(st *store.Store, interval time.Duration) *Server {
	return &Server{st: st, interval: interval}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/drives", s.drives)
	mux.HandleFunc("GET /v1/records", s.records)
	mux.HandleFunc("GET /v1/digest", s.digest)
	mux.HandleFunc("GET /v1/stream", s.stream)
	return mux
}

func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	srv := &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}

type badRequest struct{ error }

func (s *Server) filter(r *http.Request) (store.Filter, error) {
	q := r.URL.Query()
	f := store.Filter{Drive: q.Get("drive"), Lanes: q["lane"], To: q.Get("to"), For: q.Get("reader"), Topics: q["topic"]}
	if f.Drive == "" {
		return store.Filter{}, badRequest{errors.New("drive is required")}
	}
	for _, k := range q["kind"] {
		kind, err := kinds.Parse(k)
		if err != nil {
			return store.Filter{}, badRequest{err}
		}
		f.Kinds = append(f.Kinds, kind)
	}
	if v := q.Get("since"); v != "" {
		seq, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return store.Filter{}, badRequest{fmt.Errorf("since must be a seq: %w", err)}
		}
		f.After = seq
	}
	if v := q.Get("since_time"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return store.Filter{}, badRequest{fmt.Errorf("since_time must be RFC3339: %w", err)}
		}
		f.Since = t
	}
	f.Limit = defaultLimit
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return store.Filter{}, badRequest{errors.New("limit must be a positive integer")}
		}
		f.Limit = min(n, maxLimit)
	}
	f.IncludeExpired = q.Get("expired") == "1"
	return f, nil
}

func (s *Server) drives(w http.ResponseWriter, r *http.Request) {
	drives, err := s.st.Drives(r.Context())
	respond(w, drives, err)
}

func (s *Server) records(w http.ResponseWriter, r *http.Request) {
	f, err := s.filter(r)
	if err != nil {
		respond(w, nil, err)
		return
	}
	records, err := s.st.Query(r.Context(), f)
	if records == nil {
		records = []store.Record{}
	}
	respond(w, records, err)
}

func (s *Server) digest(w http.ResponseWriter, r *http.Request) {
	f, err := s.filter(r)
	if err != nil {
		respond(w, nil, err)
		return
	}
	if f.Since.IsZero() {
		f.Since = s.st.Now().Add(-inbox.DigestWindow)
	}
	v, err := inbox.Digest(r.Context(), s.st, f.Drive, f.Since)
	respond(w, v, err)
}

func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	f, err := s.filter(r)
	if err != nil {
		respond(w, nil, err)
		return
	}
	if f.After == 0 {
		if f.After, err = s.st.MaxSeq(r.Context()); err != nil {
			respond(w, nil, err)
			return
		}
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		respond(w, nil, errors.New("streaming unsupported"))
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	tick := time.NewTicker(s.interval)
	defer tick.Stop()
	for {
		records, err := s.st.Query(r.Context(), f)
		if err != nil {
			return
		}
		for _, rec := range records {
			if _, err := fmt.Fprintf(w, "id: %d\nevent: record\ndata: %s\n\n", rec.Seq, render.JSON(rec)); err != nil {
				return
			}
			f.After = rec.Seq
		}
		flusher.Flush()
		select {
		case <-r.Context().Done():
			return
		case <-tick.C:
		}
	}
}

func respond(w http.ResponseWriter, body any, err error) {
	w.Header().Set("Content-Type", "application/json")
	var bad badRequest
	switch {
	case errors.As(err, &bad):
		w.WriteHeader(http.StatusBadRequest)
		body = map[string]string{"error": err.Error()}
	case err != nil:
		w.WriteHeader(http.StatusInternalServerError)
		body = map[string]string{"error": err.Error()}
	}
	_ = json.NewEncoder(w).Encode(body)
}
