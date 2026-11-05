package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"time"

	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/peers"
	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
)

const (
	defaultLimit = 100
	maxLimit     = 500
)

// FollowPath is where a reader posts a FollowRequest for its stream of wakes.
const FollowPath = "/v1/follow"

const followHeartbeat = 30 * time.Second

// FollowRequest names the store a reader reads and what it reads.
type FollowRequest struct {
	Home   string       `json:"home"`
	Filter store.Filter `json:"filter"`
}

type Server struct {
	st       *store.Store
	interval time.Duration
	hub      *inbox.Hub
}

func New(st *store.Store, interval time.Duration) *Server {
	return &Server{st: st, interval: interval, hub: inbox.NewHub(st, interval)}
}

// Kick reports a commit to every stream, ahead of the next interval.
func (s *Server) Kick() {
	s.hub.Kick()
}

// Close ends every stream.
func (s *Server) Close() {
	s.hub.Close()
}

// Follow streams wakes to each in this process, for a command the daemon runs.
func (s *Server) Follow(ctx context.Context, f store.Filter, each func(store.Wake) error) error {
	follower := s.hub.Follow(f) //nolint:contextcheck // the hub outlives one follower.
	defer follower.Leave()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.hub.Done():
			return fmt.Errorf("%w: the hub closed", store.ErrUnrouted)
		case <-follower.Ready():
			if err := each(follower.Take()); err != nil {
				return err
			}
		}
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/drives", s.drives)
	mux.HandleFunc("GET /v1/records", s.records)
	mux.HandleFunc("GET /v1/digest", s.digest)
	mux.HandleFunc("GET /v1/stream", s.stream)
	mux.HandleFunc("POST "+FollowPath, s.follow)
	mux.HandleFunc("GET /v1/lanes", s.lanes)
	mux.HandleFunc("GET /v1/peers", s.peers)
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
	f := store.Filter{Drive: q.Get("drive"), Lanes: q["lane"], LaneAliases: true, To: q.Get("to"), For: q.Get("reader"), Topics: q["topic"], Stack: q.Get("stack"), Target: q.Get("target")}
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
	if v := q.Get("pr"); v != "" {
		pr, err := store.ParsePR(v)
		if err != nil {
			return store.Filter{}, badRequest{err}
		}
		f.PR = pr
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
	if err == nil {
		err = inbox.Annotate(r.Context(), s.st, records)
	}
	if records == nil {
		records = []store.Record{}
	}
	respond(w, records, err)
}

func (s *Server) lanes(w http.ResponseWriter, r *http.Request) {
	f, err := s.filter(r)
	if err != nil {
		respond(w, nil, err)
		return
	}
	records, err := s.st.LaneTips(r.Context(), f.Drive, maxLimit)
	if records == nil {
		records = []store.Record{}
	}
	respond(w, records, err)
}

func (s *Server) peers(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := peers.Filter{Session: q.Get("session"), Agent: q.Get("agent"), Name: q.Get("name"), Role: q.Get("role"), Drive: q.Get("drive"), Lane: q.Get("lane"), Tag: q.Get("tag"), Repo: q.Get("repo"), Worktree: q.Get("worktree"), Branch: q.Get("branch"), Query: q.Get("q"), Rel: q.Get("rel"), All: q.Get("all") == "1"}
	pr, build, ref := q.Get("pr"), q.Get("build"), q.Get("ref")
	selectors := 0
	for _, v := range []string{pr, build, ref} {
		if v != "" {
			selectors++
		}
	}
	switch {
	case selectors > 1:
		respond(w, nil, badRequest{errors.New("pass one of pr, build or ref")})
		return
	case pr != "":
		n, err := strconv.Atoi(pr)
		if err != nil {
			respond(w, nil, badRequest{errors.New("pr must be a number")})
			return
		}
		f.Kind, f.Key = "pr", strconv.Itoa(n)
	case build != "":
		f.Kind, f.Key = "build", build
	case ref != "":
		kind, key, err := store.ParseRef(ref)
		if err != nil {
			respond(w, nil, badRequest{err})
			return
		}
		f.Kind, f.Key = kind, key
	}
	views, err := peers.Resolve(r.Context(), s.st, f)
	if views == nil {
		views = []peers.View{}
	}
	respond(w, views, err)
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
	head, err := s.st.MaxSeq(r.Context())
	if err != nil {
		respond(w, nil, err)
		return
	}
	if f.After == 0 {
		f.After = head
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
	if !s.emit(r.Context(), w, &f, head) {
		return
	}
	follower := s.hub.Follow(f) //nolint:contextcheck // the hub outlives one request.
	defer follower.Leave()
	tick := time.NewTicker(s.interval)
	defer tick.Stop()
	for {
		flusher.Flush()
		select {
		case <-r.Context().Done():
			return
		case <-s.hub.Done():
			return
		case <-follower.Ready():
			wake := follower.Take()
			f.After = max(f.After, wake.First-1)
			if !s.emit(r.Context(), w, &f, wake.Head) {
				return
			}
		case <-tick.C:
			if r.Context().Err() != nil {
				return
			}
		}
	}
}

func (s *Server) emit(ctx context.Context, w http.ResponseWriter, f *store.Filter, head int64) bool {
	for {
		page := *f
		page.Before = head + 1
		records, err := s.st.Query(ctx, page)
		if err != nil {
			return false
		}
		for _, rec := range records {
			if _, err := fmt.Fprintf(w, "id: %d\nevent: record\ndata: %s\n\n", rec.Seq, render.JSON(rec)); err != nil {
				return false
			}
			f.After = rec.Seq
		}
		if len(records) < f.Limit {
			return true
		}
	}
}

func (s *Server) follow(w http.ResponseWriter, r *http.Request) {
	var in FollowRequest
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		respond(w, nil, badRequest{fmt.Errorf("decode follow request: %w", err)})
		return
	}
	if filepath.Clean(in.Home) != filepath.Clean(s.st.HomeDir()) {
		w.WriteHeader(http.StatusConflict)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		respond(w, nil, errors.New("streaming unsupported"))
		return
	}
	follower := s.hub.Follow(in.Filter) //nolint:contextcheck // the hub outlives one request.
	defer follower.Leave()
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	lines := json.NewEncoder(w)
	heartbeat := time.NewTicker(followHeartbeat)
	defer heartbeat.Stop()
	for {
		var wake store.Wake
		select {
		case <-r.Context().Done():
			return
		case <-s.hub.Done():
			return
		case <-follower.Ready():
			wake = follower.Take()
		case <-heartbeat.C:
		}
		if err := lines.Encode(wake); err != nil {
			return
		}
		flusher.Flush()
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
