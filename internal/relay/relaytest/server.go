package relaytest

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/e2e"
)

type Member struct {
	ID   string `json:"id"`
	Pub  []byte `json:"pub"`
	Name string `json:"name"`
	Tier string `json:"tier"`
	By   string `json:"by"`
	At   string `json:"at"`
}

type queued struct {
	receipt  string
	envelope *e2e.Envelope
	control  *e2e.Control
}

type Server struct {
	*httptest.Server
	Mux *http.ServeMux

	key *ecdsa.PrivateKey

	mu       sync.Mutex
	members  map[string]Member
	queues   map[string][]queued
	inflight map[string]map[string]queued
	epoch    uint64
	wraps    map[uint64][]e2e.Wrap
	log      map[string]e2e.Envelope
	nextID   int
	Pushes   [][]byte
}

func NewServer(t testing.TB) *Server {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{
		Mux:      http.NewServeMux(),
		key:      key,
		members:  map[string]Member{},
		queues:   map[string][]queued{},
		inflight: map[string]map[string]queued{},
		wraps:    map[uint64][]e2e.Wrap{},
		log:      map[string]e2e.Envelope{},
	}
	s.Mux.HandleFunc("GET /v1/relay", func(w http.ResponseWriter, r *http.Request) {
		reply(w, map[string]any{"pub": s.Pub(), "fingerprint": ""})
	})
	s.Mux.HandleFunc("POST /v1/join", s.signed(s.join))
	s.Mux.HandleFunc("POST /v1/push", s.signed(s.push))
	s.Mux.HandleFunc("GET /v1/pull", s.signed(s.pull))
	s.Mux.HandleFunc("POST /v1/ack", s.signed(s.ack))
	s.Mux.HandleFunc("PUT /v1/keys/{epoch}", s.signed(s.putKeys))
	s.Mux.HandleFunc("GET /v1/keys", s.signed(s.getKeys))
	s.Mux.HandleFunc("GET /v1/members", s.signed(s.listMembers))
	s.Mux.HandleFunc("GET /v1/status", s.signed(s.status))
	s.Server = httptest.NewServer(s.Mux)
	t.Cleanup(s.Close)
	return s
}

func (s *Server) Pub() []byte {
	pub, err := s.key.PublicKey.Bytes()
	if err != nil {
		panic(err)
	}
	return pub
}

func (s *Server) AddMember(id e2e.Identity, tier string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.members[id.ID()] = Member{ID: id.ID(), Pub: id.PublicKey(), Tier: tier, At: time.Now().UTC().Format(time.RFC3339Nano)}
}

func (s *Server) Epoch() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.epoch
}

func (s *Server) Control(c e2e.Control) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.controlLocked(c)
}

func (s *Server) controlLocked(c e2e.Control) {
	c.V = 1
	s.nextID++
	c.ID = fmt.Sprintf("%026d", s.nextID)
	if c.At == "" {
		c.At = time.Now().UTC().Format(time.RFC3339Nano)
	}
	d := sha256.Sum256(c.Message())
	sig, err := ecdsa.SignASN1(rand.Reader, s.key, d[:])
	if err != nil {
		panic(err)
	}
	c.Sig = sig
	for id := range s.members {
		s.enqueueLocked(id, queued{control: &c})
	}
}

func (s *Server) enqueueLocked(node string, q queued) {
	s.nextID++
	q.receipt = "r" + strconv.Itoa(s.nextID)
	s.queues[node] = append(s.queues[node], q)
}

type handler func(w http.ResponseWriter, r *http.Request, caller Member, body []byte)

func (s *Server) signed(h handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			fail(w, 400, "bad_request", err.Error())
			return
		}
		node := r.Header.Get("X-CCI-Node")
		sig, err := base64.StdEncoding.DecodeString(r.Header.Get("X-CCI-Sig"))
		if err != nil {
			fail(w, 401, "unsigned", "bad signature encoding")
			return
		}
		s.mu.Lock()
		m, ok := s.members[node]
		s.mu.Unlock()
		if !ok {
			fail(w, 403, "unknown_node", node)
			return
		}
		if !e2e.VerifyRequest(m.Pub, r.Method, r.URL.RequestURI(), r.Header.Get("X-CCI-Time"), r.Header.Get("X-CCI-Nonce"), body, sig) {
			fail(w, 401, "unsigned", "signature does not verify")
			return
		}
		h(w, r, m, body)
	}
}

func (s *Server) join(w http.ResponseWriter, r *http.Request, caller Member, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	reply(w, map[string]any{"node": caller.ID, "tier": caller.Tier, "queue": "cci-node-" + caller.ID, "epoch": s.epoch})
}

func (s *Server) push(w http.ResponseWriter, r *http.Request, caller Member, body []byte) {
	var req struct {
		Mesh      string         `json:"mesh"`
		Envelopes []e2e.Envelope `json:"envelopes"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		fail(w, 400, "bad_request", err.Error())
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Pushes = append(s.Pushes, bytes.Clone(body))
	if caller.Tier == e2e.TierPaired {
		delete(s.members, caller.ID)
		delete(s.queues, caller.ID)
		s.controlLocked(e2e.Control{Type: e2e.ControlUnpaired, Mesh: req.Mesh, Node: caller.ID, Reason: "violation", By: "relay"})
		fail(w, 403, "unpaired", "paired members are read-only")
		return
	}
	type result struct {
		OriginSeq int64  `json:"origin_seq"`
		Duplicate bool   `json:"duplicate,omitempty"`
		Reason    string `json:"reason,omitempty"`
	}
	var accepted, rejected []result
	for _, e := range req.Envelopes {
		if e.Origin != caller.ID || e.Tier != caller.Tier || !e2e.VerifyEnvelope(caller.Pub, e) {
			rejected = append(rejected, result{OriginSeq: e.OriginSeq, Reason: "forbidden"})
			continue
		}
		k := fmt.Sprintf("%s/%s/%s/%020d", e.Mesh, e.Drive, e.Origin, e.OriginSeq)
		if _, ok := s.log[k]; ok {
			accepted = append(accepted, result{OriginSeq: e.OriginSeq, Duplicate: true})
			continue
		}
		s.log[k] = e
		accepted = append(accepted, result{OriginSeq: e.OriginSeq})
		for id := range s.members {
			if id != caller.ID {
				s.enqueueLocked(id, queued{envelope: &e})
			}
		}
	}
	reply(w, map[string]any{"accepted": accepted, "rejected": rejected})
}

func (s *Server) pull(w http.ResponseWriter, r *http.Request, caller Member, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.queues[caller.ID]
	n := min(len(q), 10)
	msgs := make([]map[string]any, 0, n)
	if s.inflight[caller.ID] == nil {
		s.inflight[caller.ID] = map[string]queued{}
	}
	for _, m := range q[:n] {
		s.inflight[caller.ID][m.receipt] = m
		msg := map[string]any{"receipt": m.receipt}
		if m.envelope != nil {
			msg["envelope"] = m.envelope
		}
		if m.control != nil {
			msg["control"] = m.control
		}
		msgs = append(msgs, msg)
	}
	s.queues[caller.ID] = q[n:]
	reply(w, map[string]any{"messages": msgs})
}

func (s *Server) ack(w http.ResponseWriter, r *http.Request, caller Member, body []byte) {
	var req struct {
		Receipts []string `json:"receipts"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		fail(w, 400, "bad_request", err.Error())
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rc := range req.Receipts {
		delete(s.inflight[caller.ID], rc)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) Replay(node string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.log {
		if e.Origin != node {
			s.enqueueLocked(node, queued{envelope: &e})
		}
	}
}

func (s *Server) ForgeControl(c e2e.Control) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	real := s.key
	s.key = key
	s.controlLocked(c)
	s.key = real
}

func (s *Server) putKeys(w http.ResponseWriter, r *http.Request, caller Member, body []byte) {
	epoch, err := strconv.ParseUint(r.PathValue("epoch"), 10, 64)
	if err != nil {
		fail(w, 400, "bad_request", err.Error())
		return
	}
	var req struct {
		Mesh  string     `json:"mesh"`
		Wraps []e2e.Wrap `json:"wraps"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		fail(w, 400, "bad_request", err.Error())
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if caller.Tier != e2e.TierOwner {
		fail(w, 403, "forbidden", "owner only")
		return
	}
	for _, wr := range req.Wraps {
		if _, ok := s.members[wr.To]; !ok || wr.From != caller.ID || wr.Epoch != epoch {
			fail(w, 400, "bad_request", "wrap does not match")
			return
		}
	}
	switch epoch {
	case s.epoch + 1:
		s.epoch = epoch
		s.wraps[epoch] = req.Wraps
		s.controlLocked(e2e.Control{Type: e2e.ControlRekey, Mesh: req.Mesh, By: caller.ID, Epoch: epoch})
	case s.epoch:
		s.wraps[epoch] = append(s.wraps[epoch], req.Wraps...)
	default:
		fail(w, 409, "conflict", fmt.Sprintf("epoch %d, current %d", epoch, s.epoch))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) getKeys(w http.ResponseWriter, r *http.Request, caller Member, body []byte) {
	since, _ := strconv.ParseUint(r.URL.Query().Get("since"), 10, 64)
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []e2e.Wrap{}
	for e, ws := range s.wraps {
		if e < since {
			continue
		}
		for _, wr := range ws {
			if wr.To == caller.ID {
				out = append(out, wr)
			}
		}
	}
	reply(w, map[string]any{"epoch": s.epoch, "wraps": out})
}

func (s *Server) listMembers(w http.ResponseWriter, r *http.Request, caller Member, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []Member{}
	for _, m := range s.members {
		out = append(out, m)
	}
	reply(w, map[string]any{"epoch": s.epoch, "members": out})
}

func (s *Server) status(w http.ResponseWriter, r *http.Request, caller Member, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	reply(w, map[string]any{"node": caller.ID, "tier": caller.Tier, "queue": "cci-node-" + caller.ID, "queue_depth": len(s.queues[caller.ID]), "epoch": s.epoch})
}

func reply(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		panic(err)
	}
}

func fail(w http.ResponseWriter, code int, kind, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": kind, "message": msg})
}
