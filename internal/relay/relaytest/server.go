package relaytest

import (
	"bytes"
	"cmp"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/e2e"
)

const (
	maxBody = 1 << 20
	maxPush = 25
)

type Member struct {
	ID         string `json:"id"`
	Pub        []byte `json:"pub"`
	Name       string `json:"name"`
	Tier       string `json:"tier"`
	Allow      *Allow `json:"allow,omitempty"`
	By         string `json:"by"`
	At         string `json:"at"`
	keysFrom   string
	delegation *e2e.Delegation
	previous   *e2e.Delegation
}

func (m Member) carries(d *e2e.Delegation) bool {
	switch {
	case m.delegation == nil || d == nil:
		return m.delegation == nil && d == nil
	case bytes.Equal(m.delegation.Sig, d.Sig):
		return true
	}
	return m.previous != nil && bytes.Equal(m.previous.Sig, d.Sig)
}

type Allow struct {
	Drives []string `json:"drives"`
	Kinds  []string `json:"kinds"`
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
	pairs    map[string]*pairCode
	consent  map[string][]byte
	nonces   map[string]string
	issuers  map[string]string
	rings    map[string]*fakeRing
	ringDown bool
	Pushes   [][]byte
	Proofs   []json.RawMessage
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
		rings:    map[string]*fakeRing{},
		log:      map[string]e2e.Envelope{},
		pairs:    map[string]*pairCode{},
		consent:  map[string][]byte{},
		nonces:   map[string]string{},
		issuers:  map[string]string{},
	}
	s.Mux.HandleFunc("GET /v1/relay", func(w http.ResponseWriter, r *http.Request) {
		reply(w, map[string]any{"pub": s.Pub(), "fingerprint": ""})
	})
	s.Mux.HandleFunc("POST /v1/join", s.signed(s.join))
	s.Mux.HandleFunc("POST /v1/delegate", s.signed(s.delegate))
	s.Mux.HandleFunc("GET /v1/ring", s.signed(s.getRing))
	s.Mux.HandleFunc("PUT /v1/ring", s.signed(s.putRing))
	s.Mux.HandleFunc("POST /v1/push", s.signed(s.push))
	s.Mux.HandleFunc("GET /v1/pull", s.signed(s.pull))
	s.Mux.HandleFunc("POST /v1/ack", s.signed(s.ack))
	s.Mux.HandleFunc("PUT /v1/keys/{epoch}", s.signed(s.putKeys))
	s.Mux.HandleFunc("GET /v1/keys", s.signed(s.getKeys))
	s.Mux.HandleFunc("GET /v1/members", s.signed(s.listMembers))
	s.Mux.HandleFunc("GET /v1/status", s.signed(s.status))
	s.Mux.HandleFunc("GET /v1/log/origins", s.signed(s.logOrigins))
	s.Mux.HandleFunc("GET /v1/log", s.signed(s.readLog))
	s.pairRoutes()
	s.enrollRoutes()
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

func (s *Server) AddService(id e2e.Identity, allow Allow) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.members[id.ID()] = Member{ID: id.ID(), Pub: id.PublicKey(), Tier: e2e.TierService, Allow: &allow, At: time.Now().UTC().Format(time.RFC3339Nano)}
}

func (s *Server) Epoch() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.epoch
}

func (s *Server) Ring(owner string) (uint64, []uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	held := s.ringOfLocked(owner)
	return held.gen, slices.Sorted(maps.Keys(held.epochs))
}

func (s *Server) FailRingWrites(down bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ringDown = down
}

func (s *Server) InjectWrap(w e2e.Wrap) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.wraps[w.Epoch] = append([]e2e.Wrap{w}, s.wraps[w.Epoch]...)
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
		if len(body) > maxBody {
			fail(w, 400, "bad_request", fmt.Sprintf("the body exceeds %d bytes", maxBody))
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
		if r.URL.Path == "/v1/join" && (!ok || m.delegation != nil) {
			m, ok = s.delegatedLocked(body)
		}
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

func (s *Server) delegatedLocked(body []byte) (Member, bool) {
	var req struct {
		Delegation *e2e.Delegation `json:"delegation"`
	}
	if json.Unmarshal(body, &req) != nil || req.Delegation == nil {
		return Member{}, false
	}
	d := req.Delegation
	svc, ok := s.members[d.Service]
	if !ok || svc.Tier != e2e.TierService && svc.Tier != e2e.TierOwner || !e2e.VerifyDelegation(svc.Pub, *d) || d.Instance != e2e.NodeID(d.InstancePub) {
		return Member{}, false
	}
	if svc.Tier == e2e.TierOwner {
		m, ok := s.members[d.Instance]
		return m, ok && m.delegation != nil && bytes.Equal(m.delegation.Sig, d.Sig)
	}
	m := Member{ID: d.Instance, Pub: d.InstancePub, Tier: e2e.TierService, By: d.Service, delegation: d, keysFrom: d.Service}
	s.members[m.ID] = m
	return m, true
}

func (s *Server) delegate(w http.ResponseWriter, r *http.Request, caller Member, body []byte) {
	var req struct {
		Delegation e2e.Delegation `json:"delegation"`
		RingWrap   e2e.Wrap       `json:"ring_wrap"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		fail(w, 400, "bad_request", err.Error())
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d := req.Delegation
	if caller.Tier != e2e.TierOwner || d.Service != caller.ID || !e2e.VerifyDelegation(caller.Pub, d) || d.Instance != e2e.NodeID(d.InstancePub) {
		fail(w, 403, "forbidden", "an owner registers only a delegation it signed")
		return
	}
	if have, ok := s.members[d.Instance]; ok && have.delegation == nil {
		fail(w, 409, "conflict", d.Instance+" is a member, not an instance")
		return
	}
	if wr := req.RingWrap; wr.To != d.Instance || wr.From != caller.ID || !strings.HasSuffix(wr.Mesh, "#ring") {
		fail(w, 400, "bad_request", "ring wrap does not match")
		return
	}
	held := s.ringOfLocked(caller.ID)
	if held.gen == 0 || req.RingWrap.Epoch != held.gen {
		fail(w, 409, "conflict", fmt.Sprintf("ring %d, current %d", req.RingWrap.Epoch, held.gen))
		return
	}
	renewed := Member{ID: d.Instance, Pub: d.InstancePub, Tier: e2e.TierService, By: caller.ID, At: time.Now().UTC().Format(time.RFC3339Nano), delegation: &d}
	if have, ok := s.members[d.Instance]; ok && !bytes.Equal(have.delegation.Sig, d.Sig) {
		renewed.previous = have.delegation
	}
	s.members[d.Instance] = renewed
	held.to[d.Instance] = req.RingWrap
	w.WriteHeader(http.StatusNoContent)
}

type fakeRing struct {
	gen    uint64
	epochs map[uint64]e2e.SealedEpoch
	to     map[string]e2e.Wrap
}

func (s *Server) ringOfLocked(issuer string) *fakeRing {
	if s.rings[issuer] == nil {
		s.rings[issuer] = &fakeRing{epochs: map[uint64]e2e.SealedEpoch{}, to: map[string]e2e.Wrap{}}
	}
	return s.rings[issuer]
}

func (s *Server) ringLocked(issuer, id string, since uint64) map[string]any {
	held := s.ringOfLocked(issuer)
	epochs := []e2e.SealedEpoch{}
	for _, e := range slices.Sorted(maps.Keys(held.epochs)) {
		if e >= since {
			epochs = append(epochs, held.epochs[e])
		}
	}
	out := map[string]any{"gen": held.gen, "epochs": epochs}
	if wr, ok := held.to[id]; ok && wr.Epoch == held.gen {
		out["wrap"] = wr
	}
	return out
}

func (s *Server) getRing(w http.ResponseWriter, r *http.Request, caller Member, _ []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if caller.Tier != e2e.TierOwner {
		fail(w, 403, "forbidden", "owner only")
		return
	}
	reply(w, s.ringLocked(caller.ID, caller.ID, 0))
}

type ringRequest struct {
	Gen    uint64            `json:"gen"`
	Epochs []e2e.SealedEpoch `json:"epochs"`
	Wraps  []struct {
		Wrap e2e.Wrap `json:"wrap"`
	} `json:"wraps"`
}

func (s *Server) putRing(w http.ResponseWriter, r *http.Request, caller Member, body []byte) {
	var req ringRequest
	if err := json.Unmarshal(body, &req); err != nil {
		fail(w, 400, "bad_request", err.Error())
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case caller.Tier != e2e.TierOwner:
		fail(w, 403, "forbidden", "owner only")
		return
	case s.ringDown:
		fail(w, 503, "unavailable", "ring writes are down")
		return
	}
	next, ok := s.nextRingLocked(w, caller.ID, s.epoch, req)
	if !ok {
		return
	}
	s.rings[caller.ID] = next
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) nextRingLocked(w http.ResponseWriter, owner string, epoch uint64, req ringRequest) (*fakeRing, bool) {
	switch {
	case req.Gen != s.ringOfLocked(owner).gen+1:
		fail(w, 409, "conflict", fmt.Sprintf("ring %d, current %d", req.Gen, s.ringOfLocked(owner).gen))
		return nil, false
	case !slices.ContainsFunc(req.Epochs, func(e e2e.SealedEpoch) bool { return e.Epoch == epoch }):
		fail(w, 400, "bad_request", "the ring misses the current epoch")
		return nil, false
	}
	next := &fakeRing{gen: req.Gen, epochs: map[uint64]e2e.SealedEpoch{}, to: map[string]e2e.Wrap{}}
	for _, e := range req.Epochs {
		next.epochs[e.Epoch] = e
	}
	for _, m := range req.Wraps {
		next.to[m.Wrap.To] = m.Wrap
	}
	return next, true
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
	if len(req.Envelopes) == 0 || len(req.Envelopes) > maxPush {
		fail(w, 400, "bad_request", fmt.Sprintf("push carries 1 to %d envelopes, not %d", maxPush, len(req.Envelopes)))
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Pushes = append(s.Pushes, bytes.Clone(body))
	if caller.Tier == e2e.TierPaired {
		delete(s.members, caller.ID)
		delete(s.queues, caller.ID)
		s.controlLocked(e2e.Control{Type: e2e.ControlUnpaired, Mesh: req.Mesh, Node: caller.ID, Reason: "violation", By: "relay", Epoch: s.epoch})
		fail(w, 403, "unpaired", "paired members are read-only")
		return
	}
	for _, e := range req.Envelopes {
		if e.Epoch < s.epoch {
			fail(w, 409, "stale_epoch", fmt.Sprintf("epoch %d, current %d", e.Epoch, s.epoch))
			return
		}
	}
	type result struct {
		OriginSeq int64  `json:"origin_seq"`
		Duplicate bool   `json:"duplicate,omitempty"`
		Reason    string `json:"reason,omitempty"`
	}
	var accepted, rejected []result
	for _, e := range req.Envelopes {
		if e.Origin != caller.ID || e.Tier != caller.Tier || !e2e.VerifyEnvelope(caller.Pub, e) || !s.members[caller.ID].carries(e.Delegation) || e.Tier == e2e.TierOwner && strings.Contains(e.KindClass, ".") {
			rejected = append(rejected, result{OriginSeq: e.OriginSeq, Reason: "forbidden"})
			continue
		}
		k := fmt.Sprintf("%s/%s/%s/%020d", e.Mesh, e.Drive, e.Origin, e.OriginSeq)
		if stored, ok := s.log[k]; ok {
			if !reflect.DeepEqual(stored, e) {
				rejected = append(rejected, result{OriginSeq: e.OriginSeq, Reason: "conflict"})
				continue
			}
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
	answer := map[string]any{"messages": msgs}
	if m := s.members[caller.ID]; m.delegation != nil && m.keysFrom == "" {
		answer["delegation"] = m.delegation
	}
	reply(w, answer)
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
		Mesh   string       `json:"mesh"`
		Wraps  []e2e.Wrap   `json:"wraps"`
		Rotate bool         `json:"rotate"`
		Ring   *ringRequest `json:"ring"`
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
	var ring *fakeRing
	if req.Ring != nil {
		var ok bool
		if ring, ok = s.nextRingLocked(w, caller.ID, epoch, *req.Ring); !ok {
			return
		}
	}
	switch {
	case req.Rotate && epoch == s.epoch+1:
		s.epoch = epoch
		s.wraps[epoch] = req.Wraps
		if ring != nil {
			s.rings[caller.ID] = ring
		}
		s.controlLocked(e2e.Control{Type: e2e.ControlRekey, Mesh: req.Mesh, By: caller.ID, Epoch: epoch})
	case !req.Rotate && epoch == s.epoch:
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
	to := caller.ID
	if caller.keysFrom != "" {
		to = caller.keysFrom
	}
	var registered *e2e.Delegation
	if m := s.members[caller.ID]; m.delegation != nil && m.keysFrom == "" {
		registered = m.delegation
	}
	out := []e2e.Wrap{}
	for e, ws := range s.wraps {
		if e < since {
			continue
		}
		for _, wr := range ws {
			if wr.To == to {
				out = append(out, wr)
			}
		}
	}
	resp := map[string]any{"epoch": s.epoch, "wraps": out, "delegation": registered}
	if registered != nil && s.members[registered.Service].Tier == e2e.TierOwner {
		resp["ring"] = s.ringLocked(registered.Service, caller.ID, since)
	}
	reply(w, resp)
}

func (s *Server) listMembers(w http.ResponseWriter, r *http.Request, caller Member, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []Member{}
	for _, m := range s.members {
		if m.delegation == nil {
			out = append(out, m)
		}
	}
	reply(w, map[string]any{"epoch": s.epoch, "members": out})
}

func (s *Server) status(w http.ResponseWriter, r *http.Request, caller Member, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	reply(w, map[string]any{"node": caller.ID, "tier": caller.Tier, "queue": "cci-node-" + caller.ID, "queue_depth": len(s.queues[caller.ID]), "epoch": s.epoch})
}

func (s *Server) logOrigins(w http.ResponseWriter, r *http.Request, caller Member, body []byte) {
	q := r.URL.Query()
	s.mu.Lock()
	defer s.mu.Unlock()
	last := map[string]int64{}
	for _, e := range s.log {
		if e.Mesh == q.Get("mesh") && e.Drive == q.Get("drive") {
			last[e.Origin] = max(last[e.Origin], e.OriginSeq)
		}
	}
	out := make([]map[string]any, 0, len(last))
	for o, n := range last {
		out = append(out, map[string]any{"origin": o, "last": n})
	}
	reply(w, map[string]any{"origins": out})
}

func (s *Server) readLog(w http.ResponseWriter, r *http.Request, caller Member, body []byte) {
	q := r.URL.Query()
	after, _ := strconv.ParseInt(q.Get("after"), 10, 64)
	limit, err := strconv.Atoi(q.Get("limit"))
	if err != nil || limit < 0 || limit > 100 {
		fail(w, 400, "bad_request", fmt.Sprintf("%q is not between 0 and 100", q.Get("limit")))
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []e2e.Envelope{}
	for _, e := range s.log {
		if e.Mesh == q.Get("mesh") && e.Drive == q.Get("drive") && e.Origin == q.Get("origin") && e.OriginSeq > after {
			out = append(out, e)
		}
	}
	slices.SortFunc(out, func(a, b e2e.Envelope) int { return cmp.Compare(a.OriginSeq, b.OriginSeq) })
	more := len(out) > limit
	if more {
		out = out[:limit]
	}
	reply(w, map[string]any{"envelopes": out, "more": more})
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
