package relaytest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/yasyf/cc-inbox/e2e"
	"github.com/yasyf/cc-inbox/internal/node"
)

type keyRef struct {
	ID  string `json:"id"`
	Pub []byte `json:"pub"`
}

type pairCode struct {
	mesh, code, fingerprint, nonce string
	pub                            []byte
	expires                        time.Time
	owner                          *keyRef
	tier                           string
	grant                          *e2e.Grant
	wrap                           *e2e.Wrap
	roster                         []keyRef
	rosterSig                      []byte
}

type attestation struct {
	KeyID         string `json:"key_id"`
	Sig           string `json:"sig"`
	SignedBy      string `json:"signed_by"`
	RequestedFrom string `json:"requested_from"`
}

func (s *Server) pairRoutes() {
	s.Mux.HandleFunc("POST /v1/pair/join", s.pairJoin)
	s.Mux.HandleFunc("GET /v1/pair/status", s.pairStatus)
	s.Mux.HandleFunc("GET /v1/pair/{code}", s.signed(s.pairLookup))
	s.Mux.HandleFunc("POST /v1/pair/accept", s.signed(s.pairAccept))
	s.Mux.HandleFunc("POST /v1/unpair", s.signed(s.unpair))
}

func (s *Server) TrustConsentKey(pub []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sum := sha256.Sum256(pub)
	s.consent[hex.EncodeToString(sum[:])] = pub
}

func (s *Server) ExpirePairing(code string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pairs[code].expires = time.Now()
}

func (s *Server) selfSigned(w http.ResponseWriter, r *http.Request, pub, body []byte) bool {
	sig, err := base64.StdEncoding.DecodeString(r.Header.Get("X-CCI-Sig"))
	if err != nil || r.Header.Get("X-CCI-Node") != node.IDOf(pub) ||
		!e2e.VerifyRequest(pub, r.Method, r.URL.RequestURI(), r.Header.Get("X-CCI-Time"), r.Header.Get("X-CCI-Nonce"), body, sig) {
		fail(w, 401, "unsigned", "signature does not verify against the presented key")
		return false
	}
	return true
}

func (s *Server) pairJoin(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Mesh, Code, Fingerprint string
		Pub                     []byte
	}
	body, ok := readJSON(w, r, &req)
	if !ok || !s.selfSigned(w, r, req.Pub, body) {
		return
	}
	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		panic(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, taken := s.pairs[req.Code]; taken {
		fail(w, 409, "conflict", "code taken")
		return
	}
	p := &pairCode{mesh: req.Mesh, code: req.Code, fingerprint: req.Fingerprint, pub: req.Pub, nonce: base64.StdEncoding.EncodeToString(nonce), expires: time.Now().Add(10 * time.Minute)}
	s.pairs[req.Code] = p
	reply(w, map[string]any{"expires_at": p.expires})
}

func (s *Server) pairStatus(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	pub, err := base64.StdEncoding.DecodeString(q.Get("pub"))
	if err != nil || !s.selfSigned(w, r, pub, nil) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.pairs[q.Get("code")]
	if !ok || !slices.Equal(p.pub, pub) || p.mesh != q.Get("mesh") {
		fail(w, 404, "not_found", "no such code for this key")
		return
	}
	out := map[string]any{"status": "pending", "relay_pub": s.Pub()}
	switch {
	case p.owner != nil:
		out["status"], out["owner"], out["wrap"], out["roster"], out["roster_sig"] = "accepted", p.owner, p.wrap, p.roster, p.rosterSig
		out["tier"], out["grant"] = p.tier, p.grant
	case time.Now().After(p.expires):
		out["status"] = "expired"
	}
	reply(w, out)
}

func (s *Server) pairLookup(w http.ResponseWriter, r *http.Request, caller Member, _ []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.live(w, caller, r.PathValue("code"))
	if !ok {
		return
	}
	reply(w, map[string]any{"fingerprint": p.fingerprint, "pub": p.pub, "mesh": p.mesh, "nonce": p.nonce, "expires_at": p.expires})
}

func (s *Server) live(w http.ResponseWriter, caller Member, code string) (*pairCode, bool) {
	if caller.Tier != e2e.TierOwner {
		fail(w, 403, "forbidden", "owner only")
		return nil, false
	}
	p, ok := s.pairs[code]
	switch {
	case !ok || p.owner != nil:
		fail(w, 404, "not_found", "no live code")
		return nil, false
	case time.Now().After(p.expires):
		fail(w, 410, "expired", "code expired")
		return nil, false
	}
	return p, true
}

func (s *Server) pairAccept(w http.ResponseWriter, r *http.Request, caller Member, body []byte) {
	var req struct {
		Mesh, Code, Fingerprint, Nonce, Tier, Name string
		Argv                                       []string
		Attestation                                attestation
		Wrap                                       e2e.Wrap
		Grant                                      e2e.Grant
		Roster                                     []keyRef
		RosterSig                                  []byte `json:"roster_sig"`
		Allow                                      *struct {
			Drives, Kinds []string
			Label         string
		}
	}
	if err := json.Unmarshal(body, &req); err != nil {
		fail(w, 400, "bad_request", err.Error())
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.live(w, caller, req.Code)
	if !ok {
		return
	}
	argv := []string{"cci", "pair", "accept", p.code, p.fingerprint}
	if req.Tier == e2e.TierService {
		a := req.Allow
		argv = append(argv, e2e.TierService, a.Label+"|"+join(a.Drives)+"|"+join(a.Kinds), req.Name)
	}
	id := node.IDOf(p.pub)
	switch {
	case !slices.Equal(req.Argv, argv) || req.Nonce != p.nonce || req.Fingerprint != p.fingerprint:
		fail(w, 400, "bad_request", "argv, nonce or fingerprint does not match the code")
	case !s.attests(req.Attestation, p.nonce, argv):
		fail(w, 403, "forbidden", "attestation does not verify against an enrolled consent key")
	case req.Wrap.To != id || req.Wrap.From != caller.ID || req.Wrap.Epoch != s.epoch:
		fail(w, 400, "bad_request", "wrap does not match")
	default:
		p.owner = &keyRef{ID: caller.ID, Pub: caller.Pub}
		p.wrap, p.roster, p.rosterSig, p.tier, p.grant = &req.Wrap, req.Roster, req.RosterSig, req.Tier, &req.Grant
		at := time.Now().UTC().Format(time.RFC3339Nano)
		s.members[id] = Member{ID: id, Pub: p.pub, Name: req.Name, Tier: req.Tier, By: caller.ID, At: at}
		s.wraps[s.epoch] = append(s.wraps[s.epoch], req.Wrap)
		s.controlLocked(e2e.Control{Type: e2e.ControlJoined, Mesh: req.Mesh, Node: id, Pub: p.pub, Tier: req.Tier, By: caller.ID, Grant: &req.Grant})
		reply(w, map[string]any{"node": id, "tier": req.Tier, "by": caller.ID, "at": at})
	}
}

func (s *Server) attests(a attestation, nonceB64 string, argv []string) bool {
	pub, ok := s.consent[a.KeyID]
	nonce, err1 := base64.StdEncoding.DecodeString(nonceB64)
	sig, err2 := base64.StdEncoding.DecodeString(a.Sig)
	if !ok || err1 != nil || err2 != nil {
		return false
	}
	key, err := ecdsa.ParseUncompressedPublicKey(elliptic.P256(), pub)
	if err != nil {
		return false
	}
	digest := sha256.Sum256(append(nonce, ConsentSubject(argv, a.RequestedFrom)...))
	return ecdsa.VerifyASN1(key, digest[:], sig)
}

func ConsentSubject(argv []string, origin string) []byte {
	var msg []byte
	for _, a := range argv {
		msg = binary.BigEndian.AppendUint64(msg, uint64(len(a)))
		msg = append(msg, a...)
	}
	msg = append(append(msg, 0), origin...)
	sum := sha256.Sum256(msg)
	return sum[:]
}

func (s *Server) unpair(w http.ResponseWriter, r *http.Request, caller Member, body []byte) {
	var req struct{ Mesh, Node string }
	if err := json.Unmarshal(body, &req); err != nil {
		fail(w, 400, "bad_request", err.Error())
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	reason := "owner"
	switch {
	case req.Node == caller.ID:
		reason = "left"
	case caller.Tier != e2e.TierOwner:
		fail(w, 403, "forbidden", "owner only")
		return
	}
	if _, ok := s.members[req.Node]; !ok {
		fail(w, 404, "not_found", "no such member")
		return
	}
	delete(s.members, req.Node)
	delete(s.queues, req.Node)
	for e, ws := range s.wraps {
		s.wraps[e] = slices.DeleteFunc(ws, func(w e2e.Wrap) bool { return w.To == req.Node })
	}
	s.controlLocked(e2e.Control{Type: e2e.ControlUnpaired, Mesh: req.Mesh, Node: req.Node, Reason: reason, By: caller.ID, Epoch: s.epoch})
	reply(w, map[string]any{"at": time.Now().UTC()})
}

func readJSON(w http.ResponseWriter, r *http.Request, v any) ([]byte, bool) {
	body, err := io.ReadAll(r.Body)
	if err == nil {
		err = json.Unmarshal(body, v)
	}
	if err != nil {
		fail(w, 400, "bad_request", err.Error())
		return nil, false
	}
	return body, true
}

func join(xs []string) string { return strings.Join(xs, ",") }
