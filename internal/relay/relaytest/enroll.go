package relaytest

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"time"

	"github.com/yasyf/cc-inbox/e2e"
	"github.com/yasyf/cc-inbox/internal/node"
)

func (s *Server) enrollRoutes() {
	s.Mux.HandleFunc("POST /v1/found", s.found)
	s.Mux.HandleFunc("POST /v1/nonce", s.signed(s.nonce))
	s.Mux.HandleFunc("POST /v1/enroll", s.signed(s.enroll))
}

func (s *Server) found(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Mesh, Name  string
		Pub         []byte
		Endorsement node.Endorsement
		Proof       json.RawMessage
	}
	body, ok := readJSON(w, r, &req)
	if !ok || !s.selfSigned(w, r, req.Pub, body) {
		return
	}
	if len(req.Proof) == 0 || string(req.Proof) == "null" {
		fail(w, 403, "forbidden", "no caller proof")
		return
	}
	if err := node.VerifyEndorsement(req.Endorsement, req.Pub); err != nil {
		fail(w, 400, "bad_request", err.Error())
		return
	}
	consent, err := base64.StdEncoding.DecodeString(req.Endorsement.PublicKey)
	if err != nil {
		fail(w, 400, "bad_request", err.Error())
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Proofs = append(s.Proofs, req.Proof)
	id := node.IDOf(req.Pub)
	s.members[id] = Member{ID: id, Pub: req.Pub, Name: req.Name, Tier: e2e.TierOwner, By: "founder", At: time.Now().UTC().Format(time.RFC3339Nano)}
	s.consent[req.Endorsement.KeyID] = consent
	reply(w, map[string]any{"member": s.members[id]})
}

func (s *Server) nonce(w http.ResponseWriter, r *http.Request, caller Member, _ []byte) {
	if caller.Tier != e2e.TierOwner {
		fail(w, 403, "forbidden", "owner only")
		return
	}
	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		panic(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nonces[base64.StdEncoding.EncodeToString(nonce)] = caller.ID
	reply(w, map[string]any{"nonce": nonce, "expires_at": time.Now().Add(5 * time.Minute)})
}

func (s *Server) enroll(w http.ResponseWriter, r *http.Request, caller Member, body []byte) {
	var req struct {
		Mesh, Name, Tier string
		Pub, Nonce       []byte
		Allow            *struct {
			Drives, Kinds []string
			Label         string
		}
		Endorsement *node.Endorsement
		Attestation attestation
		Grant       *e2e.Grant
	}
	if err := json.Unmarshal(body, &req); err != nil {
		fail(w, 400, "bad_request", err.Error())
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	nonce := base64.StdEncoding.EncodeToString(req.Nonce)
	scope := ""
	if req.Allow != nil {
		scope = req.Allow.Label + "|" + join(req.Allow.Drives) + "|" + join(req.Allow.Kinds)
	}
	argv := []string{"cci", "mesh", "enroll", node.FingerprintOf(req.Pub), req.Tier, scope}
	switch {
	case caller.Tier != e2e.TierOwner:
		fail(w, 403, "forbidden", "owner only")
	case s.nonces[nonce] != caller.ID:
		fail(w, 403, "forbidden", "unknown or spent nonce")
	case (req.Tier == e2e.TierService) != (req.Allow != nil):
		fail(w, 400, "bad_request", "a service member and only a service member carries a scope")
	case req.Grant == nil:
		fail(w, 400, "bad_request", "an enrollment carries the enrolling owner's grant")
	case req.Grant.By != caller.ID || !req.Grant.Verify(caller.Pub, req.Mesh, node.IDOf(req.Pub), req.Pub, req.Tier):
		fail(w, 403, "forbidden", "the grant does not verify against the enrolling owner")
	case !s.attests(req.Attestation, nonce, argv):
		fail(w, 403, "forbidden", "attestation does not verify against an enrolled consent key")
	default:
		delete(s.nonces, nonce)
		id := node.IDOf(req.Pub)
		s.members[id] = Member{ID: id, Pub: req.Pub, Name: req.Name, Tier: req.Tier, By: caller.ID, At: time.Now().UTC().Format(time.RFC3339Nano)}
		s.controlLocked(e2e.Control{Type: e2e.ControlJoined, Mesh: req.Mesh, Node: id, Pub: req.Pub, Tier: req.Tier, By: caller.ID, Grant: req.Grant})
		reply(w, map[string]any{"member": s.members[id]})
	}
}
