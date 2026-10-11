package e2e

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

type Control struct {
	V      int             `json:"v"`
	Type   string          `json:"type"`
	Mesh   string          `json:"mesh"`
	Node   string          `json:"node"`
	Pub    []byte          `json:"pub"`
	Tier   string          `json:"tier"`
	Reason string          `json:"reason"`
	By     string          `json:"by"`
	Epoch  uint64          `json:"epoch"`
	At     string          `json:"at"`
	ID     string          `json:"id"`
	Sig    []byte          `json:"sig"`
	Grant  *Grant          `json:"grant,omitempty"`
	Raw    json.RawMessage `json:"-"`
}

func (c *Control) UnmarshalJSON(b []byte) error {
	type plain Control
	if err := json.Unmarshal(b, (*plain)(c)); err != nil {
		return err
	}
	c.Raw = append(json.RawMessage(nil), b...)
	return nil
}

func (c Control) Message() []byte {
	return []byte(joinLines("cci-mesh/control/v1", c.Type, c.Mesh, c.Node, b64(c.Pub), c.Tier, c.Reason, c.By, u64(c.Epoch), c.At, c.ID))
}

func (c Control) Verify(relayPub []byte) bool {
	return verify(relayPub, c.Message(), c.Sig)
}

const (
	ControlUnpaired = "peer.unpaired"
	ControlJoined   = "peer.joined"
	ControlRekey    = "rekey"
)

func RequestMessage(method, path, ts, nonce, bodySHA string) []byte {
	return []byte(joinLines("cci-relay/req/v1", method, path, ts, nonce, bodySHA))
}

func VerifyRequest(pub []byte, method, path, ts, nonce string, body, sig []byte) bool {
	sum := sha256.Sum256(body)
	return verify(pub, RequestMessage(method, path, ts, nonce, hex.EncodeToString(sum[:])), sig)
}

type Grant struct {
	By  string `json:"by"`
	At  string `json:"at"`
	Sig []byte `json:"sig"`
}

func GrantMessage(mesh, id string, pub []byte, tier, by, at string) []byte {
	return []byte(joinLines("cci-mesh/member/v1", mesh, id, b64(pub), tier, by, at))
}

func (g Grant) Verify(byPub []byte, mesh, id string, pub []byte, tier string) bool {
	return verify(byPub, GrantMessage(mesh, id, pub, tier, g.By, g.At), g.Sig)
}
