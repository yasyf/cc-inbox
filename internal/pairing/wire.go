package pairing

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"github.com/yasyf/cc-inbox/e2e"
	"github.com/yasyf/cc-inbox/internal/node"
	"github.com/yasyf/cc-inbox/internal/relay"
)

type joinRequest struct {
	Mesh        string `json:"mesh"`
	Code        string `json:"code"`
	Pub         []byte `json:"pub"`
	Fingerprint string `json:"fingerprint"`
}

type joinReply struct {
	ExpiresAt time.Time `json:"expires_at"`
}

type keyRef struct {
	ID  string `json:"id"`
	Pub []byte `json:"pub"`
}

type statusReply struct {
	Status    string     `json:"status"`
	Owner     *keyRef    `json:"owner"`
	Wrap      *e2e.Wrap  `json:"wrap"`
	RelayPub  []byte     `json:"relay_pub"`
	Roster    []keyRef   `json:"roster"`
	RosterSig []byte     `json:"roster_sig"`
	Grant     *e2e.Grant `json:"grant"`
	Tier      string     `json:"tier"`
}

type pending struct {
	Fingerprint string    `json:"fingerprint"`
	Pub         []byte    `json:"pub"`
	Mesh        string    `json:"mesh"`
	Nonce       string    `json:"nonce"`
	ExpiresAt   time.Time `json:"expires_at"`
}

type Allow struct {
	Drives []string `json:"drives"`
	Kinds  []string `json:"kinds"`
	Label  string   `json:"label"`
}

type acceptRequest struct {
	Mesh        string      `json:"mesh"`
	Code        string      `json:"code"`
	Fingerprint string      `json:"fingerprint"`
	Argv        []string    `json:"argv"`
	Nonce       string      `json:"nonce"`
	Attestation Attestation `json:"attestation"`
	Wrap        e2e.Wrap    `json:"wrap"`
	Tier        node.Tier   `json:"tier"`
	Allow       *Allow      `json:"allow,omitempty"`
	Name        string      `json:"name,omitempty"`
	Grant       e2e.Grant   `json:"grant"`
	Roster      []keyRef    `json:"roster"`
	RosterSig   []byte      `json:"roster_sig"`
}

type accepted struct {
	Node string    `json:"node"`
	Tier node.Tier `json:"tier"`
	By   string    `json:"by"`
	At   time.Time `json:"at"`
}

type unpairRequest struct {
	Mesh string `json:"mesh"`
	Node string `json:"node"`
}

type unpairReply struct {
	At time.Time `json:"at"`
}

type Directory struct {
	ID    string    `json:"id"`
	Pub   []byte    `json:"pub"`
	Name  string    `json:"name"`
	Tier  node.Tier `json:"tier"`
	Allow *Allow    `json:"allow,omitempty"`
	By    string    `json:"by"`
	At    time.Time `json:"at"`
}

type membersReply struct {
	Epoch   uint64      `json:"epoch"`
	Members []Directory `json:"members"`
}

type relayKey struct {
	Pub         []byte `json:"pub"`
	Fingerprint string `json:"fingerprint"`
}

func fetchRelayKey(ctx context.Context, c relay.Client) ([]byte, error) {
	var k relayKey
	if err := c.Call(ctx, http.MethodGet, "/v1/relay", nil, &k); err != nil {
		return nil, err
	}
	return k.Pub, nil
}

func statusPath(mesh, code string, pub []byte) string {
	q := url.Values{"mesh": {mesh}, "code": {code}, "pub": {b64(pub)}}
	return "/v1/pair/status?" + q.Encode()
}

func lookupPath(mesh, code string) string {
	return "/v1/pair/" + code + "?" + url.Values{"mesh": {mesh}}.Encode()
}

func FetchDirectory(ctx context.Context, c relay.Client) ([]Directory, error) {
	var r membersReply
	if err := c.Call(ctx, http.MethodGet, "/v1/members?"+url.Values{"mesh": {c.Config.Mesh}}.Encode(), nil, &r); err != nil {
		return nil, err
	}
	return r.Members, nil
}
