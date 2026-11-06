package relay

import (
	"time"

	"github.com/yasyf/cc-inbox/e2e"
)

type relayKey struct {
	Pub         []byte `json:"pub"`
	Fingerprint string `json:"fingerprint"`
}

type joinRequest struct {
	Mesh       string          `json:"mesh"`
	Pub        []byte          `json:"pub"`
	Delegation *e2e.Delegation `json:"delegation,omitempty"`
}

type joinResponse struct {
	Node  string `json:"node"`
	Tier  string `json:"tier"`
	Queue string `json:"queue"`
	Epoch uint64 `json:"epoch"`
}

type pushRequest struct {
	Mesh      string         `json:"mesh"`
	Envelopes []e2e.Envelope `json:"envelopes"`
}

type pushResult struct {
	OriginSeq int64  `json:"origin_seq"`
	Duplicate bool   `json:"duplicate,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

type pushResponse struct {
	Accepted []pushResult `json:"accepted"`
	Rejected []pushResult `json:"rejected"`
}

type message struct {
	Receipt  string        `json:"receipt"`
	Envelope *e2e.Envelope `json:"envelope,omitempty"`
	Control  *e2e.Control  `json:"control,omitempty"`
}

type pullResponse struct {
	Messages   []message       `json:"messages"`
	Delegation *e2e.Delegation `json:"delegation,omitempty"`
}

type ackRequest struct {
	Mesh     string   `json:"mesh"`
	Receipts []string `json:"receipts"`
	Tap      string   `json:"tap,omitempty"`
}

type keysRequest struct {
	Mesh   string     `json:"mesh"`
	Wraps  []e2e.Wrap `json:"wraps"`
	Rotate bool       `json:"rotate,omitempty"`
}

type keysResponse struct {
	Epoch      uint64          `json:"epoch"`
	Wraps      []e2e.Wrap      `json:"wraps"`
	Ring       *ringResponse   `json:"ring,omitempty"`
	Delegation *e2e.Delegation `json:"delegation,omitempty"`
}

type ringResponse struct {
	Gen    uint64            `json:"gen"`
	Wrap   *e2e.Wrap         `json:"wrap,omitempty"`
	Epochs []e2e.SealedEpoch `json:"epochs"`
}

type ringMemberWrap struct {
	Wrap    e2e.Wrap `json:"wrap"`
	Expires string   `json:"expires,omitempty"`
}

type ringRequest struct {
	Mesh   string            `json:"mesh"`
	Gen    uint64            `json:"gen"`
	Epochs []e2e.SealedEpoch `json:"epochs"`
	Wraps  []ringMemberWrap  `json:"wraps"`
}

type delegateRequest struct {
	Mesh       string         `json:"mesh"`
	Delegation e2e.Delegation `json:"delegation"`
	RingWrap   e2e.Wrap       `json:"ring_wrap"`
}

type RemoteMember struct {
	ID    string    `json:"id"`
	Pub   []byte    `json:"pub"`
	Name  string    `json:"name"`
	Tier  string    `json:"tier"`
	Allow *Allow    `json:"allow,omitempty"`
	By    string    `json:"by"`
	At    time.Time `json:"at"`
}

type Allow struct {
	Drives []string `json:"drives"`
	Kinds  []string `json:"kinds"`
}

type membersResponse struct {
	Epoch   uint64         `json:"epoch"`
	Members []RemoteMember `json:"members"`
}

type unpairRequest struct {
	Mesh string `json:"mesh"`
	Node string `json:"node"`
}

type RemoteStatus struct {
	Node       string `json:"node"`
	Tier       string `json:"tier"`
	Queue      string `json:"queue"`
	QueueDepth int    `json:"queue_depth"`
	Epoch      uint64 `json:"epoch"`
}

type apiError struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}
