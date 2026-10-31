// Package service posts records to a relay mesh as a scoped service member
// that keeps no local store, such as a webhook receiver.
package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/yasyf/cc-inbox/e2e"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/node"
)

// Writer seals each record under the mesh's current group key, which an owner
// in Owners wrapped to Key, and pushes it to the relay at URL.
type Writer struct {
	URL    string
	Mesh   string
	Key    e2e.Identity
	Owners [][]byte
	Lane   string
	HTTP   *http.Client
	Now    func() time.Time
}

// Record is one record a service writes. ID makes a redelivery a duplicate.
type Record struct {
	ID      string
	Drive   string
	Kind    string
	At      time.Time
	Text    string
	Topic   string
	To      []string
	PRs     []string
	URL     string
	Outcome string
	Commit  string
	Attrs   map[string]string
}

// ParseKey reads a service key from the PKCS#8 PEM a secret store holds.
func ParseKey(pem []byte) (e2e.Identity, error) {
	return node.Parse(pem)
}

// Post seals r and pushes it, failing unless the relay accepts it.
func (w Writer) Post(ctx context.Context, r Record) error {
	kind, err := kinds.Parse(r.Kind)
	if err != nil {
		return err
	}
	key, err := w.groupKey(ctx)
	if err != nil {
		return err
	}
	now := w.Now()
	rec := row{
		Origin: w.Key.ID(), OriginSeq: now.UnixMicro(), ID: r.ID, Drive: r.Drive, Lane: w.Lane, Kind: string(kind), At: r.At.UTC(),
		Text: r.Text, Topic: r.Topic, To: r.To, Refs: refs{URL: r.URL, PRs: r.PRs},
		Fields: fields{Attrs: r.Attrs, Outcome: r.Outcome, Commit: r.Commit}, Source: "service",
	}
	pt, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	env, err := e2e.Seal(key, w.Key, e2e.Envelope{
		Mesh: w.Mesh, Drive: rec.Drive, OriginSeq: rec.OriginSeq, Tier: e2e.TierService,
		KindClass: rec.Kind, TS: rec.At.Format(time.RFC3339Nano),
	}, pt)
	if err != nil {
		return err
	}
	var pushed struct {
		Accepted []struct {
			OriginSeq int64 `json:"origin_seq"`
		} `json:"accepted"`
		Rejected []struct {
			Reason string `json:"reason"`
		} `json:"rejected"`
	}
	if err := w.call(ctx, http.MethodPost, "/v1/push", map[string]any{"mesh": w.Mesh, "envelopes": []e2e.Envelope{env}}, &pushed); err != nil {
		return err
	}
	if len(pushed.Rejected) > 0 {
		return fmt.Errorf("relay refused %s on %s: %s", rec.Kind, rec.Drive, pushed.Rejected[0].Reason)
	}
	if len(pushed.Accepted) != 1 {
		return errors.New("relay answered the push without accepting the record")
	}
	return nil
}

func (w Writer) groupKey(ctx context.Context) (e2e.GroupKey, error) {
	var keys struct {
		Wraps []e2e.Wrap `json:"wraps"`
	}
	if err := w.call(ctx, http.MethodGet, "/v1/keys?"+url.Values{"mesh": {w.Mesh}, "since": {"0"}}.Encode(), nil, &keys); err != nil {
		return e2e.GroupKey{}, err
	}
	owners := make(map[string]e2e.Member, len(w.Owners))
	for _, pub := range w.Owners {
		id := e2e.NodeID(pub)
		owners[id] = e2e.Member{Node: id, PublicKey: pub, Tier: e2e.TierOwner}
	}
	var best e2e.GroupKey
	var errs []error
	for _, wrap := range keys.Wraps {
		k, err := e2e.UnwrapKey(wrap, w.Mesh, w.Key, owners)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if k.Epoch > best.Epoch {
			best = k
		}
	}
	if best.Epoch == 0 {
		return e2e.GroupKey{}, errors.Join(fmt.Errorf("%w: no wrap to %s from a pinned owner in mesh %s", e2e.ErrNoGroupKey, w.Key.ID(), w.Mesh), errors.Join(errs...))
	}
	return best, nil
}

func (w Writer) call(ctx context.Context, method, path string, in, out any) error {
	var body []byte
	if in != nil {
		var err error
		if body, err = json.Marshal(in); err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimSuffix(w.URL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if err := e2e.SignRequest(w.Key, req, path, body, w.Now()); err != nil {
		return err
	}
	resp, err := w.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("relay %s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("relay %s %s answered %d: %s", method, path, resp.StatusCode, raw)
	}
	return json.Unmarshal(raw, out)
}

type row struct {
	Origin    string    `json:"origin"`
	OriginSeq int64     `json:"origin_seq"`
	ID        string    `json:"id,omitempty"`
	Drive     string    `json:"drive"`
	Lane      string    `json:"lane"`
	Kind      string    `json:"kind"`
	At        time.Time `json:"at"`
	Text      string    `json:"text"`
	Topic     string    `json:"topic,omitempty"`
	To        []string  `json:"to,omitempty"`
	Refs      refs      `json:"refs"`
	Fields    fields    `json:"fields"`
	Source    string    `json:"source"`
}

type refs struct {
	URL string   `json:"url,omitempty"`
	PRs []string `json:"prs,omitempty"`
}

type fields struct {
	Attrs   map[string]string `json:"attrs,omitempty"`
	Outcome string            `json:"outcome,omitempty"`
	Commit  string            `json:"commit,omitempty"`
}
