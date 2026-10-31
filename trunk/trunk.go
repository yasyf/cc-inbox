// Package trunk turns Trunk merge-queue webhooks, which Svix signs, into one
// queue record per pull request state change.
package trunk

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/yasyf/cc-inbox/service"
)

// Tolerance is how far a delivery's signed timestamp may sit from now.
const Tolerance = 5 * time.Minute

var ErrSignature = errors.New("trunk: webhook signature does not verify")

// Verify checks a Svix signature: HMAC-SHA256 under the whsec_ secret over
// "<svix-id>.<svix-timestamp>.<body>", any of the space-separated v1 entries.
func Verify(secret string, header http.Header, body []byte, now time.Time) error {
	key, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(secret, "whsec_"))
	if err != nil {
		return fmt.Errorf("trunk: webhook secret: %w", err)
	}
	id, ts := header.Get("svix-id"), header.Get("svix-timestamp")
	sec, err := strconv.ParseInt(ts, 10, 64)
	if err != nil || id == "" {
		return ErrSignature
	}
	if d := now.Sub(time.Unix(sec, 0)); d > Tolerance || d < -Tolerance {
		return fmt.Errorf("%w: signed %s ago", ErrSignature, d.Round(time.Second))
	}
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(id + "." + ts + "."))
	mac.Write(body)
	want := mac.Sum(nil)
	for _, entry := range strings.Fields(header.Get("svix-signature")) {
		version, sig, _ := strings.Cut(entry, ",")
		got, err := base64.StdEncoding.DecodeString(sig)
		if version == "v1" && err == nil && hmac.Equal(got, want) {
			return nil
		}
	}
	return ErrSignature
}

// Event is a pull_request.* delivery; batch and test events carry no pr_number.
type Event struct {
	Type       string    `json:"type"`
	Action     string    `json:"action"`
	PR         int       `json:"pr_number"`
	Timestamp  time.Time `json:"timestamp"`
	URL        string    `json:"http_details_url"`
	Target     string    `json:"target_branch"`
	TestSHA    string    `json:"test_branch_sha"`
	Repository struct {
		Owner string `json:"owner"`
		Name  string `json:"name"`
	} `json:"repository"`
	QueuedReason       string `json:"queued_reason"`
	FailureReason      string `json:"failure_reason"`
	CancellationReason string `json:"cancellation_reason"`
	Failing            []struct {
		Name       string `json:"name"`
		Conclusion string `json:"conclusion"`
		URL        string `json:"http_url"`
	} `json:"failure_causing_statuses"`
}

func (e Event) reason() string {
	for _, r := range []string{e.FailureReason, e.CancellationReason, e.QueuedReason} {
		if r != "" {
			return r
		}
	}
	return ""
}

// Record is e as the queue record for delivery id, false for a delivery
// about no single pull request.
func (e Event) Record(id, drive string) (service.Record, bool) {
	if !strings.HasPrefix(e.Type, "pull_request.") || e.PR <= 0 {
		return service.Record{}, false
	}
	pr := fmt.Sprintf("%s/%s#%d", e.Repository.Owner, e.Repository.Name, e.PR)
	text := fmt.Sprintf("%s %s in Trunk's merge queue", pr, e.Action)
	attrs := map[string]string{"target": e.Target}
	if r := e.reason(); r != "" {
		text += ": " + r
		attrs["reason"] = r
	}
	if len(e.Failing) > 0 {
		failing := make([]string, 0, len(e.Failing))
		for _, s := range e.Failing {
			failing = append(failing, fmt.Sprintf("%s %s %s", s.Name, s.Conclusion, s.URL))
		}
		text += "; failing: " + strings.Join(failing, ", ")
		attrs["failing"] = strings.Join(failing, ", ")
	}
	return service.Record{
		ID: "trunk:" + id, Drive: drive, Kind: "queue", At: e.Timestamp, Text: text,
		PRs: []string{pr}, URL: e.URL, Outcome: e.Action, Commit: e.TestSHA, Attrs: attrs,
	}, true
}

// Handler is the webhook endpoint: it verifies each delivery and posts its
// record to Drive through Writer.
type Handler struct {
	Secret string
	Writer service.Writer
	Drive  string
}

// Handle answers one delivery with the status Svix reads: 2xx settles it, and
// any other code has Svix retry it later.
func (h Handler) Handle(ctx context.Context, header http.Header, body []byte) int {
	if err := Verify(h.Secret, header, body, h.Writer.Now()); err != nil {
		slog.Warn("trunk webhook refused", "err", err)
		return http.StatusUnauthorized
	}
	var e Event
	if err := json.Unmarshal(body, &e); err != nil {
		slog.Warn("trunk webhook undecodable", "err", err)
		return http.StatusBadRequest
	}
	r, ok := e.Record(header.Get("svix-id"), h.Drive)
	if !ok {
		return http.StatusNoContent
	}
	if err := h.Writer.Post(ctx, r); err != nil {
		slog.Error("trunk webhook not relayed", "type", e.Type, "pr", e.PR, "err", err)
		return http.StatusBadGateway
	}
	slog.Info("trunk webhook relayed", "type", e.Type, "pr", e.PR)
	return http.StatusOK
}
