package trunk_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/e2e"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/relay"
	"github.com/yasyf/cc-inbox/internal/relay/relaytest"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/service"
	"github.com/yasyf/cc-inbox/trunk"
)

var secret = "whsec_" + base64.StdEncoding.EncodeToString([]byte("trunk webhook signing secret"))

const failed = `{"type":"pull_request.failed","action":"failed","status":"failed","pr_number":45700,
"timestamp":"2026-10-30T14:33:00Z","http_details_url":"https://app.trunk.io/acme/merge/app/45700",
"target_branch":"dev","test_branch_sha":"abc123","repository":{"host":"github.com","owner":"acme","name":"app"},
"failure_reason":"TEST_RUN_REQUIRED_STATUS_FAILED",
"failure_causing_statuses":[{"name":"buildkite/test","conclusion":"failure","http_url":"https://buildkite.com/acme/test/builds/9"}]}`

func sign(t *testing.T, key, id string, at time.Time, body string) http.Header {
	t.Helper()
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(key, "whsec_"))
	if err != nil {
		t.Fatal(err)
	}
	ts := strconv.FormatInt(at.Unix(), 10)
	mac := hmac.New(sha256.New, raw)
	mac.Write([]byte(id + "." + ts + "." + body))
	h := http.Header{}
	h.Set("svix-id", id)
	h.Set("svix-timestamp", ts)
	h.Set("svix-signature", "v1,"+base64.StdEncoding.EncodeToString(mac.Sum(nil)))
	return h
}

func TestVerify(t *testing.T) {
	now := time.Now()
	other := "whsec_" + base64.StdEncoding.EncodeToString([]byte("another secret"))
	rotated := sign(t, secret, "msg_1", now, failed)
	rotated.Set("svix-signature", sign(t, other, "msg_1", now, failed).Get("svix-signature")+" "+rotated.Get("svix-signature"))
	for _, tc := range []struct {
		name   string
		header http.Header
		body   string
		ok     bool
	}{
		{"signed", sign(t, secret, "msg_1", now, failed), failed, true},
		{"one of two signatures", rotated, failed, true},
		{"body changed", sign(t, secret, "msg_1", now, failed), strings.Replace(failed, "45700", "45701", 1), false},
		{"other secret", sign(t, other, "msg_1", now, failed), failed, false},
		{"stale", sign(t, secret, "msg_1", now.Add(-10*time.Minute), failed), failed, false},
		{"unsigned", http.Header{}, failed, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := trunk.Verify(secret, tc.header, []byte(tc.body), now)
			if tc.ok != (err == nil) || err != nil && !errors.Is(err, trunk.ErrSignature) {
				t.Fatalf("verify = %v, want ok=%v", err, tc.ok)
			}
		})
	}
}

func handler(t *testing.T) (trunk.Handler, *store.Store, *relay.Node, *relaytest.Server) {
	t.Helper()
	srv := relaytest.NewServer(t)
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(k)
	if err != nil {
		t.Fatal(err)
	}
	svc, err := e2e.ParseKey(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	if err != nil {
		t.Fatal(err)
	}
	st, owner := relaytest.JoinOwner(t, srv, svc)
	w := service.Writer{URL: srv.URL, Mesh: relay.DefaultMesh, Key: svc, Owners: [][]byte{owner.Identity.PublicKey()}, Lane: "trunk", HTTP: srv.Client(), Now: time.Now}
	return trunk.Handler{Secret: secret, Writer: w, Drive: "trunk"}, st, owner, srv
}

func queue(t *testing.T, st *store.Store, owner *relay.Node) []store.Record {
	t.Helper()
	if _, err := owner.Pull(context.Background(), 0, nil); err != nil {
		t.Fatal(err)
	}
	recs, err := st.Query(context.Background(), store.Filter{Drive: "trunk", Kinds: []kinds.Kind{kinds.Queue}})
	if err != nil {
		t.Fatal(err)
	}
	return recs
}

func TestAFailedPullRequestReachesTheOwnerOnceAcrossRedeliveries(t *testing.T) {
	h, st, owner, _ := handler(t)
	for range 2 {
		if code := h.Handle(context.Background(), sign(t, secret, "msg_1", time.Now(), failed), []byte(failed)); code != http.StatusOK {
			t.Fatalf("handle = %d, want 200", code)
		}
	}
	recs := queue(t, st, owner)
	if len(recs) != 1 {
		t.Fatalf("owner holds %d queue records, want 1", len(recs))
	}
	r := recs[0]
	want := "acme/app#45700 failed in Trunk's merge queue: TEST_RUN_REQUIRED_STATUS_FAILED; failing: buildkite/test failure https://buildkite.com/acme/test/builds/9"
	if r.Text != want || r.Fields.Outcome != "failed" || r.Fields.Commit != "abc123" ||
		r.Refs.URL != "https://app.trunk.io/acme/merge/app/45700" || r.Refs.PRs[0] != (store.PR{Repo: "acme/app", Number: 45700}) {
		t.Fatalf("owner read %+v", r)
	}
}

func TestAnUnsignedDeliveryIsRefusedAndNotRelayed(t *testing.T) {
	h, _, _, srv := handler(t)
	header := sign(t, secret, "msg_1", time.Now(), failed)
	if code := h.Handle(context.Background(), header, []byte(strings.Replace(failed, "failed", "merged", 2))); code != http.StatusUnauthorized {
		t.Fatalf("handle = %d, want 401", code)
	}
	if len(srv.Pushes) != 0 {
		t.Fatalf("relay saw %d pushes, want none", len(srv.Pushes))
	}
}

func TestABatchDeliveryIsSettledWithoutARecord(t *testing.T) {
	h, _, _, srv := handler(t)
	batch := `{"type":"pull_request_batch.pending_failure","action":"pending_failure","pr_numbers":[1,2],"timestamp":"2026-10-30T14:33:00Z","repository":{"owner":"acme","name":"app"}}`
	if code := h.Handle(context.Background(), sign(t, secret, "msg_2", time.Now(), batch), []byte(batch)); code != http.StatusNoContent {
		t.Fatalf("handle = %d, want 204", code)
	}
	if len(srv.Pushes) != 0 {
		t.Fatalf("relay saw %d pushes, want none", len(srv.Pushes))
	}
}

func TestARelayFailureAsksSvixToRetry(t *testing.T) {
	h, _, _, srv := handler(t)
	srv.Close()
	if code := h.Handle(context.Background(), sign(t, secret, "msg_1", time.Now(), failed), []byte(failed)); code/100 == 2 {
		t.Fatalf("handle = %d with the relay down, want a retryable code", code)
	}
}
